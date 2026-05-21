
use serde::{self, Deserialize, Serialize};
use str0m::media::{MediaTime, Frequency};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, IceConnectionState, Input, Output, RtcConfig};
use str0m::change::{SdpPendingOffer, SdpOffer, SdpAnswer};
use str0m::Rtc;

use uuid::Uuid;
use std::{sync::Arc, net::SocketAddr, time::Instant};
use tokio::sync::{Mutex, mpsc, broadcast};
use tokio::net::UdpSocket;

use crate::audio::{AudioBuf, PlaybackHandle, SAMPLE_RATE};

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SignalPayload {
    Offer { sdp: String },
    Answer { sdp: String },
}

#[derive(Clone, PartialEq)]
pub enum PeerState {
    Connecting,
    Connected,
    Failed,
}

pub enum PeerTaskCmd {
    RemoteSignal(String),
    SetMute(bool),
    Shutdown
}

pub enum PeerEvent {
    Connected,
    Disconnected,
    SdpReady(String)
}

pub struct PeerHandle {
    pid: Uuid,
    cmd_tx: mpsc::UnboundedSender<PeerTaskCmd>,
    event_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<PeerEvent>>>,
    state: PeerState
}

pub fn is_offerer(pid: Uuid, remote_pid: Uuid) -> bool {
    return pid < remote_pid;
}

pub fn spawn_peer_task(pid: Uuid, remote_pid: Uuid, audio_rx: broadcast::Receiver<Vec<u8>>) -> PeerHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<PeerTaskCmd>();
    let (event_tx, event_rx) = mpsc::unbounded_channel::<PeerEvent>();

    let offerer = is_offerer(pid, remote_pid);

    tokio::spawn(
        async move {
            if let Err(e) = run_peer_task(offerer, cmd_rx, event_tx, audio_rx).await {
                eprintln!("[Peer {remote_pid:.8}] task error: {e}");
            }
        }
    );

    PeerHandle {
        pid: remote_pid,
        cmd_tx,
        event_rx: Arc::new(Mutex::new(event_rx)),
        state: PeerState::Connecting,
    }

}

async fn run_peer_task(offerer: bool,
mut cmd_rx: mpsc::UnboundedReceiver<PeerTaskCmd>,
event_tx: mpsc::UnboundedSender<PeerEvent>,
mut audio_rx: broadcast::Receiver<Vec<u8>>) -> Result<(), Box<dyn std::error::Error>> {
    
    let mut rtc = RtcConfig::new().enable_opus(true).build(Instant::now());
    
    // Create udp socket
    let sock_res: Result<UdpSocket, std::io::Error> = UdpSocket::bind("0.0.0.0:0").await;
    if let Err(e) = sock_res { return Err(Box::new(e)); }

    let udp_sock: UdpSocket = sock_res.unwrap();
    let udp_port = udp_sock.local_addr().ok().unwrap().port();

    // Use a std (sync) socket to discover the local outbound IP — no packet is sent
    let probe = std::net::UdpSocket::bind("0.0.0.0:0")?;
    probe.connect("8.8.8.8:80")?;
    let ip = probe.local_addr()?.ip();

    // Create candidate addr
    let candidate_addr: SocketAddr = (ip, udp_port).into();

    // Create ICE host candidate
    let ice_host_candidate = Candidate::host(candidate_addr, "udp").expect("Error creating host candidate");

    // Register host candidate
    rtc.add_local_candidate(ice_host_candidate);


    let mut sdp_api = rtc.sdp_api();
    let mid = sdp_api.add_media(str0m::media::MediaKind::Audio, str0m::media::Direction::SendRecv, Some("Mid".to_string()), None, None);

    let mut pending_offer: Option<SdpPendingOffer> = None;

    if offerer {
        if let Some((offer, pending)) = sdp_api.apply() {
            if let Ok(ser_sdp) = serde_json::to_string(&SignalPayload::Offer {sdp: offer.to_sdp_string() }) {
                pending_offer = Some(pending);
                let _ = event_tx.send(PeerEvent::SdpReady(ser_sdp));
            }
        }
    } else {
        let _ = sdp_api.apply();
    }

    let playback = PlaybackHandle::start().ok().unwrap();
    let mut decoder  = opus::Decoder::new(SAMPLE_RATE, opus::Channels::Mono)?;
    let mut pcm_buf  = vec![0.0f32; 5760]; // max Opus frame

    let mut rtp_ts: u32 = 0;
    let mut muted       = false;

    let mut timeout_dur = std::time::Duration::from_millis(100);
    let mut recv_buf = vec![0u8; 2048];

    // Poll loop
    loop {
        // Drain all output str0m wants to produce
        loop {
            match rtc.poll_output()? {
                Output::Transmit(t) => {
                    udp_sock.send_to(&t.contents, t.destination).await?;
                }
                Output::Timeout(t) => {
                    let now = Instant::now();
                    timeout_dur = t.saturating_duration_since(now);
                    break;
                }
                Output::Event(event) => {
                    handle_rtc_event(
                        event,
                        &event_tx,
                        &mut rtc,
                        &mut pending_offer,
                        &mut decoder,
                        &mut pcm_buf,
                        &playback.buf,
                    );
                }
            }
        }

        if !rtc.is_alive() { break; }

        // Wait for the next input — whichever arrives first
        tokio::select! {
            // Timeout tick
            _ = tokio::time::sleep(timeout_dur) => {
                rtc.handle_input(Input::Timeout(Instant::now()))?;
            }

            // Incoming UDP packet (RTP/STUN/DTLS from remote peer)
            result = udp_sock.recv_from(&mut recv_buf) => {
                if let Ok((len, src)) = result {
                    let receive = Receive::new(
                        Protocol::Udp,
                        src,
                        candidate_addr,
                        &recv_buf[..len],
                    )?;
                    rtc.handle_input(Input::Receive(Instant::now(), receive))?;
                }
            }

            // Encoded Opus frame from AudioCapture broadcast
            frame_result = audio_rx.recv() => {
                if let Ok(opus_bytes) = frame_result {
                    if !muted && rtc.is_connected() {
                        if let Some(writer) = rtc.writer(mid) {
                            // Extract pt first so the borrow from payload_params() drops
                            // before write() consumes writer (write takes self by value)
                            let pt = writer.payload_params().next().map(|p| p.pt());
                            if let Some(pt) = pt {
                                let _ = writer.write(
                                    pt,
                                    Instant::now(),
                                    MediaTime::new(rtp_ts as u64, Frequency::FORTY_EIGHT_KHZ),
                                    opus_bytes,
                                );
                            }
                        }
                        rtp_ts = rtp_ts.wrapping_add(960);
                    }
                }
            }

            // Command from Iced
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(PeerTaskCmd::RemoteSignal(s)) => {
                        handle_remote_signal(&s, &mut rtc, &mut pending_offer, &event_tx);
                    }
                    Some(PeerTaskCmd::SetMute(m)) => { muted = m; }
                    Some(PeerTaskCmd::Shutdown) | None => break,
                }
            }
        }
    }

    let _ = event_tx.send(PeerEvent::Disconnected);
    Ok(())
}

fn handle_rtc_event(
    event: Event,
    event_tx: &mpsc::UnboundedSender<PeerEvent>,
    _rtc: &mut Rtc,
    _pending: &mut Option<SdpPendingOffer>,
    decoder: &mut opus::Decoder,
    pcm_buf: &mut Vec<f32>,
    playback_buf: &AudioBuf,
) {
    match event {
        Event::IceConnectionStateChange(IceConnectionState::Connected) => {
            let _ = event_tx.send(PeerEvent::Connected);
        }
        Event::IceConnectionStateChange(IceConnectionState::Disconnected) => {
            let _ = event_tx.send(PeerEvent::Disconnected);
        }
        Event::MediaData(data) => {
            match decoder.decode_float(&data.data, pcm_buf, false) {
                Ok(samples) => {
                    let mut buf = playback_buf.lock().unwrap();
                    buf.extend(pcm_buf[..samples].iter().copied());
                }
                Err(e) => eprintln!("[peer] opus decode error: {e}"),
            }
        }
        _ => {}
    }
}


fn handle_remote_signal(
    payload_str: &str,
    rtc: &mut Rtc,
    pending: &mut Option<SdpPendingOffer>,
    event_tx: &mpsc::UnboundedSender<PeerEvent>,
) {
    let payload: SignalPayload = match serde_json::from_str(payload_str) {
        Ok(p)  => p,
        Err(e) => { eprintln!("[peer] bad signal payload: {e}"); return; }
    };

    match payload {
        SignalPayload::Offer { sdp } => {
            let offer = match SdpOffer::from_sdp_string(&sdp) {
                Ok(o)  => o,
                Err(e) => { eprintln!("[peer] bad SDP offer: {e}"); return; }
            };
            match rtc.sdp_api().accept_offer(offer) {
                Ok(answer) => {
                    let p = SignalPayload::Answer { sdp: answer.to_sdp_string() };
                    if let Ok(s) = serde_json::to_string(&p) {
                        let _ = event_tx.send(PeerEvent::SdpReady(s));
                    }
                }
                Err(e) => eprintln!("[peer] accept_offer error: {e}"),
            }
        }
        SignalPayload::Answer { sdp } => {
            let answer = match SdpAnswer::from_sdp_string(&sdp) {
                Ok(a)  => a,
                Err(e) => { eprintln!("[peer] bad SDP answer: {e}"); return; }
            };
            if let Some(p) = pending.take() {
                if let Err(e) = rtc.sdp_api().accept_answer(p, answer) {
                    eprintln!("[peer] accept_answer error: {e}");
                }
            }
        }
    }
}