use std::{collections::VecDeque, sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use std::sync::mpsc;
use tokio::sync::broadcast;

pub static SAMPLE_RATE: u32 = 48_000;
pub static FRAME_SAMPLES: usize = 960;

pub fn accumulate(buf: &mut Vec<f32>, new_samples: &[f32]) -> Vec<Vec<f32>> {

    let mut output: Vec<Vec<f32>> = Vec::new();

    buf.extend_from_slice(new_samples);
    while buf.len() >= FRAME_SAMPLES {
        let drain: Vec<f32> = buf.drain(..FRAME_SAMPLES).collect();
        output.push(drain);
    }

    output
}

pub type AudioBuf = Arc<Mutex<VecDeque<f32>>>;

pub struct AudioCapture {
    pub tx: broadcast::Sender<Vec<u8>>,
    shutdown: Arc<AtomicBool>,
    _thread: std::thread::JoinHandle<()>,
}

// Signal the capture thread to exit when AudioCapture is dropped.
// The thread checks the flag every ~100ms via recv_timeout, exits cleanly,
// drops the cpal stream (releasing the microphone), then ends.
impl Drop for AudioCapture {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl AudioCapture {

    pub fn start() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (broadcast_tx, _) = broadcast::channel::<Vec<u8>>(64);
        let tx_clone = broadcast_tx.clone();

        let (pcm_tx, pcm_rx) = mpsc::channel::<Vec<f32>>();

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        let thread = std::thread::spawn(move || {
            let host = cpal::default_host();

            // Errors from device discovery are logged but don't crash the app.
            // The broadcast channel stays alive; receivers just never get data.
            let device = match host.default_input_device() {
                Some(d) => d,
                None => {
                    eprintln!("[audio capture] no input device available");
                    return;
                }
            };

            let config = cpal::StreamConfig {
                channels:    1,
                sample_rate: SAMPLE_RATE,
                buffer_size: cpal::BufferSize::Default,
            };

            let mut accum = Vec::<f32>::new();

            // Move pcm_tx directly into the callback — when the stream is dropped
            // (on thread exit), the closure is dropped, pcm_tx is dropped.
            let stream = match device.build_input_stream(
                &config,
                move |data: &[f32], _| {
                    for frame in accumulate(&mut accum, data) {
                        let _ = pcm_tx.send(frame);
                    }
                },
                |err| eprintln!("[audio capture] cpal error: {err}"),
                None,
            ) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[audio capture] failed to build input stream: {e}");
                    return;
                }
            };

            if let Err(e) = stream.play() {
                eprintln!("[audio capture] failed to start stream: {e}");
                return;
            }

            let mut encoder = match opus::Encoder::new(
                SAMPLE_RATE,
                opus::Channels::Mono,
                opus::Application::Voip,
            ) {
                Ok(e) => e,
                Err(e) => {
                    eprintln!("[audio capture] failed to create encoder: {e}");
                    return;
                }
            };

            let mut out = vec![0u8; 4096];

            // Encode loop — polls shutdown flag every 100ms via recv_timeout
            while !shutdown_clone.load(Ordering::Relaxed) {
                match pcm_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                    Ok(frame) => {
                        match encoder.encode_float(&frame, &mut out) {
                            Ok(len) => { let _ = tx_clone.send(out[..len].to_vec()); }
                            Err(e)  => eprintln!("[audio capture] encode error: {e}"),
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            // stream dropped here → cpal callback closure dropped → pcm_tx dropped
            //                    → microphone released
        });

        Ok(AudioCapture { tx: broadcast_tx, shutdown, _thread: thread })
    }

}

pub struct PlaybackHandle {
    pub buf: AudioBuf,
    shutdown: Arc<AtomicBool>,
    _thread: std::thread::JoinHandle<()>,
}

// Same pattern as AudioCapture: drop triggers shutdown, thread sees flag
// within 100ms and exits, dropping the cpal output stream.
impl Drop for PlaybackHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl PlaybackHandle {
    pub fn start() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let buf: AudioBuf = Arc::new(Mutex::new(VecDeque::with_capacity(SAMPLE_RATE as usize)));
        let buf_clone = buf.clone();

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        let thread = std::thread::spawn(move || {
            let host = cpal::default_host();

            let device = match host.default_output_device() {
                Some(d) => d,
                None => {
                    eprintln!("[playback] no output device available");
                    return;
                }
            };

            let config = cpal::StreamConfig {
                channels:    1,
                sample_rate: SAMPLE_RATE,
                buffer_size: cpal::BufferSize::Default,
            };

            let stream = match device.build_output_stream(
                &config,
                move |data: &mut [f32], _| {
                    let mut b = buf_clone.lock().unwrap();
                    for sample in data.iter_mut() {
                        *sample = b.pop_front().unwrap_or(0.0);
                    }
                },
                |err| eprintln!("[playback] cpal error: {err}"),
                None,
            ) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[playback] failed to build output stream: {e}");
                    return;
                }
            };

            if let Err(e) = stream.play() {
                eprintln!("[playback] failed to start stream: {e}");
                return;
            }

            // Keep thread alive — polls shutdown flag every 100ms
            while !shutdown_clone.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            // stream dropped here → output device released
        });

        Ok(PlaybackHandle { buf, shutdown, _thread: thread })
    }
}
