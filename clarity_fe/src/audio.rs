use std::{collections::VecDeque, sync::{Arc, Mutex}};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use std::sync::mpsc;
use tokio::sync::{broadcast};

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
    _thread: std::thread::JoinHandle<()>,
}

impl AudioCapture {

    pub fn start() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (broadcast_tx, _) = broadcast::channel::<Vec<u8>>(64);
        let tx_clone = broadcast_tx.clone();

        let (pcm_tx, pcm_rx) = mpsc::channel::<Vec<f32>>();

        let thread = std::thread::spawn(move || {
            let host   = cpal::default_host();
            let device = host.default_input_device().expect("no input device");

            let config = cpal::StreamConfig {
                channels:    1,
                sample_rate: SAMPLE_RATE,
                buffer_size: cpal::BufferSize::Default,
            };

            let mut accum = Vec::<f32>::new();
            let sender    = pcm_tx.clone();

            let stream = device
                .build_input_stream(
                    &config,
                    move |data: &[f32], _| {
                        for frame in accumulate(&mut accum, data) {
                            let _ = sender.send(frame);
                        }
                    },
                    |err| eprintln!("[audio capture] cpal error: {err}"),
                    None,
                )
                .expect("failed to build input stream");

            stream.play().expect("failed to start capture stream");

            // Encode loop — runs until the channel sender is dropped
            let mut encoder = opus::Encoder::new(
                SAMPLE_RATE,
                opus::Channels::Mono,
                opus::Application::Voip,
            )
            .expect("failed to create Opus encoder");

            let mut out = vec![0u8; 4096];

            loop {
                let Ok(frame) = pcm_rx.recv() else { break };
                match encoder.encode_float(&frame, &mut out) {
                    Ok(len) => { let _ = tx_clone.send(out[..len].to_vec()); }
                    Err(e)  => eprintln!("[audio capture] encode error: {e}"),
                }
            }
        });

        Ok(AudioCapture { tx: broadcast_tx, _thread: thread })
    }

}

pub struct PlaybackHandle {
    pub buf: AudioBuf,
    _thread: std::thread::JoinHandle<()>,
}

impl PlaybackHandle {
    pub fn start() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let buf: AudioBuf = Arc::new(Mutex::new(VecDeque::with_capacity(SAMPLE_RATE as usize)));
        let buf_clone = buf.clone();

        let thread = std::thread::spawn(move || {
            let host   = cpal::default_host();
            let device = host.default_output_device().expect("no output device");

            let config = cpal::StreamConfig {
                channels:    1,
                sample_rate: SAMPLE_RATE,
                buffer_size: cpal::BufferSize::Default,
            };

            let stream = device
                .build_output_stream(
                    &config,
                    move |data: &mut [f32], _| {
                        let mut b = buf_clone.lock().unwrap();
                        for sample in data.iter_mut() {
                            *sample = b.pop_front().unwrap_or(0.0);
                        }
                    },
                    |err| eprintln!("[playback] cpal error: {err}"),
                    None,
                )
                .expect("failed to build output stream");

            stream.play().expect("failed to start playback stream");

            // Keep the thread alive — dropping stream stops playback
            loop { std::thread::sleep(std::time::Duration::from_secs(1)); }
        });

        Ok(PlaybackHandle { buf, _thread: thread })
    }
}