mod utility;
mod client;
mod peer;
mod audio;

use std::sync::Arc;
use futures_util::{SinkExt, Stream, StreamExt};
use iced::futures::channel::mpsc::Sender;
use uuid::Uuid;
use iced::Alignment::Center;
use tokio::sync::{Mutex, mpsc};
use utility::Utility;
use client::{Client, RoomInfo, ServerMessage};

use iced::{Font, Shadow, Task, Theme, Vector, window};
use iced::Length::{Fill, FillPortion};
use iced::{Border, Element, Color};
use iced::widget::{button, column, Column, container, row, rule, scrollable, text, text_input};


#[derive(Default, Clone)]
enum Screen {
    #[default]
    Enter,
    Menu,
    Room,
}

struct MainState {
    screen: Screen,
    theme: Theme,
    my_pid: Option<Uuid>,
    client: Option<Arc<Mutex<Client>>>,
    rooms: Vec<RoomInfo>,
    current_room: Uuid,

    peers: std::collections::HashMap<Uuid, peer::PeerHandle>,
    pending_peers: Vec<Uuid>,
    is_muted: bool,
    audio_tx: Option<tokio::sync::broadcast::Sender<Vec<u8>>>,
}

impl MainState {
    fn new() -> Self {
        MainState {
            screen: Screen::Enter,
            theme: Theme::Dark,
            my_pid: None,
            client: None,
            rooms: Vec::new(),
            current_room: Uuid::default(),
            peers: std::collections::HashMap::new(),
            pending_peers: Vec::new(),
            is_muted: false,
            audio_tx: None,
        }
    }
}

#[derive(Clone)]
enum Message {
    EnterApp,
    ClientCreated(Option<Arc<Mutex<Client>>>),
    UpdateRoomListAndPid(Option<(Vec<RoomInfo>, Uuid)>),
    JoinRoom(Uuid),
    EnterRoom(Uuid),
    RoomJoined { rid: Uuid, audio_tx: Option<tokio::sync::broadcast::Sender<Vec<u8>>> },

    PeerJoined { pid: Uuid, rid: Uuid },
    PeerLeft { pid: Uuid, rid: Uuid },
    Signal { from: Uuid, to: Uuid, payload: String },
    
    AudioCaptureReady(tokio::sync::broadcast::Sender<Vec<u8>>),
    PeerConnected(Uuid),
    PeerConnectionFailed(Uuid),
    PeerSdpReady { pid: Uuid, sdp: String },
    ToggleMute,
    LeaveRoom,
    Noop,

    CloseApp,
    Quit,
}


// Allows hashing for a client for subscription building purposes
struct ClientSubscription {
    id: Uuid,
    client: Arc<Mutex<Client>>,
}

impl PartialEq for ClientSubscription {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ClientSubscription {}

impl std::hash::Hash for ClientSubscription {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

struct PeerSubscription {
    pid: Uuid,
    event_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<peer::PeerEvent>>>,
}

impl PartialEq for PeerSubscription {
    fn eq(&self, other: &Self) -> bool {
        self.pid == other.pid
    }
}

impl Eq for PeerSubscription {}

impl std::hash::Hash for PeerSubscription {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pid.hash(state);
    }
}

fn peer_stream_builder(ps: &PeerSubscription) -> std::pin::Pin<Box<dyn Stream<Item = Message> + Send + 'static>> {
    let pid = ps.pid;
    let event_rx = ps.event_rx.clone();

    iced::stream::channel(16, move |mut output: Sender<Message>| async move {
        loop {
            let event = event_rx.lock().await.recv().await;
            let msg = match event {
                Some(peer::PeerEvent::Connected)                  => Message::PeerConnected(pid),
                Some(peer::PeerEvent::Disconnected)               => Message::PeerConnectionFailed(pid),
                Some(peer::PeerEvent::SdpReady(sdp))      => Message::PeerSdpReady { pid, sdp },
                None                                              => break,
            };
            if output.send(msg).await.is_err() { break; }
        }
    }).boxed()
}

fn stream_subscription_builder(cs: &ClientSubscription) -> std::pin::Pin<Box<dyn Stream<Item = Message> + Send + 'static>> {
    let client = cs.client.clone();

    let stream_channel = iced::stream::channel(4096, |mut output: Sender<Message>| async move {
        loop {
            // try_recv() is non-blocking — the mutex is held only for the instant of the
            // call, then released. Using blocking recv() would hold the mutex forever and
            // deadlock anything else that needs to lock the client (join_room, send_signal).
            let result = {
                let c_guard = client.lock().await;
                c_guard.mpsc_receiver.try_recv()
            };

            match result {
                Ok(msg) => {
                    if let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&msg) {
                        let iced_msg = match server_msg {
                            ServerMessage::PeerJoined { pid, rid } => Message::PeerJoined { pid, rid },
                            ServerMessage::PeerLeft { pid, rid } => Message::PeerLeft { pid, rid },
                            ServerMessage::Signal { from, to, payload } => Message::Signal { from, to, payload },
                            _ => continue,
                        };
                        let _ = output.send(iced_msg).await;
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    // No message yet — yield to the executor and try again shortly
                    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
            }
        }
    });

    return stream_channel.boxed();
}

fn subscription(state: &MainState) -> iced::Subscription<Message> {
    let events = iced::event::listen_with(|event, _, _| {
        match event {
            iced::Event::Window(iced::window::Event::CloseRequested) => Some(Message::CloseApp),
            _ => None,
        }
    });

    let peer_subs = state.peers.values().map(|h| {
        iced::Subscription::run_with(
            PeerSubscription { pid: h.pid, event_rx: h.event_rx.clone() },
            peer_stream_builder,
        )
    });

    if let Some(c) = state.client.clone() && let Some(id) = state.my_pid {
        let client_wrapper = ClientSubscription { id, client: c };
        let ws_sub = iced::Subscription::run_with(client_wrapper, stream_subscription_builder);
        return iced::Subscription::batch(
            std::iter::once(ws_sub)
                .chain(std::iter::once(events))
                .chain(peer_subs)
        );
    }

    iced::Subscription::batch(std::iter::once(events).chain(peer_subs))
}


fn update(state: &mut MainState, message: Message) -> Task<Message> {
    match message {

        Message::EnterApp => {
            state.screen = Screen::Menu;
            Task::perform(
                client::Client::new(),
                Message::ClientCreated
            )
        }

        Message::ClientCreated(client) => {
            state.client = client.clone();
            Task::perform(
                async move {
                    if let Some(c) = client.clone() {
                        let c_guard = c.lock().await;
                        Some((c_guard.rooms.clone(), c_guard.id.clone()))
                    } else {
                        None
                    }
                },
                Message::UpdateRoomListAndPid
            )
        }

        Message::UpdateRoomListAndPid(some_rooms) => {
            if let Some((rooms, pid)) = some_rooms {
                state.rooms = rooms;
                state.my_pid = Some(pid);
            };
            Task::none()
        }

        Message::JoinRoom(rid) => {
            let client = state.client.clone();
            Task::perform(
                async move {
                    // Start AudioCapture BEFORE sending the join request.
                    // This ensures audio_tx is stored in state before the server
                    // has a chance to send PeerJoined messages back. If AudioCapture
                    // fails (no mic / permissions) we still join — just no outgoing audio.
                    let audio_tx = audio::AudioCapture::start().map(|a| a.tx).ok();
                    if let Some(c) = client {
                        let mut c_guard = c.lock().await;
                        c_guard.join_room(rid).await;
                    }
                    (rid, audio_tx)
                },
                |(rid, audio_tx)| Message::RoomJoined { rid, audio_tx }
            )
        }

        // RoomJoined fires after AudioCapture is ready AND the join request has been sent.
        // PeerJoined messages and RoomJoined race in Iced's queue, so we must drain
        // pending_peers here too (not just in AudioCaptureReady, which the new JoinRoom
        // flow no longer fires).
        Message::RoomJoined { rid, audio_tx } => {
            let my_pid_short = state.my_pid.map(|p| format!("{:.8}", p)).unwrap_or_default();
            eprintln!("[FE-{my_pid_short}] RoomJoined: audio_tx={}, pending_peers={}",
                if audio_tx.is_some() { "Some" } else { "None" },
                state.pending_peers.len());

            state.audio_tx = audio_tx;
            state.current_room = rid;
            state.screen = Screen::Room;

            // Drain any peers that arrived before audio_tx was set
            if let (Some(my_pid), Some(audio_tx)) = (state.my_pid, &state.audio_tx) {
                for pid in state.pending_peers.drain(..) {
                    eprintln!("[FE-{my_pid_short}] RoomJoined: draining pending peer {:.8}", pid);
                    let audio_rx = audio_tx.subscribe();
                    let handle = peer::spawn_peer_task(my_pid, pid, audio_rx);
                    state.peers.insert(pid, handle);
                }
            }
            Task::none()
        }

        Message::EnterRoom(_rid) => {
            // Kept for compatibility — audio is now started in JoinRoom instead.
            Task::none()
        }

        Message::PeerJoined { pid, rid: _ } => {
            let my_pid_short = state.my_pid.map(|p| format!("{:.8}", p)).unwrap_or_default();
            if let (Some(my_pid), Some(audio_tx)) = (state.my_pid, &state.audio_tx) {
                eprintln!("[FE-{my_pid_short}] PeerJoined({:.8}): audio_tx=Some, spawning task", pid);
                let audio_rx = audio_tx.subscribe();
                let handle = peer::spawn_peer_task(my_pid, pid, audio_rx);
                state.peers.insert(pid, handle);
            } else {
                eprintln!("[FE-{my_pid_short}] PeerJoined({:.8}): audio_tx=None, queuing", pid);
                state.pending_peers.push(pid);
            }
            Task::none()
        }

        Message::PeerLeft { pid, rid: _ } => {
            if let Some(handle) = state.peers.remove(&pid) {
                let _ = handle.cmd_tx.send(peer::PeerTaskCmd::Shutdown);
            }
            Task::none()
        }

        Message::Signal { from, to: _, payload } => {
            let my_pid_short = state.my_pid.map(|p| format!("{:.8}", p)).unwrap_or_default();
            if let Some(handle) = state.peers.get(&from) {
                eprintln!("[FE-{my_pid_short}] Signal from={:.8}: routing to peer task (len={})", from, payload.len());
                let _ = handle.cmd_tx.send(peer::PeerTaskCmd::RemoteSignal(payload));
            } else {
                eprintln!("[FE-{my_pid_short}] Signal from={:.8}: NO PEER IN STATE — dropping (have peers: {:?})",
                    from, state.peers.keys().map(|k| format!("{:.8}", k)).collect::<Vec<_>>());
            }
            Task::none()
        }

        Message::AudioCaptureReady(tx) => {
            state.audio_tx = Some(tx);
            // Drain any peers that arrived before audio was ready
            if let (Some(my_pid), Some(audio_tx)) = (state.my_pid, &state.audio_tx) {
                for pid in state.pending_peers.drain(..) {
                    let audio_rx = audio_tx.subscribe();
                    let handle = peer::spawn_peer_task(my_pid, pid, audio_rx);
                    state.peers.insert(pid, handle);
                }
            }
            Task::none()
        }

        Message::PeerConnected(pid) => {
            if let Some(handle) = state.peers.get_mut(&pid) {
                handle.state = peer::PeerState::Connected;
            }
            Task::none()
        }

        Message::PeerConnectionFailed(pid) => {
            if let Some(handle) = state.peers.get_mut(&pid) {
                handle.state = peer::PeerState::Failed;
            }
            Task::none()
        }

        Message::PeerSdpReady { pid, sdp } => {
            let my_pid_short = state.my_pid.map(|p| format!("{:.8}", p)).unwrap_or_default();
            eprintln!("[FE-{my_pid_short}] PeerSdpReady → send_signal to {:.8} (len={})", pid, sdp.len());
            if let (Some(client), Some(my_pid)) = (state.client.clone(), state.my_pid) {
                return Task::perform(
                    async move {
                        let mut guard = client.lock().await;
                        guard.send_signal(my_pid, pid, sdp).await;
                    },
                    |_| Message::Noop,
                );
            }
            Task::none()
        }

        Message::ToggleMute => {
            state.is_muted = !state.is_muted;
            for handle in state.peers.values() {
                let _ = handle.cmd_tx.send(peer::PeerTaskCmd::SetMute(state.is_muted));
            }
            Task::none()
        }

        Message::LeaveRoom => {
            for handle in state.peers.values() {
                let _ = handle.cmd_tx.send(peer::PeerTaskCmd::Shutdown);
            }
            state.peers.clear();
            state.pending_peers.clear();
            state.audio_tx = None;
            state.screen = Screen::Menu;
            Task::none()
        }

        Message::Noop => Task::none(),

        Message::CloseApp => {
            let client = state.client.clone();
            Task::perform(
                async move {
                    if let Some(c) = client {
                        let mut guard = c.lock().await;
                        guard.close().await;
                    }
                },
                |_| Message::Quit
            )
        }

        Message::Quit => {
            iced::exit()
        }

    }
}

fn view(state: &MainState) -> Element<'_, Message> {
    let screen_element: Element<'_, Message>;
    match state.screen {

        Screen::Enter => {
            screen_element = container(
                column![
                    text("Clarity").size(64),
                    button("Enter")
                        .on_press(Message::EnterApp)
                        .style(|_, _| button::Style {
                            background: Some(iced::Background::Color(Utility::accent().into())),
                            text_color: Color::WHITE,
                            border: Border { color: Color::TRANSPARENT, width: 1.0, radius: 8.0.into() },
                            ..Default::default()
                        })
                ].align_x(Center)
            ).center_x(Fill).into()
        }

        Screen::Menu => {
            screen_element = container(
                column![
                    text("Clarity").size(64),
                    row![
                        column![].width(FillPortion(1)),
                        column![
                            scrollable(
                                Column::with_children(
                                    state.rooms.iter().map(|room: &RoomInfo| -> Element<'_, Message> {
                                        container(
                                            row![
                                                text(format!("Room: {}", room.rid.clone().to_string()))
                                                    .width(iced::Length::Fill),
                                                button(column![
                                                        text(format!("People: {}", room.peer_count)), 
                                                        text("Join")
                                                    ].align_x(Center)
                                                ).style(|_, _| button::Style {
                                                    background: Some(iced::Background::Color(Utility::accent().into())),
                                                    text_color: Color::WHITE,
                                                    border: Border { color: Color::TRANSPARENT, width: 1.0, radius: 8.0.into() },
                                                    ..Default::default()
                                                }).padding(8)
                                                .on_press(Message::JoinRoom(room.rid)),
                                            ]
                                        )
                                        .style(|_| container::Style{
                                            background: Some(Utility::darker().into()),
                                            border: Border{
                                                color: Color::TRANSPARENT,
                                                width: 5.0,
                                                radius: 18.into(),
                                            },
                                            ..Default::default()
                                        })
                                        .padding(12)
                                        .align_top(65)
                                        .width(iced::Length::Fill)
                                        .center_y(Fill)
                                        .into()
                                    })
                                )
                            )
                        ].spacing(16).width(FillPortion(2)),
                        column![].width(FillPortion(1)),
                    ],
                ].align_x(Center)
            )
            .padding(iced::Padding { top: 0.0, right: 12.0, bottom: 12.0, left: 12.0 } )
            .height(Fill)
            .into()
        }

        Screen::Room => {
            let peer_list = Column::with_children(
                state.peers.values().map(|handle| {
                    let state_str = match handle.state {
                        peer::PeerState::Connecting => "connecting…",
                        peer::PeerState::Connected  => "● connected",
                        peer::PeerState::Failed     => "✕ failed",
                    };
                    container(
                        row![
                            text(format!("{:.8}…", handle.pid))
                                .width(iced::Length::Fill)
                                .size(13),
                            text(state_str).size(13),
                        ].spacing(12)
                    )
                    .style(|_| container::Style {
                        background: Some(Utility::darker().into()),
                        border: Border {
                            color: Color::TRANSPARENT,
                            width: 0.0,
                            radius: 8.into(),
                        },
                        ..Default::default()
                    })
                    .padding(10)
                    .width(iced::Length::Fill)
                    .into()
                })
            ).spacing(8);

            let mute_label = if state.is_muted { "Unmute" } else { "Mute" };

            screen_element = container(
                column![
                    text("Clarity").size(64),
                    row![
                        column![].width(FillPortion(1)),
                        column![
                            text(format!("Room: {}", state.current_room)).size(16),
                            scrollable(peer_list).height(Fill),
                            row![
                                button(mute_label)
                                    .on_press(Message::ToggleMute)
                                    .style(|_, _| button::Style {
                                        background: Some(iced::Background::Color(Utility::accent().into())),
                                        text_color: Color::WHITE,
                                        border: Border { color: Color::TRANSPARENT, width: 1.0, radius: 8.0.into() },
                                        ..Default::default()
                                    })
                                    .padding(10),
                                button("Leave Room")
                                    .on_press(Message::LeaveRoom)
                                    .style(|_, _| button::Style {
                                        background: Some(iced::Background::Color(Utility::accent().into())),
                                        text_color: Color::WHITE,
                                        border: Border { color: Color::TRANSPARENT, width: 1.0, radius: 8.0.into() },
                                        ..Default::default()
                                    })
                                    .padding(10),
                            ].spacing(12),
                        ].spacing(16).width(FillPortion(2)),
                        column![].width(FillPortion(1)),
                    ],
                ].align_x(Center)
            )
            .padding(iced::Padding { top: 0.0, right: 12.0, bottom: 12.0, left: 12.0 })
            .height(Fill)
            .into()
        }
    }

    column![
        // Title bar buffer
        container("").width(Fill).height(32),
        // Main application container
        container(
            // Text Editor
            screen_element
            
        ).style(|t: &Theme| {
            container::Style {
                text_color: Some(t.palette().text),
                background: None,
                border: Border {
                    color: Color::TRANSPARENT,
                    width: 0.0,
                    radius: 0.0.into(),
                },
                shadow: Shadow {
                    color: Color::TRANSPARENT,
                    offset: Vector::new(0.0, 0.0),
                    blur_radius: 0.0
                },
                snap: false,
            }
        })
    ].into()

    
}

pub fn main() -> iced::Result {
    iced::application(MainState::new, update, view)
    .subscription(subscription)
    .style(|_, _theme| iced::theme::Style{
        background_color: Utility::window_background(),
        text_color: Color::WHITE,
    })
    .window(window::Settings { 
        size: (720, 480).into(), 
        maximized: false, 
        fullscreen: false, 
        position: window::Position::Default, 
        min_size: Some((720, 480).into()), 
        max_size: None, 
        visible: true, 
        resizable: true, 
        closeable: true, 
        minimizable: true, 
        decorations: true, 
        transparent: true,
        blur: true,
        level: window::Level::Normal, 
        icon: None, 
        platform_specific: window::settings::PlatformSpecific { 
            title_hidden: false, 
            titlebar_transparent: true, 
            fullsize_content_view: true 
        }, 
        exit_on_close_request: false 
    })
    .centered()
    .title("")
    .theme(Theme::Dark)
    .default_font(Font::DEFAULT)
    .run()
}