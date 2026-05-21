
use std::io;
use std::sync::Arc;
use serde::{self, Serialize, Deserialize};
use uuid::Uuid;
use tokio_tungstenite::{self, WebSocketStream, tungstenite};

use tokio::{net::{TcpListener, TcpStream}, sync::{Mutex, mpsc::{self}}};
use futures_util::{SinkExt, StreamExt};


#[derive(Deserialize)]
#[serde(tag = "type")]
enum ClientMessage {
    ListRoomsReq,
    JoinRoom { id: Uuid },
    Signal { from: Uuid, to: Uuid, payload: String },
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum ServerMessage {
    ListRoomsRes { rooms: Vec<RoomInfo>},
    PeerJoined { pid: Uuid, rid: Uuid },
    Connected { pid: Uuid },
    PeerLeft { pid: Uuid, rid: Uuid },
    Signal { from: Uuid, to: Uuid, payload: String },
}


#[derive(Serialize, Default)]
struct Room {
    rid: Uuid,
    peers: Vec<Peer>
}

#[derive(Serialize)]
struct RoomInfo { 
    rid: Uuid, 
    peer_count: usize 
}

#[derive(Serialize)]
struct Peer {
    pid: Uuid,
    #[serde(skip)]
    sender: mpsc::UnboundedSender<String>,
}

type Rooms = Arc<Mutex<Vec<Room>>>;

async fn process_socket(socket: TcpStream, rooms: Rooms) {
    let ws: WebSocketStream<TcpStream> = match tokio_tungstenite::accept_async(socket).await {
        Ok(ws) => ws,
        Err(_) => {
            eprintln!("Websocket upgrade failed");
            return;
        }
    };

    let (mut sink, mut stream) = ws.split();
    let (sender, mut receiver) = mpsc::unbounded_channel::<String>();

    let peer_sender: mpsc::UnboundedSender<String> = sender.clone();
    let peer_pid: Uuid = Uuid::new_v4();

    // When connecting tell the client their peer id
    let resp: ServerMessage = ServerMessage::Connected { pid: peer_pid };                        
    let ser_resp: Result<String, serde_json::Error> = serde_json::ser::to_string(&resp);
    match ser_resp {
        Ok(ser) => {
            let _ = peer_sender.send(ser);
        }
        Err(e) => {
            eprintln!("Failed to serialize message after generating client id: {e}")
        }
    }

    // Pass the received message to the socket
    tokio::spawn(async move {
        while let Some(msg) = receiver.recv().await {
            if sink.send(tungstenite::Message::Text(msg.into())).await.is_err() { break; };
        }
    });

    // Process messages from server
    loop {
        match stream.next().await {
            Some(Ok(msg)) => {
                if let tungstenite::Message::Text(text) = msg {
                    match serde_json::de::from_str::<ClientMessage>(&text) {
                        Ok(client_msg) => {
                            match client_msg {
                                ClientMessage::ListRoomsReq => { 
                                    let rooms_vec = rooms.lock().await;

                                    let room_infos: Vec<RoomInfo> = rooms_vec.iter().map(|room| {
                                        RoomInfo {
                                            rid: room.rid,
                                            peer_count: room.peers.len(),
                                        }
                                    }).collect();
                                    let resp: ServerMessage = ServerMessage::ListRoomsRes { rooms: room_infos };
                                    
                                    let ser_resp: Result<String, serde_json::Error> = serde_json::ser::to_string(&resp);
                                    match ser_resp {
                                        Ok(ser) => {
                                            let _ = peer_sender.send(ser);
                                        }
                                        Err(e) => {
                                            eprintln!("Failed to serialize message: {e}")
                                        }
                                    }
                                }

                                ClientMessage::JoinRoom {id} => {
                                    let mut rooms_vec = rooms.lock().await;
                                    
                                    for room in rooms_vec.iter_mut() {
                                        if room.rid == id {
                                            // Notify other peers of joined peer
                                            let resp: ServerMessage = ServerMessage::PeerJoined { pid: peer_pid, rid: id };
                                            let ser_resp: Result<String, serde_json::Error> = serde_json::ser::to_string(&resp);
                                            match ser_resp {
                                                Ok(ser) => {
                                                    for peer in &room.peers {
                                                        let _ = peer.sender.send(ser.clone());
                                                    }
                                                }
                                                Err(e) => { eprintln!("Failed to serialize message: {e}"); }
                                            }
                                            // Add peer to room
                                            room.peers.push(Peer {
                                                pid: peer_pid,
                                                sender: peer_sender.clone(),
                                            });
                                            break;
                                        }
                                    }
                                }

                                ClientMessage::Signal { from, to, payload } => {
                                    let resp = ServerMessage::Signal { from, to, payload };
                                    if let Ok(ser) = serde_json::to_string(&resp) {
                                        let rooms_vec = rooms.lock().await;

                                        for room in rooms_vec.iter() {
                                            for p in &room.peers {
                                                if p.pid == to {
                                                    let _ = p.sender.send(ser.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to deserialize message: {e}");
                        }
                    }
                }
            }
            Some(Err(e)) => {
                eprintln!("Websocket error: {e}")
            }
            None => {
                // Cleanup of peer connection
                let mut rooms_vec = rooms.lock().await;
                let mut found: Option<(usize, usize)> = None; // (rid, p_idx)

                'outer: for (r_idx, room) in rooms_vec.iter().enumerate() {
                    for (p_idx, p) in room.peers.iter().enumerate() {
                        if p.pid == peer_pid {
                            found = Some((r_idx, p_idx));
                            break 'outer;
                        }
                    }
                }

                if let Some((r_idx, p_idx)) = found {
                    let room = &mut rooms_vec[r_idx];
                    room.peers.remove(p_idx);

                    let resp = ServerMessage::PeerLeft { pid: peer_pid, rid: room.rid };
                    let ser_resp: Result<String, serde_json::Error> = serde_json::ser::to_string(&resp);
                    match ser_resp {
                        Ok(ser) => {
                            for reamining_peer in &room.peers {
                                let _ = reamining_peer.sender.send(ser.clone());
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to serialize message while closing socket: {e}")
                        }
                    }
                }

                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {

    let mut rooms_start: Vec<Room> = Vec::new();
    for _ in 0..5 { rooms_start.push(Room { rid: Uuid::new_v4(), ..Default::default() })}
    let rooms: Rooms = Arc::new(Mutex::new(rooms_start));

    let listener: TcpListener = TcpListener::bind("localhost:7878").await?;
    loop {
        let rooms_clone: Arc<Mutex<Vec<Room>>> = rooms.clone();
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            process_socket(socket, rooms_clone).await 
        });
    }
}