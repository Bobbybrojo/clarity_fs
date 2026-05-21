

use std::sync::{Arc, mpsc::{self as std_mpsc, Sender, Receiver}};

use serde::{self, Deserialize, Serialize};
use tokio::{net::TcpStream, sync::{Mutex}};
use tokio_tungstenite::{self, WebSocketStream, tungstenite::{self, Message}};
use futures_util::{SinkExt, StreamExt, stream::{SplitSink, SplitStream}};
use uuid::Uuid;



#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum ClientMessage {
    ListRoomsReq,
    JoinRoom { id: Uuid },
    Signal { from: Uuid, to: Uuid, payload: String },
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    ListRoomsRes { rooms: Vec<RoomInfo>},
    PeerJoined { pid: Uuid, rid: Uuid },
    Connected { pid: Uuid },
    PeerLeft { pid: Uuid, rid: Uuid },
    Signal { from: Uuid, to: Uuid, payload: String },
}

#[derive(Deserialize, Clone)]
pub struct RoomInfo { 
    pub rid: Uuid, 
    pub peer_count: usize 
}

pub struct Client {
    pub id: Uuid,

    sender: SplitSink<WebSocketStream<TcpStream>, Message>,
    pub mpsc_receiver: Receiver<String>,

    pub rooms: Vec<RoomInfo>,
}

impl Client {
    pub async fn new() -> Option<Arc<Mutex<Self>>> {
        async {
            // Create the socket connection
            let stream = TcpStream::connect("localhost:7878").await.ok()?;
            let (ws, _resp) = tokio_tungstenite::client_async("ws://localhost:7878", stream).await.ok()?;
            let (mut sink, mut stream) = ws.split();
            
            // Get the client's id
            let msg = stream.next().await?.ok()?;
            let text = msg.into_text().ok()?;
            let ServerMessage::Connected { pid } = serde_json::de::from_str::<ServerMessage>(&text).ok()? else {
                return None;
            };

            // Get the rooms
            let ser_req = serde_json::ser::to_string(&ClientMessage::ListRoomsReq).ok()?;
            sink.send(tungstenite::Message::Text(ser_req.into())).await.ok()?;
            let msg = stream.next().await?.ok()?;
            let text = msg.into_text().ok()?;
            let ServerMessage::ListRoomsRes { rooms } = serde_json::de::from_str::<ServerMessage>(&text).ok()? else {
                return None;
            };

            let (mpsc_sender, mpsc_receiver) = std_mpsc::channel::<String>();

            Self::start_listening(stream, mpsc_sender);

            return Some(Arc::new(Mutex::new(Client { id: pid, sender: sink, mpsc_receiver, rooms })));
        }.await
    }

    pub async fn join_room(&mut self, rid: Uuid) {
        let req: ClientMessage = ClientMessage::JoinRoom { id: rid };
        if let Ok(ser_req) = serde_json::ser::to_string(&req) {
            let _ = self.sender.send(tungstenite::Message::Text(ser_req.into())).await;
        }
    }

    pub fn start_listening(mut receiver: SplitStream<WebSocketStream<TcpStream>>, sender: Sender<String>) {
        tokio::spawn(async move {
            while let Some(Ok(msg)) = receiver.next().await {
                if let tungstenite::Message::Text(text) = msg {
                    let _ = sender.send(text.to_string());
                }
            }
        });
    }

    pub async fn send_signal(&mut self, from: Uuid, to: Uuid, payload: String) {
        let msg = ClientMessage::Signal { from, to, payload };
        if let Ok(text) = serde_json::to_string(&msg) {
            let _ = self.sender.send(tungstenite::Message::text(text)).await;
        }
    }

    pub async fn close(&mut self) {
        let _ = self.sender.close().await;
    }
}