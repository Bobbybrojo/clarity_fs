

use std::sync::Arc;

use serde::{self, Deserialize, Serialize};
use tokio::{net::{TcpListener, TcpStream}, sync::{Mutex}};
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
enum ServerMessage {
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
    id: Uuid,
    sender: SplitSink<WebSocketStream<TcpStream>, Message>,
    receiver: SplitStream<WebSocketStream<TcpStream>>,
    pub rooms: Vec<RoomInfo>,
}

impl Client {
    pub async fn new() -> Option<Arc<Mutex<Self>>> {
        async {
            // Create the socket connection
            let stream = TcpStream::connect("localhost:7878").await.ok()?;
            let (ws, _resp) = tokio_tungstenite::client_async("ws://localhose:7878", stream).await.ok()?;
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

            return Some(Arc::new(Mutex::new(Client { id: pid, sender: sink, receiver: stream, rooms: rooms })));
        }.await
    }

    pub async fn join_room(&mut self, rid: Uuid) {
        let req: ClientMessage = ClientMessage::JoinRoom { id: rid };
        if let Ok(ser_req) = serde_json::ser::to_string(&req) {
            let _ = self.sender.send(tungstenite::Message::Text(ser_req.into()));
        }
    }
}