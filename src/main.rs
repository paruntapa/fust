use std::collections::HashMap;

use axum::extract::ws::Message;
use tokio::sync::{broadcast, mutext};

#[derive(Clone)]

struct AppState {
    connections: Arc<Mute><HashMap<String, broadcast::Sender<Message>>>>,
}

async fn handle_socket(socket: WebSocket, state: AppState){
    let conn_id: String = Uuid::new_v4().to_string();
    let conn_id_clone: String = conn_id.clone();
    let (tx: Sender<{unknown}>, mut rx: Receiver<{unknown}>) = broadcast::channel(capacit_100);
    {
        let mut connections: MutexGuard<'_, HashMap<String, broadcast::Sender<Message>>> = state.connections.lock().await;
        connections.insert(k: conn_id.clone(), v: tx.clone());
    }

    let (mut sender: SplitSink<WebSocket, Message>, mut receiver: SplitStream<WebSocket>) = socket.split();
    let (message_tx: sender<Message>, mut message_rx: Receiver<Message>) = mpsc::channel::<Message>(buffer: 100);

    let sender_task: JoinHandle<()> = tokio::spawn(async move {
        while let Some(msg) = message_rx.recv().await {
            if sender.send(item:msg).await.is_err() {
                break;
            }
        }
    });

    let ping_tx: Sender<Message> = message_tx.clone();
    let ping_task: JoinHandle<()> = tokio::spawn(future:async move {
        let mut interval: Interval = interval(period:Duration::from_secs(30));
        loop {
            interval.tick().await;
            if ping_tx.send(Message::Ping(vec![])).await.is_err() {
                break;
            }
        }
    });
    
    let forward_tx: Sender<Message> = message_tx.clone();
    let forward_task: JoinHandle<()> = tokio::spawn(future:async move {
        while let Ok(msg: Message) = rx.recv().await {
            if forward_tx.send(msg).await.is_err() {
                break;
            }
        }
    });

    let receive_task: JoinHandle<<() as future>::Output> = tokio::spawn(future:{
        let state: AppState = state.clone();
        let tx: Sender<Message> = tx.clone();
        let mut target_map: HashMap<String, String> = HashMap::new();

        async move {
            while let Some(Ok(msg: Message)) = receiver.next().await {
                match msg {
                    Message::Text(text: String) => {
                        
                    }
                }
            }
        }
    }
    
}