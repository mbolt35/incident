use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::time::{sleep, Duration};
use tokio::sync::{broadcast, Mutex};
use tokio::sync::broadcast::{Sender, Receiver};

pub struct Dispatcher<T> {
    sender: Sender<T>,
}

pub struct EventStream<T> {
    receiver: Mutex<Receiver<T>>,
    stop: AtomicBool,
}

impl<T> Dispatcher<T>
    where T: Clone + Send + 'static
{
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(100);

        Self {
            sender,
        }
    }

    pub fn add<F>(&self, handler: F)
        where F: Fn(T) -> bool + Send + 'static
    {
        let es = self.new_event_stream();

        tokio::spawn(async move {
            while let Some(event) = es.recv().await {
                if !handler(event) {
                    es.close();
                }
            }
        });
    }

    pub fn new_event_stream(&self) -> Arc<EventStream<T>> {
        Arc::new(EventStream::new(self.sender.subscribe()))
    }

    pub fn dispatch(&self, event: T) {
        if let Err(e) = self.sender.send(event) {
            println!("Error on Send: {}", e);
        }
    }
}

impl<T> EventStream<T>
    where T: Clone
{
    fn new(receiver: Receiver<T>) -> Self {
        Self {
            receiver: Mutex::new(receiver),
            stop: AtomicBool::new(false),
        }
    }

    pub async fn recv(&self) -> Option<T> {
        if self.stop.load(Ordering::Acquire) {
            return None;
        }

        let mut rx = self.receiver.lock().await;
        match rx.recv().await {
            Ok(event) => {
                if self.stop.load(Ordering::Acquire) {
                    return None;
                }

                Some(event)
            },
            Err(e) => {
                println!("Receive Error: {}", e);
                None
            }
        }
    }

    pub fn close(&self) -> bool {
        self.stop
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct TestEvent(i32);

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_dispatcher() {
        let dispatcher = Dispatcher::<TestEvent>::new();

        dispatcher.add(|event| -> bool {
            println!("[Event Handler #1] TestEvent({})", event.0);
            true
        });

        dispatcher.add(|event| -> bool {
            println!("[Event Handler #2] TestEvent({})", event.0);

            event.0 != 6
        });

        sleep(Duration::from_millis(50)).await;

        for i in 0..10 {
            dispatcher.dispatch(TestEvent(i + 1));
        }

        sleep(Duration::from_millis(1000)).await;
    }
}
