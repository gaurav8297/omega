use std::collections::HashSet;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time;

pub struct Suspicion
{
    num_confirm: AtomicU32,
    total_confirm: u32,
    min: Duration,
    max: Duration,
    start: Instant,
    tx: Sender<()>,
    rx: Receiver<()>,
    timer_handle: JoinHandle<()>,
    confirmations: HashSet<String>,
}

impl Suspicion
{
    pub fn new<F>(from: String,
               total_confirm: u32,
               min: Duration,
               max: Duration,
               timeout_fn: F) -> Suspicion
    where
        F: FnOnce() + Send + 'static
    {
        let mut timeout = max.clone();
        if total_confirm < 1 {
            timeout = min.clone();
        }

        let (tx, rx) = tokio::sync::broadcast::channel(1);
        let tx1 = tx.clone();
        let mut rx1 = tx.subscribe();

        let timer_handle = tokio::spawn(async move {
            time::sleep(timeout).await;
            tx1.send(());
        });

        tokio::spawn(async move {
            rx1.recv().await;
            timeout_fn();
        });

        let mut confirmations = HashSet::new();
        confirmations.insert(from);

        return Suspicion {
            num_confirm: AtomicU32::new(0),
            total_confirm,
            min,
            max,
            start: Instant::now(),
            tx,
            rx,
            timer_handle,
            confirmations,
        };
    }

    fn calc_remaining_time(&self, elapsed: Duration) -> Duration
    {
        let n = self.num_confirm.load(Relaxed);
        let frac = ((n + 1) as f64).log2() / ((self.total_confirm + 1) as f64).log2();
        let mut timeout = self.max - (self.max - self.min).mul_f64(frac);

        if timeout < self.min {
            timeout = self.min;
        }

        return timeout - elapsed;
    }

    pub fn confirm(&mut self, from: String) -> bool
    {
        if self.num_confirm.load(Relaxed) >= self.total_confirm {
            return false;
        }

        if self.confirmations.contains(&from) {
            return false;
        }

        self.confirmations.insert(from);
        self.num_confirm.fetch_add(1, Relaxed);

        let timeout = self.calc_remaining_time(self.start.elapsed());
        if self.rx.try_recv().is_err() {
            self.timer_handle.abort();

            let tx1 = self.tx.clone();
            if !timeout.is_zero() {
                self.timer_handle = tokio::spawn(async move {
                    time::sleep(timeout).await;
                    tx1.send(());
                });
            } else {
                self.tx.send(());
            }
        }
        return true;
    }
}
