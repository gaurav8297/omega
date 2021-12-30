use std::sync::RwLock;
use std::time::Duration;

pub struct Awareness
{
    max: u32,
    score: RwLock<i32>,
}

impl Awareness
{
    pub fn new(max: u32) -> Awareness
    {
        return Awareness {
            max,
            score: RwLock::new(0),
        };
    }

    pub fn apply_delta(&self, delta: i32)
    {
        let mut score = self.score.write().unwrap();
        *score += delta;
        if score.is_negative() {
            *score = 0;
        } else if *score > (self.max - 1) as i32 {
            *score = (self.max - 1) as i32;
        }
    }

    pub fn get_health_score(&self) -> i32
    {
        return *self.score.read().unwrap();
    }

    pub fn scale_timeout(&self, timeout: Duration) -> Duration
    {
        let score = self.score.read().unwrap();
        return timeout * (Duration::from_nanos(*score as u64).subsec_nanos() + 1);
    }
}