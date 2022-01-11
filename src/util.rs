use std::ops::Mul;
use std::time::Duration;

pub fn retransmit_limit(retransmit_mul: usize, n: usize) -> usize {
    let node_scale = ((n + 1) as f64).log10().ceil();
    return retransmit_mul + (node_scale as usize);
}

pub fn suspicion_timeout(suspicion_multiplier: u32, n: u32, interval: Duration) -> Duration {
    let node_scale = (n as f64).max(1.0).log10().max(1.0);
    return interval.mul_f64(node_scale).mul(suspicion_multiplier);
}
