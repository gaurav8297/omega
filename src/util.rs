pub fn retransmit_limit(retransmit_mul: usize, n: usize) -> usize {
    let node_scale = ((n + 1) as f64).log10().ceil();
    return retransmit_mul + (node_scale as usize)
}
