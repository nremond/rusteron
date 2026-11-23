//! The simulation's one source of randomness: SplitMix64 on the seed, so a
//! seed replays the same run.

#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// In [0, 1).
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// In [low, high).
    pub fn range(&mut self, low: i64, high: i64) -> i64 {
        if high <= low {
            return low;
        }
        low + (self.next_u64() % (high - low) as u64) as i64
    }

    pub fn chance(&mut self, probability: f64) -> bool {
        self.next_f64() < probability
    }

    pub fn pick(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}
