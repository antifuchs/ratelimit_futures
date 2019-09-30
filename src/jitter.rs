use std::time::Duration;

/// A time inverval that gets added to the wait time returned by the rate limiter's
/// non-conformance results. Using jitter helps prevent thundering herd scenarios.  
pub struct Jitter {
    min: Duration,
    interval: Duration,
}

impl Jitter {
    /// Constructs a new Jitter interval, waiting at most a duration of `max`.
    pub fn up_to(max: Duration) -> Jitter {
        Jitter {
            min: Duration::new(0, 0),
            interval: max,
        }
    }

    /// Constructs a new Jitter inverval, waiting at least `min` and at most `min+interval`.
    pub fn new(min: Duration, interval: Duration) -> Jitter {
        Jitter { min, interval }
    }

    /// Returns a random amount of jitter within the configured interval.
    pub(crate) fn get(&self) -> Duration {
        let range = rand::random::<f32>();
        self.min + self.interval.mul_f32(range)
    }
}
