use futures::{Async, Future, Poll};
use futures_timer::Delay;
use ratelimit_meter::{algorithms::Algorithm, DirectRateLimiter, NonConformance};
use std::io;

pub struct Ratelimit<A: Algorithm>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
{
    delay: Delay,
    limiter: DirectRateLimiter<A>,
    first_time: bool,
}

impl<A: Algorithm> Ratelimit<A>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
{
    /// Check if the rate-limiter would allow a request through.
    fn check(&mut self) -> Result<(), ()> {
        match self.limiter.check() {
            Ok(()) => Ok(()),
            Err(nc) => {
                self.delay.reset_at(nc.earliest_possible());
                Err(())
            }
        }
    }

    /// Creates a new future that resolves successfully as soon as the
    /// rate limiter allows it.
    pub fn new(limiter: DirectRateLimiter<A>) -> Self {
        Ratelimit {
            delay: Delay::new(Default::default()),
            first_time: true,
            limiter,
        }
    }
}

impl<A: Algorithm> Future for Ratelimit<A>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.first_time {
            // First time we run, let's check the rate-limiter and set
            // up a delay if we can't proceed:
            self.first_time = false;
            return match self.check() {
                Ok(_) => Ok(Async::Ready(())),
                Err(_) => Ok(Async::NotReady),
            };
        }
        match self.delay.poll() {
            // Timer says we should check the rate-limiter again, do
            // it and reset the delay otherwise.
            Ok(Async::Ready(_)) => match self.check() {
                Ok(_) => Ok(Async::Ready(())),
                Err(_) => Ok(Async::NotReady),
            },

            // timer isn't yet ready, let's wait:
            Ok(Async::NotReady) => Ok(Async::NotReady),

            // something went wrong:
            Err(e) => Err(e),
        }
    }
}
