use futures::{Async, Future, Poll};
use futures_timer::Delay;
use ratelimit_meter::{algorithms::Algorithm, DirectRateLimiter, NonConformance};
use std::io;

pub struct Ratelimit<'a, A: Algorithm>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
{
    delay: Delay,
    limiter: &'a mut DirectRateLimiter<A>,
    first_time: bool,
}

impl<'a, A: Algorithm> Ratelimit<'a, A>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
{
    /// Check if the rate-limiter would allow a request through.
    fn check(&mut self) -> Result<(), ()> {
        match self.limiter.check() {
            Ok(()) => Ok(()),
            Err(nc) => {
                let earliest = nc.earliest_possible();
                self.delay.reset_at(earliest);
                Err(())
            }
        }
    }

    /// Creates a new future that resolves successfully as soon as the
    /// rate limiter allows it.
    pub fn new(limiter: &'a mut DirectRateLimiter<A>) -> Self {
        Ratelimit {
            delay: Delay::new(Default::default()),
            first_time: true,
            limiter,
        }
    }
}

impl<'a, A: Algorithm> Future for Ratelimit<'a, A>
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
            match self.check() {
                Ok(_) => {
                    return Ok(Async::Ready(()));
                }
                Err(_) => {}
            };
        }
        match self.delay.poll() {
            // Timer says we should check the rate-limiter again, do
            // it and reset the delay otherwise.
            Ok(Async::Ready(_)) => match self.check() {
                Ok(_) => Ok(Async::Ready(())),
                Err(_) => {
                    self.delay.poll()?;
                    Ok(Async::NotReady)
                }
            },

            // timer isn't yet ready, let's wait:
            Ok(Async::NotReady) => Ok(Async::NotReady),

            // something went wrong:
            Err(e) => Err(e),
        }
    }
}
