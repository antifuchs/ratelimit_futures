//! Rate-limiting for futures.
//!
//! This crate hooks the
//! [ratelimit_meter](https://crates.io/crates/ratelimit_meter) crate up
//! to futures v0.1 (the same version supported by Tokio right now).
//!
//! # Usage & mechanics of rate limiting with futures
//!
//! To use this crate's Future type, use the provided `Ratelimit::new`
//! function. It takes a direct rate limiter (an in-memory rate limiter
//! implementation), and returns a Future that can be chained to the
//! actual work that you mean to perform:
//!
//! ```rust
//! # extern crate ratelimit_meter;
//! # extern crate ratelimit_futures;
//! # extern crate futures;
//! use futures::prelude::*;
//! use futures::future::{self, FutureResult};
//! use ratelimit_meter::{DirectRateLimiter, LeakyBucket};
//! use ratelimit_futures::Ratelimit;
//! use std::num::NonZeroU32;
//!
//! let mut lim = DirectRateLimiter::<LeakyBucket>::per_second(NonZeroU32::new(1).unwrap());
//! {
//!     let ratelimit_future = Ratelimit::new(lim.clone());
//!     let future_of_3 = ratelimit_future.and_then(|_| {
//!         Ok(3)
//!     });
//! }
//! {
//!     let ratelimit_future = Ratelimit::new(lim.clone());
//!     let future_of_4 = ratelimit_future.and_then(|_| {
//!         Ok(4)
//!     });
//! }
//! // 1 second will pass before both futures resolve.
//! ```
//!
//! In this example, we're constructing futures that can each start work
//! only once the (shared) rate limiter says that it's ok to start.
//!
//! You can probably guess the mechanics of using these rate-limiting
//! futures:
//!
//! * Chain your work to them using `.and_then`.
//! * Construct and a single rate limiter for the work that needs to count
//!   against that rate limit. You can share them using their `Clone`
//!   trait.
//! * Rate-limiting futures will wait as long as it takes to arrive at a
//!   point where code is allowed to proceed. If the shared rate limiter
//!   already allowed another piece of code to proceed, the wait time will
//!   be extended.

use futures::{Async, Future, Poll};
use futures_timer::Delay;
use ratelimit_meter::{algorithms::Algorithm, DirectRateLimiter, NonConformance};
use std::io;

pub mod sink;
pub mod stream;

/// The rate-limiter as a future.
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
                let earliest = nc.earliest_possible();
                self.delay.reset_at(earliest);
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

    /// Reset this future (but not the underlying rate-limiter) to its initial state.
    ///
    /// This allows re-using the same future to rate-limit multiple items.
    /// Calling this method should be semantically equivalent to replacing this `Ratelimit`
    /// with a newly created `Ratelimit` using the same limiter.
    pub fn restart(&mut self) {
        self.first_time = true;
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
            if self.check().is_ok() {
                return Ok(Async::Ready(()));
            }
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
