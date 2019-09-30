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
//! use futures::prelude::*;
//! use futures::future;
//! use ratelimit_meter::{DirectRateLimiter, LeakyBucket};
//! use ratelimit_futures::Ratelimit;
//! use std::num::NonZeroU32;
//!
//! let mut lim = DirectRateLimiter::<LeakyBucket>::per_second(NonZeroU32::new(1).unwrap());
//! {
//!     let ratelimit_future = Ratelimit::new(lim.clone());
//!     let future_of_3 = ratelimit_future.then(|_| { future::ready(3) });
//! }
//! {
//!     let ratelimit_future = Ratelimit::new(lim.clone());
//!     let future_of_4 = ratelimit_future.then(|_| { future::ready(4) });
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

use futures_timer::Delay;
use ratelimit_meter::{algorithms::Algorithm, DirectRateLimiter, NonConformance};
use std::future::Future;
use std::marker::Unpin;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

mod jitter;
//pub mod sink;
//pub mod stream;

pub use jitter::*;

enum LimiterState {
    Check,
    Delay,
}

/// The rate-limiter as a future.
pub struct Ratelimit<A: Algorithm>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
{
    limiter: DirectRateLimiter<A>,
    state: LimiterState,
    delay: Delay,
    jitter: Option<jitter::Jitter>,
}

impl<A: Algorithm> Ratelimit<A>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
{
    /// Creates a new future that resolves successfully as soon as the
    /// rate limiter allows it.
    pub fn new(limiter: DirectRateLimiter<A>) -> Self {
        Ratelimit {
            state: LimiterState::Check,
            delay: Delay::new(Default::default()),
            jitter: None,
            limiter,
        }
    }

    pub fn new_with_jitter(limiter: DirectRateLimiter<A>, jitter: Jitter) -> Self {
        let jitter = Some(jitter);
        Ratelimit {
            state: LimiterState::Check,
            delay: Delay::new(Default::default()),
            jitter,
            limiter,
        }
    }

    /// Reset this future (but not the underlying rate-limiter) to its initial state.
    ///
    /// This allows re-using the same future to rate-limit multiple items.
    /// Calling this method should be semantically equivalent to replacing this `Ratelimit`
    /// with a newly created `Ratelimit` using the same limiter.
    pub fn restart(&mut self) {
        self.state = LimiterState::Check;
    }
}

impl<A: Algorithm> Future for Ratelimit<A>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
    A: Unpin,
    <A as ratelimit_meter::algorithms::Algorithm>::BucketState: std::marker::Unpin,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.state {
                LimiterState::Check => {
                    if let Err(nc) = self.limiter.check() {
                        self.state = LimiterState::Delay;
                        let jitter = self
                            .jitter
                            .as_ref()
                            .and_then(|j| Some(j.get()))
                            .unwrap_or_else(|| Duration::new(0, 0));
                        self.delay.reset_at(nc.earliest_possible() + jitter);
                    } else {
                        return Poll::Ready(());
                    }
                }
                LimiterState::Delay => {
                    let delay = Pin::new(&mut self.delay);
                    match delay.poll(cx) {
                        Poll::Ready(_) => self.state = LimiterState::Check,
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}
