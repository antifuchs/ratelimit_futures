//! Rate-limiting combinator for streams.

use crate::Ratelimit;
use futures::{try_ready, Async, Future, Poll, Sink, StartSend, Stream};
use ratelimit_meter::{algorithms::Algorithm, DirectRateLimiter, NonConformance};
use std::io;

pub trait StreamExt: Stream {
    /// Limits the rate at which the current stream produces items.
    ///
    /// Note that this combinator does not buffer any items internally but
    /// instead limits `poll` calls to the underlying stream.
    fn ratelimit<A: Algorithm>(self, limiter: DirectRateLimiter<A>) -> Ratelimited<Self, A>
    where
        Self: Sized,
        <A as Algorithm>::NegativeDecision: NonConformance;
}

impl<S: Stream> StreamExt for S {
    fn ratelimit<A: Algorithm>(self, limiter: DirectRateLimiter<A>) -> Ratelimited<Self, A>
    where
        Self: Sized,
        <A as Algorithm>::NegativeDecision: NonConformance,
    {
        Ratelimited {
            inner: self,
            ratelimit: Ratelimit::new(limiter),
            check_limit: true,
        }
    }
}

/// A stream combinator which will limit the rate of items passing through.
///
/// This is produced by the [StreamExt::ratelimit] method.
pub struct Ratelimited<S: Stream, A: Algorithm>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
{
    inner: S,
    ratelimit: Ratelimit<A>,
    check_limit: bool,
}

impl<S: Stream, A: Algorithm> Ratelimited<S, A>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
{
    /// Acquires a references to the underlying stream that this combinator is pulling from.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Acquires a mutable references to the underlying stream that this combinator is pulling from.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consumes this combinator, returning the underlying stream.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: Stream, A: Algorithm> Stream for Ratelimited<S, A>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
    <S as Stream>::Error: From<io::Error>,
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<S::Item>, S::Error> {
        if self.check_limit {
            // Wait until we're good to go
            try_ready!(self.ratelimit.poll());
            self.check_limit = false;
        }

        // Poll for the next item
        let result = try_ready!(self.inner.poll());

        // Once we have produced an item, start checking the limit again
        self.ratelimit.restart();
        self.check_limit = true;

        Ok(Async::Ready(result))
    }
}

impl<S: Stream + Sink, A: Algorithm> Sink for Ratelimited<S, A>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
{
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    fn start_send(&mut self, item: S::SinkItem) -> StartSend<S::SinkItem, S::SinkError> {
        self.inner.start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), S::SinkError> {
        self.inner.poll_complete()
    }

    fn close(&mut self) -> Poll<(), S::SinkError> {
        self.inner.close()
    }
}
