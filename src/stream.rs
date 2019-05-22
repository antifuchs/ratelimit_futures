//! Rate-limiting combinator for streams.

use crate::Ratelimit;
use futures::{try_ready, Async, Future, Poll, Sink, StartSend, Stream};
use ratelimit_meter::{algorithms::Algorithm, DirectRateLimiter, NonConformance};
use std::io;

pub trait StreamExt: Stream {
    /// Limits the rate at which the stream produces items.
    ///
    /// Note that this combinator limits the rate at which it yields
    /// items, not necessarily the rate at which the underlying stream is polled.
    /// The combinator will buffer at most one item in order to adhere to the
    /// given limiter. I.e. if it already has an item buffered and needs to wait
    /// it will not `poll` the underlying stream.
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
            buf: None,
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
    buf: Option<S::Item>,
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

    /// Consumes this combinator, returning the underlying stream and any item
    /// which it has already produced but which is still being held back
    /// in order to abide by the limiter.
    pub fn into_inner(self) -> (S, Option<S::Item>) {
        (self.inner, self.buf)
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
        // Unless we've already got an item in our buffer,
        if self.buf.is_none() {
            // poll the underlying stream for the next one
            self.buf = try_ready!(self.inner.poll());
        }

        if self.buf.is_none() {
            // End of stream, no need to query the limiter
            return Ok(Async::Ready(None));
        }

        // Wait until we're good to go
        try_ready!(self.ratelimit.poll());

        // Once we have produced an item, reset the limiter future
        self.ratelimit.restart();

        Ok(Async::Ready(self.buf.take()))
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
