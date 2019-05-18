//! Rate-limiting combinator for sinks.

use crate::Ratelimit;
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use ratelimit_meter::{algorithms::Algorithm, DirectRateLimiter, NonConformance};
use std::io;

pub trait SinkExt: Sink {
    /// Limits the rate at which items can be put into the current sink.
    ///
    /// Note that this combinator does not buffer any items internally but
    /// instead limits `start_send` calls to the underlying sink.
    fn sink_ratelimit<A: Algorithm>(self, limiter: DirectRateLimiter<A>) -> Ratelimited<Self, A>
    where
        Self: Sized,
        <A as Algorithm>::NegativeDecision: NonConformance;
}

impl<S: Sink> SinkExt for S {
    fn sink_ratelimit<A: Algorithm>(self, limiter: DirectRateLimiter<A>) -> Ratelimited<Self, A>
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

/// A sink combinator which will limit the rate of items passing through.
///
/// This is produced by the [SinkExt::sink_ratelimit] methods.
pub struct Ratelimited<S: Sink, A: Algorithm>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
{
    inner: S,
    ratelimit: Ratelimit<A>,
    check_limit: bool,
}

impl<S: Sink, A: Algorithm> Ratelimited<S, A>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
{
    /// Acquires a references to the underlying sink that this combinator is pushing to.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Acquires a mutable references to the underlying sink that this combinator is pushing to.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consumes this combinator, returning the underlying sink.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: Sink + Stream, A: Algorithm> Stream for Ratelimited<S, A>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<S::Item>, S::Error> {
        self.inner.poll()
    }
}

impl<S: Sink, A: Algorithm> Sink for Ratelimited<S, A>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
    <S as Sink>::SinkError: From<io::Error>,
{
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    fn start_send(&mut self, item: S::SinkItem) -> StartSend<S::SinkItem, S::SinkError> {
        if self.check_limit {
            // Wait until we're good to go
            if let Async::NotReady = self.ratelimit.poll()? {
                return Ok(AsyncSink::NotReady(item));
            }
            self.check_limit = false;
        }

        // Try to push the item into underlying sink
        if let AsyncSink::NotReady(item) = self.inner.start_send(item)? {
            return Ok(AsyncSink::NotReady(item));
        }

        // Once we have passed on an item, start checking the limit again
        self.ratelimit.restart();
        self.check_limit = true;

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), S::SinkError> {
        self.inner.poll_complete()
    }

    fn close(&mut self) -> Poll<(), S::SinkError> {
        self.inner.close()
    }
}
