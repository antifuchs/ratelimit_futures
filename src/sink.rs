//! Rate-limiting combinator for sinks.

use crate::Ratelimit;
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use ratelimit_meter::{algorithms::Algorithm, DirectRateLimiter, NonConformance};
use std::io;

pub trait SinkExt: Sink {
    /// Limits the rate at which items can be put into the current sink.
    ///
    /// Note that this combinator limits the rate at which it accepts items,
    /// not necessarily the rate at which the underlying sink does.
    /// The combinator will buffer at most one item in order to adhere to the
    /// given limiter. I.e. if it already has an item buffered and needs to
    /// wait, it will not accept any more items until the underlying stream has
    /// accepted the current item.
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
            buf: None,
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
    buf: Option<S::SinkItem>,
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

    /// Consumes this combinator, returning the underlying sink and any item
    /// which has already been accepted by the combinator (i.e. passed the
    /// limiter) but not yet by the underlying sink.
    pub fn into_inner(self) -> (S, Option<S::SinkItem>) {
        (self.inner, self.buf)
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
        if self.buf.is_some() {
            // Internal buffer full, try to flush it
            self.poll_complete()?;

            if self.buf.is_some() {
                // Internal buffer still full, time to wait
                return Ok(AsyncSink::NotReady(item));
            }
        }

        // Check the limiter
        if let Async::NotReady = self.ratelimit.poll()? {
            return Ok(AsyncSink::NotReady(item));
        }

        // We're good to go, accept the item
        // It'll be flushed to the underlying sink when `poll_complete` is called
        self.buf = Some(item);
        // and reset the limiter future for the next item
        self.ratelimit.restart();

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), S::SinkError> {
        if let Some(item) = self.buf.take() {
            if let AsyncSink::NotReady(item) = self.inner.start_send(item)? {
                self.buf = Some(item);
                return Ok(Async::NotReady);
            }
        }

        self.inner.poll_complete()
    }

    fn close(&mut self) -> Poll<(), S::SinkError> {
        self.inner.close()
    }
}
