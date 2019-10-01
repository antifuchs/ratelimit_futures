//! Rate-limiting combinator for sinks.

use crate::Ratelimit;
use futures::sink::Sink;
use ratelimit_meter::{algorithms::Algorithm, clock, DirectRateLimiter, NonConformance};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

pub trait SinkExt<Item, S>: Sink<Item>
where
    S: Sink<Item>,
{
    /// Limits the rate at which items can be put into the current sink.
    ///
    /// Note that this combinator limits the rate at which it accepts items,
    /// not necessarily the rate at which the underlying sink does.
    /// The combinator will buffer at most one item in order to adhere to the
    /// given limiter. I.e. if it already has an item buffered and needs to
    /// wait, it will not accept any more items until the underlying stream has
    /// accepted the current item.
    fn sink_ratelimit<A: Algorithm<Instant>>(
        self,
        limiter: DirectRateLimiter<A, clock::MonotonicClock>,
    ) -> RatelimitedSink<Item, S, A>
    where
        Self: Sized,
        <A as Algorithm>::NegativeDecision: NonConformance<Instant>;
}

impl<Item, S: Sink<Item>> SinkExt<Item, S> for S {
    fn sink_ratelimit<A: Algorithm<Instant>>(
        self,
        limiter: DirectRateLimiter<A, clock::MonotonicClock>,
    ) -> RatelimitedSink<Item, S, A>
    where
        Self: Sized,
        <A as Algorithm<Instant>>::NegativeDecision: NonConformance<Instant>,
    {
        RatelimitedSink::new(self, limiter)
    }
}

#[derive(PartialEq, Debug)]
enum State {
    NotReady,
    Ready,
}

pub struct RatelimitedSink<Item, S: Sink<Item>, A: Algorithm<Instant>>
where
    <A as Algorithm<Instant>>::NegativeDecision: NonConformance<Instant>,
{
    inner: S,
    state: State,
    future: Ratelimit<A>,
    _spoop: PhantomData<Item>,
}

impl<Item, S: Sink<Item>, A: Algorithm<Instant>> RatelimitedSink<Item, S, A>
where
    <A as Algorithm<Instant>>::NegativeDecision: NonConformance<Instant>,
{
    fn new(
        inner: S,
        limiter: DirectRateLimiter<A, clock::MonotonicClock>,
    ) -> RatelimitedSink<Item, S, A> {
        let future = Ratelimit::new(limiter);
        RatelimitedSink {
            inner,
            future,
            state: State::NotReady,
            _spoop: PhantomData,
        }
    }

    /// Acquires a reference to the underlying sink that this combinator is sending into.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Acquires a mutable reference to the underlying sink that this combinator is sending into.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consumes this combinator, returning the underlying sink.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: Sink<Item>, A: Algorithm<Instant>, Item> Sink<Item> for RatelimitedSink<Item, S, A>
where
    <A as Algorithm<Instant>>::NegativeDecision: NonConformance<Instant>,
    A: Unpin,
    S: Unpin,
    Item: Unpin,
    <A as ratelimit_meter::algorithms::Algorithm<Instant>>::BucketState: std::marker::Unpin,
{
    type Error = S::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            match self.state {
                State::NotReady => {
                    let future = Pin::new(&mut self.future);
                    match future.poll(cx) {
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                        Poll::Ready(_) => {
                            self.state = State::Ready;
                        }
                    }
                }
                State::Ready => {
                    self.future.restart();
                    let inner = Pin::new(&mut self.inner);
                    return inner.poll_ready(cx);
                }
            }
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        match self.state {
            State::NotReady => {
                unreachable!("Protocol violation: should not start_send before we say we can");
            }
            State::Ready => {
                self.state = State::NotReady;
                let inner = Pin::new(&mut self.inner);
                inner.start_send(item)
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let inner = Pin::new(&mut self.inner);
        inner.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let inner = Pin::new(&mut self.inner);
        inner.poll_close(cx)
    }
}

// TODO: Implement Stream for any sinks that also implement Stream.
