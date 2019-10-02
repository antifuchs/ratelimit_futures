//! Rate-limiting combinator for streams.

use crate::Ratelimit;
use futures::future::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use ratelimit_meter::{algorithms::Algorithm, DirectRateLimiter, NonConformance};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

pub trait StreamExt: Stream {
    /// Limits the rate at which the stream produces items.
    ///
    /// Note that this combinator limits the rate at which it yields
    /// items, not necessarily the rate at which the underlying stream is polled.
    /// The combinator will buffer at most one item in order to adhere to the
    /// given limiter. I.e. if it already has an item buffered and needs to wait
    /// it will not `poll` the underlying stream.
    fn ratelimit<A: Algorithm<Instant>>(
        self,
        limiter: DirectRateLimiter<A>,
    ) -> Ratelimited<Self, A>
    where
        Self: Sized,
        <A as Algorithm>::NegativeDecision: NonConformance;
}

impl<S: Stream> StreamExt for S {
    fn ratelimit<A: Algorithm<Instant>>(self, limiter: DirectRateLimiter<A>) -> Ratelimited<Self, A>
    where
        Self: Sized,
        <A as Algorithm<Instant>>::NegativeDecision: NonConformance,
    {
        Ratelimited {
            inner: self,
            ratelimit: Ratelimit::new(limiter),
            buf: None,
            state: State::NotReady,
        }
    }
}

#[derive(PartialEq, Debug)]
enum State {
    NotReady,
    Ready,
}

/// A stream combinator which will limit the rate of items passing through.
///
/// This is produced by the [StreamExt::ratelimit] method.
pub struct Ratelimited<S: Stream, A: Algorithm<Instant>>
where
    <A as Algorithm<Instant>>::NegativeDecision: NonConformance<Instant>,
{
    inner: S,
    ratelimit: Ratelimit<A>,
    buf: Option<S::Item>,
    state: State,
}

impl<S: Stream, A: Algorithm<Instant>> Ratelimited<S, A>
where
    <A as Algorithm<Instant>>::NegativeDecision: NonConformance<Instant>,
{
    /// Acquires a reference to the underlying stream that this combinator is pulling from.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Acquires a mutable reference to the underlying stream that this combinator is pulling from.
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

impl<S: Stream, A: Algorithm<Instant>> Stream for Ratelimited<S, A>
where
    <A as Algorithm<Instant>>::NegativeDecision: NonConformance<Instant>,
    A: Unpin,
    S: Unpin,
    S::Item: Unpin,
    <A as ratelimit_meter::algorithms::Algorithm<Instant>>::BucketState: std::marker::Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                State::NotReady => {
                    let limit = Pin::new(&mut self.ratelimit);
                    match limit.poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(_) => {
                            self.state = State::Ready;
                        }
                    }
                }
                State::Ready => {
                    self.ratelimit.restart();
                    let inner = Pin::new(&mut self.inner);
                    match inner.poll_next(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(x) => {
                            self.state = State::NotReady;
                            return Poll::Ready(x);
                        }
                    }
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<Item, S: Stream + Sink<Item>, A: Algorithm<Instant>> Sink<Item> for Ratelimited<S, A>
where
    <A as Algorithm>::NegativeDecision: NonConformance,
    Ratelimited<S, A>: Unpin,
    S: Unpin,
    <A as ratelimit_meter::algorithms::Algorithm<Instant>>::BucketState: std::marker::Unpin,
{
    type Error = <S as Sink<Item>>::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let inner = Pin::new(&mut self.inner);
        inner.poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        let inner = Pin::new(&mut self.inner);
        inner.start_send(item)
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
