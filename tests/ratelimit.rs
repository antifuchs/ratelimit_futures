use futures::executor::block_on;
use futures::sink::SinkExt;
use futures::{stream, StreamExt};
use nonzero_ext::nonzero;
use ratelimit_futures::sink::SinkExt as FuturesSinkExt;
use ratelimit_futures::stream::StreamExt as OurStreamExt;
use ratelimit_futures::{Jitter, Ratelimit};
use ratelimit_meter::{DirectRateLimiter, LeakyBucket};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn pauses() {
    let i = Instant::now();
    let mut lim = DirectRateLimiter::<LeakyBucket>::per_second(nonzero!(10u32));

    // exhaust the limiter:
    loop {
        if lim.check().is_err() {
            break;
        }
    }

    let rl = Ratelimit::new(lim);
    block_on(rl);
    assert!(i.elapsed() >= Duration::from_millis(100));
}

#[test]
fn proceeds() {
    let i = Instant::now();
    let lim = DirectRateLimiter::<LeakyBucket>::per_second(nonzero!(10u32));
    let rl = Ratelimit::new(lim);
    block_on(rl);
    assert!(i.elapsed() <= Duration::from_millis(100));
}

#[test]
fn multiple() {
    let i = Instant::now();
    let lim = DirectRateLimiter::<LeakyBucket>::new(nonzero!(2u32), Duration::from_millis(1));
    let mut children = vec![];

    for _i in 0..20 {
        let lim = lim.clone();
        children.push(thread::spawn(move || {
            let rl = Ratelimit::new(lim);
            block_on(rl);
        }));
    }
    for child in children {
        child.join().unwrap();
    }
    // by now we've waited for, on average, 10ms; but sometimes the
    // test finishes early; let's assume it takes at least 8ms:
    let elapsed = i.elapsed();
    assert!(
        elapsed >= Duration::from_millis(8),
        "Expected to wait some time, but waited: {:?}",
        elapsed
    );
}

#[test]
fn jitters() {
    let i = Instant::now();
    let mut lim = DirectRateLimiter::<LeakyBucket>::per_second(nonzero!(10u32));

    // exhaust the limiter:
    loop {
        if lim.check().is_err() {
            break;
        }
    }

    let jitter = Jitter::new(Duration::from_millis(20), Duration::from_millis(90));
    let rl = Ratelimit::new_with_jitter(lim, jitter);
    block_on(rl);
    assert!(i.elapsed() >= Duration::from_millis(120));
    assert!(
        i.elapsed()
            <= Duration::from_millis(210) +
        // some slack to account for any test runner fuzziness:
        Duration::from_millis(10)
    );
}

#[test]
fn stream() {
    let i = Instant::now();
    let lim = DirectRateLimiter::<LeakyBucket>::per_second(nonzero!(10u32));
    let mut stream = stream::repeat(()).ratelimit(lim);

    for _ in 0..10 {
        block_on(stream.next());
    }
    assert!(i.elapsed() <= Duration::from_millis(100));

    block_on(stream.next());
    assert!(i.elapsed() > Duration::from_millis(100));
    assert!(i.elapsed() <= Duration::from_millis(200));

    block_on(stream.next());
    assert!(i.elapsed() > Duration::from_millis(200));
    assert!(i.elapsed() <= Duration::from_millis(300));
}

#[test]
fn sink() {
    let i = Instant::now();
    let lim = DirectRateLimiter::<LeakyBucket>::per_second(nonzero!(10u32));
    let mut sink = Vec::new().sink_ratelimit(lim);

    for _ in 0..10 {
        block_on(sink.send(())).unwrap();
    }
    assert!(i.elapsed() <= Duration::from_millis(100));

    block_on(sink.send(())).unwrap();
    assert!(
        i.elapsed() > Duration::from_millis(100),
        "elapsed: {:?}",
        i.elapsed()
    );
    assert!(i.elapsed() <= Duration::from_millis(200));

    block_on(sink.send(())).unwrap();
    assert!(i.elapsed() > Duration::from_millis(200));
    assert!(i.elapsed() <= Duration::from_millis(300));
}
