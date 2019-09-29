use futures::prelude::*;
use futures::stream;
use nonzero_ext::nonzero;
//use ratelimit_futures::sink::SinkExt;
//use ratelimit_futures::stream::StreamExt;
use ratelimit_futures::Ratelimit;
use ratelimit_meter::{DirectRateLimiter, LeakyBucket};
use std::io;
use std::thread;
use std::time::{Duration, Instant};
use futures::executor::block_on;

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
/*
#[test]
fn stream() {
    let i = Instant::now();
    let lim = DirectRateLimiter::<LeakyBucket>::per_second(nonzero!(10u32));
    let mut stream = stream::repeat::<_, io::Error>(()).ratelimit(lim).wait();

    for _ in 0..10 {
        stream.next().unwrap().unwrap();
    }
    assert!(i.elapsed() <= Duration::from_millis(100));

    stream.next().unwrap().unwrap();
    assert!(i.elapsed() > Duration::from_millis(100));
    assert!(i.elapsed() <= Duration::from_millis(200));

    stream.next().unwrap().unwrap();
    assert!(i.elapsed() > Duration::from_millis(200));
    assert!(i.elapsed() <= Duration::from_millis(300));
}

#[test]
fn sink() {
    let i = Instant::now();
    let lim = DirectRateLimiter::<LeakyBucket>::per_second(nonzero!(10u32));
    let mut sink = Vec::new()
        .sink_map_err::<_, io::Error>(|()| unreachable!())
        .sink_ratelimit(lim)
        .wait();

    for _ in 0..10 {
        sink.send(()).unwrap();
    }
    assert!(i.elapsed() <= Duration::from_millis(100));

    sink.send(()).unwrap();
    assert!(i.elapsed() > Duration::from_millis(100));
    assert!(i.elapsed() <= Duration::from_millis(200));

    sink.send(()).unwrap();
    assert!(i.elapsed() > Duration::from_millis(200));
    assert!(i.elapsed() <= Duration::from_millis(300));
}
*/