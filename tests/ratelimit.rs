use futures::prelude::*;
use nonzero_ext::nonzero;
use ratelimit_futures::Ratelimit;
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

    let rl = Ratelimit::new(&mut lim);
    rl.wait().unwrap();
    assert!(i.elapsed() > Duration::from_millis(100));
}

#[test]
fn proceeds() {
    let i = Instant::now();
    let mut lim = DirectRateLimiter::<LeakyBucket>::per_second(nonzero!(10u32));
    let rl = Ratelimit::new(&mut lim);
    rl.wait().unwrap();
    assert!(i.elapsed() <= Duration::from_millis(100));
}

#[test]
fn multiple() {
    let i = Instant::now();
    let lim = DirectRateLimiter::<LeakyBucket>::new(nonzero!(2u32), Duration::from_millis(1));
    let mut children = vec![];

    for _i in 0..20 {
        let mut lim = lim.clone();
        children.push(thread::spawn(move || {
            let rl = Ratelimit::new(&mut lim);
            rl.wait().unwrap();
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
