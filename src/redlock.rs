use rand::{thread_rng, Rng};
use redis::Value::Okay;
use redis::{RedisResult, Value};

use std::thread::sleep;
use std::time::{Duration, Instant};
use std::error::Error;
use std::ops::DerefMut;

const DEFAULT_RETRY_COUNT: u32 = 3;
const DEFAULT_RETRY_DELAY: u32 = 200;
const CLOCK_DRIFT_FACTOR: f32 = 0.01;
const UNLOCK_SCRIPT: &str = r"if redis.call('get',KEYS[1]) == ARGV[1] then
                                return redis.call('del',KEYS[1])
                              else
                                return 0
                              end";

/// The lock manager.
///
/// Implements the necessary functionality to acquire and release locks
/// and handles the Redis connections.
pub struct RedLock {
    /// List of all Redis clients
    pub pool: r2d2::Pool<redis::Client>,
    retry_count: u32,
    retry_delay: u32,
}

pub struct Lock<'a> {
    /// The resource to lock. Will be used as the key in Redis.
    pub resource: Vec<u8>,
    /// The value for this lock.
    pub val: Vec<u8>,
    /// Time the lock is still valid.
    /// Should only be slightly smaller than the requested TTL.
    pub validity_time: usize,
    /// Used to limit the lifetime of a lock to its lock manager.
    pub lock_manager: &'a RedLock,
}

impl RedLock {
    /// Create a new lock manager instance, defined by the given Redis connection uris.
    /// Quorum is defined to be N/2+1, with N being the number of given Redis instances.
    ///
    /// Sample URI: `"redis://127.0.0.1:6379"`
    pub fn new(pool: r2d2::Pool<redis::Client>) -> RedLock {
        RedLock {
            pool,
            retry_count: DEFAULT_RETRY_COUNT,
            retry_delay: DEFAULT_RETRY_DELAY,
        }
    }

    /// Get 20 random bytes.
    pub fn get_unique_lock_id<RNG: rand::RngCore>(&self, rand: &mut RNG) -> Vec<u8> {
        let mut randoms = Vec::with_capacity(20);
        rand.fill_bytes(&mut randoms);
        randoms
    }

    /// Set retry count and retry delay.
    ///
    /// Retry count defaults to `3`.
    /// Retry delay defaults to `200`.
    pub fn set_retry(&mut self, count: u32, delay: u32) {
        self.retry_count = count;
        self.retry_delay = delay;
    }

    fn lock_instance(
        &self,
        resource: &[u8],
        val: &[u8],
        ttl: usize,
    ) -> bool {
        let mut con = match self.pool.get() {
            Err(_) => return false,
            Ok(val) => val,
        };
        let result: RedisResult<Value> = redis::cmd("SET")
            .arg(resource)
            .arg(val)
            .arg("nx")
            .arg("px")
            .arg(ttl)
            .query(con.deref_mut());
        match result {
            Ok(Okay) => true,
            Ok(_) | Err(_) => false,
        }
    }

    /// Acquire the lock for the given resource and the requested TTL.
    ///
    /// If it succeeds, a `Lock` instance is returned,
    /// including the value and the validity time
    ///
    /// If it fails. `None` is returned.
    /// A user should retry after a short wait time.
    pub fn lock(&self, resource: &[u8], ttl: usize) -> Result<Option<Lock>, Box<dyn Error>> {
        let mut rng = thread_rng();
        let val = self.get_unique_lock_id(&mut rng);

        for _ in 0..self.retry_count {
            let start_time = Instant::now();
            let locked = self.lock_instance(resource, &val, ttl);

            let drift = (ttl as f32 * CLOCK_DRIFT_FACTOR) as usize + 2;
            let elapsed = start_time.elapsed();
            let validity_time = ttl
                - drift
                - elapsed.as_secs() as usize * 1000
                - elapsed.subsec_nanos() as usize / 1_000_000;

            if locked && validity_time > 0 {
                return Ok(Some(Lock {
                    lock_manager: self,
                    resource: resource.to_vec(),
                    val,
                    validity_time,
                }));
            } else {
                self.unlock(resource, &val);
            }

            let n = rng.gen_range(0, self.retry_delay);
            sleep(Duration::from_millis(u64::from(n)));
        }
        Ok(None)
    }

    fn unlock(&self, resource: &[u8], val: &[u8]) -> bool {
        let mut con = match self.pool.get() {
            Err(_) => return false,
            Ok(val) => val,
        };
        let script = redis::Script::new(UNLOCK_SCRIPT);
        let result: RedisResult<i32> = script.key(resource).arg(val).invoke(con.deref_mut());
        match result {
            Ok(val) => val == 1,
            Err(_) => false,
        }
    }
}
