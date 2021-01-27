use rand::{thread_rng, Rng};
use bb8_redis::{
    bb8,
    redis,
    redis::{RedisResult, Value, Value::Okay}
};

use std::error::Error;

use tokio::time::{Duration, Instant};
use std::ops::DerefMut;

const DEFAULT_RETRY_COUNT: u32 = 5;
const DEFAULT_RETRY_DELAY: u32 = 250;
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
    pub pool: bb8::Pool<bb8_redis::RedisConnectionManager>,
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

impl Lock<'_> {
    pub async fn unlock(&self) -> bool {
        self.lock_manager.unlock(self.resource.as_slice(), self.val.as_slice()).await
    }
}

impl RedLock {
    /// Create a new lock manager instance, defined by the given Redis connection pool.
    pub fn new(pool: bb8::Pool<bb8_redis::RedisConnectionManager>) -> RedLock {
        RedLock {
            pool,
        }
    }

    /// Get UUID for lock ID.
    pub fn get_unique_lock_id(&self) -> Vec<u8> {
        uuid::Uuid::new_v4().as_bytes().to_vec()
    }

    async fn lock_instance(
        &self,
        resource: &[u8],
        val: &[u8],
        ttl: usize,
    ) -> bool {
        let mut con = self.pool.get().await.unwrap();
        let result: RedisResult<Value> = redis::cmd("SET")
            .arg(resource)
            .arg(val)
            .arg("nx")
            .arg("px")
            .arg(ttl)
            .query_async(con.deref_mut())
            .await;
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
    pub async fn lock(&self, resource: &[u8], ttl: usize, retry_count: Option<u32>, retry_delay: Option<u32>) -> Result<Option<Lock<'_>>, Box<dyn Error + Send>> {
        let val = self.get_unique_lock_id();

        let retry_count = match retry_count {
            Some(count) => count,
            None => DEFAULT_RETRY_COUNT,
        };
        let retry_delay = match retry_delay {
            Some(delay) => delay,
            None => DEFAULT_RETRY_DELAY,
        };
        for _ in 0..retry_count {
            let start_time = Instant::now();
            let locked = self.lock_instance(resource, &val, ttl).await;

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
                let _ = self.unlock(resource, &val);
            }

            let n: u64 = thread_rng().gen_range(0, retry_delay).into();
            tokio::time::sleep(Duration::from_millis(n)).await;
        }
        Ok(None)
    }

    pub async fn unlock(&self, resource: &[u8], val: &[u8]) -> bool {
        let mut con = self.pool.get().await.unwrap();

        let script = redis::Script::new(UNLOCK_SCRIPT);
        let result: RedisResult<i32> = script.key(resource).arg(val).invoke_async(con.deref_mut()).await;
        match result {
            Ok(val) => val == 1,
            Err(_) => false,
        }
    }
}
