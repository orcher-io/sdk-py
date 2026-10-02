//! The shared tokio runtime used to run async Rust code from Python.

use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::runtime::Runtime as TokioRuntime;

/// Process-wide tokio runtime, created on first use.
static RUNTIME: Lazy<Arc<TokioRuntime>> = Lazy::new(|| {
    Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .thread_name("orcher-py-worker")
            .build()
            .expect("Failed to create tokio runtime"),
    )
});

/// Return a handle to the shared runtime.
pub fn get_runtime() -> Arc<TokioRuntime> {
    RUNTIME.clone()
}

/// Run a future on the shared runtime and block the calling thread until it
/// completes. Must not be called from inside the runtime.
pub fn block_on<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    RUNTIME.block_on(future)
}

/// Spawn a future on the shared runtime.
pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    RUNTIME.spawn(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_creation() {
        let rt = get_runtime();
        assert!(Arc::strong_count(&rt) >= 1);
    }

    #[test]
    fn test_block_on() {
        let result = block_on(async { 42 });
        assert_eq!(result, 42);
    }

    #[test]
    fn test_spawn() {
        let handle = spawn(async { "hello" });
        let result = block_on(handle).unwrap();
        assert_eq!(result, "hello");
    }
}
