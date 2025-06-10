use std::sync::{Arc, Mutex};

use anyhow::Result;

pub struct AppendFutureInner {
    result: Option<Result<u64>>,
    waker: Option<std::task::Waker>,
}

// AppendFuture is a future that resolves when an append operation is complete.
pub struct AppendFuture(Arc<Mutex<AppendFutureInner>>);

// Define Deref for AppendFuture to access the inner state.
impl std::ops::Deref for AppendFuture {
    type Target = Mutex<AppendFutureInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AppendFuture {
    pub fn new() -> Self {
        Self {
            0: Arc::new(Mutex::new(AppendFutureInner {
                result: None,
                waker: None,
            })),
        }
    }
    pub fn set_result(&self, result: Result<u64>) {
        let mut guard = self.lock().unwrap();
        guard.result = Some(result);
        if let Some(waker) = guard.waker.take() {
            waker.wake();
        }
    }
}

impl std::fmt::Debug for AppendFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendFuture").finish_non_exhaustive()
    }
}

// Implement Clone for AppendFuture to allow it to be cloned.
impl Clone for AppendFuture {
    fn clone(&self) -> Self {
        AppendFuture(Arc::clone(&self.0))
    }
}

// Implement Future trait for AppendFuture to allow it to be used as a future.
impl Future for AppendFuture {
    type Output = Result<u64>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut guard = self.lock().unwrap();

        if let Some(result) = guard.result.take() {
            std::task::Poll::Ready(result)
        } else {
            guard.waker = Some(cx.waker().clone());
            std::task::Poll::Pending
        }
    }
}
