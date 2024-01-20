use crate::tasks::task::TaskError;
use anyhow::anyhow;
use futures_util::future::{join_all, try_join_all, BoxFuture};
use tokio::task::JoinHandle;

/// Joins all results from handles into one,
/// if any future returns an error then all other handles will
/// be canceled and an error will be returned immediately
pub async fn join_handle_results_strict(
    handles: Vec<JoinHandle<Result<(), TaskError>>>,
) -> Result<(), anyhow::Error> {
    try_join_all(handles)
        .await
        .map(|_| ())
        .map_err(|e| e.into())
}

/// Joins all results from handles into one,
/// if any future returns an error all other handles will still be processed
pub async fn join_task_handle_results(
    handles: Vec<JoinHandle<Result<(), TaskError>>>,
) -> Result<(), TaskError> {
    let mut errors = Vec::with_capacity(handles.len());

    for handle in handles {
        match handle.await {
            Ok(result) => {
                if let Err(e) = result {
                    errors.push(e);
                }
            }
            Err(e) => errors.push(TaskError::UnexpectedError(anyhow::Error::from(e))),
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(TaskError::from(anyhow!("One or more tasks failed"))) //todo better error logging
    }
}

pub async fn join_handle_results(
    handles: Vec<JoinHandle<Result<(), TaskError>>>,
) -> Result<(), TaskError> {
    let mut errors = Vec::with_capacity(handles.len());

    for handle in handles {
        match handle.await {
            Ok(result) => {
                if let Err(e) = result {
                    errors.push(e);
                }
            }
            Err(e) => errors.push(TaskError::from(anyhow::Error::from(e))), // todo clean up
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(TaskError::from(anyhow!("One or more tasks failed"))) //todo better error logging
    }
}

/// fails if a single future fails and cancels all the other futures
pub async fn join_future_results_strict(
    futures: Vec<BoxFuture<'_, Result<(), TaskError>>>,
) -> Result<(), TaskError> {
    let results = join_all(futures).await;
    results.into_iter().collect()
}

/// fails if a single future fails but finishes all futures
pub async fn join_future_results(
    futures: Vec<BoxFuture<'_, Result<(), TaskError>>>,
) -> Result<(), TaskError> {
    let mut errors = Vec::with_capacity(futures.len());
    for future in futures {
        if let Err(err) = future.await {
            errors.push(err);
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(TaskError::UnexpectedError(anyhow!(
            "One or more tasks failed"
        ))) //todo better error logging
    }
}
