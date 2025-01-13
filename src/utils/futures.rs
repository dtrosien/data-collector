use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{StatsMap, TaskError};
use anyhow::anyhow;
use futures_util::future::{try_join_all, BoxFuture};
use tokio::task::JoinHandle;

// todo delete future utils, if not needed for error handling in the future (scnr)

/// Joins all results from handles into one,
/// if any future returns an error then all other handles will
/// be canceled and an error will be returned immediately
pub async fn join_handle_results_strict(
    handles: Vec<JoinHandle<Result<Option<StatsMap>, TaskError>>>,
) -> Result<(), anyhow::Error> {
    try_join_all(handles)
        .await
        .map(|_| ())
        .map_err(|e| e.into())
}

/// Joins all results from handles into one,
/// if any future returns an error all other handles will still be processed
pub async fn join_task_handle_results(
    handles: Vec<JoinHandle<Result<Option<StatsMap>, TaskError>>>,
) -> Result<Option<StatsMap>, TaskError> {
    let mut errors = Vec::with_capacity(handles.len());

    for handle in handles {
        match handle.await {
            Ok(result) => {
                if let Err(e) = result {
                    errors.push(e);
                }
            }
            Err(e) => errors.push(UnexpectedError(anyhow::Error::from(e))),
        }
    }

    if errors.is_empty() {
        Ok(None)
    } else {
        Err(UnexpectedError(anyhow!("One or more tasks failed"))) //todo better error logging
    }
}

pub async fn join_handle_results(
    handles: Vec<JoinHandle<Result<Option<StatsMap>, TaskError>>>,
) -> Result<Option<StatsMap>, TaskError> {
    let mut errors = Vec::with_capacity(handles.len());

    for handle in handles {
        match handle.await {
            Ok(result) => {
                if let Err(e) = result {
                    errors.push(e);
                }
            }
            Err(e) => errors.push(UnexpectedError(anyhow::Error::from(e))), // todo clean up
        }
    }

    if errors.is_empty() {
        Ok(None)
    } else {
        Err(UnexpectedError(anyhow!("One or more tasks failed"))) //todo better error logging
    }
}

/// fails if a single future fails and cancels all the other futures
// pub async fn join_future_results_strict(
//     futures: Vec<BoxFuture<'_, Result<Option<StatsMap>, TaskError>>>,
// ) -> Result<Option<StatsMap>, TaskError> {
//     let results = join_all(futures).await;
//     results.into_iter().collect()
// }
/// fails if a single future fails but finishes all futures
pub async fn join_future_results(
    futures: Vec<BoxFuture<'_, Result<Option<StatsMap>, TaskError>>>,
) -> Result<Option<StatsMap>, TaskError> {
    let mut errors = Vec::with_capacity(futures.len());
    for future in futures {
        if let Err(err) = future.await {
            errors.push(err);
        }
    }
    if errors.is_empty() {
        Ok(None)
    } else {
        Err(TaskError::UnexpectedError(anyhow!(
            "One or more tasks failed"
        ))) //todo better error logging
    }
}
