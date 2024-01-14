use crate::configuration::TaskSetting;
use crate::tasks::task::{execute_task, Task};
use crate::utils::errors::Result;
use crate::utils::futures::join_handle_results;
use reqwest::Client;
use sqlx::PgPool;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use tokio::task::JoinHandle;

pub struct ExecutionSequence(Vec<Vec<Task>>);

/// runs groups of tasks.
/// while groups are processed in sequence, all tasks inside a group are running concurrently
///
/// if one tasks failed, all other tasks will still processed, but overall the function will return an error
pub async fn run_all(execution_sequence: ExecutionSequence) -> Vec<Result<()>> {
    let mut results = Vec::new();
    for task_group in execution_sequence.0 {
        let batch: Vec<JoinHandle<Result<()>>> = task_group.into_iter().map(execute_task).collect();
        let batch_result = join_handle_results(batch).await;

        results.push(batch_result)
    }
    results
}
/// like run_all() but runs only tasks with a specific execution_sequence_positions
pub async fn run_specific(
    execution_sequence: ExecutionSequence,
    execution_sequence_position: Vec<i32>,
) -> Vec<Result<()>> {
    let mut results = Vec::new();
    for task_group in execution_sequence.0 {
        let batch: Vec<JoinHandle<Result<()>>> = task_group
            .into_iter()
            .filter(|a| execution_sequence_position.contains(&a.execution_sequence_position))
            .map(execute_task)
            .collect();
        let batch_result = join_handle_results(batch).await;

        results.push(batch_result)
    }
    results
}

#[tracing::instrument(name = "Building execution sequence", skip(tasks))]
pub fn build_execution_sequence(tasks: BinaryHeap<Reverse<Task>>) -> ExecutionSequence {
    let mut sequence = Vec::new();
    let mut current_group = Vec::new();
    let mut last_scheduled_position = None;

    // unwrap Reversed<Task> and c
    for task in tasks {
        let task = task.0;
        match last_scheduled_position {
            Some(pos) if pos == task.execution_sequence_position => current_group.push(task),
            _ => {
                if !current_group.is_empty() {
                    // replace current_group with empty vector and push its old value into sequence
                    sequence.push(std::mem::take(&mut current_group));
                }
                last_scheduled_position = Some(task.execution_sequence_position);
                current_group.push(task);
            }
        }
    }

    // push last time to ensure all tasks are scheduled (may happen if the last tasks have same priority)
    if !current_group.is_empty() {
        sequence.push(current_group);
    }

    ExecutionSequence { 0: sequence }
}

// #[tracing::instrument(
//     name = "Building tasks execution sequence",
//     skip(task_settings, pool, client)
// )]
// pub fn schedule_tasks(
//     task_settings: &[TaskSetting],
//     pool: &PgPool,
//     client: &Client,
// ) -> ExecutionSequence {
//     let task_min_heap = create_min_heap_of_tasks(task_settings, pool, client);
//     build_execution_sequence(task_min_heap)
// }

/// build Tasks based on TaskSettings and push them in a binary min heap
/// only tasks with a execution_sequence_position > 0 will be scheduled
pub fn create_tasks_heap(
    task_settings: &[TaskSetting],
    pool: &PgPool,
    client: &Client,
) -> BinaryHeap<Reverse<Task>> {
    let mut heap = BinaryHeap::with_capacity(task_settings.len());

    for ts in task_settings
        .iter()
        .filter(|s| s.execution_sequence_position >= 0)
    {
        let task = Task::new(ts, pool, client);
        // used Reverse to have a min heap
        heap.push(Reverse(task))
    }
    heap
}
//
// pub fn build_execution_sequence(task_min_heap: BinaryHeap<Reverse<Task>>) -> ExecutionSequence {
//     let mut sequence = Vec::new();
//     let mut current_group = Vec::new();
//     let mut last_scheduled_position = None;
//
//     // unwrap Reversed<Task> and c
//     for task in task_min_heap {
//         let task = task.0;
//         match last_scheduled_position {
//             Some(pos) if pos == task.execution_sequence_position => current_group.push(task),
//             _ => {
//                 if !current_group.is_empty() {
//                     // replace current_group with empty vector and push its old value into sequence
//                     sequence.push(std::mem::take(&mut current_group));
//                 }
//                 last_scheduled_position = Some(task.execution_sequence_position);
//                 current_group.push(task);
//             }
//         }
//     }
//
//     // push last time to ensure all tasks are scheduled (may happen if the last tasks have same priority)
//     if !current_group.is_empty() {
//         sequence.push(current_group);
//     }
//
//     ExecutionSequence { 0: sequence }
// }

#[cfg(test)]
mod test {
    use crate::collectors::collector_sources::CollectorSource::All;
    use crate::collectors::sp500_fields::Fields;
    use crate::configuration::TaskSetting;
    //  use crate::scheduler::schedule_tasks;
    use crate::tasks::actions::action::ActionType::Collect;
    use crate::utils::test_helpers::get_test_client;
    use sqlx::{Pool, Postgres};

    #[sqlx::test]
    async fn prio_queue_order_and_filter(pool: Pool<Postgres>) {
        // Arrange
        let priorities_in = vec![1, 500, 300, 1000, -2, 90, -20];
        let priorities_out = vec![1000, 500, 300, 90, 1];
        let base_task = TaskSetting {
            comment: None,
            actions: vec![Collect],
            sp500_fields: vec![Fields::Nyse],
            execution_sequence_position: 0,
            include_sources: vec![All],
            exclude_sources: vec![],
        };
        let mut tasks = vec![];
        for n in priorities_in {
            let mut task = base_task.clone();
            task.execution_sequence_position = n;
            tasks.push(task);
        }
        let client = get_test_client();

        // Act
        //  let mut queue = schedule_tasks(&tasks, &pool, &client).await;

        // Assert
        for n in priorities_out {
            //      assert_eq!(queue.pop().unwrap().get_priority(), n)
        }
    }
}
