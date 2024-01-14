use crate::configuration::TaskSetting;
use crate::tasks::task::{execute_task, Task};
use crate::utils::errors::Result;
use crate::utils::futures::join_handle_results;
use reqwest::Client;
use sqlx::PgPool;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub struct ExecutionSequence(Vec<Vec<Arc<Task>>>);

impl ExecutionSequence {
    /// runs groups of tasks.
    /// while groups are processed in sequence, all tasks inside a group are running concurrently
    ///
    /// if one tasks failed, all other tasks will still processed, but overall the function will return an error
    pub async fn run_all(&self) -> Vec<Result<()>> {
        let mut results = Vec::new();
        for task_group in &self.0 {
            let batch: Vec<JoinHandle<Result<()>>> = task_group
                .iter()
                .map(|task| execute_task(task.clone()))
                .collect();
            let batch_result = join_handle_results(batch).await;

            results.push(batch_result)
        }
        results
    }
    /// like run_all() but runs only tasks with a specific execution_sequence_positions
    pub async fn run_specific(&self, execution_sequence_position: Vec<i32>) -> Vec<Result<()>> {
        let mut results = Vec::new();
        for task_group in &self.0 {
            let batch: Vec<JoinHandle<Result<()>>> = task_group
                .iter()
                .filter(|a| execution_sequence_position.contains(&a.execution_sequence_position))
                .map(|task| execute_task(task.clone()))
                .collect();
            let batch_result = join_handle_results(batch).await;

            results.push(batch_result)
        }
        results
    }
}
pub struct Scheduler {
    tasks: TaskHeap,
}

pub struct TaskHeap(BinaryHeap<Reverse<Arc<Task>>>);

impl Scheduler {
    pub fn build(task_settings: &[TaskSetting], pool: &PgPool, client: &Client) -> Self {
        Scheduler {
            tasks: create_tasks_heap(task_settings, pool, client),
        }
    }

    pub fn get_ordered_tasks(&self) -> Vec<Arc<Task>> {
        self.tasks.0.iter().map(|a| a.0.clone()).collect()
    }
    #[tracing::instrument(name = "Building execution sequence", skip(self))]
    pub fn build_execution_sequence(&self) -> ExecutionSequence {
        let mut sequence = Vec::new();
        let mut current_group = Vec::new();
        let mut last_scheduled_position = None;

        // unwrap Reversed<Task> and c
        for task in &self.tasks.0 {
            let task = task.clone().0;
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

        ExecutionSequence(sequence)
    }
}

/// build Tasks based on TaskSettings and push them in a binary min heap
/// only tasks with a execution_sequence_position > 0 will be scheduled
pub fn create_tasks_heap(
    task_settings: &[TaskSetting],
    pool: &PgPool,
    client: &Client,
) -> TaskHeap {
    let mut heap = BinaryHeap::with_capacity(task_settings.len());

    for ts in task_settings
        .iter()
        .filter(|s| s.execution_sequence_position >= 0)
    {
        let task = Arc::new(Task::new(ts, pool, client));
        // used Reverse to have a min heap
        heap.push(Reverse(task))
    }
    TaskHeap(heap)
}

#[cfg(test)]
mod test {
    use crate::collectors::collector_sources::CollectorSource::All;
    use crate::collectors::sp500_fields::Fields;
    use crate::configuration::TaskSetting;
    use crate::scheduler::create_tasks_heap;
    use crate::tasks::actions::action::ActionType::Collect;
    use crate::utils::test_helpers::get_test_client;
    use sqlx::{Pool, Postgres};

    #[sqlx::test]
    async fn create_task_heap_and_filter_out_sequences_le_zero(pool: Pool<Postgres>) {
        // Arrange
        let sequence_position_in = vec![1, 500, 300, 1000, -2, 90, -20];
        let sequence_position_out = vec![1, 90, 300, 500, 1000];
        let base_task = TaskSetting {
            comment: None,
            actions: vec![Collect],
            sp500_fields: vec![Fields::Nyse],
            execution_sequence_position: 0,
            include_sources: vec![All],
            exclude_sources: vec![],
        };
        let mut tasks = vec![];
        for n in sequence_position_in {
            let mut task = base_task.clone();
            task.execution_sequence_position = n;
            tasks.push(task);
        }
        let client = get_test_client();

        // Act
        let mut heap = create_tasks_heap(&tasks, &pool, &client);

        // Assert
        for n in sequence_position_out {
            assert_eq!(heap.0.pop().unwrap().0.get_execution_sequence_position(), n)
        }
    }

    #[sqlx::test]
    async fn build_execution_sequence(pool: Pool<Postgres>) {
        // Arrange

        todo!()
        // Act

        // Assert
    }

    #[sqlx::test]
    async fn runs_execution_sequence(pool: Pool<Postgres>) {
        // Arrange
        todo!()
        // Act

        // Assert
    }

    #[sqlx::test]
    async fn runs_execution_sequence_with_filter(pool: Pool<Postgres>) {
        // Arrange
        todo!()
        // Act

        // Assert
    }
}
