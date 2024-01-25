use crate::configuration::TaskSetting;
use crate::tasks::task::{execute_task, Task};
use crate::utils::errors::Result;
use crate::utils::futures::join_handle_results;
use reqwest::Client;
use sqlx::PgPool;
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
            let batch: Vec<JoinHandle<Result<()>>> =
                task_group.iter().cloned().map(execute_task).collect();
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
                .cloned()
                .map(execute_task)
                .collect();
            if !batch.is_empty() {
                let batch_result = join_handle_results(batch).await;
                results.push(batch_result)
            }
        }
        results
    }
}
pub struct Scheduler {
    tasks: OrderedTasks,
}

impl Scheduler {
    pub fn build(task_settings: &[TaskSetting], pool: &PgPool, client: &Client) -> Self {
        Scheduler {
            tasks: build_ordered_tasks(task_settings, pool, client),
        }
    }

    pub fn get_ordered_tasks(&self) -> Vec<Arc<Task>> {
        self.tasks.0.to_vec()
    }
    #[tracing::instrument(name = "Building execution sequence", skip(self))]
    pub fn build_execution_sequence(&self) -> ExecutionSequence {
        let mut sequence = Vec::new();
        let mut current_group = Vec::new();
        let mut last_scheduled_position = None;

        // unwrap Reversed<Task> and c
        for task in &self.tasks.0 {
            let task = task.clone();
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

struct OrderedTasks(Vec<Arc<Task>>);

/// build Tasks based on TaskSettings and sort them ascending by their execution_sequence_position
/// only tasks with a execution_sequence_position >= 0 will be scheduled
fn build_ordered_tasks(
    task_settings: &[TaskSetting],
    pool: &PgPool,
    client: &Client,
) -> OrderedTasks {
    let mut tasks = Vec::new();

    for ts in task_settings
        .iter()
        .filter(|s| s.execution_sequence_position >= 0)
    {
        let task = Arc::new(Task::new(ts, pool, client));
        tasks.push(task)
    }
    tasks.sort();
    OrderedTasks(tasks)
}

#[cfg(test)]
mod test {
    use crate::collectors::collector_sources::CollectorSource::Dummy;
    use crate::collectors::sp500_fields::Fields;
    use crate::configuration::TaskSetting;
    use crate::scheduler::{build_ordered_tasks, Scheduler};
    use crate::tasks::actions::action::ActionType::Collect;
    use crate::utils::test_helpers::get_test_client;
    use sqlx::PgPool;

    fn build_test_task_settings(sequence_position_in: Vec<i32>) -> Vec<TaskSetting> {
        let base_task = TaskSetting {
            comment: None,
            actions: vec![Collect],
            sp500_fields: vec![Fields::Nyse],
            execution_sequence_position: 0,
            include_sources: vec![Dummy],
            exclude_sources: vec![],
        };
        let mut tasks = vec![];
        for n in sequence_position_in {
            let mut task = base_task.clone();
            task.execution_sequence_position = n;
            tasks.push(task);
        }

        tasks
    }

    #[sqlx::test]
    async fn create_task_heap_and_filter_out_sequences_lesser_zero(pool: PgPool) {
        // Arrange
        let sequence_position_in = vec![1, 500, 0, 300, 1000, -2, 90, -20];
        let tasks = build_test_task_settings(sequence_position_in);
        let client = get_test_client();

        // Act
        let ordered_tasks = build_ordered_tasks(&tasks, &pool, &client);

        // Assert
        let sequence_position_out = vec![(0, 0), (1, 1), (2, 90), (3, 300), (4, 500), (5, 1000)];
        for (index, position) in sequence_position_out {
            assert_eq!(
                ordered_tasks
                    .0
                    .get(index)
                    .unwrap()
                    .get_execution_sequence_position(),
                position
            )
        }
    }

    #[sqlx::test]
    async fn scheduler_builds_execution_sequence(pool: PgPool) {
        // Arrange
        let sequence_position_in = vec![8, 1, 0, 0, 0, 100, 8, -1];
        let task_settings = build_test_task_settings(sequence_position_in);
        let client = get_test_client();

        // Act
        let scheduler = Scheduler::build(&task_settings, &pool, &client);
        let sequence = scheduler.build_execution_sequence();

        // Assert - expect 3x Position 0, 1x position 1, 2x position 8 and 1x position 100
        assert_eq!(sequence.0.get(0).unwrap().len(), 3);
        assert_eq!(sequence.0.get(1).unwrap().len(), 1);
        assert_eq!(sequence.0.get(2).unwrap().len(), 2);
        assert_eq!(sequence.0.get(3).unwrap().len(), 1);
    }

    #[sqlx::test]
    async fn scheduler_runs_execution_sequence(pool: PgPool) {
        // Arrange
        let sequence_position_in = vec![8, 1, 0, 0, 0, 8, -1];
        let task_settings = build_test_task_settings(sequence_position_in);
        let client = get_test_client();

        // Act
        let scheduler = Scheduler::build(&task_settings, &pool, &client);
        let sequence = scheduler.build_execution_sequence();
        let results = sequence.run_all().await;
        // Assert - expected 3 since the single task results are merged into one for each group
        assert_eq!(results.len(), 3);
    }

    #[sqlx::test]
    async fn scheduler_runs_execution_sequence_with_filter(pool: PgPool) {
        // Arrange
        let sequence_position_in = vec![8, 1, 0, 0, 0, 8, -1];
        let task_settings = build_test_task_settings(sequence_position_in);
        let client = get_test_client();

        // Act
        let scheduler = Scheduler::build(&task_settings, &pool, &client);
        let sequence = scheduler.build_execution_sequence();
        let results = sequence.run_specific(vec![0, 1]).await;
        // Assert - expected 2 since the single task results are merged into one for each group
        assert_eq!(results.len(), 2);
    }
}
