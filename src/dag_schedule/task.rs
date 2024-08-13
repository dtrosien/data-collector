// Task Identifier: A unique identifier for each task. This is crucial for distinguishing tasks and referencing them in the DAG.
// Dependencies: A list of identifiers for tasks that must be completed before the current task can start. This attribute directly represents the edges in the DAG.
// Execution Status: Indicates the current state of the task, such as pending, in progress, completed, failed, etc. This is vital for tracking the task's lifecycle.
// Priority: An optional attribute to determine the order of task execution when multiple tasks are ready to run. Higher priority tasks can be scheduled before lower priority ones.
// Estimated Duration/Complexity: An estimation of how long the task will take or its complexity. This can help in optimizing the scheduling and allocation of resources.
// Resource Requirements: Details about the resources required to execute the task, such as CPU time, memory, I/O, etc. This is important for resource allocation and load balancing.
// Start Time and Deadline: These are optional attributes defining when a task can start and by when it should be completed. Useful for time-sensitive workflows.
// Retry Policy: Information about how to handle failures, like the maximum number of retries or backoff strategy.
// Output Artifacts: Details about any output produced by the task. This might include data files, logs, or status codes, which could be inputs for dependent tasks.
// Callback or Notification Mechanism: A way to notify other systems or components upon task completion or failure. This can be useful for triggering downstream processes.
// Metadata: Additional information like task creator, creation date, last modified date, etc., for audit and tracking purposes.

use crate::dag_schedule::schedule::TaskSpecRef;
use async_trait::async_trait;
use core::fmt::Debug;
use std::any::Any;
use std::collections::HashMap;

use crate::dag_schedule::task::TaskError::NoExecutionError;
use anyhow::Error;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tracing::log::warn;
use tracing::Instrument;
use uuid::Uuid;

#[allow(dead_code)]
#[derive(Debug)]
pub struct ExecutionStats {
    is_error: bool,
    runtime: Duration,
    retries: Option<u8>,
    pub custom_stats: Option<StatsMap>,
}

#[allow(dead_code)]
pub struct Trigger {
    _is_error: bool,
    next_tasks: Vec<TaskRef>,
}

#[derive(Clone, Debug)]
pub enum ExecutionMode {
    Once,
    Continuously { kill: broadcast::Sender<()> },
    // use sender to resubscribe, since Receiver is not clone
    RepeatLimited { count: u32 },
    RepeatForDuration { duration: Duration },
}

// todo run task based on this (fsm) or actor
#[derive(Debug)]
pub enum ExecutionState {
    Pending,
    Running,
    // Paused,
    Finished,
    Failed,
    Cancelled,
    // Retry,
    // Skipped,
}

#[derive(PartialEq, Debug)]
pub enum CycleCheck {
    Unknown,
    Visited { max_allowed: usize },
    Finished,
}

pub type StatsMap = Arc<Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>>;

#[async_trait]
pub trait Runnable: Send + Sync + Debug {
    async fn run(&self) -> Result<Option<StatsMap>, TaskError>;
}

// todo generalize for lib usage
#[derive(thiserror::Error, Debug)]
pub enum TaskError {
    #[error("Database interaction failed")]
    DatabaseError(#[source] sqlx::Error),
    #[error("The action of the task failed")]
    ClientRequestError(#[source] reqwest::Error),
    #[error("Something went wrong")]
    UnexpectedError(#[source] Error),
    #[error("Nothing was executed")]
    NoExecutionError,
}

pub type TaskRef = Arc<Mutex<Task>>;

pub type Tools = Arc<Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>>;

// pub struct ImmutableTask{
//     pub id: Uuid,
//     pub mutable_task: TaskRef,
// }
//
// impl Hash for ImmutableTask {
//     fn hash<H: Hasher>(&self, state: &mut H) {
//         self.id.hash(state)
//     }
// }
//
// impl PartialEq<Self> for ImmutableTask {
//     fn eq(&self, other: &Self) -> bool {
//         self.id.eq(&other.id)
//     }
// }
// impl Eq for ImmutableTask {}

// todo maybe build as actor if really needed
#[derive(Debug)]
pub struct Task {
    pub id: Uuid,
    pub name: String,
    pub num_ingoing_tasks: Option<usize>,
    pub outgoing_tasks: Vec<TaskRef>,
    pub cycle_check: CycleCheck,
    pub retry_options: RetryOptions,
    pub repeat: Option<usize>,
    pub execution_mode: ExecutionMode,
    pub tools: Tools,
    pub runnable: Arc<dyn Runnable>,
    // pub s_finished: Option<mpsc::Sender<(bool, Vec<TaskRef>)>>,
    pub execution_state: ExecutionState,
    pub stats: Option<ExecutionStats>,
    // todo hier die stats einfuegen oder spaeter die ergebnisse sammeln in einer map im scheduler??
    pub job_handle: Option<JoinHandle<()>>, // todo save handle handle of runnable to be able to cancel jobs
}

impl Task {
    pub fn new(
        name: String,
        runnable: Arc<dyn Runnable>,
        tools: Tools,
        _s_finished: Option<mpsc::Sender<(bool, Vec<TaskRef>)>>,
    ) -> TaskRef {
        let task = Task {
            id: Uuid::new_v4(),
            name,
            num_ingoing_tasks: None,
            outgoing_tasks: Vec::new(),
            cycle_check: CycleCheck::Unknown,
            retry_options: RetryOptions::default(),
            repeat: None,
            execution_mode: ExecutionMode::Once,
            tools,
            runnable,
            // s_finished,
            execution_state: ExecutionState::Pending,
            stats: None,
            job_handle: None,
        };
        Arc::new(Mutex::new(task))
    }

    pub fn new_from_spec(
        task_spec: TaskSpecRef,
        // s_finished: Option<mpsc::Sender<(bool, Vec<TaskRef>)>>,
    ) -> TaskRef {
        let task = Task {
            id: task_spec.get_uuid(),
            name: task_spec.name.clone(),
            num_ingoing_tasks: None,
            outgoing_tasks: Vec::new(),
            cycle_check: CycleCheck::Unknown,
            retry_options: task_spec.retry_options,
            repeat: None,
            execution_mode: task_spec.execution_mode.clone(),
            tools: task_spec.tools.clone(),
            runnable: task_spec.runnable.clone(),
            // s_finished,
            execution_state: ExecutionState::Pending,
            stats: None,
            job_handle: None,
        };
        Arc::new(Mutex::new(task))
    }

    #[tracing::instrument(name = "Start task", skip_all, fields(self.name = %self.name) )]
    pub async fn run(
        &mut self,
        s_finished: mpsc::Sender<(bool, Vec<TaskRef>)>,
    ) -> anyhow::Result<ExecutionStats, TaskError> {
        // init stats .. think about whats helpful
        let mut stats = ExecutionStats {
            is_error: false,
            runtime: Default::default(),
            retries: None,
            custom_stats: None,
        };
        let f = self.runnable.clone();
        let r = self.retry_options;

        let span = tracing::Span::current();

        let result = tokio::spawn(async move { retry(r, || f.run()).instrument(span).await })
            .await
            .map_err(|e| TaskError::UnexpectedError(Error::from(e)))?;

        if result.is_err() {
            self.execution_state = ExecutionState::Failed;
            s_finished
                .send((true, self.outgoing_tasks.clone()))
                .await
                .expect("TODO: panic message");
        } else {
            self.execution_state = ExecutionState::Finished;
            s_finished
                .send((false, self.outgoing_tasks.clone()))
                .await
                .expect("TODO: panic message");
        }

        result.map(|s| {
            stats.custom_stats = s;
            stats
        })
    }
}

impl Hash for Task {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl PartialEq<Self> for Task {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for Task {}

#[derive(Clone, Copy, Debug)]
pub struct RetryOptions {
    max_retries: u32,
    back_off: BackOff,
}

impl Default for RetryOptions {
    fn default() -> Self {
        RetryOptions {
            max_retries: 0,
            back_off: BackOff::Constant {
                back_off: Default::default(),
            },
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum BackOff {
    Constant {
        back_off: Duration,
    },
    Linear {
        min_back_off: Duration,
        max_back_off: Duration,
    },
    Exponential {
        base: u32,
        min_back_off: Duration,
        max_back_off: Duration,
    },
}

#[tracing::instrument(level = "debug", skip(f))]
async fn retry<T, F, Fut>(options: RetryOptions, mut f: F) -> Result<Option<T>, TaskError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<T>, TaskError>>,
{
    let mut maybe_result: Result<Option<T>, TaskError> = Err(NoExecutionError);
    for retry_count in 0..=options.max_retries {
        match f().await {
            Ok(opt) => {
                maybe_result = Ok(opt);
                break;
            }
            Err(e) if retry_count < options.max_retries => {
                warn!(
                    "error: {}, retry executing task, retries left: {}",
                    e,
                    options.max_retries - retry_count
                );
                // add 1 since retry_count is 0 based
                let back_off = derive_back_off_time(options, retry_count.add(1));
                tokio::time::sleep(back_off).await;
            }
            Err(e) => maybe_result = Err(e),
        };
    }
    maybe_result
}

#[tracing::instrument(level = "debug", skip(options))]
fn derive_back_off_time(options: RetryOptions, current_retry_count: u32) -> Duration {
    match options.back_off {
        BackOff::Constant { back_off } => back_off,
        BackOff::Linear {
            min_back_off,
            max_back_off,
        } => max_back_off
            .saturating_sub(min_back_off)
            .checked_div(options.max_retries)
            .and_then(|s| s.checked_mul(current_retry_count))
            .unwrap_or(max_back_off),
        BackOff::Exponential {
            base,
            min_back_off,
            max_back_off,
        } => {
            let exp_back_off = min_back_off
                .checked_mul(base.pow(current_retry_count.saturating_sub(1)))
                .unwrap_or(min_back_off);
            std::cmp::min(exp_back_off, max_back_off)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::dag_schedule::task::{retry, BackOff, RetryOptions, TaskError};
    use std::cell::RefCell;

    use std::rc::Rc;
    use std::time::Duration;
    use tokio::time::Instant;

    #[tokio::test]
    async fn test_retry_logic() {
        let r = RetryOptions {
            max_retries: 3,
            back_off: BackOff::Constant {
                back_off: Duration::from_millis(10),
            },
        };
        let counter = Rc::new(RefCell::new(5));
        match retry(r, || run(counter.clone())).await {
            Ok(_) => {
                panic!("last try must fail for this test")
            }
            Err(_) => {
                // 4 trys total: 1 regular + 3 retry
                assert_eq!(counter.take(), 1);
            }
        };
    }

    #[tokio::test]
    async fn test_exponential_back_off_logic() {
        let r = RetryOptions {
            max_retries: 7,
            back_off: BackOff::Exponential {
                base: 2,
                min_back_off: Duration::from_millis(2),
                max_back_off: Duration::from_millis(500),
            },
        };
        // start time
        let start = Instant::now();
        let counter = Rc::new(RefCell::new(7));
        retry(r, || run(counter.clone())).await.unwrap();

        // elapsed time
        let elapsed = start.elapsed();

        assert_eq!(counter.take(), 0);
        assert!((256..1000).contains(&elapsed.as_millis())) // bigger interval for slower envs
    }

    #[tokio::test]
    async fn test_linear_back_off_logic() {
        let r = RetryOptions {
            max_retries: 2,
            back_off: BackOff::Linear {
                min_back_off: Duration::from_millis(1),
                max_back_off: Duration::from_millis(100),
            },
        };
        // start time
        let start = Instant::now();
        let counter = Rc::new(RefCell::new(2));
        retry(r, || run(counter.clone())).await.unwrap();

        // elapsed time
        let elapsed = start.elapsed();

        assert_eq!(counter.take(), 0);
        assert!((150..1000).contains(&elapsed.as_millis())) // bigger interval for slower envs
    }

    async fn run(counter: Rc<RefCell<u8>>) -> Result<Option<()>, TaskError> {
        let mut counter_ref = counter.borrow_mut();
        if *counter_ref > 0 {
            *counter_ref = counter_ref.saturating_sub(1);
            return Err(TaskError::NoExecutionError);
        }
        Ok(Some(()))
    }
}
