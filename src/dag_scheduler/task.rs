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

use crate::dag_scheduler::scheduler::TaskSpecRef;
use async_trait::async_trait;
use std::any::Any;
use std::collections::HashMap;

use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tracing::log::warn;
use uuid::Uuid;

pub struct ExecutionStats {
    is_error: bool,
    runtime: Duration,
    retries: Option<u8>,
    pub custom_stats: Option<StatsMap>,
}

pub struct Trigger {
    is_error: bool,
    next_tasks: Vec<TaskRef>,
}

#[derive(Clone)]
pub enum ExecutionMode {
    Once,
    Continuously { kill: broadcast::Sender<()> }, // use sender to resubscribe, since Receiver is not clone
    RepeatLimited { count: u32 },
    RepeatForDuration { duration: Duration },
}

// todo run task based on this (fsm)
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

pub type StatsMap = Arc<Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>>;

#[async_trait]
pub trait Runnable: Send + Sync {
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
    UnexpectedError(#[from] anyhow::Error),
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

pub struct Task {
    pub id: Uuid,
    pub name: String,
    pub num_ingoing_tasks: Option<usize>,
    pub outgoing_tasks: Vec<TaskRef>,
    pub retry: Option<u8>,
    pub execution_mode: ExecutionMode,
    pub tools: Tools,
    pub runnable: Arc<dyn Runnable>,
    pub s_finished: mpsc::Sender<(bool, Vec<TaskRef>)>,
    pub stats: Option<ExecutionStats>, // todo hier die stats einfuegen oder spaeter die ergebnisse sammeln in einer map im scheduler??
    pub job_handle: Option<JoinHandle<()>>, // todo save handle handle of runnable to be able to cancel jobs
}

impl Task {
    pub fn new(
        name: String,
        runnable: Arc<dyn Runnable>,
        tools: Tools,
        s_finished: mpsc::Sender<(bool, Vec<TaskRef>)>,
    ) -> TaskRef {
        let task = Task {
            id: Uuid::new_v4(),
            name,
            num_ingoing_tasks: None,
            outgoing_tasks: Vec::new(),
            retry: None,
            execution_mode: ExecutionMode::Once,
            tools,
            runnable,
            s_finished,
            stats: None,
            job_handle: None,
        };
        Arc::new(Mutex::new(task))
    }

    pub fn new_from_spec(
        task_spec: TaskSpecRef,
        s_finished: mpsc::Sender<(bool, Vec<TaskRef>)>,
    ) -> TaskRef {
        let task = Task {
            id: task_spec.id,
            name: task_spec.name.clone(),
            num_ingoing_tasks: None,
            outgoing_tasks: Vec::new(),
            retry: task_spec.retry,
            execution_mode: task_spec.execution_mode.clone(),
            tools: task_spec.tools.clone(),
            runnable: task_spec.runnable.clone(),
            s_finished,
            stats: None,
            job_handle: None,
        };
        Arc::new(Mutex::new(task))
    }

    pub async fn run(&self) -> anyhow::Result<ExecutionStats, TaskError> {
        // init stats .. think about whats helpful
        let mut stats = ExecutionStats {
            is_error: false,
            runtime: Default::default(),
            retries: None,
            custom_stats: None,
        };

        let f = &self.runnable;

        let a = retry(self.retry.unwrap_or(0), Duration::from_secs(1), || f.run())
            .await
            .expect("TODO: panic message");

        let result = self.runnable.run().await;

        if result.is_err() {
            self.s_finished
                .send((true, self.outgoing_tasks.clone()))
                .await
                .expect("TODO: panic message");
        } else {
            self.s_finished
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

async fn retry<T, F, Fut>(max_tries: u8, delay: Duration, mut f: F) -> Result<Option<T>, TaskError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<T>, TaskError>>,
{
    let mut retries_left = max_tries;
    loop {
        retries_left = retries_left.saturating_sub(1);
        match f().await {
            Ok(opt) => return Ok(opt),
            Err(e) => {
                if max_tries > 0 {
                    warn!(
                        "error: , retry send request, retries left: {}",
                        retries_left
                    );
                }
                if retries_left == 0 {
                    return Err(e);
                };
                tokio::time::sleep(delay).await;
            }
        };
    }
}
