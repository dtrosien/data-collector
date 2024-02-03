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

use async_trait::async_trait;
use std::any::Any;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use tokio::sync::Mutex;
use uuid::Uuid;

#[async_trait]
pub trait Runnable: Send + Sync {
    async fn run(&self) -> anyhow::Result<(), TaskError>;
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

pub struct Task {
    pub id: Uuid,
    pub name: String,
    pub num_ingoing_tasks: Option<u16>,
    pub outgoing_tasks: HashMap<String, TaskRef>,
    pub retry: Option<u8>,
    pub repeat: Option<u8>,
    pub tools: Tools,
    pub runnable: Box<dyn Runnable>,
}

impl Task {
    // Implement the new function for Task
    pub fn new(name: String, runnable: Box<dyn Runnable>, tools: Tools) -> TaskRef {
        let task = Task {
            id: Uuid::new_v4(),
            name,
            num_ingoing_tasks: None,
            outgoing_tasks: HashMap::new(),
            retry: None,
            repeat: None,
            tools,
            runnable,
        };
        Arc::new(Mutex::new(task))
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
