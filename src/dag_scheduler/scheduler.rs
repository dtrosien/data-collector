use crate::dag_scheduler::task::{ExecutionStats, Runnable, TaskRef, Tools};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

pub struct Scheduler {
    tasks: HashMap<Uuid, TaskRef>,
    results: HashMap<Uuid, anyhow::Result<ExecutionStats, anyhow::Error>>, // maybe use task error
}

pub struct TaskSpec {
    pub name: String,
    pub ingoing_tasks: Vec<Arc<TaskSpec>>,
    pub retry: Option<u8>,
    pub repeat: Option<u8>,
    pub tools: Tools,
    pub runnable: Box<dyn Runnable>,
}

impl Scheduler {
    pub async fn build_tasks(_task_specs: Vec<TaskSpec>) {
        // todo build tasksrefs from taskspecs (FROM trait is not working here since TaskRef is not a struct) and insert into map
    }

    pub async fn run_all(&mut self) {
        let mut source_tasks = Vec::new();
        let (trigger_sender, mut trigger_reciever) = mpsc::channel(100);

        // identify source tasks
        for (_, task_ref) in self.tasks.iter() {
            if task_ref.lock().await.num_ingoing_tasks.is_none() {
                task_ref.lock().await.s_finished = trigger_sender.clone();
                source_tasks.push(task_ref.clone());
            }
        }

        // start source tasks
        for task_ref in source_tasks {
            let task = task_ref.lock().await;
            task.run().await.unwrap();
        }

        // handle received finished triggers from tasks
        for task in trigger_reciever.recv().await.unwrap().1.iter() {
            let mut locked_task = task.lock().await;
            locked_task.num_ingoing_tasks =
                locked_task.num_ingoing_tasks.map(|i| i.saturating_sub(1));
            if let Some(num_ingoing_tasks) = locked_task.num_ingoing_tasks {
                if num_ingoing_tasks == 0 {
                    locked_task.run().await.unwrap();
                }
            }
            drop(locked_task); // unnecessary  if nothing happens anymore in the loop
        }
    }
}
