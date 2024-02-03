use crate::dag_scheduler::task::TaskRef;
use std::collections::HashMap;

pub struct Scheduler {
    tasks: HashMap<TaskRef, Vec<TaskRef>>,
}

impl Scheduler {
    pub async fn run_all(&self) {
        let mut initial_tasks = Vec::new();

        for (task_ref, _) in self.tasks.iter() {
            let task = task_ref.lock().await;
            if task.num_ingoing_tasks.is_none() {
                initial_tasks.push(task_ref.clone());
            }
        }

        for task_ref in initial_tasks {
            let task = task_ref.lock().await;
            task.runnable.run().await.unwrap(); // Ensure runnable is properly defined to support .await
        }
    }
}
