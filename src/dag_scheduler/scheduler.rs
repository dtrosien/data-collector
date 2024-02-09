use crate::dag_scheduler::task::{ExecutionStats, Runnable, Task, TaskRef, Tools};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

pub struct Scheduler {
    tasks: HashMap<Uuid, TaskRef>,
    results: HashMap<Uuid, anyhow::Result<ExecutionStats, anyhow::Error>>, // maybe use task error
    trigger_sender: mpsc::Sender<(bool, Vec<TaskRef>)>,
    trigger_receiver: mpsc::Receiver<(bool, Vec<TaskRef>)>,
}

pub type TaskSpecRef = Arc<TaskSpec>;

pub struct TaskSpec {
    pub id: Uuid,
    pub name: String,
    pub retry: Option<u8>,
    pub repeat: Option<u8>,
    pub tools: Tools,
    pub runnable: Arc<dyn Runnable>,
}

pub type TaskDependenciesSpecs = HashMap<TaskSpecRef, Vec<TaskSpecRef>>;

impl Scheduler {
    pub fn new() -> Self {
        let (trigger_sender, trigger_receiver) = mpsc::channel(100);
        Scheduler {
            tasks: Default::default(),
            results: Default::default(),
            trigger_sender,
            trigger_receiver,
        }
    }

    pub async fn schedule_tasks(&mut self, task_dependencies_specs: TaskDependenciesSpecs) {
        let mut tasks_map: HashMap<Uuid, TaskRef> = HashMap::new();
        let mut outgoings_map: HashMap<Uuid, Vec<Uuid>> = HashMap::new();

        // create task and count ingoing tasks (dependencies)
        for (task_spec, dependencies) in &task_dependencies_specs {
            let task_id = task_spec.id;
            let task = Task::new_from_spec(task_spec.clone(), self.trigger_sender.clone());
            task.lock().await.num_ingoing_tasks = Some(dependencies.len());
            tasks_map.insert(task_id, task.clone());
            for dependency in dependencies {
                // For each ingoing task (dependency), add the current task to its list of outgoing tasks
                outgoings_map
                    .entry(dependency.id)
                    .or_default()
                    .push(task_id);
            }
        }

        // todo renaming vars
        // add outgoing tasks to all tasks
        for (dep_uuid, task_uuids) in &outgoings_map {
            if let Some(dep_task) = tasks_map.get(dep_uuid) {
                for task_uuid in task_uuids {
                    if let Some(task) = tasks_map.get(task_uuid) {
                        let mut locked_task = task.lock().await;
                        locked_task.outgoing_tasks.push(dep_task.clone());
                    }
                }
            }
        }
        self.tasks = tasks_map;
    }

    // pub async fn schedule_tasks_old(&self, task_dependencies_specs: TaskDependenciesSpecs) {
    //     let mut tasks_map: HashMap<String, TaskRef> = HashMap::new();
    //     let mut tasks: Vec<TaskRef> = Vec::new();
    //     let mut outgoings_list: Vec<(TaskSpecRef, TaskRef)> = Vec::new();
    //
    //     for (task_spec, deps) in task_dependencies_specs.clone() {
    //         let task = Task::new_from_spec(task_spec.clone(), self.trigger_sender.clone());
    //         task.lock().await.num_ingoing_tasks = Some(deps.len());
    //         tasks_map.insert(task_spec.name.clone(), task.clone());
    //         tasks.push(task.clone());
    //         for dep in deps {
    //             outgoings_list.push((dep, task.clone()));
    //         }
    //     }
    //
    //     for task in tasks {
    //         for (ts, t) in outgoings_list.iter() {
    //             let mut locked_task = task.lock().await;
    //             if ts.name == locked_task.name {
    //                 let outgoing_task = tasks_map.get(&ts.name).unwrap();
    //                 locked_task.outgoing_tasks.push(outgoing_task.clone())
    //             }
    //         }
    //     }
    // }

    pub async fn run_all(&mut self) {
        let mut source_tasks = Vec::new();

        // identify source tasks
        for (_, task_ref) in self.tasks.iter() {
            if task_ref.lock().await.num_ingoing_tasks.is_none() {
                task_ref.lock().await.s_finished = self.trigger_sender.clone();
                source_tasks.push(task_ref.clone());
            }
        }

        // start source tasks
        for task_ref in source_tasks {
            let task = task_ref.lock().await;
            task.run().await.unwrap();
        }

        // handle received finished triggers from tasks
        for task in self.trigger_receiver.recv().await.unwrap().1.iter() {
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

#[cfg(test)]
mod test {
    use crate::tasks::runnable::Runnable;
    use crate::tasks::task::TaskError;
    use async_trait::async_trait;

    struct TestRunner {}
    #[async_trait]
    impl Runnable for TestRunner {
        async fn run(&self) -> Result<(), TaskError> {
            Ok(())
        }
    }
    fn build_test_runnable() -> Box<dyn Runnable> {
        Box::new(TestRunner {})
    }

    #[tokio::test]
    async fn test_build_schedule() {}
}
