use crate::dag_scheduler::task::{
    ExecutionMode, ExecutionStats, RetryOptions, Runnable, Task, TaskRef, Tools,
};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
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
    pub retry_options: RetryOptions,
    pub execution_mode: ExecutionMode,
    pub tools: Tools,
    pub runnable: Arc<dyn Runnable>,
}

// Implement PartialEq and Eq based on the id field
impl PartialEq for TaskSpec {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for TaskSpec {}

impl Hash for TaskSpec {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
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
    use crate::dag_scheduler::scheduler::{
        Scheduler, TaskDependenciesSpecs, TaskSpec, TaskSpecRef,
    };
    use crate::dag_scheduler::task::{ExecutionMode, RetryOptions, Runnable, StatsMap};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use uuid::Uuid;

    struct TestRunner {}
    #[async_trait]
    impl Runnable for TestRunner {
        async fn run(&self) -> Result<Option<StatsMap>, crate::dag_scheduler::task::TaskError> {
            Ok(None)
        }
    }
    fn build_test_runner() -> Arc<dyn Runnable> {
        Arc::new(TestRunner {})
    }

    #[tokio::test]
    async fn test_build_schedule() {
        let mut scheduler = Scheduler::new();

        let runner_1 = build_test_runner();
        let runner_2 = build_test_runner();
        let runner_3 = build_test_runner();
        let task_spec_1 = TaskSpec {
            id: Uuid::new_v4(),
            name: "1".to_string(),
            retry_options: RetryOptions::default(),
            execution_mode: ExecutionMode::Once,
            tools: Arc::new(Default::default()),
            runnable: runner_1,
        };

        let task_spec_2 = TaskSpec {
            id: Uuid::new_v4(),
            name: "2".to_string(),
            retry_options: RetryOptions::default(),
            execution_mode: ExecutionMode::Once,
            tools: Arc::new(Default::default()),
            runnable: runner_2,
        };
        let task_spec_3 = TaskSpec {
            id: Uuid::new_v4(),
            name: "3".to_string(),
            retry_options: RetryOptions::default(),
            execution_mode: ExecutionMode::Once,
            tools: Arc::new(Default::default()),
            runnable: runner_3,
        };

        let deps = vec![
            TaskSpecRef::from(task_spec_1),
            TaskSpecRef::from(task_spec_2),
        ];
        let mut tasks_specs: TaskDependenciesSpecs = HashMap::new();
        tasks_specs.insert(TaskSpecRef::from(task_spec_3), deps);

        scheduler.schedule_tasks(tasks_specs).await;
        // scheduler.run_all().await; // todo fix lock
    }
}
