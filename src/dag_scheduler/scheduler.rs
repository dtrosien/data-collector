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
                                                                           // trigger_receiver: Option<mpsc::Receiver<(bool, Vec<TaskRef>)>>,
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

impl Default for Scheduler {
    fn default() -> Self {
        Scheduler::new()
    }
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            tasks: Default::default(),
            results: Default::default(),
            // trigger_receiver: None,
        }
    }

    pub async fn create_schedule(
        &mut self,
        task_dependencies_specs: TaskDependenciesSpecs,
    ) -> HashMap<Uuid, TaskRef> {
        let tasks_map = self.create_tasks_from_specs(&task_dependencies_specs).await;
        self.add_outgoing_tasks_to_tasks_in_map(&task_dependencies_specs, &tasks_map)
            .await;
        tasks_map
    }

    /// creates TaskRef from TaskDependenciesSpecs,
    /// counts ingoing tasks for each task and put them in a HashMap
    async fn create_tasks_from_specs(
        &self,
        specs: &TaskDependenciesSpecs,
    ) -> HashMap<Uuid, TaskRef> {
        let mut tasks_map: HashMap<Uuid, TaskRef> = HashMap::new();
        for (task_spec, dependencies) in specs {
            let task = Task::new_from_spec(task_spec.clone());
            if !dependencies.is_empty() {
                task.lock().await.num_ingoing_tasks = Some(dependencies.len());
            }
            tasks_map.insert(task_spec.id, task.clone());
        }
        tasks_map
    }

    /// add outgoings to tasks
    /// looks if there is a proper task in the task map, which should be the outgoing task
    /// then reverse the direction and go through the dependencies and add the outgoing task to their outgoing tasks
    async fn add_outgoing_tasks_to_tasks_in_map(
        &self,
        specs: &TaskDependenciesSpecs,
        tasks_map: &HashMap<Uuid, TaskRef>,
    ) {
        for (task_spec, dependencies) in specs {
            if let Some(outgoing_task) = tasks_map.get(&task_spec.id) {
                for dep_task_spec in dependencies {
                    if let Some(task) = tasks_map.get(&dep_task_spec.id) {
                        let mut locked_task = task.lock().await;
                        locked_task.outgoing_tasks.push(outgoing_task.clone());
                    }
                }
            }
        }
    }

    pub async fn schedule_tasks(&mut self, task_dependencies_specs: TaskDependenciesSpecs) {
        self.tasks = self.create_schedule(task_dependencies_specs).await;
    }

    pub async fn run_all(&mut self) {
        let (trigger_sender, mut trigger_receiver) = mpsc::channel(100);

        self.start_source_tasks(trigger_sender.clone()).await;

        // handle received finished triggers from tasks
        for _ in 0..self.tasks.len() {
            if let Some(msg) = trigger_receiver.recv().await {
                let (_result, tasks) = msg;
                println!("number received next tasks: {}", tasks.len());
                for task in tasks {
                    // different scope for locking task
                    {
                        let mut locked_task = task.lock().await;
                        locked_task.num_ingoing_tasks =
                            locked_task.num_ingoing_tasks.map(|i| i.saturating_sub(1));
                        println!(
                            "name: {:?} current count {:?}",
                            &locked_task.name, &locked_task.num_ingoing_tasks
                        );
                    }
                    if let Some(0) = task.lock().await.num_ingoing_tasks {
                        let task = task.clone();
                        let trigger_sender = trigger_sender.clone();
                        tokio::spawn(async move {
                            task.lock().await.run(trigger_sender).await.unwrap();
                        });
                    }
                }
            }
        }
    }

    async fn start_source_tasks(&self, trigger_sender: mpsc::Sender<(bool, Vec<TaskRef>)>) {
        let mut source_tasks = Vec::new();
        // identify source tasks
        for (_, task_ref) in self.tasks.iter() {
            if task_ref.lock().await.num_ingoing_tasks.is_none() {
                source_tasks.push(task_ref.clone());
            }
        }

        if source_tasks.is_empty() {
            panic!("No source tasks defined")
        }

        // start source tasks
        for task_ref in source_tasks {
            let trigger_sender = trigger_sender.clone();
            tokio::spawn(async move {
                let mut task = task_ref.lock().await;
                task.run(trigger_sender).await.unwrap();
            });
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
    use rand::rngs::OsRng;
    use rand::Rng;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    struct TestRunner {}
    #[async_trait]
    impl Runnable for TestRunner {
        async fn run(&self) -> Result<Option<StatsMap>, crate::dag_scheduler::task::TaskError> {
            let stats: StatsMap = Arc::new(Mutex::new(HashMap::new()));
            let mut rng = OsRng;
            let number = rng.gen_range(100..=300);
            tokio::time::sleep(Duration::from_millis(number)).await;
            stats.lock().await.insert("errors".to_string(), Arc::new(0));
            Ok(Some(stats))
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
        let runner_4 = build_test_runner();
        let runner_5 = build_test_runner();

        let task_spec_1 = TaskSpec {
            id: Uuid::new_v4(),
            name: "task_1".to_string(),
            retry_options: RetryOptions::default(),
            execution_mode: ExecutionMode::Once,
            tools: Arc::new(Default::default()),
            runnable: runner_1,
        };

        let task_spec_2 = TaskSpec {
            id: Uuid::new_v4(),
            name: "task_2".to_string(),
            retry_options: RetryOptions::default(),
            execution_mode: ExecutionMode::Once,
            tools: Arc::new(Default::default()),
            runnable: runner_2,
        };
        let task_spec_3 = TaskSpec {
            id: Uuid::new_v4(),
            name: "task_3".to_string(),
            retry_options: RetryOptions::default(),
            execution_mode: ExecutionMode::Once,
            tools: Arc::new(Default::default()),
            runnable: runner_3,
        };

        let task_spec_4 = TaskSpec {
            id: Uuid::new_v4(),
            name: "task_4".to_string(),
            retry_options: RetryOptions::default(),
            execution_mode: ExecutionMode::Once,
            tools: Arc::new(Default::default()),
            runnable: runner_4,
        };

        let task_spec_5 = TaskSpec {
            id: Uuid::new_v4(),
            name: "task_5".to_string(),
            retry_options: RetryOptions::default(),
            execution_mode: ExecutionMode::Once,
            tools: Arc::new(Default::default()),
            runnable: runner_5,
        };

        let task_spec_ref_1 = TaskSpecRef::from(task_spec_1);
        let task_spec_ref_2 = TaskSpecRef::from(task_spec_2);
        let task_spec_ref_3 = TaskSpecRef::from(task_spec_3);
        let task_spec_ref_4 = TaskSpecRef::from(task_spec_4);
        let task_spec_ref_5 = TaskSpecRef::from(task_spec_5);

        let deps3 = vec![task_spec_ref_1.clone(), task_spec_ref_2.clone()];
        let deps4 = vec![task_spec_ref_1.clone()];
        let deps5 = vec![task_spec_ref_4.clone(), task_spec_ref_3.clone()];

        let mut tasks_specs: TaskDependenciesSpecs = HashMap::new();
        tasks_specs.insert(task_spec_ref_3, deps3);
        tasks_specs.insert(task_spec_ref_2, vec![]);
        tasks_specs.insert(task_spec_ref_1, vec![]);
        tasks_specs.insert(task_spec_ref_4, deps4);
        tasks_specs.insert(task_spec_ref_5, deps5);

        scheduler.schedule_tasks(tasks_specs.clone()).await;

        for (_, task) in scheduler.tasks.iter() {
            let name = task.lock().await.name.clone();
            let i = task.lock().await.num_ingoing_tasks;
            let out = task.lock().await.outgoing_tasks.len();
            println!("name: {}, in: {:?}, out: {:?}", name, i, out);
        }

        scheduler.run_all().await;
    }
}
