use crate::dag_scheduler::task::{
    ExecutionMode, ExecutionStats, RetryOptions, Runnable, Task, TaskError, TaskRef, Tools,
};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

pub struct Scheduler {
    tasks: HashMap<Uuid, TaskRef>,
    results: HashMap<Uuid, anyhow::Result<ExecutionStats, TaskError>>,
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
                self.start_outgoing_tasks(&tasks, trigger_sender.clone())
                    .await;
            }
        }
    }

    async fn start_source_tasks(&self, trigger_sender: mpsc::Sender<(bool, Vec<TaskRef>)>) {
        let mut no_source_tasks = true;
        // identify source tasks
        for (_, task) in self.tasks.iter() {
            if task.lock().await.num_ingoing_tasks.is_none() {
                let trigger_sender = trigger_sender.clone();
                let task = task.clone();
                tokio::spawn(async move {
                    let mut task = task.lock().await;
                    task.run(trigger_sender).await.unwrap();
                });
                no_source_tasks = false;
            }
        }
        if no_source_tasks {
            panic!("No source tasks defined")
        }
    }

    async fn start_outgoing_tasks(
        &self,
        tasks: &Vec<TaskRef>,
        trigger_sender: mpsc::Sender<(bool, Vec<TaskRef>)>,
    ) {
        for task in tasks {
            // different scope for locking task and updating remaining incoming tasks
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

#[cfg(test)]
mod test {
    use crate::dag_scheduler::scheduler::{
        Scheduler, TaskDependenciesSpecs, TaskSpec, TaskSpecRef,
    };
    use crate::dag_scheduler::task::{ExecutionMode, RetryOptions, Runnable, StatsMap};
    use async_trait::async_trait;
    use rand::rngs::OsRng;
    use rand::seq::SliceRandom;
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
            let number = rng.gen_range(1..=30);
            tokio::time::sleep(Duration::from_millis(number)).await;
            stats.lock().await.insert("errors".to_string(), Arc::new(0));
            Ok(Some(stats))
        }
    }
    fn build_test_runner() -> Arc<dyn Runnable> {
        Arc::new(TestRunner {})
    }

    #[tokio::test]
    async fn test_build_specific_schedule() {
        let mut scheduler = Scheduler::new();

        let task_specs = create_test_task_specs(5);

        let deps3 = vec![
            task_specs.get(&1).unwrap().clone(),
            task_specs.get(&2).unwrap().clone(),
        ];
        let deps4 = vec![task_specs.get(&1).unwrap().clone()];
        let deps5 = vec![
            task_specs.get(&4).unwrap().clone(),
            task_specs.get(&3).unwrap().clone(),
        ];

        let mut tasks_specs: TaskDependenciesSpecs = HashMap::new();
        tasks_specs.insert(task_specs.get(&3).unwrap().clone(), deps3);
        tasks_specs.insert(task_specs.get(&2).unwrap().clone(), vec![]);
        tasks_specs.insert(task_specs.get(&1).unwrap().clone(), vec![]);
        tasks_specs.insert(task_specs.get(&4).unwrap().clone(), deps4);
        tasks_specs.insert(task_specs.get(&5).unwrap().clone(), deps5);

        scheduler.schedule_tasks(tasks_specs.clone()).await;

        assert_eq!(scheduler.tasks.len(), 5);

        for (_, task) in scheduler.tasks.iter() {
            let locked_task = task.lock().await;
            match locked_task.name.as_str() {
                "task_1" => {
                    assert_eq!(locked_task.outgoing_tasks.len(), 2);
                    assert_eq!(locked_task.num_ingoing_tasks, None);
                }
                "task_2" => {
                    assert_eq!(locked_task.outgoing_tasks.len(), 1);
                    assert_eq!(locked_task.num_ingoing_tasks, None);
                }
                "task_3" => {
                    assert_eq!(locked_task.outgoing_tasks.len(), 1);
                    assert_eq!(locked_task.num_ingoing_tasks, Some(2));
                }
                "task_4" => {
                    assert_eq!(locked_task.outgoing_tasks.len(), 1);
                    assert_eq!(locked_task.num_ingoing_tasks, Some(1));
                }
                "task_5" => {
                    assert_eq!(locked_task.outgoing_tasks.len(), 0);
                    assert_eq!(locked_task.num_ingoing_tasks, Some(2));
                }
                a => panic!("name did not match expected names: {}", a),
            }
        }
    }

    #[tokio::test]
    async fn test_build_and_run_random_schedule() {
        let mut scheduler = Scheduler::new();
        let task_specs = create_test_task_specs(100);
        let tasks_dep_specs = create_random_task_dependencies(&task_specs, 100);

        scheduler.schedule_tasks(tasks_dep_specs).await;

        // for (_, task) in scheduler.tasks.iter() {
        //     let name = task.lock().await.name.clone();
        //     let i = task.lock().await.num_ingoing_tasks;
        //     let out = task.lock().await.outgoing_tasks.len();
        //
        //     let mut next = Vec::new();
        //     for a in task.lock().await.outgoing_tasks.iter() {
        //         let name = a.lock().await.name.clone();
        //         next.push(name);
        //     }
        //
        //     println!("name: {}, in: {:?}, out: {:?}", name, i, next);
        // }

        tokio::select! {
          _ =  scheduler.run_all() => {}
         _ = tokio::time::sleep(Duration::from_secs(60)) => {
                panic!("scheduled tasks did not finish in time, maybe (undetected) cycle")
            }
        }
    }

    #[tokio::test]
    async fn test_detects_cycles() {
        let mut scheduler = Scheduler::new();
        let task_specs = create_test_task_specs(1000);
        let tasks_dep_specs = create_random_task_dependencies_with_cycles(&task_specs, 1000);

        todo!()
    }

    fn create_test_task_specs(num_tasks: usize) -> HashMap<usize, TaskSpecRef> {
        let mut task_spec_refs = HashMap::new();

        for i in 1..=num_tasks {
            let runner = build_test_runner();
            let task_spec = TaskSpec {
                id: Uuid::new_v4(),
                name: format!("task_{}", i),
                retry_options: RetryOptions::default(),
                execution_mode: ExecutionMode::Once,
                tools: Arc::new(Default::default()),
                runnable: runner,
            };

            let task_spec_ref = TaskSpecRef::from(task_spec);
            // Use the task number as the key
            task_spec_refs.insert(i, task_spec_ref);
        }

        task_spec_refs
    }

    fn create_random_task_dependencies(
        task_specs: &HashMap<usize, TaskSpecRef>,
        max_deps: usize,
    ) -> TaskDependenciesSpecs {
        let mut rng = rand::thread_rng();
        let mut task_deps: TaskDependenciesSpecs = HashMap::new();
        let mut keys: Vec<&usize> = task_specs.keys().collect();
        // Ensure the keys are sorted to respect the DAG property
        keys.sort();

        // Iterate through tasks in sorted order
        for &key in &keys {
            let task_spec_ref = task_specs.get(key).unwrap().clone();

            // Determine valid dependencies (only tasks with a smaller index)
            let valid_deps: Vec<&usize> = keys.iter().filter(|&&k| k < key).copied().collect();

            // Randomly decide the number of dependencies
            let num_deps = rng.gen_range(0..=max_deps.min(valid_deps.len()));

            // Randomly select task specs to be dependencies from valid_deps
            let deps: Vec<TaskSpecRef> = valid_deps
                .choose_multiple(&mut rng, num_deps)
                .map(|&k| task_specs.get(k).unwrap().clone())
                .collect();

            task_deps.insert(task_spec_ref, deps);
        }

        task_deps
    }

    fn create_random_task_dependencies_with_cycles(
        task_specs: &HashMap<usize, TaskSpecRef>,
        max_deps: usize,
    ) -> TaskDependenciesSpecs {
        let mut rng = rand::thread_rng();
        let mut task_deps: TaskDependenciesSpecs = HashMap::new();
        let keys: Vec<&usize> = task_specs.keys().collect();

        // Randomly choose one task to have no dependencies
        let no_deps_key = keys.choose(&mut rng).expect("No keys available");
        let no_deps_task = task_specs.get(no_deps_key).unwrap().clone();
        task_deps.insert(no_deps_task, vec![]);

        // For the rest of the tasks, assign dependencies
        for key in keys.iter().filter(|&k| k != no_deps_key) {
            let task_spec_ref = task_specs.get(key).unwrap().clone();

            // Randomly decide the number of dependencies (up to max_deps)
            let num_deps = rng.gen_range(0..=max_deps.min(keys.len() - 1));

            // Randomly select task specs to be dependencies, excluding the current task to reduce circular dependencies
            let deps: Vec<TaskSpecRef> = keys
                .iter()
                .filter(|&k| k != key && k != no_deps_key) // Corrected comparison here
                .map(|&k| task_specs.get(k).unwrap().clone())
                .collect::<Vec<_>>()
                .choose_multiple(&mut rng, num_deps)
                .cloned()
                .collect();

            task_deps.insert(task_spec_ref, deps);
        }

        task_deps
    }
}
