use gbe_jobs_domain::{JobDefinition, OrgId, TaskDefinition};
use gbe_nexus_memory::{MemoryTransport, MemoryTransportConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// -- Inline mock executor (tests/ can't see #[cfg(test)] items in src/) --

use async_trait::async_trait;

#[derive(Debug, Clone)]
struct TaskResult {
    exit_code: i32,
    output: Vec<String>,
}

struct TaskContext {
    org_id: OrgId,
    date: String,
    router_socket: String,
}

#[async_trait]
trait TaskExecutor: Send + Sync {
    async fn execute(
        &self,
        task: &TaskDefinition,
        ctx: &TaskContext,
    ) -> Result<TaskResult, String>;
}

struct MockExecutor {
    results: Mutex<HashMap<String, TaskResult>>,
    call_order: Mutex<Vec<String>>,
}

impl MockExecutor {
    fn new() -> Self {
        Self {
            results: Mutex::new(HashMap::new()),
            call_order: Mutex::new(Vec::new()),
        }
    }

    fn set_result(&self, task_name: &str, result: TaskResult) {
        self.results
            .lock()
            .unwrap()
            .insert(task_name.to_string(), result);
    }

    fn call_order(&self) -> Vec<String> {
        self.call_order.lock().unwrap().clone()
    }
}

#[async_trait]
impl TaskExecutor for MockExecutor {
    async fn execute(
        &self,
        task: &TaskDefinition,
        _ctx: &TaskContext,
    ) -> Result<TaskResult, String> {
        self.call_order
            .lock()
            .unwrap()
            .push(task.name.clone());
        let results = self.results.lock().unwrap();
        Ok(results
            .get(&task.name)
            .cloned()
            .unwrap_or(TaskResult {
                exit_code: 0,
                output: vec![],
            }))
    }
}

// -- Test helpers --

fn load_fixture() -> JobDefinition {
    let yaml = std::fs::read("fixtures/daily-report.yaml").expect("fixture not found");
    serde_yaml::from_slice(&yaml).expect("invalid YAML")
}

fn test_ctx() -> TaskContext {
    TaskContext {
        org_id: OrgId::new("org_test").unwrap(),
        date: "2026-02-16".to_string(),
        router_socket: "/tmp/test-not-used.sock".to_string(),
    }
}

// -- Inline orchestrator (simplified for integration test) --

use std::collections::HashSet;
use tokio::task::JoinSet;

struct TaskCompletion {
    name: String,
    result: Result<TaskResult, String>,
}

async fn run_dag(
    def: &JobDefinition,
    executor: Arc<dyn TaskExecutor>,
    ctx: &TaskContext,
) -> Result<Vec<(String, TaskResult)>, String> {
    let mut completed: HashSet<String> = HashSet::new();
    let mut dispatched: HashSet<String> = HashSet::new();
    let mut results: Vec<(String, TaskResult)> = Vec::new();
    let mut join_set: JoinSet<TaskCompletion> = JoinSet::new();

    // Seed roots
    for task_def in &def.tasks {
        if task_def.depends_on.is_empty() {
            let td = task_def.clone();
            let ex = executor.clone();
            let org = ctx.org_id.clone();
            let date = ctx.date.clone();
            let sock = ctx.router_socket.clone();
            dispatched.insert(td.name.clone());
            let name = td.name.clone();
            join_set.spawn(async move {
                let c = TaskContext {
                    org_id: org,
                    date,
                    router_socket: sock,
                };
                let result = ex.execute(&td, &c).await;
                TaskCompletion { name, result }
            });
        }
    }

    while let Some(join_result) = join_set.join_next().await {
        let completion = join_result.map_err(|e| format!("join: {e}"))?;
        match completion.result {
            Ok(ref tr) if tr.exit_code == 0 => {
                completed.insert(completion.name.clone());
                results.push((completion.name, tr.clone()));
                for task_def in &def.tasks {
                    if dispatched.contains(&task_def.name) {
                        continue;
                    }
                    if task_def.depends_on.iter().all(|d| completed.contains(d)) {
                        let td = task_def.clone();
                        let ex = executor.clone();
                        let org = ctx.org_id.clone();
                        let date = ctx.date.clone();
                        let sock = ctx.router_socket.clone();
                        dispatched.insert(td.name.clone());
                        let name = td.name.clone();
                        join_set.spawn(async move {
                            let c = TaskContext {
                                org_id: org,
                                date,
                                router_socket: sock,
                            };
                            let result = ex.execute(&td, &c).await;
                            TaskCompletion { name, result }
                        });
                    }
                }
            }
            Ok(ref tr) => {
                join_set.abort_all();
                return Err(format!("task {} failed: exit {}", completion.name, tr.exit_code));
            }
            Err(e) => {
                join_set.abort_all();
                return Err(e);
            }
        }
    }
    Ok(results)
}

// -- Tests --

#[test]
fn fixture_loads_and_validates() {
    let def = load_fixture();
    assert_eq!(def.name, "Daily Usage Report");
    assert_eq!(def.tasks.len(), 3);
    def.validate().expect("DAG should be valid");
}

#[test]
fn fixture_roots_correct() {
    let def = load_fixture();
    let roots = def.roots();
    assert_eq!(roots, vec!["fetch-data"]);
}

#[test]
fn fixture_topological_order() {
    let def = load_fixture();
    let order = def.topological_order().unwrap();
    assert_eq!(order, vec!["fetch-data", "transform", "send-report"]);
}

#[tokio::test]
async fn integration_mock_executor_runs_all_tasks() {
    let def = load_fixture();
    let mock = Arc::new(MockExecutor::new());
    let ctx = test_ctx();

    let results = run_dag(&def, mock.clone(), &ctx).await.unwrap();
    assert_eq!(results.len(), 3);

    let order = mock.call_order();
    assert_eq!(order, vec!["fetch-data", "transform", "send-report"]);
}

#[tokio::test]
async fn integration_mock_executor_fail_fast() {
    let def = load_fixture();
    let mock = Arc::new(MockExecutor::new());
    mock.set_result(
        "fetch-data",
        TaskResult {
            exit_code: 1,
            output: vec!["connection refused".to_string()],
        },
    );
    let ctx = test_ctx();

    let err = run_dag(&def, mock.clone(), &ctx).await.unwrap_err();
    assert!(err.contains("fetch-data"));

    // transform and send-report should never have been called
    let order = mock.call_order();
    assert_eq!(order, vec!["fetch-data"]);
}

#[tokio::test]
async fn integration_state_store_records_created() {
    use gbe_jobs_domain::{JobId, TaskId};

    let def = load_fixture();
    let store = Arc::new(gbe_runner::MemoryStateStore::new());
    let transport: Arc<dyn gbe_nexus::Transport> =
        Arc::new(MemoryTransport::new(MemoryTransportConfig::default()));
    let state_mgr = gbe_runner::NexusStateManager::new(store.clone(), transport);

    let job_id = JobId::new("job_test-integration").unwrap();
    let org_id = OrgId::new("org_test").unwrap();
    let task_ids: Vec<(String, TaskId)> = def
        .tasks
        .iter()
        .enumerate()
        .map(|(i, t)| {
            (
                t.name.clone(),
                TaskId::new(&format!("task_test-{i}")).unwrap(),
            )
        })
        .collect();

    use gbe_runner::StateManager;
    state_mgr
        .create_job(&job_id, &org_id, &def, &task_ids)
        .await
        .unwrap();

    // Verify job record
    use gbe_state_store::StateStore;
    let job_key = gbe_jobs_domain::keys::job_key("daily-usage-report", "job_test-integration");
    let record = store.get(&job_key).await.unwrap().expect("job record missing");
    let state_field = record.fields.get("state").unwrap();
    assert_eq!(state_field.as_ref(), b"running");

    // Verify task records
    for (name, task_id) in &task_ids {
        let task_def = def.tasks.iter().find(|t| &t.name == name).unwrap();
        let task_key =
            gbe_jobs_domain::keys::task_key(task_def.task_type.as_str(), task_id.as_str());
        let record = store.get(&task_key).await.unwrap().expect("task record missing");
        let state = record.fields.get("state").unwrap();
        if task_def.depends_on.is_empty() {
            assert_eq!(state.as_ref(), b"pending", "root task should be pending");
        } else {
            assert_eq!(state.as_ref(), b"blocked", "dependent task should be blocked");
        }
    }
}
