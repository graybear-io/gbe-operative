use gbe_jobs_domain::{JobDefinition, TaskOutcome};
use gbe_oracle::SimpleOracle;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::{error, info};

use crate::operative::{Operative, OperativeError};

/// Error from the driver loop.
#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error("oracle: {0}")]
    Oracle(#[from] gbe_oracle::OracleError),

    #[error("task {task} failed: exit {exit_code}")]
    TaskFailed {
        task: String,
        exit_code: i32,
        error: String,
    },

    #[error("task {task} execution error: {source}")]
    Execution {
        task: String,
        source: OperativeError,
    },

    #[error("task join error: {0}")]
    Join(String),
}

/// Result of one task within the JoinSet.
struct TaskCompletion {
    name: String,
    result: Result<TaskOutcome, OperativeError>,
}

/// Run a job to completion using a SimpleOracle and an Operative.
///
/// Dispatches ready tasks concurrently via JoinSet. Fail-fast on
/// first task failure. Returns all successful outcomes on completion.
pub async fn run_job(
    def: &JobDefinition,
    operative: Arc<dyn Operative>,
) -> Result<Vec<(String, TaskOutcome)>, DriverError> {
    let mut oracle = SimpleOracle::new(def.clone())?;
    let mut join_set: JoinSet<TaskCompletion> = JoinSet::new();
    let mut results: Vec<(String, TaskOutcome)> = Vec::new();

    // Seed with root tasks
    let initial: Vec<_> = oracle.ready_tasks().into_iter().cloned().collect();
    for td in initial {
        let op = operative.clone();
        oracle.mark_dispatched(&td.name);
        let name = td.name.clone();
        info!(task = %name, "dispatching task");
        join_set.spawn(async move {
            let result = op.execute(&td).await;
            TaskCompletion { name, result }
        });
    }

    // Process completions
    while let Some(join_result) = join_set.join_next().await {
        let completion = join_result.map_err(|e| DriverError::Join(e.to_string()))?;

        match completion.result {
            Ok(ref outcome) => match outcome {
                TaskOutcome::Completed { .. } => {
                    info!(task = %completion.name, "task completed");
                    let newly_ready: Vec<_> = oracle
                        .task_completed(&completion.name)
                        .into_iter()
                        .cloned()
                        .collect();
                    results.push((completion.name, outcome.clone()));

                    for td in newly_ready {
                        let op = operative.clone();
                        oracle.mark_dispatched(&td.name);
                        let name = td.name.clone();
                        info!(task = %name, "dispatching task");
                        join_set.spawn(async move {
                            let result = op.execute(&td).await;
                            TaskCompletion { name, result }
                        });
                    }
                }
                TaskOutcome::Failed {
                    exit_code, error, ..
                } => {
                    error!(task = %completion.name, exit_code, "task failed");
                    oracle.task_failed(&completion.name);
                    join_set.abort_all();
                    return Err(DriverError::TaskFailed {
                        task: completion.name,
                        exit_code: *exit_code,
                        error: error.clone(),
                    });
                }
            },
            Err(e) => {
                error!(task = %completion.name, error = %e, "operative error");
                oracle.task_failed(&completion.name);
                join_set.abort_all();
                return Err(DriverError::Execution {
                    task: completion.name,
                    source: e,
                });
            }
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_operative::MockOperative;
    use gbe_jobs_domain::{JobDefinition, TaskDefinition, TaskParams, TaskType};

    fn linear_dag() -> JobDefinition {
        JobDefinition {
            v: 1,
            name: "Linear".to_string(),
            job_type: "linear".to_string(),
            tasks: vec![
                TaskDefinition {
                    name: "a".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec![],
                    params: TaskParams::default(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "b".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["a".to_string()],
                    params: TaskParams::default(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "c".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["b".to_string()],
                    params: TaskParams::default(),
                    timeout_secs: None,
                    max_retries: None,
                },
            ],
        }
    }

    fn diamond_dag() -> JobDefinition {
        JobDefinition {
            v: 1,
            name: "Diamond".to_string(),
            job_type: "diamond".to_string(),
            tasks: vec![
                TaskDefinition {
                    name: "root".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec![],
                    params: TaskParams::default(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "left".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["root".to_string()],
                    params: TaskParams::default(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "right".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["root".to_string()],
                    params: TaskParams::default(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "join".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["left".to_string(), "right".to_string()],
                    params: TaskParams::default(),
                    timeout_secs: None,
                    max_retries: None,
                },
            ],
        }
    }

    fn shell_dag() -> JobDefinition {
        let mut params = TaskParams::default();
        params
            .entries
            .insert("command".to_string(), "echo hello".to_string());
        JobDefinition {
            v: 1,
            name: "Shell".to_string(),
            job_type: "shell".to_string(),
            tasks: vec![TaskDefinition {
                name: "greet".to_string(),
                task_type: TaskType::new("shell").unwrap(),
                depends_on: vec![],
                params,
                timeout_secs: None,
                max_retries: None,
            }],
        }
    }

    #[tokio::test]
    async fn linear_dag_runs_in_order() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        let results = run_job(&linear_dag(), mock).await.unwrap();
        let names: Vec<&str> = results.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn diamond_dag_completes_all() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        let results = run_job(&diamond_dag(), mock).await.unwrap();
        let mut names: Vec<String> = results.into_iter().map(|(n, _)| n).collect();
        names.sort();
        assert_eq!(names, vec!["join", "left", "right", "root"]);
    }

    #[tokio::test]
    async fn fail_fast_on_task_failure() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        mock.set_outcome(
            "a",
            TaskOutcome::Failed {
                exit_code: 1,
                error: "boom".to_string(),
            },
        );

        let err = run_job(&linear_dag(), mock).await.unwrap_err();
        match err {
            DriverError::TaskFailed { task, exit_code, .. } => {
                assert_eq!(task, "a");
                assert_eq!(exit_code, 1);
            }
            other => panic!("expected TaskFailed, got: {other}"),
        }
    }

    #[tokio::test]
    async fn shell_operative_integration() {
        use crate::shell_operative::ShellOperative;
        let op = Arc::new(ShellOperative::for_types(&["shell"]).unwrap());
        let results = run_job(&shell_dag(), op).await.unwrap();
        assert_eq!(results.len(), 1);
        match &results[0].1 {
            TaskOutcome::Completed { output, .. } => {
                assert_eq!(output, &["hello"]);
            }
            TaskOutcome::Failed { .. } => panic!("expected success"),
        }
    }
}
