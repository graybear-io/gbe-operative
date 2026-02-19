use async_trait::async_trait;
use gbe_jobs_domain::{TaskDefinition, TaskOutcome, TaskType};
use std::collections::HashMap;
use std::sync::Mutex;

use crate::operative::{Operative, OperativeError};

/// Mock operative for testing. Returns configurable outcomes per task name.
pub struct MockOperative {
    task_types: Vec<TaskType>,
    outcomes: Mutex<HashMap<String, TaskOutcome>>,
    default_outcome: TaskOutcome,
}

impl MockOperative {
    pub fn new(task_types: Vec<TaskType>) -> Self {
        Self {
            task_types,
            outcomes: Mutex::new(HashMap::new()),
            default_outcome: TaskOutcome::Completed {
                output: vec![],
                result_ref: None,
            },
        }
    }

    pub fn set_outcome(&self, task_name: &str, outcome: TaskOutcome) {
        self.outcomes
            .lock()
            .unwrap()
            .insert(task_name.to_string(), outcome);
    }
}

#[async_trait]
impl Operative for MockOperative {
    fn handles(&self) -> &[TaskType] {
        &self.task_types
    }

    async fn execute(&self, task: &TaskDefinition) -> Result<TaskOutcome, OperativeError> {
        let outcomes = self.outcomes.lock().unwrap();
        Ok(outcomes
            .get(&task.name)
            .cloned()
            .unwrap_or_else(|| self.default_outcome.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn returns_default_outcome() {
        let op = MockOperative::new(vec![TaskType::new("work").unwrap()]);
        let task = TaskDefinition {
            name: "test".to_string(),
            task_type: TaskType::new("work").unwrap(),
            depends_on: vec![],
            params: Default::default(),
            timeout_secs: None,
            max_retries: None,
        };

        let outcome = op.execute(&task).await.unwrap();
        assert!(matches!(outcome, TaskOutcome::Completed { .. }));
    }

    #[tokio::test]
    async fn returns_configured_outcome() {
        let op = MockOperative::new(vec![TaskType::new("work").unwrap()]);
        op.set_outcome(
            "fail-task",
            TaskOutcome::Failed {
                exit_code: 1,
                error: "boom".to_string(),
            },
        );

        let task = TaskDefinition {
            name: "fail-task".to_string(),
            task_type: TaskType::new("work").unwrap(),
            depends_on: vec![],
            params: Default::default(),
            timeout_secs: None,
            max_retries: None,
        };

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Failed { exit_code, error } => {
                assert_eq!(exit_code, 1);
                assert_eq!(error, "boom");
            }
            _ => panic!("expected Failed"),
        }
    }
}
