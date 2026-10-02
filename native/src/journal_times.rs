//! When the engine journaled what an activation can hand its workflow.
//!
//! The jobs sdk-core derives from the journal carry no times, so they are read
//! from the journal itself and handed to Python beside the jobs. The
//! workflow's clock is built from them: it starts when the engine journaled
//! the start, and moves to when each result the workflow receives was
//! journaled, never to this machine's clock.

use std::collections::HashMap;

use orcher_sdk_core::proto::orcher::v1::{journal_entry::Attributes, EntryType, JournalEntry};
use serde::Serialize;

/// The times an activation's workflow clock is built from, all in
/// milliseconds since the UNIX epoch.
#[derive(Debug, Default, PartialEq, Serialize)]
pub struct JournalTimes {
    /// When the workflow's start was journaled.
    pub started_at_ms: Option<i64>,
    /// When each result was journaled, keyed as the workflow context holds
    /// it: a step's name, `timer:{id}`, `child:{workflow id}`.
    pub resolved_at: HashMap<String, i64>,
    /// When each event was journaled, per name, in journal order.
    pub events: HashMap<String, Vec<i64>>,
}

impl JournalTimes {
    /// Reads the times from `journal`. An entry without a timestamp is skipped.
    pub fn read(journal: &[JournalEntry]) -> Self {
        let mut times = Self::default();
        for entry in journal {
            let Some(at_ms) = entry.timestamp.as_ref().map(|t| {
                t.seconds
                    .saturating_mul(1000)
                    .saturating_add(i64::from(t.nanos) / 1_000_000)
            }) else {
                continue;
            };
            if entry.entry_type == EntryType::WorkflowExecutionStarted as i32 {
                times.started_at_ms.get_or_insert(at_ms);
            }
            let key = match &entry.attributes {
                Some(Attributes::StepCompleted(a)) => a.step_name.clone(),
                Some(Attributes::TimerFired(a)) => format!("timer:{}", a.timer_id),
                Some(Attributes::ChildWorkflowExecutionCompleted(a)) => {
                    format!("child:{}", a.workflow_id)
                }
                Some(Attributes::ChildWorkflowExecutionFailed(a)) => {
                    format!("child:{}", a.workflow_id)
                }
                Some(Attributes::ChildWorkflowExecutionCanceled(a)) => {
                    format!("child:{}", a.workflow_id)
                }
                Some(Attributes::ChildWorkflowExecutionTerminated(a)) => {
                    format!("child:{}", a.workflow_id)
                }
                Some(Attributes::ChildWorkflowExecutionTimedOut(a)) => {
                    format!("child:{}", a.workflow_id)
                }
                Some(Attributes::EventReceived(a)) => {
                    times
                        .events
                        .entry(a.event_name.clone())
                        .or_default()
                        .push(at_ms);
                    continue;
                }
                _ => continue,
            };
            times.resolved_at.entry(key).or_insert(at_ms);
        }
        times
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_sdk_core::proto::orcher::v1::{
        ChildWorkflowExecutionCanceledEventAttributes, EventReceivedEventAttributes,
        StepCompletedEventAttributes, TimerFiredEventAttributes,
        WorkflowExecutionStartedEventAttributes,
    };
    use orcher_sdk_core::proto::prost_types::Timestamp;

    fn entry(entry_type: EntryType, at_ms: Option<i64>, attributes: Attributes) -> JournalEntry {
        JournalEntry {
            entry_type: entry_type as i32,
            timestamp: at_ms.map(|ms| Timestamp {
                seconds: ms / 1000,
                nanos: ((ms % 1000) * 1_000_000) as i32,
            }),
            attributes: Some(attributes),
            ..Default::default()
        }
    }

    #[test]
    fn reads_each_result_under_the_key_the_workflow_holds_it_by() {
        let journal = vec![
            entry(
                EntryType::WorkflowExecutionStarted,
                Some(1_600_000_000_000),
                Attributes::WorkflowExecutionStarted(
                    WorkflowExecutionStartedEventAttributes::default(),
                ),
            ),
            entry(
                EntryType::StepCompleted,
                Some(1_600_000_007_250),
                Attributes::StepCompleted(StepCompletedEventAttributes {
                    step_name: "charge_0".into(),
                    ..Default::default()
                }),
            ),
            entry(
                EntryType::TimerFired,
                Some(1_600_000_014_000),
                Attributes::TimerFired(TimerFiredEventAttributes {
                    timer_id: "timer_1".into(),
                    ..Default::default()
                }),
            ),
            entry(
                EntryType::ChildWorkflowExecutionCanceled,
                Some(1_600_000_021_000),
                Attributes::ChildWorkflowExecutionCanceled(
                    ChildWorkflowExecutionCanceledEventAttributes {
                        workflow_id: "child_2".into(),
                        ..Default::default()
                    },
                ),
            ),
            entry(
                EntryType::EventReceived,
                Some(1_600_000_028_000),
                Attributes::EventReceived(EventReceivedEventAttributes {
                    event_name: "go".into(),
                    ..Default::default()
                }),
            ),
            entry(
                EntryType::EventReceived,
                Some(1_600_000_035_000),
                Attributes::EventReceived(EventReceivedEventAttributes {
                    event_name: "go".into(),
                    ..Default::default()
                }),
            ),
            // No timestamp: nothing to read.
            entry(
                EntryType::StepCompleted,
                None,
                Attributes::StepCompleted(StepCompletedEventAttributes {
                    step_name: "untimed_3".into(),
                    ..Default::default()
                }),
            ),
        ];

        let times = JournalTimes::read(&journal);

        assert_eq!(times.started_at_ms, Some(1_600_000_000_000));
        assert_eq!(
            times.resolved_at,
            HashMap::from([
                ("charge_0".to_string(), 1_600_000_007_250),
                ("timer:timer_1".to_string(), 1_600_000_014_000),
                ("child:child_2".to_string(), 1_600_000_021_000),
            ])
        );
        assert_eq!(
            times.events,
            HashMap::from([("go".to_string(), vec![1_600_000_028_000, 1_600_000_035_000])])
        );
    }

    #[test]
    fn serializes_as_the_python_worker_reads_it() {
        let times = JournalTimes {
            started_at_ms: Some(5),
            resolved_at: HashMap::from([("timer:t".to_string(), 7)]),
            events: HashMap::new(),
        };
        assert_eq!(
            serde_json::to_value(&times).unwrap(),
            serde_json::json!({
                "started_at_ms": 5,
                "resolved_at": { "timer:t": 7 },
                "events": {},
            })
        );
    }
}
