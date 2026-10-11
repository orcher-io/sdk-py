//! The children the engine journaled for a workflow run, for its replay.
//!
//! A child started without an id is named from the parent's workflow id and
//! the step sequence. Earlier SDKs named it `child_{sequence}`, which every
//! parent shared; a run that started a child under that name must keep it on
//! replay, or it would start the same child a second time under the new name.
//! The worker reads the names the journal holds from here.

use orcher_sdk_core::proto::orcher::v1::{journal_entry::Attributes, JournalEntry};

/// The ids of the children `journal` records, in journal order, each once.
pub fn read(journal: &[JournalEntry]) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    for entry in journal {
        let id = match entry.attributes.as_ref() {
            Some(Attributes::ChildWorkflowExecutionStarted(a)) => &a.workflow_id,
            Some(Attributes::ChildWorkflowExecutionCompleted(a)) => &a.workflow_id,
            Some(Attributes::ChildWorkflowExecutionFailed(a)) => &a.workflow_id,
            Some(Attributes::ChildWorkflowExecutionCanceled(a)) => &a.workflow_id,
            Some(Attributes::ChildWorkflowExecutionTerminated(a)) => &a.workflow_id,
            Some(Attributes::ChildWorkflowExecutionTimedOut(a)) => &a.workflow_id,
            _ => continue,
        };
        if !id.is_empty() && !ids.contains(id) {
            ids.push(id.clone());
        }
    }
    ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_sdk_core::proto::orcher::v1::{
        ChildWorkflowExecutionCompletedEventAttributes,
        ChildWorkflowExecutionStartedEventAttributes, EntryType, StepCompletedEventAttributes,
    };

    fn entry(entry_type: EntryType, attributes: Attributes) -> JournalEntry {
        JournalEntry {
            entry_type: entry_type as i32,
            attributes: Some(attributes),
            ..Default::default()
        }
    }

    #[test]
    fn reads_each_child_once_in_journal_order() {
        let journal = vec![
            entry(
                EntryType::ChildWorkflowExecutionStarted,
                Attributes::ChildWorkflowExecutionStarted(
                    ChildWorkflowExecutionStartedEventAttributes {
                        workflow_id: "child_0".into(),
                        ..Default::default()
                    },
                ),
            ),
            entry(
                EntryType::StepCompleted,
                Attributes::StepCompleted(StepCompletedEventAttributes {
                    step_name: "step_1".into(),
                    ..Default::default()
                }),
            ),
            entry(
                EntryType::ChildWorkflowExecutionCompleted,
                Attributes::ChildWorkflowExecutionCompleted(
                    ChildWorkflowExecutionCompletedEventAttributes {
                        workflow_id: "child_0".into(),
                        ..Default::default()
                    },
                ),
            ),
            entry(
                EntryType::ChildWorkflowExecutionStarted,
                Attributes::ChildWorkflowExecutionStarted(
                    ChildWorkflowExecutionStartedEventAttributes {
                        workflow_id: "wf-child-2".into(),
                        ..Default::default()
                    },
                ),
            ),
        ];

        assert_eq!(read(&journal), vec!["child_0".to_string(), "wf-child-2".to_string()]);
    }
}
