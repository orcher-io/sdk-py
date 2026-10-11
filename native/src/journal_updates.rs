//! The updates the engine journaled for a workflow, for its replay.
//!
//! An update handler changes the workflow's state, and the workflow runs from
//! the start on every activation, so a change made by an update is lost unless
//! the update runs again on replay. sdk-core hands the SDK only the updates
//! still waiting for a result; the ones already answered are read here from
//! the journal, where the engine records each as `UpdateAccepted` with its
//! arguments.

use orcher_sdk_core::proto::orcher::v1::{journal_entry::Attributes, EntryType, JournalEntry};
use serde::Serialize;

/// An update the engine accepted and journaled.
#[derive(Debug, PartialEq, Serialize)]
pub struct JournaledUpdate {
    pub update_id: String,
    pub update_name: String,
    /// The update's arguments as the client sent them (JSON bytes; empty for
    /// none).
    pub args: Vec<u8>,
    /// When it was journaled, in milliseconds since the UNIX epoch, if known.
    pub at_ms: Option<i64>,
}

impl JournaledUpdate {
    /// Reads the journaled updates from `journal`, in journal order. An update
    /// journaled twice under one id is read once.
    pub fn read(journal: &[JournalEntry]) -> Vec<Self> {
        let mut updates: Vec<Self> = Vec::new();
        for entry in journal {
            if entry.entry_type != EntryType::UpdateAccepted as i32 {
                continue;
            }
            let Some(Attributes::UpdateAccepted(accepted)) = &entry.attributes else {
                continue;
            };
            if updates.iter().any(|u| u.update_id == accepted.update_id) {
                continue;
            }
            updates.push(Self {
                update_id: accepted.update_id.clone(),
                update_name: accepted.update_type.clone(),
                args: accepted.args.clone(),
                at_ms: entry.timestamp.as_ref().map(|t| {
                    t.seconds
                        .saturating_mul(1000)
                        .saturating_add(i64::from(t.nanos) / 1_000_000)
                }),
            });
        }
        updates
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_sdk_core::proto::orcher::v1::{
        StepCompletedEventAttributes, UpdateAcceptedEventAttributes, UpdateCompletedEventAttributes,
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

    fn accepted(id: &str, name: &str, args: &[u8], at_ms: Option<i64>) -> JournalEntry {
        entry(
            EntryType::UpdateAccepted,
            at_ms,
            Attributes::UpdateAccepted(UpdateAcceptedEventAttributes {
                update_id: id.into(),
                update_type: name.into(),
                args: args.to_vec(),
                identity: "client".into(),
            }),
        )
    }

    #[test]
    fn reads_each_accepted_update_in_journal_order() {
        let journal = vec![
            accepted("u1", "set", b"42", Some(1_000)),
            entry(
                EntryType::UpdateCompleted,
                Some(1_000),
                Attributes::UpdateCompleted(UpdateCompletedEventAttributes {
                    update_id: "u1".into(),
                    ..Default::default()
                }),
            ),
            entry(
                EntryType::StepCompleted,
                Some(1_500),
                Attributes::StepCompleted(StepCompletedEventAttributes {
                    step_name: "step_0".into(),
                    ..Default::default()
                }),
            ),
            accepted("u2", "clear", b"", None),
            accepted("u1", "set", b"42", Some(3_000)),
        ];

        assert_eq!(
            JournaledUpdate::read(&journal),
            vec![
                JournaledUpdate {
                    update_id: "u1".into(),
                    update_name: "set".into(),
                    args: b"42".to_vec(),
                    at_ms: Some(1_000),
                },
                JournaledUpdate {
                    update_id: "u2".into(),
                    update_name: "clear".into(),
                    args: vec![],
                    at_ms: None,
                },
            ]
        );
    }

    #[test]
    fn serializes_as_the_python_worker_reads_it() {
        let update = JournaledUpdate {
            update_id: "u".into(),
            update_name: "set".into(),
            args: b"7".to_vec(),
            at_ms: Some(5),
        };
        assert_eq!(
            serde_json::to_value(&update).unwrap(),
            serde_json::json!({
                "update_id": "u",
                "update_name": "set",
                "args": [55],
                "at_ms": 5,
            })
        );
    }
}
