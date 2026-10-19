//! The kernel protocol (`orcher.kernel.v1`), as Python reads and writes it.
//!
//! The kernel hands a workflow activation over as a `RunActivation`: the run's
//! history already resolved into the outcomes its code observes. Python runs
//! the workflow's code against it and answers with a `RunActivationResult`:
//! every durable call the code made, in issue order. Both cross to Python as
//! JSON, in the shapes below, so the Python package needs no protobuf
//! runtime:
//!
//! - bytes (payload data, ids) are arrays of integers;
//! - a timestamp is `*_ms`, milliseconds since the UNIX epoch, or null;
//! - a duration is `{"secs", "nanos"}`, or null;
//! - an enum is its value's name in lower case without the type's prefix
//!   (`END_TIMED_OUT` is `"timed_out"`);
//! - a `oneof` is the one key of its member that is present.
//!
//! [`activation_json`] writes a `RunActivation`; [`result_from_json`] reads a
//! `RunActivationResult`. The three functions exposed to Python check
//! recorded activations without an engine (`contract/replay_check.py`), as the
//! worker host runs them on a live worker.

use std::collections::HashMap;

use orcher_sdk_core::proto::kernel::v1::{
    command, outcome, query_answer, resolution, run_activation_result, update_answer, AwaitEvent,
    CancelChild, CancelTimer, Command, Commands, CompleteRun, End, EndCancelled, FailRun, Failure,
    FailureCode, OrphanPolicy, Outcome, Payload, ProtocolError, ProtocolErrorKind, QueryAnswer,
    RecordValue, RestartFresh, RetryPolicy, RunActivation, RunActivationResult, RunTask, SendEvent,
    StartChild, StartTimer, UpdateAnswer, UpsertLabels,
};
use orcher_sdk_core::proto::orcher::v1::PollWorkflowExecutionResponse;
use orcher_sdk_core::proto::prost::Message;
use orcher_sdk_core::proto::prost_types::{Duration, Timestamp};
use orcher_sdk_core::WorkflowExecutionTask;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde::Deserialize;
use serde_json::{json, Map, Value};

// ============================================================================
// RunActivation → JSON
// ============================================================================

/// A `RunActivation` as Python reads it.
pub(crate) fn activation_json(activation: &RunActivation) -> Value {
    let run = activation.run.clone().unwrap_or_default();
    json!({
        "activation_id": activation.activation_id,
        "run": {
            "workflow_id": run.workflow_id,
            "run_id": run.run_id,
            "workflow_type": run.workflow_type,
            "task_queue": run.task_queue,
            "namespace": run.namespace,
        },
        "resolutions": activation.resolutions.iter().map(|resolution| {
            let mut out = Map::new();
            out.insert("step_id".into(), json!(resolution.step_id));
            out.insert("at_ms".into(), json!(resolution.at.as_ref().map(millis)));
            out.insert("replaying".into(), json!(resolution.replaying));
            // Where it comes in the run's order of outcomes: which of two came
            // first is decided by it, never by their times.
            out.insert("sequence".into(), json!(resolution.sequence));
            let (kind, what) = match &resolution.what {
                Some(resolution::What::Started(started)) => ("started", json!({
                    "input": started.input.iter().map(payload_json).collect::<Vec<_>>(),
                    "attempt": started.attempt,
                    "started_at_ms": started.started_at.as_ref().map(millis),
                    "random_seed": started.random_seed,
                    "history_lineage": started.history_lineage,
                })),
                Some(resolution::What::Step(step)) => ("step", json!({
                    "outcome": step.outcome.as_ref().map(outcome_json),
                })),
                Some(resolution::What::Child(child)) => ("child", json!({
                    "child_workflow_id": child.child_workflow_id,
                    "child_run_id": child.child_run_id,
                    "outcome": child.outcome.as_ref().map(outcome_json),
                })),
                Some(resolution::What::Timer(_)) => ("timer", json!({})),
                Some(resolution::What::Event(event)) => ("event", json!({
                    "event_id": event.event_id,
                    "name": event.name,
                    "payload": event.payload.as_ref().map(payload_json),
                })),
                Some(resolution::What::Update(update)) => ("update", json!({
                    "update_id": update.update_id,
                    "name": update.name,
                    "args": update.args.iter().map(payload_json).collect::<Vec<_>>(),
                })),
                Some(resolution::What::Cancel(cancel)) => ("cancel", json!({
                    "reason": cancel.reason,
                    "requested_at_ms": cancel.requested_at.as_ref().map(millis),
                })),
                Some(resolution::What::ChildStarted(child)) => ("child_started", json!({
                    "child_workflow_id": child.child_workflow_id,
                    "child_run_id": child.child_run_id,
                })),
                // A resolution of no kind Python could know: it answers the
                // activation with a protocol error.
                None => ("unknown", json!({})),
            };
            out.insert(kind.into(), what);
            Value::Object(out)
        }).collect::<Vec<_>>(),
        "queries": activation.queries.iter().map(|query| json!({
            "query_id": query.query_id,
            "name": query.name,
            "args": query.args.iter().map(payload_json).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}

fn payload_json(payload: &Payload) -> Value {
    json!({ "data": payload.data, "metadata": payload.metadata })
}

fn outcome_json(outcome: &Outcome) -> Value {
    let mut out = Map::new();
    match &outcome.result {
        Some(outcome::Result::Value(value)) => {
            out.insert("value".into(), payload_json(value));
        }
        Some(outcome::Result::Failure(failure)) => {
            out.insert("failure".into(), failure_json(failure));
        }
        None => {}
    }
    out.insert(
        "end".into(),
        json!(enum_name(
            End::try_from(outcome.end)
                .unwrap_or(End::Unspecified)
                .as_str_name(),
            "END_"
        )),
    );
    out.insert("attempts".into(), json!(outcome.attempts));
    Value::Object(out)
}

fn failure_json(failure: &Failure) -> Value {
    json!({
        "message": failure.message,
        "error_type": failure.error_type,
        "non_retryable": failure.non_retryable,
        "code": enum_name(
            FailureCode::try_from(failure.code)
                .unwrap_or(FailureCode::Unspecified)
                .as_str_name(),
            "FAILURE_CODE_",
        ),
        "details": failure.details,
    })
}

/// `END_TIMED_OUT` with prefix `END_` is `timed_out`.
fn enum_name(name: &str, prefix: &str) -> String {
    name.strip_prefix(prefix)
        .unwrap_or(name)
        .to_ascii_lowercase()
}

/// `timed_out` with prefix `END_` is `END_TIMED_OUT`.
fn enum_wire_name(name: &str, prefix: &str) -> String {
    format!("{prefix}{}", name.to_ascii_uppercase())
}

fn millis(t: &Timestamp) -> i64 {
    t.seconds
        .saturating_mul(1000)
        .saturating_add(i64::from(t.nanos) / 1_000_000)
}

// ============================================================================
// JSON → RunActivationResult
// ============================================================================

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ResultJson {
    activation_id: Vec<u8>,
    commands: Option<Vec<CommandJson>>,
    workflow_failed: Option<FailureJson>,
    protocol_error: Option<ProtocolErrorJson>,
    queries: Vec<QueryAnswerJson>,
    updates: Vec<UpdateAnswerJson>,
    calls_before_failure: Option<Vec<CommandJson>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct PayloadJson {
    data: Vec<u8>,
    metadata: HashMap<String, Vec<u8>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FailureJson {
    message: String,
    error_type: String,
    non_retryable: bool,
    code: String,
    details: Vec<u8>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProtocolErrorJson {
    message: String,
    kind: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DurationJson {
    secs: i64,
    nanos: i32,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct OutcomeJson {
    value: Option<PayloadJson>,
    failure: Option<FailureJson>,
    end: String,
    attempts: u32,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RetryPolicyJson {
    first_delay: Option<DurationJson>,
    delay_multiplier: f64,
    max_delay: Option<DurationJson>,
    max_attempts: u32,
    never_retry_types: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct QueryAnswerJson {
    query_id: String,
    value: Option<PayloadJson>,
    failure: Option<FailureJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct UpdateAnswerJson {
    update_id: String,
    value: Option<PayloadJson>,
    rejected: Option<FailureJson>,
    failed: Option<FailureJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CommandJson {
    seq: u64,
    satisfied: bool,
    step_id: String,
    run_task: Option<RunTaskJson>,
    start_timer: Option<StartTimerJson>,
    cancel_timer: Option<StepRefJson>,
    start_child: Option<StartChildJson>,
    cancel_child: Option<StepRefJson>,
    send_event: Option<SendEventJson>,
    record_value: Option<RecordValueJson>,
    await_event: Option<AwaitEventJson>,
    complete_run: Option<CompleteRunJson>,
    fail_run: Option<FailRunJson>,
    end_cancelled: Option<EndCancelledJson>,
    restart_fresh: Option<RestartFreshJson>,
    upsert_labels: Option<UpsertLabelsJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RunTaskJson {
    task_type: String,
    task_queue: String,
    input: Vec<PayloadJson>,
    queue_timeout: Option<DurationJson>,
    attempt_timeout: Option<DurationJson>,
    heartbeat_timeout: Option<DurationJson>,
    retry_policy: Option<RetryPolicyJson>,
    headers: HashMap<String, PayloadJson>,
    priority: u32,
    cost: u32,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct StartTimerJson {
    duration: Option<DurationJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct StepRefJson {
    step_id: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct StartChildJson {
    workflow_id: String,
    workflow_type: String,
    task_queue: String,
    namespace: String,
    input: Vec<PayloadJson>,
    execution_timeout: Option<DurationJson>,
    orphan_policy: String,
    retry_policy: Option<RetryPolicyJson>,
    headers: HashMap<String, PayloadJson>,
    priority: u32,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SendEventJson {
    workflow_id: String,
    run_id: String,
    name: String,
    payload: Option<PayloadJson>,
    headers: HashMap<String, PayloadJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RecordValueJson {
    outcome: Option<OutcomeJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AwaitEventJson {
    name: String,
    timeout: Option<DurationJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CompleteRunJson {
    result: Option<PayloadJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FailRunJson {
    failure: Option<FailureJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct EndCancelledJson {
    details: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RestartFreshJson {
    workflow_type: String,
    task_queue: String,
    input: Vec<PayloadJson>,
    execution_timeout: Option<DurationJson>,
    retry_policy: Option<RetryPolicyJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct UpsertLabelsJson {
    labels: HashMap<String, String>,
}

impl From<PayloadJson> for Payload {
    fn from(payload: PayloadJson) -> Self {
        Payload {
            data: payload.data,
            metadata: payload.metadata,
            ..Default::default()
        }
    }
}

impl From<DurationJson> for Duration {
    fn from(d: DurationJson) -> Self {
        Duration {
            seconds: d.secs,
            nanos: d.nanos,
        }
    }
}

fn payloads(payloads: Vec<PayloadJson>) -> Vec<Payload> {
    payloads.into_iter().map(Payload::from).collect()
}

fn headers(headers: HashMap<String, PayloadJson>) -> HashMap<String, Payload> {
    headers
        .into_iter()
        .map(|(name, value)| (name, value.into()))
        .collect()
}

impl FailureJson {
    fn into_failure(self) -> Result<Failure, String> {
        let code = if self.code.is_empty() {
            FailureCode::Unspecified
        } else {
            FailureCode::from_str_name(&enum_wire_name(&self.code, "FAILURE_CODE_"))
                .ok_or_else(|| format!("unknown failure code '{}'", self.code))?
        };
        Ok(Failure {
            message: self.message,
            error_type: self.error_type,
            non_retryable: self.non_retryable,
            code: code as i32,
            details: self.details,
            ..Default::default()
        })
    }
}

impl RetryPolicyJson {
    fn into_policy(self) -> RetryPolicy {
        RetryPolicy {
            first_delay: self.first_delay.map(Into::into),
            delay_multiplier: self.delay_multiplier,
            max_delay: self.max_delay.map(Into::into),
            max_attempts: self.max_attempts,
            never_retry_types: self.never_retry_types,
            ..Default::default()
        }
    }
}

impl OutcomeJson {
    fn into_outcome(self) -> Result<Outcome, String> {
        let result = match (self.value, self.failure) {
            (Some(value), None) => Some(outcome::Result::Value(value.into())),
            (None, Some(failure)) => Some(outcome::Result::Failure(failure.into_failure()?)),
            (None, None) => None,
            (Some(_), Some(_)) => {
                return Err("an outcome carries a value or a failure, not both".to_string())
            }
        };
        let end = if self.end.is_empty() {
            End::Unspecified
        } else {
            End::from_str_name(&enum_wire_name(&self.end, "END_"))
                .ok_or_else(|| format!("unknown end '{}'", self.end))?
        };
        Ok(Outcome {
            result,
            end: end as i32,
            attempts: self.attempts,
            ..Default::default()
        })
    }
}

impl CommandJson {
    fn into_command(self) -> Result<Command, String> {
        let mut what = Vec::new();
        if let Some(task) = self.run_task {
            what.push(command::What::RunTask(RunTask {
                task_type: task.task_type,
                task_queue: task.task_queue,
                input: payloads(task.input),
                queue_timeout: task.queue_timeout.map(Into::into),
                attempt_timeout: task.attempt_timeout.map(Into::into),
                heartbeat_timeout: task.heartbeat_timeout.map(Into::into),
                retry_policy: task.retry_policy.map(RetryPolicyJson::into_policy),
                headers: headers(task.headers),
                priority: task.priority,
                cost: task.cost,
                ..Default::default()
            }));
        }
        if let Some(timer) = self.start_timer {
            what.push(command::What::StartTimer(StartTimer {
                duration: timer.duration.map(Into::into),
                ..Default::default()
            }));
        }
        if let Some(cancel) = self.cancel_timer {
            what.push(command::What::CancelTimer(CancelTimer {
                step_id: cancel.step_id,
                ..Default::default()
            }));
        }
        if let Some(child) = self.start_child {
            let orphan_policy = if child.orphan_policy.is_empty() {
                OrphanPolicy::Unspecified
            } else {
                OrphanPolicy::from_str_name(&enum_wire_name(&child.orphan_policy, "ORPHAN_POLICY_"))
                    .ok_or_else(|| format!("unknown orphan policy '{}'", child.orphan_policy))?
            };
            what.push(command::What::StartChild(StartChild {
                workflow_id: child.workflow_id,
                workflow_type: child.workflow_type,
                task_queue: child.task_queue,
                namespace: child.namespace,
                input: payloads(child.input),
                execution_timeout: child.execution_timeout.map(Into::into),
                orphan_policy: orphan_policy as i32,
                retry_policy: child.retry_policy.map(RetryPolicyJson::into_policy),
                headers: headers(child.headers),
                priority: child.priority,
                ..Default::default()
            }));
        }
        if let Some(cancel) = self.cancel_child {
            what.push(command::What::CancelChild(CancelChild {
                step_id: cancel.step_id,
                ..Default::default()
            }));
        }
        if let Some(event) = self.send_event {
            what.push(command::What::SendEvent(SendEvent {
                workflow_id: event.workflow_id,
                run_id: event.run_id,
                name: event.name,
                payload: event.payload.map(Into::into),
                headers: headers(event.headers),
                ..Default::default()
            }));
        }
        if let Some(record) = self.record_value {
            what.push(command::What::RecordValue(RecordValue {
                outcome: record.outcome.map(OutcomeJson::into_outcome).transpose()?,
                ..Default::default()
            }));
        }
        if let Some(wait) = self.await_event {
            what.push(command::What::AwaitEvent(AwaitEvent {
                name: wait.name,
                timeout: wait.timeout.map(Into::into),
                ..Default::default()
            }));
        }
        if let Some(complete) = self.complete_run {
            what.push(command::What::CompleteRun(CompleteRun {
                result: complete.result.map(Into::into),
                ..Default::default()
            }));
        }
        if let Some(fail) = self.fail_run {
            what.push(command::What::FailRun(FailRun {
                failure: fail.failure.map(FailureJson::into_failure).transpose()?,
                ..Default::default()
            }));
        }
        if let Some(cancelled) = self.end_cancelled {
            what.push(command::What::EndCancelled(EndCancelled {
                details: cancelled.details,
                ..Default::default()
            }));
        }
        if let Some(restart) = self.restart_fresh {
            what.push(command::What::RestartFresh(RestartFresh {
                workflow_type: restart.workflow_type,
                task_queue: restart.task_queue,
                input: payloads(restart.input),
                execution_timeout: restart.execution_timeout.map(Into::into),
                retry_policy: restart.retry_policy.map(RetryPolicyJson::into_policy),
                ..Default::default()
            }));
        }
        if let Some(labels) = self.upsert_labels {
            what.push(command::What::UpsertLabels(UpsertLabels {
                labels: labels.labels,
                ..Default::default()
            }));
        }
        if what.len() > 1 {
            return Err("a command is of one kind".to_string());
        }
        Ok(Command {
            seq: self.seq,
            satisfied: self.satisfied,
            step_id: self.step_id,
            what: what.pop(),
            ..Default::default()
        })
    }
}

/// Reads a `RunActivationResult` from the JSON Python answers with.
///
/// # Errors
///
/// A message naming what is wrong with the JSON: it does not parse, a field
/// or enum value is unknown, or a `oneof` has more than one member.
pub(crate) fn result_from_json(json: &str) -> Result<RunActivationResult, String> {
    let answer: ResultJson =
        serde_json::from_str(json).map_err(|e| format!("not an activation result: {e}"))?;
    let outcomes = [
        answer.commands.is_some(),
        answer.workflow_failed.is_some(),
        answer.protocol_error.is_some(),
    ];
    if outcomes.iter().filter(|set| **set).count() > 1 {
        return Err(
            "an activation result carries commands, a workflow failure or a protocol error, \
             not more than one"
                .to_string(),
        );
    }
    if answer.calls_before_failure.is_some() && answer.workflow_failed.is_none() {
        return Err(
            "calls_before_failure goes with a workflow failure, not without one".to_string(),
        );
    }
    let calls_before_failure = answer
        .calls_before_failure
        .map(|calls| {
            Ok::<_, String>(Commands {
                commands: calls
                    .into_iter()
                    .map(CommandJson::into_command)
                    .collect::<Result<_, _>>()?,
                ..Default::default()
            })
        })
        .transpose()?;
    let outcome = if let Some(commands) = answer.commands {
        Some(run_activation_result::Outcome::Commands(Commands {
            commands: commands
                .into_iter()
                .map(CommandJson::into_command)
                .collect::<Result<_, _>>()?,
            ..Default::default()
        }))
    } else if let Some(failure) = answer.workflow_failed {
        Some(run_activation_result::Outcome::WorkflowFailed(
            failure.into_failure()?,
        ))
    } else if let Some(error) = answer.protocol_error {
        let kind = if error.kind.is_empty() {
            ProtocolErrorKind::Unspecified
        } else {
            ProtocolErrorKind::from_str_name(&enum_wire_name(&error.kind, "PROTOCOL_ERROR_KIND_"))
                .ok_or_else(|| format!("unknown protocol error kind '{}'", error.kind))?
        };
        Some(run_activation_result::Outcome::ProtocolError(
            ProtocolError {
                message: error.message,
                kind: kind as i32,
                ..Default::default()
            },
        ))
    } else {
        None
    };
    let queries = answer
        .queries
        .into_iter()
        .map(|query| {
            let answer = match (query.value, query.failure) {
                (Some(value), None) => Some(query_answer::Answer::Value(value.into())),
                (None, Some(failure)) => {
                    Some(query_answer::Answer::Failure(failure.into_failure()?))
                }
                (None, None) => None,
                _ => return Err("a query answer is a value or a failure".to_string()),
            };
            Ok(QueryAnswer {
                query_id: query.query_id,
                answer,
                ..Default::default()
            })
        })
        .collect::<Result<_, String>>()?;
    let updates = answer
        .updates
        .into_iter()
        .map(|update| {
            let answer = match (update.value, update.rejected, update.failed) {
                (Some(value), None, None) => Some(update_answer::Answer::Value(value.into())),
                (None, Some(rejected), None) => {
                    Some(update_answer::Answer::Rejected(rejected.into_failure()?))
                }
                (None, None, Some(failed)) => {
                    Some(update_answer::Answer::Failed(failed.into_failure()?))
                }
                (None, None, None) => None,
                _ => {
                    return Err("an update answer is a value, a rejection or a failure".to_string())
                }
            };
            Ok(UpdateAnswer {
                update_id: update.update_id,
                answer,
                ..Default::default()
            })
        })
        .collect::<Result<_, String>>()?;
    Ok(RunActivationResult {
        activation_id: answer.activation_id,
        outcome,
        queries,
        updates,
        calls_before_failure,
        ..Default::default()
    })
}

// ============================================================================
// Python functions: recorded activations, without an engine
// ============================================================================

/// The workflow task a recorded activation (an encoded
/// `orcher.v1.PollWorkflowExecutionResponse`) carries.
fn recorded_task(bytes: &[u8]) -> PyResult<WorkflowExecutionTask> {
    PollWorkflowExecutionResponse::decode(bytes)
        .map(WorkflowExecutionTask::from)
        .map_err(|e| PyValueError::new_err(format!("not a recorded activation: {e}")))
}

/// `_kernel_resolve(poll_response: bytes, namespace: str) -> str`
///
/// A recorded activation's history resolved by the kernel, as the JSON of the
/// `RunActivation` the worker hands the workflow's code.
#[pyfunction]
pub(crate) fn _kernel_resolve(poll_response: &[u8], namespace: &str) -> PyResult<String> {
    let task = recorded_task(poll_response)?;
    let activation = orcher_sdk_core::kernel::resolve(&task, namespace);
    Ok(activation_json(&activation).to_string())
}

/// `_kernel_execution_result(run_id: str, result_json: str) -> str`
///
/// An activation result (its JSON, as the worker answers with it) as the
/// kernel turns it into what the workflow driver checks and sends, as JSON.
/// Raises `ValueError` when the kernel refuses it (a protocol error).
#[pyfunction]
pub(crate) fn _kernel_execution_result(run_id: &str, result_json: &str) -> PyResult<String> {
    let answer = result_from_json(result_json).map_err(PyValueError::new_err)?;
    let result = orcher_sdk_core::kernel::execution_result(run_id, answer)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    serde_json::to_string(&result).map_err(|e| PyValueError::new_err(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_sdk_core::proto::kernel::v1::{
        ChildSettled, Resolution, RunRef, RunStarted, StepSettled,
    };

    fn json_payload(data: &str) -> Payload {
        Payload {
            data: data.as_bytes().to_vec(),
            metadata: [("encoding".to_string(), b"json".to_vec())].into(),
            ..Default::default()
        }
    }

    #[test]
    fn an_activation_crosses_with_every_resolution_kind_named() {
        let activation = RunActivation {
            activation_id: vec![1, 2],
            run: Some(RunRef {
                workflow_id: "wf".into(),
                run_id: "run".into(),
                workflow_type: "order".into(),
                task_queue: "q".into(),
                namespace: "ns".into(),
                ..Default::default()
            }),
            resolutions: vec![
                Resolution {
                    at: Some(Timestamp {
                        seconds: 2,
                        nanos: 5_000_000,
                    }),
                    what: Some(resolution::What::Started(RunStarted {
                        input: vec![json_payload("{}")],
                        attempt: 1,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                Resolution {
                    step_id: "charge_1".into(),
                    sequence: 2,
                    what: Some(resolution::What::Step(StepSettled {
                        outcome: Some(Outcome {
                            result: Some(outcome::Result::Failure(Failure {
                                message: "declined".into(),
                                code: FailureCode::Application as i32,
                                ..Default::default()
                            })),
                            end: End::Failed as i32,
                            attempts: 3,
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                Resolution {
                    step_id: "kid".into(),
                    what: Some(resolution::What::Child(ChildSettled {
                        child_workflow_id: "kid".into(),
                        outcome: Some(Outcome {
                            end: End::TimedOut as i32,
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                Resolution::default(),
            ],
            queries: vec![],
            ..Default::default()
        };
        let json = activation_json(&activation);
        assert_eq!(json["activation_id"], json!([1, 2]));
        assert_eq!(json["run"]["namespace"], "ns");
        let resolutions = json["resolutions"].as_array().unwrap();
        assert_eq!(resolutions[0]["at_ms"], 2005);
        assert_eq!(resolutions[0]["started"]["input"][0]["data"], json!(b"{}"));
        assert_eq!(resolutions[1]["sequence"], 2);
        assert_eq!(resolutions[1]["step"]["outcome"]["end"], "failed");
        assert_eq!(
            resolutions[1]["step"]["outcome"]["failure"]["code"],
            "application"
        );
        assert_eq!(resolutions[1]["step"]["outcome"]["attempts"], 3);
        assert_eq!(resolutions[2]["child"]["outcome"]["end"], "timed_out");
        assert_eq!(resolutions[3]["at_ms"], Value::Null);
        assert!(resolutions[3].get("unknown").is_some());
    }

    #[test]
    fn a_result_is_read_into_the_kernel_message() {
        let answer = result_from_json(
            r#"{
                "activation_id": [7],
                "commands": [
                    {"seq": 1, "satisfied": true, "step_id": "a_1", "run_task": {}},
                    {"seq": 2, "step_id": "b_2", "run_task": {
                        "task_type": "b", "input": [{"data": [123, 125]}],
                        "attempt_timeout": {"secs": 30, "nanos": 5},
                        "retry_policy": {"max_attempts": 4, "never_retry_types": ["X"]}
                    }},
                    {"start_child": {"workflow_id": "kid", "orphan_policy": "abandon"}},
                    {"record_value": {"outcome": {"value": {"data": [49]}, "end": "completed", "attempts": 1}}},
                    {"complete_run": {"result": {"data": [110]}}}
                ],
                "queries": [{"query_id": "q", "failure": {"message": "no"}}],
                "updates": [{"update_id": "u", "rejected": {"message": "closed"}}]
            }"#,
        )
        .unwrap();
        assert_eq!(answer.activation_id, [7]);
        let Some(run_activation_result::Outcome::Commands(commands)) = answer.outcome else {
            panic!("{:?}", answer.outcome);
        };
        assert_eq!(commands.commands.len(), 5);
        assert!(commands.commands[0].satisfied);
        let Some(command::What::RunTask(task)) = &commands.commands[1].what else {
            panic!();
        };
        assert_eq!(task.task_type, "b");
        assert_eq!(
            task.attempt_timeout,
            Some(Duration {
                seconds: 30,
                nanos: 5
            })
        );
        assert_eq!(task.retry_policy.as_ref().unwrap().max_attempts, 4);
        let Some(command::What::StartChild(child)) = &commands.commands[2].what else {
            panic!();
        };
        assert_eq!(child.orphan_policy, OrphanPolicy::Abandon as i32);
        assert!(matches!(
            answer.queries[0].answer,
            Some(query_answer::Answer::Failure(_))
        ));
        assert!(matches!(
            answer.updates[0].answer,
            Some(update_answer::Answer::Rejected(_))
        ));
    }

    /// A restart keeps the execution timeout the workflow asked for.
    #[test]
    fn a_restart_carries_its_execution_timeout() {
        let answer = result_from_json(
            r#"{"commands": [{"restart_fresh": {
                "execution_timeout": {"secs": 3600, "nanos": 0}
            }}]}"#,
        )
        .unwrap();
        let Some(run_activation_result::Outcome::Commands(commands)) = answer.outcome else {
            panic!("{:?}", answer.outcome);
        };
        let Some(command::What::RestartFresh(restart)) = &commands.commands[0].what else {
            panic!("{:?}", commands.commands);
        };
        assert_eq!(
            restart.execution_timeout,
            Some(Duration {
                seconds: 3600,
                nanos: 0
            })
        );
    }

    /// A restart's retry policy reaches the kernel command; without one the
    /// fresh run keeps the policy of the run it replaces.
    #[test]
    fn a_restart_carries_its_retry_policy() {
        let restart = |json: &str| {
            let answer = result_from_json(json).unwrap();
            let Some(run_activation_result::Outcome::Commands(mut commands)) = answer.outcome
            else {
                panic!("{:?}", answer.outcome);
            };
            let Some(command::What::RestartFresh(restart)) = commands.commands.remove(0).what
            else {
                panic!("not a restart");
            };
            restart
        };
        let with_policy = restart(
            r#"{"commands": [{"restart_fresh": {
                "retry_policy": {"max_attempts": 4, "never_retry_types": ["Invalid"]}
            }}]}"#,
        );
        let policy = with_policy.retry_policy.unwrap();
        assert_eq!(policy.max_attempts, 4);
        assert_eq!(policy.never_retry_types, ["Invalid"]);
        assert_eq!(policy.first_delay, None);
        let without = restart(r#"{"commands": [{"restart_fresh": {}}]}"#);
        assert_eq!(without.retry_policy, None);
    }

    #[test]
    fn what_is_not_a_result_is_refused() {
        for json in [
            r#"{"commands": [], "workflow_failed": {}}"#,
            r#"{"commands": [{"run_task": {}, "start_timer": {}}]}"#,
            r#"{"commands": [{"start_child": {"orphan_policy": "sometimes"}}]}"#,
            r#"{"workflow_failed": {"code": "bad"}}"#,
            r#"{"commands": [], "calls_before_failure": []}"#,
            r#"{"calls_before_failure": []}"#,
            r#"{"surprise": 1}"#,
        ] {
            assert!(result_from_json(json).is_err(), "{json}");
        }
    }

    /// A failure carries the calls made before it; one made before any
    /// call carries an empty list, and one that does not say carries none.
    #[test]
    fn a_failure_is_read_with_the_calls_made_before_it() {
        let answer = result_from_json(
            r#"{"workflow_failed": {"message": "nope"},
                "calls_before_failure": [{"satisfied": true, "step_id": "a_1", "run_task": {}}]}"#,
        )
        .unwrap();
        assert!(matches!(
            answer.outcome,
            Some(run_activation_result::Outcome::WorkflowFailed(_))
        ));
        let calls = answer.calls_before_failure.unwrap().commands;
        assert_eq!(calls.len(), 1);
        assert!(calls[0].satisfied);
        assert_eq!(calls[0].step_id, "a_1");
        assert!(matches!(calls[0].what, Some(command::What::RunTask(_))));

        let none_made =
            result_from_json(r#"{"workflow_failed": {}, "calls_before_failure": []}"#).unwrap();
        assert_eq!(none_made.calls_before_failure, Some(Commands::default()));
        let unsaid = result_from_json(r#"{"workflow_failed": {}}"#).unwrap();
        assert_eq!(unsaid.calls_before_failure, None);
    }

    #[test]
    fn a_protocol_error_is_read_with_its_kind() {
        let answer =
            result_from_json(r#"{"protocol_error": {"message": "?", "kind": "undecodable"}}"#)
                .unwrap();
        let Some(run_activation_result::Outcome::ProtocolError(error)) = answer.outcome else {
            panic!();
        };
        assert_eq!(error.kind, ProtocolErrorKind::Undecodable as i32);
    }
}
