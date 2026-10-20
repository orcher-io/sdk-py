//! The kernel protocol (`orcher.kernel.v1`), for checking recorded
//! activations without an engine.
//!
//! On a live worker, the worker host hands Python each activation as an
//! encoded `RunActivation` and takes back an encoded `RunActivationResult`;
//! Python reads and writes them with the messages generated from the same
//! protocol (`orcher._proto`). The two functions here do what the worker host
//! does with a recorded activation, so `contract/replay_check.py` can run the
//! SDK against recordings exactly as a live worker runs it.

use std::borrow::Cow;

use orcher_sdk_core::proto::kernel::v1::RunActivationResult;
use orcher_sdk_core::proto::orcher::v1::PollWorkflowExecutionResponse;
use orcher_sdk_core::proto::prost::Message;
use orcher_sdk_core::WorkflowExecutionTask;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// The workflow task a recorded activation (an encoded
/// `orcher.v1.PollWorkflowExecutionResponse`) carries.
fn recorded_task(bytes: &[u8]) -> PyResult<WorkflowExecutionTask> {
    PollWorkflowExecutionResponse::decode(bytes)
        .map(WorkflowExecutionTask::from)
        .map_err(|e| PyValueError::new_err(format!("not a recorded activation: {e}")))
}

/// `_kernel_resolve(poll_response: bytes, namespace: str) -> bytes`
///
/// A recorded activation's history resolved by the kernel: the encoded
/// `RunActivation` the worker hands the workflow's code.
#[pyfunction]
pub(crate) fn _kernel_resolve(
    poll_response: &[u8],
    namespace: &str,
) -> PyResult<Cow<'static, [u8]>> {
    let task = recorded_task(poll_response)?;
    let activation = orcher_sdk_core::kernel::resolve(&task, namespace);
    Ok(Cow::Owned(activation.encode_to_vec()))
}

/// `_kernel_execution_result(run_id: str, result: bytes) -> str`
///
/// An activation result (an encoded `RunActivationResult`, as the worker
/// answers with it) as the kernel turns it into what the workflow driver
/// checks and sends, as JSON. Raises `ValueError` for bytes that are not a
/// `RunActivationResult`, or one the kernel refuses (a protocol error).
#[pyfunction]
pub(crate) fn _kernel_execution_result(run_id: &str, result: &[u8]) -> PyResult<String> {
    let answer = decode_result(result)?;
    let result = orcher_sdk_core::kernel::execution_result(run_id, answer)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    serde_json::to_string(&result).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// An encoded `RunActivationResult`, as Python answers an activation.
pub(crate) fn decode_result(bytes: &[u8]) -> PyResult<RunActivationResult> {
    RunActivationResult::decode(bytes)
        .map_err(|e| PyValueError::new_err(format!("not a RunActivationResult: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_that_are_not_a_result_are_refused() {
        pyo3::prepare_freethreaded_python();
        let refused = decode_result(&[0xff, 0xff, 0xff]).unwrap_err();
        Python::with_gil(|py| {
            assert!(refused.is_instance_of::<PyValueError>(py));
            assert!(refused.to_string().contains("not a RunActivationResult"));
        });
    }

    #[test]
    fn a_result_crosses_unchanged() {
        let sent = RunActivationResult {
            activation_id: vec![1, 2],
            ..Default::default()
        };
        assert_eq!(decode_result(&sent.encode_to_vec()).unwrap(), sent);
    }
}
