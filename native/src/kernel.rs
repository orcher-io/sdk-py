//! The kernel protocol (`orcher.kernel.v1`), for checking recorded
//! activations without an engine.
//!
//! On a live worker, the worker host hands Python each activation as an
//! encoded `RunActivation` and takes back an encoded `RunActivationResult`;
//! Python reads and writes them with the messages generated from the same
//! protocol (`orcher._proto`). The two functions here do what the worker host
//! does with a recorded activation, through sdk-core's encoded replay tools,
//! so `contract/replay_check.py` can run the SDK against recordings exactly
//! as a live worker runs it.

use std::borrow::Cow;

use orcher_sdk_core::kernel::encoded;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// `_kernel_resolve(poll_response: bytes, namespace: str) -> bytes`
///
/// A recorded activation (an encoded `orcher.v1.PollWorkflowExecutionResponse`)
/// resolved by the kernel: the encoded `RunActivation` the worker hands the
/// workflow's code. Raises `ValueError` for bytes that are not a recorded
/// activation.
#[pyfunction]
pub(crate) fn _kernel_resolve(
    poll_response: &[u8],
    namespace: &str,
) -> PyResult<Cow<'static, [u8]>> {
    encoded::resolve_recorded(poll_response, namespace)
        .map(Cow::Owned)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// `_kernel_execution_result(run_id: str, result: bytes) -> str`
///
/// An activation result (an encoded `RunActivationResult`, as the worker
/// answers with it) as the kernel turns it into what the workflow driver
/// checks and sends, as JSON. Raises `ValueError` for bytes that are not a
/// `RunActivationResult`, or one the kernel refuses (a protocol error).
#[pyfunction]
pub(crate) fn _kernel_execution_result(run_id: &str, result: &[u8]) -> PyResult<String> {
    let result = encoded::execution_result(run_id, result)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    serde_json::to_string(&result).map_err(|e| PyValueError::new_err(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_that_are_not_the_messages_are_refused() {
        pyo3::prepare_freethreaded_python();
        let refused = _kernel_execution_result("run", &[0xff, 0xff, 0xff]).unwrap_err();
        Python::with_gil(|py| {
            assert!(refused.is_instance_of::<PyValueError>(py));
            assert!(
                refused.to_string().contains("RunActivationResult"),
                "{refused}"
            );
        });
        assert!(_kernel_resolve(&[0xff, 0xff, 0xff], "default").is_err());
    }
}
