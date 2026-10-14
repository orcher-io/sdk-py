//! Invoking actor operations from a client.
//!
//! A worker runs actor operations through the worker host, which also reads
//! and writes their state. Invoking one is a client call: the server queues
//! the operation under its concurrency rules, a worker runs it, and its
//! result comes back. Invocations go out on the client's own channel, with
//! the client's credentials.

use std::sync::Arc;

use orcher_sdk_core::poller::channel::ChannelManager;
use orcher_sdk_core::proto::orcher::v1::{
    actor_service_client::ActorServiceClient, ExecutionStatus, InvokeOperationRequest,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::PyResult;
use tokio::sync::Mutex;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

/// How long an invocation waits for its result when the caller does not say.
const DEFAULT_INVOKE_TIMEOUT_MS: u64 = 30_000;

/// The credential headers on actor invocations.
///
/// sdk-core's clients send `authorization: Bearer <key>` and
/// `x-organization-id` on everything they send. Invocations go out on this
/// crate's own channel, so they need the same two headers attached here, or a
/// server that requires authentication rejects them. Not `Debug`: it holds
/// the key.
#[derive(Clone, Default)]
pub(crate) struct Credentials {
    pub(crate) api_key: Option<String>,
    pub(crate) organization_id: Option<String>,
}

impl tonic::service::Interceptor for Credentials {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(ref org_id) = self.organization_id {
            if let Ok(value) = org_id.parse() {
                request.metadata_mut().insert("x-organization-id", value);
            }
        }
        if let Some(ref key) = self.api_key {
            if let Ok(value) = format!("Bearer {key}").parse() {
                request.metadata_mut().insert("authorization", value);
            }
        }
        Ok(request)
    }
}

/// The connection invocations share, and the credentials they carry.
#[derive(Clone)]
pub(crate) struct ActorChannels {
    manager: Arc<Mutex<ChannelManager>>,
    credentials: Credentials,
}

impl ActorChannels {
    pub(crate) fn new(manager: ChannelManager, credentials: Credentials) -> Self {
        Self {
            manager: Arc::new(Mutex::new(manager)),
            credentials,
        }
    }
}

/// An actor service client that sends the credentials.
type ActorClient = ActorServiceClient<InterceptedService<Channel, Credentials>>;

/// A client on the shared channel.
///
/// The manager's lock is held only to take the channel out, never across a
/// call: an invocation lasts as long as its operation runs, and every other
/// invocation would queue behind it.
async fn client(channels: &ActorChannels) -> PyResult<ActorClient> {
    let mut manager = channels.manager.lock().await;
    let channel = manager
        .get()
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("gRPC connect error: {}", e)))?;
    // Payloads up to the configured message limit, not tonic's 4 MiB.
    let max = manager.max_message_bytes();
    Ok(
        ActorServiceClient::with_interceptor(channel, channels.credentials.clone())
            .max_decoding_message_size(max)
            .max_encoding_message_size(max),
    )
}

/// Invokes an actor operation and waits for its result bytes. Raises
/// `RuntimeError` when the call fails or the operation does not succeed.
pub(crate) async fn invoke(
    channels: &ActorChannels,
    actor_name: String,
    key: String,
    operation: String,
    payload: Vec<u8>,
    timeout_ms: Option<u64>,
) -> PyResult<Vec<u8>> {
    let response = client(channels)
        .await?
        .invoke_operation(InvokeOperationRequest {
            actor_name,
            key,
            operation,
            payload,
            timeout_ms: timeout_ms.unwrap_or(DEFAULT_INVOKE_TIMEOUT_MS),
            ..Default::default()
        })
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("InvokeOperation RPC error: {}", e)))?
        .into_inner();
    if response.status != ExecutionStatus::Success as i32 {
        return Err(PyRuntimeError::new_err(format!(
            "Actor operation failed: {} ({})",
            response.error_message, response.error_code
        )));
    }
    Ok(response.result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_sdk_core::proto::orcher::v1::actor_service_server::{
        ActorService, ActorServiceServer,
    };
    use orcher_sdk_core::proto::orcher::v1::*;
    use std::time::Duration;
    use tokio::sync::{mpsc, Notify};
    use tonic::{Request, Response, Status};

    /// An actor service whose invocation of operation `slow` answers only
    /// once released, and of any other operation at once.
    struct SlowInvoke {
        slow_arrived: mpsc::UnboundedSender<()>,
        release: Arc<Notify>,
    }

    #[tonic::async_trait]
    impl ActorService for SlowInvoke {
        async fn invoke_operation(
            &self,
            request: Request<InvokeOperationRequest>,
        ) -> Result<Response<InvokeOperationResponse>, Status> {
            let request = request.into_inner();
            if request.operation == "slow" {
                let _ = self.slow_arrived.send(());
                self.release.notified().await;
            }
            Ok(Response::new(InvokeOperationResponse {
                status: ExecutionStatus::Success as i32,
                result: request.payload,
                ..Default::default()
            }))
        }
        async fn get_state(
            &self,
            _: Request<GetStateRequest>,
        ) -> Result<Response<GetStateResponse>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn set_state(
            &self,
            _: Request<SetStateRequest>,
        ) -> Result<Response<SetStateResponse>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn delete_state(
            &self,
            _: Request<DeleteStateRequest>,
        ) -> Result<Response<DeleteStateResponse>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn list_state_keys(
            &self,
            _: Request<ListStateKeysRequest>,
        ) -> Result<Response<ListStateKeysResponse>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn poll_actor_operation(
            &self,
            _: Request<PollActorOperationRequest>,
        ) -> Result<Response<PollActorOperationResponse>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn complete_actor_operation(
            &self,
            _: Request<CompleteActorOperationRequest>,
        ) -> Result<Response<CompleteActorOperationResponse>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn register_handlers(
            &self,
            _: Request<RegisterHandlersRequest>,
        ) -> Result<Response<RegisterHandlersResponse>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn heartbeat(
            &self,
            _: Request<HeartbeatRequest>,
        ) -> Result<Response<HeartbeatResponse>, Status> {
            Err(Status::unimplemented(""))
        }
    }

    /// The `authorization` and `x-organization-id` headers of each call.
    type Seen = Arc<std::sync::Mutex<Vec<(Option<String>, Option<String>)>>>;

    async fn serve(service: SlowInvoke, seen: Seen) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let record = move |request: Request<()>| {
            let header = |name| {
                request
                    .metadata()
                    .get(name)
                    .map(|v| v.to_str().unwrap().to_string())
            };
            seen.lock()
                .unwrap()
                .push((header("authorization"), header("x-organization-id")));
            Ok(request)
        };
        tokio::spawn(
            tonic::transport::Server::builder()
                .layer(tonic::service::interceptor(record))
                .add_service(ActorServiceServer::new(service))
                .serve_with_incoming(
                    tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
                        .unwrap(),
                ),
        );
        format!("http://{address}")
    }

    #[tokio::test]
    async fn an_invocation_in_flight_does_not_hold_up_the_others() {
        let (arrived_tx, mut arrived) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let url = serve(
            SlowInvoke {
                slow_arrived: arrived_tx,
                release: release.clone(),
            },
            Arc::default(),
        )
        .await;
        let channels = ActorChannels::new(ChannelManager::new(url), Credentials::default());

        let slow = tokio::spawn({
            let channels = channels.clone();
            async move {
                invoke(
                    &channels,
                    "a".into(),
                    "k".into(),
                    "slow".into(),
                    vec![1],
                    None,
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(10), arrived.recv())
            .await
            .expect("the slow invocation never reached the server")
            .unwrap();

        let quick = tokio::time::timeout(
            Duration::from_secs(5),
            invoke(
                &channels,
                "a".into(),
                "k".into(),
                "quick".into(),
                vec![2],
                None,
            ),
        )
        .await
        .expect("an invocation waited for an unrelated one to finish");
        assert_eq!(quick.unwrap(), vec![2]);

        release.notify_one();
        assert_eq!(slow.await.unwrap().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn invocations_carry_the_credentials() {
        let (arrived_tx, _arrived) = mpsc::unbounded_channel();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let url = serve(
            SlowInvoke {
                slow_arrived: arrived_tx,
                release: Arc::new(Notify::new()),
            },
            seen.clone(),
        )
        .await;
        let channels = ActorChannels::new(
            ChannelManager::new(url),
            Credentials {
                api_key: Some("orch_secret".into()),
                organization_id: Some("org_123".into()),
            },
        );
        invoke(&channels, "a".into(), "k".into(), "op".into(), vec![], None)
            .await
            .unwrap();

        let seen = seen.lock().unwrap();
        assert_eq!(
            seen.as_slice(),
            &[(
                Some("Bearer orch_secret".to_string()),
                Some("org_123".to_string())
            )]
        );
    }
}
