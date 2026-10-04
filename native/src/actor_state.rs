//! Actor state RPCs, made on the worker's actor-state channel.
//!
//! An actor operation reads and writes its state while it runs, and several
//! operations run at once, so these calls share one channel manager.

use std::sync::Arc;

use orcher_sdk_core::poller::channel::ChannelManager;
use orcher_sdk_core::proto::orcher::v1::{
    actor_service_client::ActorServiceClient, DeleteStateRequest, GetStateRequest,
    ListStateKeysRequest, SetStateRequest,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::PyResult;
use tokio::sync::Mutex;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

/// The credential headers on the worker's own actor RPCs.
///
/// sdk-core's drivers send `authorization: Bearer <key>` and
/// `x-organization-id` on everything they send. Actor registration, state and
/// invocation go out on this crate's own channel, so they need the same two
/// headers attached here, or a server that requires authentication rejects
/// them while the pollers beside them are accepted. Not `Debug`: it holds the
/// key.
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

/// The connection the worker's actor RPCs share, and the credentials they
/// carry.
#[derive(Clone)]
pub(crate) struct StateChannels {
    manager: Arc<Mutex<Option<ChannelManager>>>,
    credentials: Credentials,
}

impl StateChannels {
    pub(crate) fn new(manager: ChannelManager, credentials: Credentials) -> Self {
        Self {
            manager: Arc::new(Mutex::new(Some(manager))),
            credentials,
        }
    }
}

/// An actor service client that sends the worker's credentials.
pub(crate) type ActorClient = ActorServiceClient<InterceptedService<Channel, Credentials>>;

/// A client on the shared channel.
///
/// The manager's lock is held only to take the channel out, never across a
/// call: an RPC can last as long as the server takes, and every other actor
/// operation's state calls would queue behind it.
pub(crate) async fn client(channels: &StateChannels) -> PyResult<ActorClient> {
    let mut guard = channels.manager.lock().await;
    let manager = guard
        .as_mut()
        .ok_or_else(|| PyRuntimeError::new_err("Channel manager not available"))?;
    let channel = manager
        .get()
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("gRPC connect error: {}", e)))?;
    // Actor state up to the worker's message limit, not tonic's 4 MiB.
    let max = manager.max_message_bytes();
    Ok(
        ActorServiceClient::with_interceptor(channel, channels.credentials.clone())
            .max_decoding_message_size(max)
            .max_encoding_message_size(max),
    )
}

/// Reads one state key; `None` when the key does not exist.
pub(crate) async fn get_state(
    channels: &StateChannels,
    actor_name: String,
    key: String,
    state_key: String,
    execution_id: String,
) -> PyResult<Option<Vec<u8>>> {
    let response = client(channels)
        .await?
        .get_state(GetStateRequest {
            actor_name,
            key,
            state_key,
            execution_id,
        })
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("GetState RPC error: {}", e)))?
        .into_inner();
    Ok(response.exists.then_some(response.value))
}

/// Writes one state key.
pub(crate) async fn set_state(
    channels: &StateChannels,
    actor_name: String,
    key: String,
    state_key: String,
    value: Vec<u8>,
    execution_id: String,
) -> PyResult<()> {
    client(channels)
        .await?
        .set_state(SetStateRequest {
            actor_name,
            key,
            state_key,
            value,
            execution_id,
            expected_version: String::new(),
        })
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("SetState RPC error: {}", e)))?;
    Ok(())
}

/// Deletes one state key; returns whether it existed.
pub(crate) async fn delete_state(
    channels: &StateChannels,
    actor_name: String,
    key: String,
    state_key: String,
    execution_id: String,
) -> PyResult<bool> {
    let response = client(channels)
        .await?
        .delete_state(DeleteStateRequest {
            actor_name,
            key,
            state_key,
            execution_id,
        })
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("DeleteState RPC error: {}", e)))?;
    Ok(response.into_inner().existed)
}

/// Lists the state keys under `prefix`.
pub(crate) async fn list_state_keys(
    channels: &StateChannels,
    actor_name: String,
    key: String,
    execution_id: String,
    prefix: String,
) -> PyResult<Vec<String>> {
    let response = client(channels)
        .await?
        .list_state_keys(ListStateKeysRequest {
            actor_name,
            key,
            execution_id,
            prefix,
        })
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("ListStateKeys RPC error: {}", e)))?;
    Ok(response.into_inner().keys)
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

    /// An actor service whose GetState answers only once released, and whose
    /// other state calls answer at once.
    struct SlowGetState {
        get_state_arrived: mpsc::UnboundedSender<()>,
        release: Arc<Notify>,
    }

    #[tonic::async_trait]
    impl ActorService for SlowGetState {
        async fn get_state(
            &self,
            _: Request<GetStateRequest>,
        ) -> Result<Response<GetStateResponse>, Status> {
            let _ = self.get_state_arrived.send(());
            self.release.notified().await;
            Ok(Response::new(GetStateResponse::default()))
        }
        async fn set_state(
            &self,
            _: Request<SetStateRequest>,
        ) -> Result<Response<SetStateResponse>, Status> {
            Ok(Response::new(SetStateResponse::default()))
        }
        async fn delete_state(
            &self,
            _: Request<DeleteStateRequest>,
        ) -> Result<Response<DeleteStateResponse>, Status> {
            Ok(Response::new(DeleteStateResponse::default()))
        }
        async fn list_state_keys(
            &self,
            _: Request<ListStateKeysRequest>,
        ) -> Result<Response<ListStateKeysResponse>, Status> {
            Ok(Response::new(ListStateKeysResponse {
                keys: vec!["k".into()],
                ..Default::default()
            }))
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
        async fn invoke_operation(
            &self,
            _: Request<InvokeOperationRequest>,
        ) -> Result<Response<InvokeOperationResponse>, Status> {
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

    #[tokio::test]
    async fn a_state_call_in_flight_does_not_hold_up_the_others() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (arrived_tx, mut arrived) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let service = SlowGetState {
            get_state_arrived: arrived_tx,
            release: release.clone(),
        };
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(ActorServiceServer::new(service))
                .serve_with_incoming(
                    tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
                        .unwrap(),
                ),
        );

        let channels = StateChannels::new(
            ChannelManager::new(format!("http://{address}")),
            Credentials::default(),
        );

        // One operation's read is waiting on the server...
        let reading = tokio::spawn({
            let channels = channels.clone();
            async move {
                get_state(&channels, "a".into(), "k".into(), "s".into(), "e".into()).await
            }
        });
        tokio::time::timeout(Duration::from_secs(10), arrived.recv())
            .await
            .expect("GetState never reached the server")
            .unwrap();

        // ...and another operation's calls still go through meanwhile.
        let listed = tokio::time::timeout(
            Duration::from_secs(5),
            list_state_keys(&channels, "a".into(), "k".into(), "e".into(), String::new()),
        )
        .await
        .expect("ListStateKeys waited for an unrelated GetState to finish");
        assert_eq!(listed.unwrap(), vec!["k".to_string()]);
        tokio::time::timeout(
            Duration::from_secs(5),
            set_state(&channels, "a".into(), "k".into(), "s".into(), vec![1], "e".into()),
        )
        .await
        .expect("SetState waited for an unrelated GetState to finish")
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            delete_state(&channels, "a".into(), "k".into(), "s".into(), "e".into()),
        )
        .await
        .expect("DeleteState waited for an unrelated GetState to finish")
        .unwrap();

        release.notify_one();
        assert_eq!(reading.await.unwrap().unwrap(), None);
    }

    #[tokio::test]
    async fn actor_rpcs_carry_the_worker_credentials() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (arrived_tx, _arrived) = mpsc::unbounded_channel();
        let service = SlowGetState {
            get_state_arrived: arrived_tx,
            release: Arc::new(Notify::new()),
        };
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let record = {
            let seen = seen.clone();
            move |request: Request<()>| {
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
            }
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

        let channels = StateChannels::new(
            ChannelManager::new(format!("http://{address}")),
            Credentials {
                api_key: Some("orch_secret".into()),
                organization_id: Some("org_123".into()),
            },
        );
        list_state_keys(&channels, "a".into(), "k".into(), "e".into(), String::new())
            .await
            .unwrap();
        set_state(&channels, "a".into(), "k".into(), "s".into(), vec![1], "e".into())
            .await
            .unwrap();
        delete_state(&channels, "a".into(), "k".into(), "s".into(), "e".into())
            .await
            .unwrap();
        // Registration and invocation take their client from the same place.
        client(&channels)
            .await
            .unwrap()
            .register_handlers(RegisterHandlersRequest::default())
            .await
            .unwrap_err();

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 4);
        for headers in seen.iter() {
            assert_eq!(
                headers,
                &(
                    Some("Bearer orch_secret".to_string()),
                    Some("org_123".to_string())
                )
            );
        }
    }
}
