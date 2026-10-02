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
use tonic::transport::Channel;

/// The channel manager the actor state calls share.
pub(crate) type StateChannels = Arc<Mutex<Option<ChannelManager>>>;

/// A client on the shared channel.
///
/// The manager's lock is held only to take the channel out, never across a
/// call: an RPC can last as long as the server takes, and every other actor
/// operation's state calls would queue behind it.
async fn client(channels: &StateChannels) -> PyResult<ActorServiceClient<Channel>> {
    let mut guard = channels.lock().await;
    let manager = guard
        .as_mut()
        .ok_or_else(|| PyRuntimeError::new_err("Channel manager not available"))?;
    let channel = manager
        .get()
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("gRPC connect error: {}", e)))?;
    Ok(ActorServiceClient::new(channel))
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

        let channels: StateChannels =
            Arc::new(Mutex::new(Some(ChannelManager::new(format!("http://{address}")))));

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
}
