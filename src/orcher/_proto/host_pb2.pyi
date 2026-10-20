from google.protobuf import duration_pb2 as _duration_pb2
from . import kernel_pb2 as _kernel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ActorMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    ACTOR_MODE_UNSPECIFIED: _ClassVar[ActorMode]
    ACTOR_MODE_EXCLUSIVE: _ClassVar[ActorMode]
    ACTOR_MODE_SHARED: _ClassVar[ActorMode]
ACTOR_MODE_UNSPECIFIED: ActorMode
ACTOR_MODE_EXCLUSIVE: ActorMode
ACTOR_MODE_SHARED: ActorMode

class TaskAssignment(_message.Message):
    __slots__ = ["token", "task_id", "task_type", "run", "input", "attempt", "attempt_timeout", "heartbeat_timeout", "headers"]
    class HeadersEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _kernel_pb2.Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_kernel_pb2.Payload, _Mapping]] = ...) -> None: ...
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    token: bytes
    task_id: str
    task_type: str
    run: _kernel_pb2.RunRef
    input: _containers.RepeatedCompositeFieldContainer[_kernel_pb2.Payload]
    attempt: int
    attempt_timeout: _duration_pb2.Duration
    heartbeat_timeout: _duration_pb2.Duration
    headers: _containers.MessageMap[str, _kernel_pb2.Payload]
    def __init__(self, token: _Optional[bytes] = ..., task_id: _Optional[str] = ..., task_type: _Optional[str] = ..., run: _Optional[_Union[_kernel_pb2.RunRef, _Mapping]] = ..., input: _Optional[_Iterable[_Union[_kernel_pb2.Payload, _Mapping]]] = ..., attempt: _Optional[int] = ..., attempt_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., heartbeat_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., headers: _Optional[_Mapping[str, _kernel_pb2.Payload]] = ...) -> None: ...

class TaskHeartbeat(_message.Message):
    __slots__ = ["token", "details"]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    token: bytes
    details: _kernel_pb2.Payload
    def __init__(self, token: _Optional[bytes] = ..., details: _Optional[_Union[_kernel_pb2.Payload, _Mapping]] = ...) -> None: ...

class HeartbeatAnswer(_message.Message):
    __slots__ = ["cancel_requested"]
    CANCEL_REQUESTED_FIELD_NUMBER: _ClassVar[int]
    cancel_requested: bool
    def __init__(self, cancel_requested: bool = ...) -> None: ...

class TaskOutcome(_message.Message):
    __slots__ = ["token", "value", "failure"]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    token: bytes
    value: _kernel_pb2.Payload
    failure: _kernel_pb2.Failure
    def __init__(self, token: _Optional[bytes] = ..., value: _Optional[_Union[_kernel_pb2.Payload, _Mapping]] = ..., failure: _Optional[_Union[_kernel_pb2.Failure, _Mapping]] = ...) -> None: ...

class ActorOperation(_message.Message):
    __slots__ = ["operation_id", "execution_id", "actor_name", "key", "operation", "payload", "mode", "metadata"]
    class MetadataEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    ACTOR_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    execution_id: str
    actor_name: str
    key: str
    operation: str
    payload: _kernel_pb2.Payload
    mode: ActorMode
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, operation_id: _Optional[str] = ..., execution_id: _Optional[str] = ..., actor_name: _Optional[str] = ..., key: _Optional[str] = ..., operation: _Optional[str] = ..., payload: _Optional[_Union[_kernel_pb2.Payload, _Mapping]] = ..., mode: _Optional[_Union[ActorMode, str]] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ActorResult(_message.Message):
    __slots__ = ["operation_id", "execution_id", "value", "failure"]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    execution_id: str
    value: _kernel_pb2.Payload
    failure: _kernel_pb2.Failure
    def __init__(self, operation_id: _Optional[str] = ..., execution_id: _Optional[str] = ..., value: _Optional[_Union[_kernel_pb2.Payload, _Mapping]] = ..., failure: _Optional[_Union[_kernel_pb2.Failure, _Mapping]] = ...) -> None: ...
