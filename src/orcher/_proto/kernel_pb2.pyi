from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StepIdScheme(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    STEP_ID_SCHEME_UNSPECIFIED: _ClassVar[StepIdScheme]
    STEP_ID_SCHEME_KERNEL: _ClassVar[StepIdScheme]
    STEP_ID_SCHEME_RUST_V0: _ClassVar[StepIdScheme]
    STEP_ID_SCHEME_PYTHON_V0: _ClassVar[StepIdScheme]
    STEP_ID_SCHEME_TYPESCRIPT_V0: _ClassVar[StepIdScheme]

class FailureCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    FAILURE_CODE_UNSPECIFIED: _ClassVar[FailureCode]
    FAILURE_CODE_APPLICATION: _ClassVar[FailureCode]
    FAILURE_CODE_TIMED_OUT: _ClassVar[FailureCode]
    FAILURE_CODE_CANCELLED: _ClassVar[FailureCode]
    FAILURE_CODE_TERMINATED: _ClassVar[FailureCode]
    FAILURE_CODE_NON_DETERMINISM: _ClassVar[FailureCode]
    FAILURE_CODE_PAYLOAD_TOO_LARGE: _ClassVar[FailureCode]
    FAILURE_CODE_INVALID_COMMAND: _ClassVar[FailureCode]

class End(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    END_UNSPECIFIED: _ClassVar[End]
    END_COMPLETED: _ClassVar[End]
    END_FAILED: _ClassVar[End]
    END_CANCELLED: _ClassVar[End]
    END_TERMINATED: _ClassVar[End]
    END_TIMED_OUT: _ClassVar[End]

class OrphanPolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    ORPHAN_POLICY_UNSPECIFIED: _ClassVar[OrphanPolicy]
    ORPHAN_POLICY_TERMINATE: _ClassVar[OrphanPolicy]
    ORPHAN_POLICY_CANCEL: _ClassVar[OrphanPolicy]
    ORPHAN_POLICY_ABANDON: _ClassVar[OrphanPolicy]

class ProtocolErrorKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    PROTOCOL_ERROR_KIND_UNSPECIFIED: _ClassVar[ProtocolErrorKind]
    PROTOCOL_ERROR_KIND_UNKNOWN_RESOLUTION: _ClassVar[ProtocolErrorKind]
    PROTOCOL_ERROR_KIND_UNDECODABLE: _ClassVar[ProtocolErrorKind]
    PROTOCOL_ERROR_KIND_UNSUPPORTED_VERSION: _ClassVar[ProtocolErrorKind]
STEP_ID_SCHEME_UNSPECIFIED: StepIdScheme
STEP_ID_SCHEME_KERNEL: StepIdScheme
STEP_ID_SCHEME_RUST_V0: StepIdScheme
STEP_ID_SCHEME_PYTHON_V0: StepIdScheme
STEP_ID_SCHEME_TYPESCRIPT_V0: StepIdScheme
FAILURE_CODE_UNSPECIFIED: FailureCode
FAILURE_CODE_APPLICATION: FailureCode
FAILURE_CODE_TIMED_OUT: FailureCode
FAILURE_CODE_CANCELLED: FailureCode
FAILURE_CODE_TERMINATED: FailureCode
FAILURE_CODE_NON_DETERMINISM: FailureCode
FAILURE_CODE_PAYLOAD_TOO_LARGE: FailureCode
FAILURE_CODE_INVALID_COMMAND: FailureCode
END_UNSPECIFIED: End
END_COMPLETED: End
END_FAILED: End
END_CANCELLED: End
END_TERMINATED: End
END_TIMED_OUT: End
ORPHAN_POLICY_UNSPECIFIED: OrphanPolicy
ORPHAN_POLICY_TERMINATE: OrphanPolicy
ORPHAN_POLICY_CANCEL: OrphanPolicy
ORPHAN_POLICY_ABANDON: OrphanPolicy
PROTOCOL_ERROR_KIND_UNSPECIFIED: ProtocolErrorKind
PROTOCOL_ERROR_KIND_UNKNOWN_RESOLUTION: ProtocolErrorKind
PROTOCOL_ERROR_KIND_UNDECODABLE: ProtocolErrorKind
PROTOCOL_ERROR_KIND_UNSUPPORTED_VERSION: ProtocolErrorKind

class WorkerHello(_message.Message):
    __slots__ = ["protocol_versions", "sdk_name", "sdk_version", "language", "legacy_step_ids", "capabilities", "supported_step_ids"]
    PROTOCOL_VERSIONS_FIELD_NUMBER: _ClassVar[int]
    SDK_NAME_FIELD_NUMBER: _ClassVar[int]
    SDK_VERSION_FIELD_NUMBER: _ClassVar[int]
    LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    LEGACY_STEP_IDS_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    SUPPORTED_STEP_IDS_FIELD_NUMBER: _ClassVar[int]
    protocol_versions: _containers.RepeatedScalarFieldContainer[int]
    sdk_name: str
    sdk_version: str
    language: str
    legacy_step_ids: StepIdScheme
    capabilities: Capabilities
    supported_step_ids: _containers.RepeatedScalarFieldContainer[StepIdScheme]
    def __init__(self, protocol_versions: _Optional[_Iterable[int]] = ..., sdk_name: _Optional[str] = ..., sdk_version: _Optional[str] = ..., language: _Optional[str] = ..., legacy_step_ids: _Optional[_Union[StepIdScheme, str]] = ..., capabilities: _Optional[_Union[Capabilities, _Mapping]] = ..., supported_step_ids: _Optional[_Iterable[_Union[StepIdScheme, str]]] = ...) -> None: ...

class Capabilities(_message.Message):
    __slots__ = ["binary_payloads", "reports_pressure"]
    BINARY_PAYLOADS_FIELD_NUMBER: _ClassVar[int]
    REPORTS_PRESSURE_FIELD_NUMBER: _ClassVar[int]
    binary_payloads: bool
    reports_pressure: bool
    def __init__(self, binary_payloads: bool = ..., reports_pressure: bool = ...) -> None: ...

class HelloAccepted(_message.Message):
    __slots__ = ["protocol_version", "granted"]
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    GRANTED_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    granted: Capabilities
    def __init__(self, protocol_version: _Optional[int] = ..., granted: _Optional[_Union[Capabilities, _Mapping]] = ...) -> None: ...

class RunRef(_message.Message):
    __slots__ = ["workflow_id", "run_id", "workflow_type", "task_queue", "namespace"]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    TASK_QUEUE_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    run_id: str
    workflow_type: str
    task_queue: str
    namespace: str
    def __init__(self, workflow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., workflow_type: _Optional[str] = ..., task_queue: _Optional[str] = ..., namespace: _Optional[str] = ...) -> None: ...

class Payload(_message.Message):
    __slots__ = ["data", "metadata"]
    class MetadataEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    DATA_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    metadata: _containers.ScalarMap[str, bytes]
    def __init__(self, data: _Optional[bytes] = ..., metadata: _Optional[_Mapping[str, bytes]] = ...) -> None: ...

class Failure(_message.Message):
    __slots__ = ["message", "error_type", "non_retryable", "code", "details"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    NON_RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    message: str
    error_type: str
    non_retryable: bool
    code: FailureCode
    details: bytes
    def __init__(self, message: _Optional[str] = ..., error_type: _Optional[str] = ..., non_retryable: bool = ..., code: _Optional[_Union[FailureCode, str]] = ..., details: _Optional[bytes] = ...) -> None: ...

class RetryPolicy(_message.Message):
    __slots__ = ["first_delay", "delay_multiplier", "max_delay", "max_attempts", "never_retry_types"]
    FIRST_DELAY_FIELD_NUMBER: _ClassVar[int]
    DELAY_MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    MAX_DELAY_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    NEVER_RETRY_TYPES_FIELD_NUMBER: _ClassVar[int]
    first_delay: _duration_pb2.Duration
    delay_multiplier: float
    max_delay: _duration_pb2.Duration
    max_attempts: int
    never_retry_types: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, first_delay: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., delay_multiplier: _Optional[float] = ..., max_delay: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., max_attempts: _Optional[int] = ..., never_retry_types: _Optional[_Iterable[str]] = ...) -> None: ...

class RunActivation(_message.Message):
    __slots__ = ["activation_id", "run", "resolutions", "queries"]
    ACTIVATION_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    RESOLUTIONS_FIELD_NUMBER: _ClassVar[int]
    QUERIES_FIELD_NUMBER: _ClassVar[int]
    activation_id: bytes
    run: RunRef
    resolutions: _containers.RepeatedCompositeFieldContainer[Resolution]
    queries: _containers.RepeatedCompositeFieldContainer[QueryAsked]
    def __init__(self, activation_id: _Optional[bytes] = ..., run: _Optional[_Union[RunRef, _Mapping]] = ..., resolutions: _Optional[_Iterable[_Union[Resolution, _Mapping]]] = ..., queries: _Optional[_Iterable[_Union[QueryAsked, _Mapping]]] = ...) -> None: ...

class Resolution(_message.Message):
    __slots__ = ["step_id", "at", "replaying", "sequence", "started", "step", "child", "timer", "event", "update", "cancel", "child_started"]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    AT_FIELD_NUMBER: _ClassVar[int]
    REPLAYING_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    STARTED_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    CHILD_FIELD_NUMBER: _ClassVar[int]
    TIMER_FIELD_NUMBER: _ClassVar[int]
    EVENT_FIELD_NUMBER: _ClassVar[int]
    UPDATE_FIELD_NUMBER: _ClassVar[int]
    CANCEL_FIELD_NUMBER: _ClassVar[int]
    CHILD_STARTED_FIELD_NUMBER: _ClassVar[int]
    step_id: str
    at: _timestamp_pb2.Timestamp
    replaying: bool
    sequence: int
    started: RunStarted
    step: StepSettled
    child: ChildSettled
    timer: TimerElapsed
    event: EventArrived
    update: UpdateArrived
    cancel: CancelAsked
    child_started: ChildStarted
    def __init__(self, step_id: _Optional[str] = ..., at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., replaying: bool = ..., sequence: _Optional[int] = ..., started: _Optional[_Union[RunStarted, _Mapping]] = ..., step: _Optional[_Union[StepSettled, _Mapping]] = ..., child: _Optional[_Union[ChildSettled, _Mapping]] = ..., timer: _Optional[_Union[TimerElapsed, _Mapping]] = ..., event: _Optional[_Union[EventArrived, _Mapping]] = ..., update: _Optional[_Union[UpdateArrived, _Mapping]] = ..., cancel: _Optional[_Union[CancelAsked, _Mapping]] = ..., child_started: _Optional[_Union[ChildStarted, _Mapping]] = ...) -> None: ...

class RunStarted(_message.Message):
    __slots__ = ["input", "attempt", "started_at", "random_seed", "parent", "headers", "step_ids", "history_lineage"]
    class HeadersEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Payload, _Mapping]] = ...) -> None: ...
    INPUT_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    RANDOM_SEED_FIELD_NUMBER: _ClassVar[int]
    PARENT_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    STEP_IDS_FIELD_NUMBER: _ClassVar[int]
    HISTORY_LINEAGE_FIELD_NUMBER: _ClassVar[int]
    input: _containers.RepeatedCompositeFieldContainer[Payload]
    attempt: int
    started_at: _timestamp_pb2.Timestamp
    random_seed: bytes
    parent: RunRef
    headers: _containers.MessageMap[str, Payload]
    step_ids: StepIdScheme
    history_lineage: str
    def __init__(self, input: _Optional[_Iterable[_Union[Payload, _Mapping]]] = ..., attempt: _Optional[int] = ..., started_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., random_seed: _Optional[bytes] = ..., parent: _Optional[_Union[RunRef, _Mapping]] = ..., headers: _Optional[_Mapping[str, Payload]] = ..., step_ids: _Optional[_Union[StepIdScheme, str]] = ..., history_lineage: _Optional[str] = ...) -> None: ...

class StepSettled(_message.Message):
    __slots__ = ["outcome"]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    outcome: Outcome
    def __init__(self, outcome: _Optional[_Union[Outcome, _Mapping]] = ...) -> None: ...

class ChildStarted(_message.Message):
    __slots__ = ["child_workflow_id", "child_run_id"]
    CHILD_WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    CHILD_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    child_workflow_id: str
    child_run_id: str
    def __init__(self, child_workflow_id: _Optional[str] = ..., child_run_id: _Optional[str] = ...) -> None: ...

class ChildSettled(_message.Message):
    __slots__ = ["child_workflow_id", "child_run_id", "outcome"]
    CHILD_WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    CHILD_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    child_workflow_id: str
    child_run_id: str
    outcome: Outcome
    def __init__(self, child_workflow_id: _Optional[str] = ..., child_run_id: _Optional[str] = ..., outcome: _Optional[_Union[Outcome, _Mapping]] = ...) -> None: ...

class TimerElapsed(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class EventArrived(_message.Message):
    __slots__ = ["event_id", "name", "payload"]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    name: str
    payload: Payload
    def __init__(self, event_id: _Optional[str] = ..., name: _Optional[str] = ..., payload: _Optional[_Union[Payload, _Mapping]] = ...) -> None: ...

class UpdateArrived(_message.Message):
    __slots__ = ["update_id", "name", "args"]
    UPDATE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    update_id: str
    name: str
    args: _containers.RepeatedCompositeFieldContainer[Payload]
    def __init__(self, update_id: _Optional[str] = ..., name: _Optional[str] = ..., args: _Optional[_Iterable[_Union[Payload, _Mapping]]] = ...) -> None: ...

class CancelAsked(_message.Message):
    __slots__ = ["reason", "requested_at"]
    REASON_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_AT_FIELD_NUMBER: _ClassVar[int]
    reason: str
    requested_at: _timestamp_pb2.Timestamp
    def __init__(self, reason: _Optional[str] = ..., requested_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class Outcome(_message.Message):
    __slots__ = ["value", "failure", "end", "attempts"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    value: Payload
    failure: Failure
    end: End
    attempts: int
    def __init__(self, value: _Optional[_Union[Payload, _Mapping]] = ..., failure: _Optional[_Union[Failure, _Mapping]] = ..., end: _Optional[_Union[End, str]] = ..., attempts: _Optional[int] = ...) -> None: ...

class QueryAsked(_message.Message):
    __slots__ = ["query_id", "name", "args"]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    query_id: str
    name: str
    args: _containers.RepeatedCompositeFieldContainer[Payload]
    def __init__(self, query_id: _Optional[str] = ..., name: _Optional[str] = ..., args: _Optional[_Iterable[_Union[Payload, _Mapping]]] = ...) -> None: ...

class RunActivationResult(_message.Message):
    __slots__ = ["activation_id", "commands", "workflow_failed", "protocol_error", "queries", "updates", "calls_before_failure"]
    ACTIVATION_ID_FIELD_NUMBER: _ClassVar[int]
    COMMANDS_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FAILED_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_ERROR_FIELD_NUMBER: _ClassVar[int]
    QUERIES_FIELD_NUMBER: _ClassVar[int]
    UPDATES_FIELD_NUMBER: _ClassVar[int]
    CALLS_BEFORE_FAILURE_FIELD_NUMBER: _ClassVar[int]
    activation_id: bytes
    commands: Commands
    workflow_failed: Failure
    protocol_error: ProtocolError
    queries: _containers.RepeatedCompositeFieldContainer[QueryAnswer]
    updates: _containers.RepeatedCompositeFieldContainer[UpdateAnswer]
    calls_before_failure: Commands
    def __init__(self, activation_id: _Optional[bytes] = ..., commands: _Optional[_Union[Commands, _Mapping]] = ..., workflow_failed: _Optional[_Union[Failure, _Mapping]] = ..., protocol_error: _Optional[_Union[ProtocolError, _Mapping]] = ..., queries: _Optional[_Iterable[_Union[QueryAnswer, _Mapping]]] = ..., updates: _Optional[_Iterable[_Union[UpdateAnswer, _Mapping]]] = ..., calls_before_failure: _Optional[_Union[Commands, _Mapping]] = ...) -> None: ...

class Commands(_message.Message):
    __slots__ = ["commands"]
    COMMANDS_FIELD_NUMBER: _ClassVar[int]
    commands: _containers.RepeatedCompositeFieldContainer[Command]
    def __init__(self, commands: _Optional[_Iterable[_Union[Command, _Mapping]]] = ...) -> None: ...

class Command(_message.Message):
    __slots__ = ["seq", "satisfied", "step_id", "run_task", "start_timer", "cancel_timer", "start_child", "cancel_child", "send_event", "record_value", "await_event", "complete_run", "fail_run", "end_cancelled", "restart_fresh", "upsert_labels"]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    SATISFIED_FIELD_NUMBER: _ClassVar[int]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_TASK_FIELD_NUMBER: _ClassVar[int]
    START_TIMER_FIELD_NUMBER: _ClassVar[int]
    CANCEL_TIMER_FIELD_NUMBER: _ClassVar[int]
    START_CHILD_FIELD_NUMBER: _ClassVar[int]
    CANCEL_CHILD_FIELD_NUMBER: _ClassVar[int]
    SEND_EVENT_FIELD_NUMBER: _ClassVar[int]
    RECORD_VALUE_FIELD_NUMBER: _ClassVar[int]
    AWAIT_EVENT_FIELD_NUMBER: _ClassVar[int]
    COMPLETE_RUN_FIELD_NUMBER: _ClassVar[int]
    FAIL_RUN_FIELD_NUMBER: _ClassVar[int]
    END_CANCELLED_FIELD_NUMBER: _ClassVar[int]
    RESTART_FRESH_FIELD_NUMBER: _ClassVar[int]
    UPSERT_LABELS_FIELD_NUMBER: _ClassVar[int]
    seq: int
    satisfied: bool
    step_id: str
    run_task: RunTask
    start_timer: StartTimer
    cancel_timer: CancelTimer
    start_child: StartChild
    cancel_child: CancelChild
    send_event: SendEvent
    record_value: RecordValue
    await_event: AwaitEvent
    complete_run: CompleteRun
    fail_run: FailRun
    end_cancelled: EndCancelled
    restart_fresh: RestartFresh
    upsert_labels: UpsertLabels
    def __init__(self, seq: _Optional[int] = ..., satisfied: bool = ..., step_id: _Optional[str] = ..., run_task: _Optional[_Union[RunTask, _Mapping]] = ..., start_timer: _Optional[_Union[StartTimer, _Mapping]] = ..., cancel_timer: _Optional[_Union[CancelTimer, _Mapping]] = ..., start_child: _Optional[_Union[StartChild, _Mapping]] = ..., cancel_child: _Optional[_Union[CancelChild, _Mapping]] = ..., send_event: _Optional[_Union[SendEvent, _Mapping]] = ..., record_value: _Optional[_Union[RecordValue, _Mapping]] = ..., await_event: _Optional[_Union[AwaitEvent, _Mapping]] = ..., complete_run: _Optional[_Union[CompleteRun, _Mapping]] = ..., fail_run: _Optional[_Union[FailRun, _Mapping]] = ..., end_cancelled: _Optional[_Union[EndCancelled, _Mapping]] = ..., restart_fresh: _Optional[_Union[RestartFresh, _Mapping]] = ..., upsert_labels: _Optional[_Union[UpsertLabels, _Mapping]] = ...) -> None: ...

class RunTask(_message.Message):
    __slots__ = ["task_type", "task_queue", "input", "queue_timeout", "attempt_timeout", "heartbeat_timeout", "retry_policy", "headers", "priority", "cost"]
    class HeadersEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Payload, _Mapping]] = ...) -> None: ...
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    TASK_QUEUE_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    QUEUE_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    COST_FIELD_NUMBER: _ClassVar[int]
    task_type: str
    task_queue: str
    input: _containers.RepeatedCompositeFieldContainer[Payload]
    queue_timeout: _duration_pb2.Duration
    attempt_timeout: _duration_pb2.Duration
    heartbeat_timeout: _duration_pb2.Duration
    retry_policy: RetryPolicy
    headers: _containers.MessageMap[str, Payload]
    priority: int
    cost: int
    def __init__(self, task_type: _Optional[str] = ..., task_queue: _Optional[str] = ..., input: _Optional[_Iterable[_Union[Payload, _Mapping]]] = ..., queue_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., attempt_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., heartbeat_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., retry_policy: _Optional[_Union[RetryPolicy, _Mapping]] = ..., headers: _Optional[_Mapping[str, Payload]] = ..., priority: _Optional[int] = ..., cost: _Optional[int] = ...) -> None: ...

class StartTimer(_message.Message):
    __slots__ = ["duration"]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    duration: _duration_pb2.Duration
    def __init__(self, duration: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class CancelTimer(_message.Message):
    __slots__ = ["step_id"]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    step_id: str
    def __init__(self, step_id: _Optional[str] = ...) -> None: ...

class StartChild(_message.Message):
    __slots__ = ["workflow_id", "workflow_type", "task_queue", "namespace", "input", "execution_timeout", "orphan_policy", "retry_policy", "headers", "priority"]
    class HeadersEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Payload, _Mapping]] = ...) -> None: ...
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    TASK_QUEUE_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    ORPHAN_POLICY_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    workflow_type: str
    task_queue: str
    namespace: str
    input: _containers.RepeatedCompositeFieldContainer[Payload]
    execution_timeout: _duration_pb2.Duration
    orphan_policy: OrphanPolicy
    retry_policy: RetryPolicy
    headers: _containers.MessageMap[str, Payload]
    priority: int
    def __init__(self, workflow_id: _Optional[str] = ..., workflow_type: _Optional[str] = ..., task_queue: _Optional[str] = ..., namespace: _Optional[str] = ..., input: _Optional[_Iterable[_Union[Payload, _Mapping]]] = ..., execution_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., orphan_policy: _Optional[_Union[OrphanPolicy, str]] = ..., retry_policy: _Optional[_Union[RetryPolicy, _Mapping]] = ..., headers: _Optional[_Mapping[str, Payload]] = ..., priority: _Optional[int] = ...) -> None: ...

class CancelChild(_message.Message):
    __slots__ = ["step_id"]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    step_id: str
    def __init__(self, step_id: _Optional[str] = ...) -> None: ...

class SendEvent(_message.Message):
    __slots__ = ["workflow_id", "run_id", "name", "payload", "headers"]
    class HeadersEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Payload
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Payload, _Mapping]] = ...) -> None: ...
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    run_id: str
    name: str
    payload: Payload
    headers: _containers.MessageMap[str, Payload]
    def __init__(self, workflow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., name: _Optional[str] = ..., payload: _Optional[_Union[Payload, _Mapping]] = ..., headers: _Optional[_Mapping[str, Payload]] = ...) -> None: ...

class RecordValue(_message.Message):
    __slots__ = ["outcome"]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    outcome: Outcome
    def __init__(self, outcome: _Optional[_Union[Outcome, _Mapping]] = ...) -> None: ...

class AwaitEvent(_message.Message):
    __slots__ = ["name", "timeout"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    name: str
    timeout: _duration_pb2.Duration
    def __init__(self, name: _Optional[str] = ..., timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class CompleteRun(_message.Message):
    __slots__ = ["result"]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: Payload
    def __init__(self, result: _Optional[_Union[Payload, _Mapping]] = ...) -> None: ...

class FailRun(_message.Message):
    __slots__ = ["failure"]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    failure: Failure
    def __init__(self, failure: _Optional[_Union[Failure, _Mapping]] = ...) -> None: ...

class EndCancelled(_message.Message):
    __slots__ = ["details"]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    details: str
    def __init__(self, details: _Optional[str] = ...) -> None: ...

class RestartFresh(_message.Message):
    __slots__ = ["workflow_type", "task_queue", "input", "execution_timeout", "retry_policy"]
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    TASK_QUEUE_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    workflow_type: str
    task_queue: str
    input: _containers.RepeatedCompositeFieldContainer[Payload]
    execution_timeout: _duration_pb2.Duration
    retry_policy: RetryPolicy
    def __init__(self, workflow_type: _Optional[str] = ..., task_queue: _Optional[str] = ..., input: _Optional[_Iterable[_Union[Payload, _Mapping]]] = ..., execution_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., retry_policy: _Optional[_Union[RetryPolicy, _Mapping]] = ...) -> None: ...

class UpsertLabels(_message.Message):
    __slots__ = ["labels"]
    class LabelsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    LABELS_FIELD_NUMBER: _ClassVar[int]
    labels: _containers.ScalarMap[str, str]
    def __init__(self, labels: _Optional[_Mapping[str, str]] = ...) -> None: ...

class QueryAnswer(_message.Message):
    __slots__ = ["query_id", "value", "failure"]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    query_id: str
    value: Payload
    failure: Failure
    def __init__(self, query_id: _Optional[str] = ..., value: _Optional[_Union[Payload, _Mapping]] = ..., failure: _Optional[_Union[Failure, _Mapping]] = ...) -> None: ...

class UpdateAnswer(_message.Message):
    __slots__ = ["update_id", "value", "rejected", "failed"]
    UPDATE_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    REJECTED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    update_id: str
    value: Payload
    rejected: Failure
    failed: Failure
    def __init__(self, update_id: _Optional[str] = ..., value: _Optional[_Union[Payload, _Mapping]] = ..., rejected: _Optional[_Union[Failure, _Mapping]] = ..., failed: _Optional[_Union[Failure, _Mapping]] = ...) -> None: ...

class ProtocolError(_message.Message):
    __slots__ = ["message", "kind"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    message: str
    kind: ProtocolErrorKind
    def __init__(self, message: _Optional[str] = ..., kind: _Optional[_Union[ProtocolErrorKind, str]] = ...) -> None: ...

class DeterminismViolation(_message.Message):
    __slots__ = ["run", "seq", "step_id", "field", "expected", "actual"]
    RUN_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_FIELD_NUMBER: _ClassVar[int]
    run: RunRef
    seq: int
    step_id: str
    field: str
    expected: str
    actual: str
    def __init__(self, run: _Optional[_Union[RunRef, _Mapping]] = ..., seq: _Optional[int] = ..., step_id: _Optional[str] = ..., field: _Optional[str] = ..., expected: _Optional[str] = ..., actual: _Optional[str] = ...) -> None: ...

class Pressure(_message.Message):
    __slots__ = ["loop_delay_ms", "queue_depth"]
    LOOP_DELAY_MS_FIELD_NUMBER: _ClassVar[int]
    QUEUE_DEPTH_FIELD_NUMBER: _ClassVar[int]
    loop_delay_ms: int
    queue_depth: int
    def __init__(self, loop_delay_ms: _Optional[int] = ..., queue_depth: _Optional[int] = ...) -> None: ...
