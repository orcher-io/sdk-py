from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ErrorCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    ERROR_CODE_UNSPECIFIED: _ClassVar[ErrorCode]
    ERROR_CODE_WORKFLOW_NOT_FOUND: _ClassVar[ErrorCode]
    ERROR_CODE_WORKFLOW_ALREADY_EXISTS: _ClassVar[ErrorCode]
    ERROR_CODE_WORKFLOW_EXECUTION_FAILED: _ClassVar[ErrorCode]
    ERROR_CODE_WORKFLOW_CANCELED: _ClassVar[ErrorCode]
    ERROR_CODE_WORKFLOW_TERMINATED: _ClassVar[ErrorCode]
    ERROR_CODE_WORKFLOW_TIMEOUT: _ClassVar[ErrorCode]
    ERROR_CODE_TIMEOUT: _ClassVar[ErrorCode]
    ERROR_CODE_CONNECTION_FAILED: _ClassVar[ErrorCode]
    ERROR_CODE_UNAUTHENTICATED: _ClassVar[ErrorCode]
    ERROR_CODE_PERMISSION_DENIED: _ClassVar[ErrorCode]
    ERROR_CODE_INVALID_ARGUMENT: _ClassVar[ErrorCode]
    ERROR_CODE_NOT_FOUND: _ClassVar[ErrorCode]
    ERROR_CODE_ALREADY_EXISTS: _ClassVar[ErrorCode]
    ERROR_CODE_UNAVAILABLE: _ClassVar[ErrorCode]
    ERROR_CODE_RESOURCE_EXHAUSTED: _ClassVar[ErrorCode]
    ERROR_CODE_FAILED_PRECONDITION: _ClassVar[ErrorCode]
    ERROR_CODE_ABORTED: _ClassVar[ErrorCode]
    ERROR_CODE_OUT_OF_RANGE: _ClassVar[ErrorCode]
    ERROR_CODE_UNIMPLEMENTED: _ClassVar[ErrorCode]
    ERROR_CODE_DATA_LOSS: _ClassVar[ErrorCode]
    ERROR_CODE_INTERNAL: _ClassVar[ErrorCode]
    ERROR_CODE_UNKNOWN: _ClassVar[ErrorCode]

class WorkflowStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    WORKFLOW_STATUS_UNSPECIFIED: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_RUNNING: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_COMPLETED: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_FAILED: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_CANCELLED: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_TERMINATED: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_TIMED_OUT: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_RESTARTED_FRESH: _ClassVar[WorkflowStatus]

class IdReusePolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    ID_REUSE_POLICY_UNSPECIFIED: _ClassVar[IdReusePolicy]
    ID_REUSE_POLICY_ALLOW_DUPLICATE: _ClassVar[IdReusePolicy]
    ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY: _ClassVar[IdReusePolicy]
    ID_REUSE_POLICY_REJECT_DUPLICATE: _ClassVar[IdReusePolicy]
    ID_REUSE_POLICY_TERMINATE_IF_RUNNING: _ClassVar[IdReusePolicy]

class ListOrder(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    LIST_ORDER_UNSPECIFIED: _ClassVar[ListOrder]
    LIST_ORDER_START_TIME_ASC: _ClassVar[ListOrder]
    LIST_ORDER_START_TIME_DESC: _ClassVar[ListOrder]
    LIST_ORDER_CLOSE_TIME_ASC: _ClassVar[ListOrder]
    LIST_ORDER_CLOSE_TIME_DESC: _ClassVar[ListOrder]
ERROR_CODE_UNSPECIFIED: ErrorCode
ERROR_CODE_WORKFLOW_NOT_FOUND: ErrorCode
ERROR_CODE_WORKFLOW_ALREADY_EXISTS: ErrorCode
ERROR_CODE_WORKFLOW_EXECUTION_FAILED: ErrorCode
ERROR_CODE_WORKFLOW_CANCELED: ErrorCode
ERROR_CODE_WORKFLOW_TERMINATED: ErrorCode
ERROR_CODE_WORKFLOW_TIMEOUT: ErrorCode
ERROR_CODE_TIMEOUT: ErrorCode
ERROR_CODE_CONNECTION_FAILED: ErrorCode
ERROR_CODE_UNAUTHENTICATED: ErrorCode
ERROR_CODE_PERMISSION_DENIED: ErrorCode
ERROR_CODE_INVALID_ARGUMENT: ErrorCode
ERROR_CODE_NOT_FOUND: ErrorCode
ERROR_CODE_ALREADY_EXISTS: ErrorCode
ERROR_CODE_UNAVAILABLE: ErrorCode
ERROR_CODE_RESOURCE_EXHAUSTED: ErrorCode
ERROR_CODE_FAILED_PRECONDITION: ErrorCode
ERROR_CODE_ABORTED: ErrorCode
ERROR_CODE_OUT_OF_RANGE: ErrorCode
ERROR_CODE_UNIMPLEMENTED: ErrorCode
ERROR_CODE_DATA_LOSS: ErrorCode
ERROR_CODE_INTERNAL: ErrorCode
ERROR_CODE_UNKNOWN: ErrorCode
WORKFLOW_STATUS_UNSPECIFIED: WorkflowStatus
WORKFLOW_STATUS_RUNNING: WorkflowStatus
WORKFLOW_STATUS_COMPLETED: WorkflowStatus
WORKFLOW_STATUS_FAILED: WorkflowStatus
WORKFLOW_STATUS_CANCELLED: WorkflowStatus
WORKFLOW_STATUS_TERMINATED: WorkflowStatus
WORKFLOW_STATUS_TIMED_OUT: WorkflowStatus
WORKFLOW_STATUS_RESTARTED_FRESH: WorkflowStatus
ID_REUSE_POLICY_UNSPECIFIED: IdReusePolicy
ID_REUSE_POLICY_ALLOW_DUPLICATE: IdReusePolicy
ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY: IdReusePolicy
ID_REUSE_POLICY_REJECT_DUPLICATE: IdReusePolicy
ID_REUSE_POLICY_TERMINATE_IF_RUNNING: IdReusePolicy
LIST_ORDER_UNSPECIFIED: ListOrder
LIST_ORDER_START_TIME_ASC: ListOrder
LIST_ORDER_START_TIME_DESC: ListOrder
LIST_ORDER_CLOSE_TIME_ASC: ListOrder
LIST_ORDER_CLOSE_TIME_DESC: ListOrder

class ClientCall(_message.Message):
    __slots__ = ["start_workflow", "wait_for_result", "get_status", "describe", "list_workflows", "search_workflows", "send_event", "query", "update", "cancel", "terminate", "reset", "invoke_actor", "create_namespace", "get_namespace", "list_namespaces", "update_namespace", "deprecate_namespace", "delete_namespace", "namespace", "timeout"]
    START_WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    WAIT_FOR_RESULT_FIELD_NUMBER: _ClassVar[int]
    GET_STATUS_FIELD_NUMBER: _ClassVar[int]
    DESCRIBE_FIELD_NUMBER: _ClassVar[int]
    LIST_WORKFLOWS_FIELD_NUMBER: _ClassVar[int]
    SEARCH_WORKFLOWS_FIELD_NUMBER: _ClassVar[int]
    SEND_EVENT_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    UPDATE_FIELD_NUMBER: _ClassVar[int]
    CANCEL_FIELD_NUMBER: _ClassVar[int]
    TERMINATE_FIELD_NUMBER: _ClassVar[int]
    RESET_FIELD_NUMBER: _ClassVar[int]
    INVOKE_ACTOR_FIELD_NUMBER: _ClassVar[int]
    CREATE_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GET_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    LIST_NAMESPACES_FIELD_NUMBER: _ClassVar[int]
    UPDATE_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    DEPRECATE_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    DELETE_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    start_workflow: StartWorkflow
    wait_for_result: WaitForResult
    get_status: GetStatus
    describe: Describe
    list_workflows: ListWorkflows
    search_workflows: SearchWorkflows
    send_event: SendEvent
    query: Query
    update: Update
    cancel: Cancel
    terminate: Terminate
    reset: Reset
    invoke_actor: InvokeActor
    create_namespace: CreateNamespace
    get_namespace: GetNamespace
    list_namespaces: ListNamespaces
    update_namespace: UpdateNamespace
    deprecate_namespace: DeprecateNamespace
    delete_namespace: DeleteNamespace
    namespace: str
    timeout: _duration_pb2.Duration
    def __init__(self, start_workflow: _Optional[_Union[StartWorkflow, _Mapping]] = ..., wait_for_result: _Optional[_Union[WaitForResult, _Mapping]] = ..., get_status: _Optional[_Union[GetStatus, _Mapping]] = ..., describe: _Optional[_Union[Describe, _Mapping]] = ..., list_workflows: _Optional[_Union[ListWorkflows, _Mapping]] = ..., search_workflows: _Optional[_Union[SearchWorkflows, _Mapping]] = ..., send_event: _Optional[_Union[SendEvent, _Mapping]] = ..., query: _Optional[_Union[Query, _Mapping]] = ..., update: _Optional[_Union[Update, _Mapping]] = ..., cancel: _Optional[_Union[Cancel, _Mapping]] = ..., terminate: _Optional[_Union[Terminate, _Mapping]] = ..., reset: _Optional[_Union[Reset, _Mapping]] = ..., invoke_actor: _Optional[_Union[InvokeActor, _Mapping]] = ..., create_namespace: _Optional[_Union[CreateNamespace, _Mapping]] = ..., get_namespace: _Optional[_Union[GetNamespace, _Mapping]] = ..., list_namespaces: _Optional[_Union[ListNamespaces, _Mapping]] = ..., update_namespace: _Optional[_Union[UpdateNamespace, _Mapping]] = ..., deprecate_namespace: _Optional[_Union[DeprecateNamespace, _Mapping]] = ..., delete_namespace: _Optional[_Union[DeleteNamespace, _Mapping]] = ..., namespace: _Optional[str] = ..., timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class ClientAnswer(_message.Message):
    __slots__ = ["error", "started", "result", "status", "description", "page", "value", "reset_to", "namespace", "namespaces", "done"]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    STARTED_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    RESET_TO_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    NAMESPACES_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    error: ClientError
    started: Started
    result: WorkflowResult
    status: WorkflowStatus
    description: WorkflowDescription
    page: WorkflowPage
    value: bytes
    reset_to: WorkflowRef
    namespace: Namespace
    namespaces: NamespacePage
    done: Done
    def __init__(self, error: _Optional[_Union[ClientError, _Mapping]] = ..., started: _Optional[_Union[Started, _Mapping]] = ..., result: _Optional[_Union[WorkflowResult, _Mapping]] = ..., status: _Optional[_Union[WorkflowStatus, str]] = ..., description: _Optional[_Union[WorkflowDescription, _Mapping]] = ..., page: _Optional[_Union[WorkflowPage, _Mapping]] = ..., value: _Optional[bytes] = ..., reset_to: _Optional[_Union[WorkflowRef, _Mapping]] = ..., namespace: _Optional[_Union[Namespace, _Mapping]] = ..., namespaces: _Optional[_Union[NamespacePage, _Mapping]] = ..., done: _Optional[_Union[Done, _Mapping]] = ...) -> None: ...

class Done(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ClientError(_message.Message):
    __slots__ = ["code", "message", "retryable", "run_id", "status"]
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    code: ErrorCode
    message: str
    retryable: bool
    run_id: str
    status: WorkflowStatus
    def __init__(self, code: _Optional[_Union[ErrorCode, str]] = ..., message: _Optional[str] = ..., retryable: bool = ..., run_id: _Optional[str] = ..., status: _Optional[_Union[WorkflowStatus, str]] = ...) -> None: ...

class WorkflowRef(_message.Message):
    __slots__ = ["workflow_id", "run_id"]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    run_id: str
    def __init__(self, workflow_id: _Optional[str] = ..., run_id: _Optional[str] = ...) -> None: ...

class RetryPolicy(_message.Message):
    __slots__ = ["max_attempts", "initial_interval", "max_interval", "backoff_coefficient", "non_retryable_errors"]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    INITIAL_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    MAX_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_COEFFICIENT_FIELD_NUMBER: _ClassVar[int]
    NON_RETRYABLE_ERRORS_FIELD_NUMBER: _ClassVar[int]
    max_attempts: int
    initial_interval: _duration_pb2.Duration
    max_interval: _duration_pb2.Duration
    backoff_coefficient: float
    non_retryable_errors: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, max_attempts: _Optional[int] = ..., initial_interval: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., max_interval: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., backoff_coefficient: _Optional[float] = ..., non_retryable_errors: _Optional[_Iterable[str]] = ...) -> None: ...

class StartWorkflow(_message.Message):
    __slots__ = ["workflow_id", "workflow_type", "task_queue", "input", "execution_timeout", "run_timeout", "task_timeout", "cron_schedule", "retry_policy", "id_reuse_policy"]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    TASK_QUEUE_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    RUN_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    TASK_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    CRON_SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    ID_REUSE_POLICY_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    workflow_type: str
    task_queue: str
    input: bytes
    execution_timeout: _duration_pb2.Duration
    run_timeout: _duration_pb2.Duration
    task_timeout: _duration_pb2.Duration
    cron_schedule: str
    retry_policy: RetryPolicy
    id_reuse_policy: IdReusePolicy
    def __init__(self, workflow_id: _Optional[str] = ..., workflow_type: _Optional[str] = ..., task_queue: _Optional[str] = ..., input: _Optional[bytes] = ..., execution_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., run_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., task_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., cron_schedule: _Optional[str] = ..., retry_policy: _Optional[_Union[RetryPolicy, _Mapping]] = ..., id_reuse_policy: _Optional[_Union[IdReusePolicy, str]] = ...) -> None: ...

class Started(_message.Message):
    __slots__ = ["workflow"]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    workflow: WorkflowRef
    def __init__(self, workflow: _Optional[_Union[WorkflowRef, _Mapping]] = ...) -> None: ...

class WaitForResult(_message.Message):
    __slots__ = ["workflow", "wait"]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    WAIT_FIELD_NUMBER: _ClassVar[int]
    workflow: WorkflowRef
    wait: _duration_pb2.Duration
    def __init__(self, workflow: _Optional[_Union[WorkflowRef, _Mapping]] = ..., wait: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class WorkflowResult(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class GetStatus(_message.Message):
    __slots__ = ["workflow"]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    workflow: WorkflowRef
    def __init__(self, workflow: _Optional[_Union[WorkflowRef, _Mapping]] = ...) -> None: ...

class Describe(_message.Message):
    __slots__ = ["workflow"]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    workflow: WorkflowRef
    def __init__(self, workflow: _Optional[_Union[WorkflowRef, _Mapping]] = ...) -> None: ...

class WorkflowSummary(_message.Message):
    __slots__ = ["workflow", "workflow_type", "task_queue", "namespace", "status", "start_time", "close_time", "execution_duration", "journal_length", "parent", "labels", "annotations", "tags", "started_by", "attempt", "cron_schedule", "state_transition_count"]
    class LabelsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class AnnotationsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    TASK_QUEUE_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    CLOSE_TIME_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_DURATION_FIELD_NUMBER: _ClassVar[int]
    JOURNAL_LENGTH_FIELD_NUMBER: _ClassVar[int]
    PARENT_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    ANNOTATIONS_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    STARTED_BY_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    CRON_SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    STATE_TRANSITION_COUNT_FIELD_NUMBER: _ClassVar[int]
    workflow: WorkflowRef
    workflow_type: str
    task_queue: str
    namespace: str
    status: WorkflowStatus
    start_time: _timestamp_pb2.Timestamp
    close_time: _timestamp_pb2.Timestamp
    execution_duration: _duration_pb2.Duration
    journal_length: int
    parent: ParentRef
    labels: _containers.ScalarMap[str, str]
    annotations: _containers.ScalarMap[str, str]
    tags: _containers.RepeatedScalarFieldContainer[str]
    started_by: str
    attempt: int
    cron_schedule: str
    state_transition_count: int
    def __init__(self, workflow: _Optional[_Union[WorkflowRef, _Mapping]] = ..., workflow_type: _Optional[str] = ..., task_queue: _Optional[str] = ..., namespace: _Optional[str] = ..., status: _Optional[_Union[WorkflowStatus, str]] = ..., start_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., close_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., execution_duration: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., journal_length: _Optional[int] = ..., parent: _Optional[_Union[ParentRef, _Mapping]] = ..., labels: _Optional[_Mapping[str, str]] = ..., annotations: _Optional[_Mapping[str, str]] = ..., tags: _Optional[_Iterable[str]] = ..., started_by: _Optional[str] = ..., attempt: _Optional[int] = ..., cron_schedule: _Optional[str] = ..., state_transition_count: _Optional[int] = ...) -> None: ...

class ParentRef(_message.Message):
    __slots__ = ["namespace", "workflow"]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    workflow: WorkflowRef
    def __init__(self, namespace: _Optional[str] = ..., workflow: _Optional[_Union[WorkflowRef, _Mapping]] = ...) -> None: ...

class WorkflowDescription(_message.Message):
    __slots__ = ["summary", "pending_tasks", "pending_timers", "pending_events", "config"]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    PENDING_TASKS_FIELD_NUMBER: _ClassVar[int]
    PENDING_TIMERS_FIELD_NUMBER: _ClassVar[int]
    PENDING_EVENTS_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    summary: WorkflowSummary
    pending_tasks: int
    pending_timers: int
    pending_events: int
    config: WorkflowConfig
    def __init__(self, summary: _Optional[_Union[WorkflowSummary, _Mapping]] = ..., pending_tasks: _Optional[int] = ..., pending_timers: _Optional[int] = ..., pending_events: _Optional[int] = ..., config: _Optional[_Union[WorkflowConfig, _Mapping]] = ...) -> None: ...

class WorkflowConfig(_message.Message):
    __slots__ = ["task_queue", "execution_timeout", "run_timeout", "task_timeout", "retry_policy", "cron_schedule"]
    TASK_QUEUE_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    RUN_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    TASK_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    CRON_SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    task_queue: str
    execution_timeout: _duration_pb2.Duration
    run_timeout: _duration_pb2.Duration
    task_timeout: _duration_pb2.Duration
    retry_policy: RetryPolicy
    cron_schedule: str
    def __init__(self, task_queue: _Optional[str] = ..., execution_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., run_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., task_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., retry_policy: _Optional[_Union[RetryPolicy, _Mapping]] = ..., cron_schedule: _Optional[str] = ...) -> None: ...

class ListWorkflows(_message.Message):
    __slots__ = ["page_size", "page_token", "workflow_type", "task_queue", "statuses", "order"]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    TASK_QUEUE_FIELD_NUMBER: _ClassVar[int]
    STATUSES_FIELD_NUMBER: _ClassVar[int]
    ORDER_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_token: bytes
    workflow_type: str
    task_queue: str
    statuses: _containers.RepeatedScalarFieldContainer[WorkflowStatus]
    order: ListOrder
    def __init__(self, page_size: _Optional[int] = ..., page_token: _Optional[bytes] = ..., workflow_type: _Optional[str] = ..., task_queue: _Optional[str] = ..., statuses: _Optional[_Iterable[_Union[WorkflowStatus, str]]] = ..., order: _Optional[_Union[ListOrder, str]] = ...) -> None: ...

class SearchWorkflows(_message.Message):
    __slots__ = ["query", "page_size", "page_token"]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    query: str
    page_size: int
    page_token: bytes
    def __init__(self, query: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[bytes] = ...) -> None: ...

class WorkflowPage(_message.Message):
    __slots__ = ["workflows", "next_page_token"]
    WORKFLOWS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    workflows: _containers.RepeatedCompositeFieldContainer[WorkflowSummary]
    next_page_token: bytes
    def __init__(self, workflows: _Optional[_Iterable[_Union[WorkflowSummary, _Mapping]]] = ..., next_page_token: _Optional[bytes] = ...) -> None: ...

class SendEvent(_message.Message):
    __slots__ = ["workflow", "event_name", "payload"]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    EVENT_NAME_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    workflow: WorkflowRef
    event_name: str
    payload: bytes
    def __init__(self, workflow: _Optional[_Union[WorkflowRef, _Mapping]] = ..., event_name: _Optional[str] = ..., payload: _Optional[bytes] = ...) -> None: ...

class Query(_message.Message):
    __slots__ = ["workflow", "query_name", "args"]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    QUERY_NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    workflow: WorkflowRef
    query_name: str
    args: bytes
    def __init__(self, workflow: _Optional[_Union[WorkflowRef, _Mapping]] = ..., query_name: _Optional[str] = ..., args: _Optional[bytes] = ...) -> None: ...

class Update(_message.Message):
    __slots__ = ["workflow", "update_name", "args"]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    UPDATE_NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    workflow: WorkflowRef
    update_name: str
    args: bytes
    def __init__(self, workflow: _Optional[_Union[WorkflowRef, _Mapping]] = ..., update_name: _Optional[str] = ..., args: _Optional[bytes] = ...) -> None: ...

class Cancel(_message.Message):
    __slots__ = ["workflow", "cleanup_timeout"]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    CLEANUP_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    workflow: WorkflowRef
    cleanup_timeout: _duration_pb2.Duration
    def __init__(self, workflow: _Optional[_Union[WorkflowRef, _Mapping]] = ..., cleanup_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class Terminate(_message.Message):
    __slots__ = ["workflow", "reason"]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    workflow: WorkflowRef
    reason: str
    def __init__(self, workflow: _Optional[_Union[WorkflowRef, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class Reset(_message.Message):
    __slots__ = ["workflow", "event_id", "reason"]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    workflow: WorkflowRef
    event_id: int
    reason: str
    def __init__(self, workflow: _Optional[_Union[WorkflowRef, _Mapping]] = ..., event_id: _Optional[int] = ..., reason: _Optional[str] = ...) -> None: ...

class InvokeActor(_message.Message):
    __slots__ = ["actor_name", "key", "operation", "payload", "operation_timeout", "idempotency_key"]
    ACTOR_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    OPERATION_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    actor_name: str
    key: str
    operation: str
    payload: bytes
    operation_timeout: _duration_pb2.Duration
    idempotency_key: str
    def __init__(self, actor_name: _Optional[str] = ..., key: _Optional[str] = ..., operation: _Optional[str] = ..., payload: _Optional[bytes] = ..., operation_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class Namespace(_message.Message):
    __slots__ = ["name", "status", "description", "owner_email", "retention_period_days", "data", "created_at", "updated_at"]
    class DataEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    OWNER_EMAIL_FIELD_NUMBER: _ClassVar[int]
    RETENTION_PERIOD_DAYS_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    name: str
    status: str
    description: str
    owner_email: str
    retention_period_days: int
    data: _containers.ScalarMap[str, str]
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, name: _Optional[str] = ..., status: _Optional[str] = ..., description: _Optional[str] = ..., owner_email: _Optional[str] = ..., retention_period_days: _Optional[int] = ..., data: _Optional[_Mapping[str, str]] = ..., created_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class CreateNamespace(_message.Message):
    __slots__ = ["name", "description", "retention_period_days", "owner_email"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    RETENTION_PERIOD_DAYS_FIELD_NUMBER: _ClassVar[int]
    OWNER_EMAIL_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    retention_period_days: int
    owner_email: str
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., retention_period_days: _Optional[int] = ..., owner_email: _Optional[str] = ...) -> None: ...

class GetNamespace(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ListNamespaces(_message.Message):
    __slots__ = ["page_size", "page_offset"]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_OFFSET_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_offset: int
    def __init__(self, page_size: _Optional[int] = ..., page_offset: _Optional[int] = ...) -> None: ...

class NamespacePage(_message.Message):
    __slots__ = ["namespaces", "total"]
    NAMESPACES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    namespaces: _containers.RepeatedCompositeFieldContainer[Namespace]
    total: int
    def __init__(self, namespaces: _Optional[_Iterable[_Union[Namespace, _Mapping]]] = ..., total: _Optional[int] = ...) -> None: ...

class UpdateNamespace(_message.Message):
    __slots__ = ["name", "description", "owner_email", "retention_period_days"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    OWNER_EMAIL_FIELD_NUMBER: _ClassVar[int]
    RETENTION_PERIOD_DAYS_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    owner_email: str
    retention_period_days: int
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., owner_email: _Optional[str] = ..., retention_period_days: _Optional[int] = ...) -> None: ...

class DeprecateNamespace(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class DeleteNamespace(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...
