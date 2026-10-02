"""Client API for ORCHER Python SDK.

This module provides the Client class for starting, querying, and managing
workflows from external applications.

Example:
    >>> from orcher.client import Client, ClientConfig
    >>>
    >>> async with Client(ClientConfig(server_url="http://localhost:50051")) as client:
    ...     handle = await client.start_workflow(
    ...         workflow_type="greeting-workflow",
    ...         workflow_id="greeting-1",
    ...         task_queue="greetings",
    ...         args=("World",),
    ...     )
    ...     result = await handle.result()
    ...     print(result)
"""

from orcher.client.client import Client
from orcher.client.config import ClientConfig
from orcher.client.workflow_handle import WorkflowHandle

__all__ = [
    "ClientConfig",
    "Client",
    "WorkflowHandle",
]
