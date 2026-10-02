"""
Testing Utilities for ORCHER Python SDK

This module provides utilities for testing workflows and tasks in-memory
without requiring a running ORCHER server.

Features:
- TestWorkflowEnvironment for isolated workflow testing
- Task mocking with fluent API
- Time control for deterministic testing
- Execution tracing and assertions

Example:
    >>> from orcher.testing import TestWorkflowEnvironment
    >>> from myapp.workflows import OrderWorkflow
    >>>
    >>> async def test_order_workflow():
    ...     env = TestWorkflowEnvironment()
    ...
    ...     # Mock task responses
    ...     env.mock_task("charge_card").returns({"charge_id": "ch_123"})
    ...     env.mock_task("reserve_inventory").returns({"reserved": True})
    ...
    ...     # Execute workflow
    ...     result = await env.execute_workflow(
    ...         OrderWorkflow,
    ...         {"order_id": "order-123", "amount": 99.99}
    ...     )
    ...
    ...     # Assertions
    ...     assert result["status"] == "completed"
    ...     env.assert_task_called("charge_card", times=1)
"""

from orcher.testing.builders import (
    ExecutionTraceBuilder,
    TaskMockBuilder,
    WorkflowExecutionBuilder,
)
from orcher.testing.environment import TestWorkflowEnvironment
from orcher.testing.mocks import MockTaskBuilder, MockTaskRegistry
from orcher.testing.time_controller import TimeController
from orcher.testing.types import (
    ExecutionStatus,
    ExecutionTrace,
    TaskCall,
    TaskExecution,
    TestEnvOptions,
    TestEnvStats,
    TestWorkflowOptions,
    WorkflowExecutionOptions,
    WorkflowTestEnvOptions,
)

__all__ = [
    # Main entry point
    "TestWorkflowEnvironment",
    # Mocking
    "MockTaskBuilder",
    "MockTaskRegistry",
    # Time control
    "TimeController",
    # Builders
    "WorkflowExecutionBuilder",
    "TaskMockBuilder",
    "ExecutionTraceBuilder",
    # Types
    "ExecutionStatus",
    "ExecutionTrace",
    "TaskExecution",
    "TaskCall",
    "WorkflowTestEnvOptions",
    "WorkflowExecutionOptions",
    "TestEnvStats",
    # Aliases
    "TestEnvOptions",
    "TestWorkflowOptions",
]
