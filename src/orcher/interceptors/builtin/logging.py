"""Logging interceptor for workflow and task executions."""

import logging
import time
from typing import Any

from orcher.interceptors.base import (
    ExecutionInfo,
    InterceptorContext,
    NextFn,
    TaskInterceptor,
    WorkflowInterceptor,
)


class LoggingConfig:
    """Configuration for logging interceptors."""

    def __init__(
        self,
        *,
        log_input: bool = True,
        log_output: bool = True,
        log_duration: bool = True,
        max_input_length: int = 500,
        max_output_length: int = 500,
        logger: logging.Logger | None = None,
    ) -> None:
        self.log_input = log_input
        self.log_output = log_output
        self.log_duration = log_duration
        self.max_input_length = max_input_length
        self.max_output_length = max_output_length
        self.logger = logger or logging.getLogger("orcher")


def _truncate(value: Any, max_length: int) -> str:
    """Stringify a value for logging, truncated to ``max_length`` characters."""
    s = str(value)
    if len(s) > max_length:
        return s[:max_length] + "..."
    return s


class WorkflowLoggingInterceptor(WorkflowInterceptor):
    """Interceptor that logs workflow execution."""

    def __init__(self, config: LoggingConfig | None = None) -> None:
        self._config = config or LoggingConfig()

    @property
    def order(self) -> int:
        return 50  # Framework level

    async def intercept_execute(
        self,
        context: InterceptorContext,
        input_data: Any,
        next_fn: NextFn,
    ) -> Any:
        logger = self._config.logger
        workflow_type = context.workflow_type
        workflow_id = context.workflow_id

        input_str = ""
        if self._config.log_input:
            input_str = f" input={_truncate(input_data, self._config.max_input_length)}"
        logger.info(f"Workflow started: {workflow_type} (id={workflow_id}){input_str}")

        start_time = time.perf_counter()
        try:
            result = await next_fn(input_data)

            duration_ms = (time.perf_counter() - start_time) * 1000
            output_str = ""
            duration_str = ""
            if self._config.log_output:
                output_str = f" output={_truncate(result, self._config.max_output_length)}"
            if self._config.log_duration:
                duration_str = f" duration={duration_ms:.2f}ms"
            logger.info(
                f"Workflow completed: {workflow_type} (id={workflow_id}){output_str}{duration_str}"
            )

            return result
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            duration_str = ""
            if self._config.log_duration:
                duration_str = f" duration={duration_ms:.2f}ms"
            logger.error(
                f"Workflow failed: {workflow_type} (id={workflow_id}) error={e}{duration_str}"
            )
            raise


class TaskLoggingInterceptor(TaskInterceptor):
    """Interceptor that logs task execution."""

    def __init__(self, config: LoggingConfig | None = None) -> None:
        self._config = config or LoggingConfig()

    @property
    def order(self) -> int:
        return 50  # Framework level

    async def intercept_execute(
        self,
        context: InterceptorContext,
        input_data: Any,
        next_fn: NextFn,
    ) -> Any:
        logger = self._config.logger
        task_name = context.task_name or "unknown"
        workflow_id = context.workflow_id

        input_str = ""
        if self._config.log_input:
            input_str = f" input={_truncate(input_data, self._config.max_input_length)}"
        logger.info(f"Task started: {task_name} (workflow={workflow_id}){input_str}")

        start_time = time.perf_counter()
        try:
            result = await next_fn(input_data)

            duration_ms = (time.perf_counter() - start_time) * 1000
            output_str = ""
            duration_str = ""
            if self._config.log_output:
                output_str = f" output={_truncate(result, self._config.max_output_length)}"
            if self._config.log_duration:
                duration_str = f" duration={duration_ms:.2f}ms"
            logger.info(
                f"Task completed: {task_name} (workflow={workflow_id}){output_str}{duration_str}"
            )

            return result
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            duration_str = ""
            if self._config.log_duration:
                duration_str = f" duration={duration_ms:.2f}ms"
            logger.error(
                f"Task failed: {task_name} (workflow={workflow_id}) error={e}{duration_str}"
            )
            raise

    async def on_retry(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
        attempt: int,
        max_attempts: int,
    ) -> None:
        """Log retry attempts."""
        logger = self._config.logger
        task_name = context.task_name or "unknown"
        logger.warning(
            f"Task retry: {task_name} (attempt {attempt}/{max_attempts}) error={info.error}"
        )


class LoggingInterceptor:
    """Factory for creating workflow and task logging interceptors.

    Both interceptors share one configuration.

    Example usage:
        logging_interceptor = LoggingInterceptor(log_input=True, log_output=True)
        service = (
            Worker.builder()
            .workflow_interceptor(logging_interceptor.workflow())
            .task_interceptor(logging_interceptor.task())
            .build()
        )
    """

    def __init__(
        self,
        *,
        log_input: bool = True,
        log_output: bool = True,
        log_duration: bool = True,
        max_input_length: int = 500,
        max_output_length: int = 500,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = LoggingConfig(
            log_input=log_input,
            log_output=log_output,
            log_duration=log_duration,
            max_input_length=max_input_length,
            max_output_length=max_output_length,
            logger=logger,
        )

    def workflow(self) -> WorkflowLoggingInterceptor:
        """Create a workflow logging interceptor."""
        return WorkflowLoggingInterceptor(self._config)

    def task(self) -> TaskLoggingInterceptor:
        """Create a task logging interceptor."""
        return TaskLoggingInterceptor(self._config)
