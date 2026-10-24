"""
Structured Logging Utility for Ecommerce Data Lakehouse
========================================================
Provides structured logging with pipeline stage tracking, metric logging,
and error context capture for observability across all pipeline components.
"""

import logging
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from functools import wraps


class PipelineLogger:
    """Structured logger with pipeline context awareness.

    Wraps Python's standard logging with structured JSON output for
    easy parsing by log aggregation tools (ELK, CloudWatch, etc.).

    Example:
        >>> logger = PipelineLogger("bronze-ingestion", layer="bronze")
        >>> logger.info("Processing batch", batch_id="b-001", record_count=50000)
        >>> with logger.stage("kafka-read"):
        ...     data = read_from_kafka()
    """

    def __init__(
        self,
        name: str,
        layer: Optional[str] = None,
        level: int = logging.INFO,
    ):
        """Initialize pipeline logger.

        Args:
            name: Logger name (typically the module or pipeline stage).
            layer: Medallion layer context ("bronze", "silver", "gold").
            level: Logging level.
        """
        self.name = name
        self.layer = layer
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)

        # Avoid duplicate handlers on re-initialization
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(message)s"))
            self._logger.addHandler(handler)

    def _format_message(
        self, level: str, message: str, **kwargs: Any
    ) -> str:
        """Format a structured log entry as JSON.

        Args:
            level: Log level string.
            message: Human-readable message.
            **kwargs: Additional structured fields.

        Returns:
            JSON-formatted log string.
        """
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "logger": self.name,
            "level": level,
            "message": message,
        }
        if self.layer:
            entry["layer"] = self.layer
        entry.update(kwargs)
        return json.dumps(entry)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log an info message with structured context."""
        self._logger.info(self._format_message("INFO", message, **kwargs))

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log a warning message with structured context."""
        self._logger.warning(self._format_message("WARNING", message, **kwargs))

    def error(self, message: str, **kwargs: Any) -> None:
        """Log an error message with structured context."""
        self._logger.error(self._format_message("ERROR", message, **kwargs))

    def metric(
        self,
        metric_name: str,
        value: float,
        unit: str = "count",
        **tags: Any,
    ) -> None:
        """Log a metric data point for monitoring dashboards.

        Args:
            metric_name: Name of the metric (e.g., "records_processed").
            value: Numeric value of the metric.
            unit: Unit of measurement (e.g., "count", "bytes", "ms").
            **tags: Additional metric tags/dimensions.
        """
        self._logger.info(
            self._format_message(
                "METRIC",
                f"{metric_name}={value} {unit}",
                metric_name=metric_name,
                metric_value=value,
                metric_unit=unit,
                **tags,
            )
        )

    class _StageContext:
        """Context manager for tracking pipeline stage duration."""

        def __init__(self, logger: "PipelineLogger", stage_name: str):
            self.logger = logger
            self.stage_name = stage_name
            self.start_time = 0.0

        def __enter__(self):
            self.start_time = time.time()
            self.logger.info(f"Stage started: {self.stage_name}")
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            duration_ms = (time.time() - self.start_time) * 1000
            if exc_type:
                self.logger.error(
                    f"Stage failed: {self.stage_name}",
                    duration_ms=round(duration_ms, 2),
                    error_type=exc_type.__name__,
                    error_message=str(exc_val),
                )
            else:
                self.logger.info(
                    f"Stage completed: {self.stage_name}",
                    duration_ms=round(duration_ms, 2),
                )
                self.logger.metric(
                    f"stage_duration_{self.stage_name}",
                    round(duration_ms, 2),
                    unit="ms",
                )
            return False  # Don't suppress exceptions

    def stage(self, stage_name: str) -> _StageContext:
        """Create a context manager for tracking a pipeline stage.

        Args:
            stage_name: Human-readable name for the pipeline stage.

        Returns:
            Context manager that logs start/end/duration of the stage.
        """
        return self._StageContext(self, stage_name)


def log_execution(logger: Optional[PipelineLogger] = None):
    """Decorator to log function execution time and errors.

    Args:
        logger: PipelineLogger instance. Creates a default if not provided.

    Returns:
        Decorated function with automatic logging.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _logger = logger or PipelineLogger(func.__module__)
            _logger.info(
                f"Executing {func.__name__}",
                function=func.__name__,
                args_count=len(args),
            )
            start = time.time()
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.time() - start) * 1000
                _logger.info(
                    f"Completed {func.__name__}",
                    function=func.__name__,
                    duration_ms=round(duration_ms, 2),
                )
                return result
            except Exception as e:
                duration_ms = (time.time() - start) * 1000
                _logger.error(
                    f"Failed {func.__name__}: {e}",
                    function=func.__name__,
                    duration_ms=round(duration_ms, 2),
                    error_type=type(e).__name__,
                )
                raise
        return wrapper
    return decorator
