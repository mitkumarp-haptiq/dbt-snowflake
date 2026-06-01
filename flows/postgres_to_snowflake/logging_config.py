"""Centralized structlog + Azure Monitor configuration for the pipeline.

Call ``configure_structlog()`` at flow entry (after dotenv is loaded).
It is safe to call multiple times -- subsequent calls are no-ops.

Environment knobs (all optional):
    STRUCTLOG_JSON       "true" for JSON lines output.
                         Default: pretty console rendering.
    STRUCTLOG_LOG_LEVEL  Minimum severity: DEBUG, INFO, WARNING, ERROR.
                         Default: INFO.
    APPLICATIONINSIGHTS_CONNECTION_STRING
                         When set, Prefect + pipeline logs are exported
                         directly to Azure Monitor / Application Insights.
    AZURE_MONITOR_SERVICE_NAME
                         Service name shown in Azure Monitor.
                         Default: postgres-to-snowflake.
"""

from __future__ import annotations

import json
import logging
import os
import sys

import structlog

_PIPELINE_LOGGER_ROOT = "prefect"
_PIPELINE_LOGGER_NAMESPACE = "prefect.pipeline.postgres_to_snowflake"
_DEFAULT_SERVICE_NAME = "postgres-to-snowflake"
_configured = False


def _env_flag(name: str, default: bool = False) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes")


def _env_log_level() -> int:
    raw = (os.environ.get("STRUCTLOG_LOG_LEVEL") or "INFO").strip().upper()
    return getattr(logging, raw, logging.INFO)


class _AzureMonitorJSONFormatter(logging.Formatter):
    """Render structlog event dicts as JSON strings for Azure Monitor."""

    def format(self, record: logging.LogRecord) -> str:
        if isinstance(record.msg, dict):
            payload = dict(record.msg)
        else:
            payload = {"event": record.getMessage()}

        if record.exc_info:
            payload.setdefault("exception", self.formatException(record.exc_info))

        return json.dumps(payload, default=str, ensure_ascii=True)


def _attach_azure_monitor() -> None:
    """Attach direct Azure Monitor logging when a connection string is set."""

    connection_string = os.environ.get(
        "APPLICATIONINSIGHTS_CONNECTION_STRING", ""
    ).strip()
    if not connection_string:
        return

    try:
        from azure.monitor.opentelemetry import configure_azure_monitor
        from opentelemetry.sdk.resources import Resource
    except ImportError:
        logging.getLogger(__name__).warning(
            "APPLICATIONINSIGHTS_CONNECTION_STRING is set but "
            "azure-monitor-opentelemetry is not installed. Run: "
            "pip install azure-monitor-opentelemetry"
        )
        return

    service_name = (
        os.environ.get("AZURE_MONITOR_SERVICE_NAME", _DEFAULT_SERVICE_NAME).strip()
        or _DEFAULT_SERVICE_NAME
    )

    # Prefect emits its own logs under ``prefect.*``. Our flow loggers are
    # intentionally named under the same namespace so a single logger_name can
    # forward both Prefect internals and first-party pipeline logs.
    configure_azure_monitor(
        connection_string=connection_string,
        logger_name=_PIPELINE_LOGGER_ROOT,
        logging_formatter=_AzureMonitorJSONFormatter(),
        disable_metrics=True,
        disable_tracing=True,
        enable_live_metrics=False,
        enable_performance_counters=False,
        resource=Resource.create({"service.name": service_name}),
    )


def configure_structlog() -> None:
    """Wire up structlog with stdlib logging so Prefect, libraries, and our
    code all flow through the same processor pipeline.

    Safe to call multiple times -- only the first call takes effect.

    When ``APPLICATIONINSIGHTS_CONNECTION_STRING`` is set, Prefect flow/task
    logs plus this pipeline's structured logs are forwarded directly to Azure
    Monitor / Application Insights without a separate collector.
    """

    global _configured  # noqa: PLW0603
    if _configured:
        return
    _configured = True

    json_mode = _env_flag("STRUCTLOG_JSON")
    log_level = _env_log_level()

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if json_mode:
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=shared_processors,
    )

    root = logging.getLogger()
    root.handlers.clear()

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)
    root.setLevel(log_level)

    _attach_azure_monitor()


def get_logger(
    *, component: str | None = None, **initial_context: object
) -> structlog.stdlib.BoundLogger:
    """Return a pipeline logger under ``prefect.*`` pre-bound with context."""

    logger_name = _PIPELINE_LOGGER_NAMESPACE
    if component:
        logger_name = f"{logger_name}.{component}"
    return structlog.get_logger(logger_name, **initial_context)
