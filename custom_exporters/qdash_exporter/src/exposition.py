from __future__ import annotations

import logging
from http.server import BaseHTTPRequestHandler
from typing import TYPE_CHECKING, ClassVar

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.exposition import CONTENT_TYPE_LATEST

from models import (
    Batch,
    BatchValidationError,
    InternalServerError,
    MetricKind,
    ServiceUnavailableError,
    infer_metric_from_filename,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from config import TargetsConfig
    from storage import SpoolBuffer

logger = logging.getLogger(f"qdash_exporter.{__name__}")


class PullService:
    """A service that builds Prometheus exposition responses from pending batches."""

    def __init__(self, spool: SpoolBuffer, targets: TargetsConfig) -> None:
        self._spool = spool
        self._targets = targets

    def build_response(self) -> tuple[bytes, list[str], int]:
        """Construct a Prometheus exposition response from pending batches.

        Returns:
            A tuple of the response body (bytes), the list of served filenames,
            and the number of delivered metric families (distinct metric names).

        Raises:
            ServiceUnavailableError: If there are no deliverable buffered samples.
            InternalServerError: If an unexpected error occurs while reading batches.

        """
        # Serve and read the spool for pending batches
        enabled_metrics = self._targets.enabled_metrics()
        loaded: list[tuple[str, Batch]] = []
        for filename in self._spool.list_pending_filenames():
            metric = infer_metric_from_filename(filename, enabled_metrics)
            if metric is None:
                # Disabled/unknown metric: skip without opening the file
                continue
            try:
                batch = self._spool.read_batch(filename)
            except (BatchValidationError, OSError) as exc:
                logger.exception("Failed to read pending batch %s.", filename)
                msg = f"cannot read pending batch {filename}: {exc}"
                raise InternalServerError(msg) from exc
            loaded.append((filename, batch))

        # Build Prometheus families from the loaded batches
        families = _build_families(self._targets, (batch for _, batch in loaded))

        # Check if there are any deliverable samples
        total_samples = sum(len(family.samples) for family in families.values())
        if total_samples == 0:
            msg = "no deliverable buffered samples"
            raise ServiceUnavailableError(msg)

        # Render the Prometheus exposition response
        registry = CollectorRegistry()
        registry.register(_StaticCollector(list(families.values())))
        body = generate_latest(registry)

        return body, [filename for filename, _ in loaded], len(families)

    def confirm_served(self, filenames: list[str]) -> None:
        """Delete served batches (delegates to spool.delete_pending).

        Args:
            filenames: A list of filenames to delete from the spool.

        """
        self._spool.delete_pending(filenames)


class _StaticCollector:
    """A prometheus_client collector that yields a fixed set of families."""

    def __init__(self, families: list[GaugeMetricFamily]) -> None:
        self._families = families

    def collect(self) -> Iterable[GaugeMetricFamily]:
        """Yield the fixed set of GaugeMetricFamily objects.

        Returns:
            An iterable of GaugeMetricFamily objects.

        """
        return iter(self._families)


def _metric_name(kind: MetricKind, metric: str) -> str:
    return f"qdash_{kind.value}_{metric}"


def _labels_for(kind: MetricKind) -> list[str]:
    if kind == MetricKind.QUBIT:
        return ["chip_id", "qubit_id", "unit"]
    return ["chip_id", "coupling_id", "unit"]


def _get_or_create_family(
    families: dict[str, GaugeMetricFamily],
    name: str,
    kind: MetricKind,
    metric: str,
    *,
    is_error: bool,
) -> GaugeMetricFamily:
    """Get or create a GaugeMetricFamily for the given metric.

    Args:
        families: A dictionary mapping metric names to GaugeMetricFamily objects.
        name: The name of the metric.
        kind: The kind of metric (MetricKind.QUBIT or MetricKind.COUPLING).
        metric: The metric name.
        is_error: Whether this is an error metric.

    Returns:
        The existing or newly created GaugeMetricFamily object.

    """
    if name not in families:
        suffix = " error" if is_error else ""
        families[name] = GaugeMetricFamily(
            name,
            f"QDash {kind.value} calibration metric '{metric}'{suffix}",
            labels=_labels_for(kind),
        )
    return families[name]


def _build_families(
    targets: TargetsConfig, batches: Iterable[Batch]
) -> dict[str, GaugeMetricFamily]:
    """Build Prometheus GaugeMetricFamily objects from batches.

    Args:
        targets: The TargetsConfig object for metric kind inference.
        batches: An iterable of Batch objects to process.

    Returns:
        A dictionary mapping metric names to GaugeMetricFamily objects.

    """
    families: dict[str, GaugeMetricFamily] = {}
    for batch in batches:
        kind = targets.kind_of(batch.metric)
        name = _metric_name(kind, batch.metric)
        family = _get_or_create_family(
            families, name, kind, batch.metric, is_error=False
        )

        for record in batch.records:
            # Determine the appropriate ID based on the metric kind.
            qid = record.qubit_id if kind == MetricKind.QUBIT else record.coupling_id
            family.add_metric(
                [batch.chip_id, qid, record.unit],  # type: ignore[list-item]
                record.value,
                timestamp=record.timestamp_ms / 1000.0,
            )

            # Add a paired metric with `_error` suffix if record has a non-zero error.
            if kind == MetricKind.QUBIT and record.error is not None:
                error_family = _get_or_create_family(
                    families, f"{name}_error", kind, batch.metric, is_error=True
                )
                error_family.add_metric(
                    [batch.chip_id, qid, record.unit],  # type: ignore[list-item]
                    record.error,
                    timestamp=record.timestamp_ms / 1000.0,
                )

    return families


class MetricsRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the /metrics endpoint."""

    pull_service: ClassVar[PullService]

    def do_GET(self) -> None:
        """Handle GET requests to /metrics."""
        self._detail: str | None = None
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        try:
            body, served, family_count = self.pull_service.build_response()
        except ServiceUnavailableError as exc:
            self._send_plain_text(503, str(exc))
            return
        except InternalServerError:
            logger.exception("Internal server error while building /metrics response.")
            self._send_plain_text(500, "internal server error")
            return
        except Exception:
            logger.exception("Unexpected error while building /metrics response.")
            self._send_plain_text(500, "internal server error")
            return

        self._detail = f"served {family_count} metric(s)"
        if not self._write_response(body):
            return
        self.pull_service.confirm_served(served)

    def _write_response(self, body: bytes) -> bool:
        try:
            self.send_response(200)
            for name, value in (
                ("Content-Type", CONTENT_TYPE_LATEST),
                ("Content-Length", str(len(body))),
            ):
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
        except OSError:
            logger.exception("Response write failed; keeping batch.")
            return False
        return True

    def _send_plain_text(self, status_code: int, message: str) -> None:
        self._detail = message
        body = message.encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_request(
        self,
        code: int | str = "-",
        size: int | str = "-",  # noqa: ARG002
    ) -> None:
        """Log the access line without the trailing response-size field."""
        self.log_message('"%s" %s', self.requestline, str(code))

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        """Log an arbitrary message to the logger, including the client address."""
        detail = getattr(self, "_detail", None)
        if detail:
            logger.info("%s - %s (%s)", self.address_string(), format % args, detail)
        else:
            logger.info("%s - %s", self.address_string(), format % args)
