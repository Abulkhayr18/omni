
"""
OMNI - Lightweight Data Lineage Tracker

Captures table-level lineage and execution metadata.
Uses only Python's standard library.
"""

import json
import uuid
import traceback

from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager


class LineageTracker:
    def __init__(
        self,
        pipeline_name,
        transformation_name,
        script_name,
        metadata_dir="lineage_metadata"
    ):
        self.pipeline_name = pipeline_name
        self.transformation_name = transformation_name
        self.script_name = script_name

        self.run_id = str(uuid.uuid4())
        self.started_at = datetime.now(timezone.utc)

        self.inputs = []
        self.outputs = []
        self.transformations = []

        self.metadata_dir = Path(metadata_dir)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

        self.output_file = self.metadata_dir / "lineage_events.jsonl"

    def add_input(self, table_name, description=None):
        """Register an upstream input table."""
        self.inputs.append({
            "table": table_name,
            "description": description
        })

    def add_output(self, table_name, description=None):
        """Register a downstream output table."""
        self.outputs.append({
            "table": table_name,
            "description": description
        })

    def add_transformation(self, name, description):
        """Document an operation performed by the pipeline."""
        self.transformations.append({
            "name": name,
            "description": description
        })

    def _write_event(self, status, error=None):
        """Append one execution event to the JSONL metadata file."""

        completed_at = datetime.now(timezone.utc)

        event = {
            "event_id": str(uuid.uuid4()),
            "run_id": self.run_id,
            "pipeline": self.pipeline_name,
            "transformation": self.transformation_name,
            "script": self.script_name,
            "status": status,
            "started_at": self.started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_seconds": (
                completed_at - self.started_at
            ).total_seconds(),
            "inputs": self.inputs,
            "outputs": self.outputs,
            "transformations": self.transformations,
            "error": error
        }

        with self.output_file.open("a", encoding="utf-8") as file:
            file.write(json.dumps(event, default=str) + "\n")

    @contextmanager
    def track(self):
        """
        Record successful or failed execution automatically.
        Exceptions are recorded and then re-raised.
        """
        try:
            yield self
        except Exception:
            self._write_event(
                status="FAILED",
                error=traceback.format_exc()
            )
            raise
        else:
            self._write_event(status="SUCCESS")
