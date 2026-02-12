"""Progress reporting for pipeline stages using rich.progress."""

from contextlib import contextmanager
from typing import Protocol

from rich.progress import Progress


class ProgressTracker(Protocol):
    """Protocol for progress tracker objects.

    Defines the interface required by pipeline functions for progress reporting.
    """

    def update_description(self, description: str) -> None:
        """Update the description of the progress task.

        Args:
            description: New description (e.g., current filename or table name)
        """
        ...

    def advance(self, amount: int = 1) -> None:
        """Advance the progress bar.

        Args:
            amount: Number of units to advance (default: 1)
        """
        ...


class PipelineProgress:
    """Wrapper around rich.progress.Progress for pipeline tracking.

    Provides context managers for ingestion, transform, and export stages,
    each with per-file or per-table progress tracking.
    """

    def __init__(self):
        """Initialize progress tracker."""
        self.progress = Progress()

    @contextmanager
    def ingestion_tracker(self, total_files: int):
        """Context manager for file ingestion progress.

        Args:
            total_files: Total number of files to ingest

        Yields:
            Tracker object with update_description() and advance() methods
        """
        with self.progress:
            task_id = self.progress.add_task(
                "[cyan]Ingesting...", total=total_files
            )
            yield _ProgressTracker(self.progress, task_id)

    @contextmanager
    def transform_tracker(self, total_tables: int):
        """Context manager for table transform progress.

        Args:
            total_tables: Total number of tables to transform

        Yields:
            Tracker object with update_description() and advance() methods
        """
        with self.progress:
            task_id = self.progress.add_task(
                "[cyan]Transforming...", total=total_tables
            )
            yield _ProgressTracker(self.progress, task_id)

    @contextmanager
    def export_tracker(self, total_tables: int):
        """Context manager for table export progress.

        Args:
            total_tables: Total number of tables to export

        Yields:
            Tracker object with update_description() and advance() methods
        """
        with self.progress:
            task_id = self.progress.add_task(
                "[cyan]Exporting...", total=total_tables
            )
            yield _ProgressTracker(self.progress, task_id)


class _ProgressTracker:
    """Internal tracker object for updating progress bars."""

    def __init__(self, progress: Progress, task_id: int):
        """Initialize tracker.

        Args:
            progress: Rich Progress instance
            task_id: Task ID for this tracker
        """
        self.progress = progress
        self.task_id = task_id

    def update_description(self, description: str) -> None:
        """Update the description of the progress task.

        Args:
            description: New description (e.g., current filename)
        """
        self.progress.update(self.task_id, description=description)

    def advance(self, amount: int = 1) -> None:
        """Advance the progress bar.

        Args:
            amount: Number of units to advance (default: 1)
        """
        self.progress.advance(self.task_id, amount)
