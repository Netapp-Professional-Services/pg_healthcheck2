"""
Pluggable backend system for health check submission.

This module provides a flexible architecture for handling health check submissions
with multiple deployment modes:
- Direct: Synchronous database insertion (simple, no extra dependencies)
- Pooled: Connection pooling for better concurrency (requires PgBouncer/pgpool)
- Async Queue: Asynchronous processing with Celery (requires Redis/RabbitMQ)
- Disabled: Reject all submissions (read-only deployment)

Backend selection is controlled via config/trends.yaml without code changes.
"""

from abc import ABC, abstractmethod
from enum import Enum
import json
from datetime import datetime
from flask import current_app


class SubmissionMode(Enum):
    """Available submission backend modes."""
    DIRECT = "direct"
    POOLED = "pooled"
    ASYNC_QUEUE = "async_queue"
    DISABLED = "disabled"


class SubmissionBackend(ABC):
    """Abstract base class for submission backends."""

    @abstractmethod
    def submit(self, target_info, findings_json, structured_findings,
               adoc_content, analysis_results):
        """
        Submit health check data for processing.

        Args:
            target_info (dict): Target system metadata
            findings_json (str): Serialized JSON findings
            structured_findings (dict): Findings as Python dict
            adoc_content (str): AsciiDoc report content
            analysis_results (dict): Triggered rules and analysis

        Returns:
            dict: Status response with keys:
                - status: 'completed', 'accepted', or 'rejected'
                - message: Human-readable message
                - task_id: (optional) For async backends
        """
        pass

    @abstractmethod
    def health_check(self):
        """
        Check if backend is healthy and can accept submissions.

        Returns:
            bool: True if healthy, False otherwise
        """
        pass

    @abstractmethod
    def get_status(self):
        """
        Return detailed backend status information.

        Returns:
            dict: Status information including mode, health, and metrics
        """
        pass


class DirectBackend(SubmissionBackend):
    """
    Mode A: Direct synchronous database insertion.

    Simple implementation with no additional dependencies.
    Best for: Small deployments, development environments, <50 concurrent submissions

    Pros:
    - Zero configuration
    - Immediate consistency (data in DB when request returns)
    - Simple to debug

    Cons:
    - Blocks request until DB write completes
    - No automatic retry on failure
    - Limited concurrency
    """

    def __init__(self, db_config):
        self.db_config = db_config

    def submit(self, target_info, findings_json, structured_findings,
               adoc_content, analysis_results):
        """Submit directly to database (synchronous, blocking)."""
        from output_handlers.trend_shipper import ship_to_database

        try:
            ship_to_database(
                self.db_config, target_info, findings_json,
                structured_findings, adoc_content, analysis_results
            )

            return {
                "status": "completed",
                "message": "Health check stored successfully",
                "backend": "direct"
            }
        except Exception as e:
            current_app.logger.error(f"Direct submission failed: {e}")
            raise

    def health_check(self):
        """Test database connectivity."""
        import psycopg2
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.close()
            return True
        except Exception as e:
            current_app.logger.error(f"Direct backend health check failed: {e}")
            return False

    def get_status(self):
        return {
            "mode": "direct",
            "healthy": self.health_check(),
            "description": "Synchronous direct database insertion",
            "capabilities": {
                "immediate_consistency": True,
                "automatic_retry": False,
                "max_concurrent": 50
            }
        }


class PooledBackend(SubmissionBackend):
    """
    Mode B: Connection pooling for better concurrency.

    Uses psycopg2's ThreadedConnectionPool to reuse database connections.
    Best for: Medium deployments, 50-200 concurrent submissions

    Requires: PgBouncer or pgpool-II for optimal performance

    Pros:
    - Better concurrency than direct mode
    - Connection reuse reduces overhead
    - Still synchronous (immediate consistency)

    Cons:
    - Still blocks request during DB write
    - Limited by pool size
    - Requires pool configuration tuning
    """

    def __init__(self, db_config, pool_config):
        self.db_config = db_config
        self.pool_config = pool_config
        self._pool = None

    def _get_pool(self):
        """Lazy initialize connection pool."""
        if self._pool is None:
            import psycopg2.pool

            min_conn = self.pool_config.get('min_connections', 5)
            max_conn = self.pool_config.get('max_connections', 20)

            current_app.logger.info(
                f"Initializing connection pool: min={min_conn}, max={max_conn}"
            )

            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=min_conn,
                maxconn=max_conn,
                **self.db_config
            )
        return self._pool

    def submit(self, target_info, findings_json, structured_findings,
               adoc_content, analysis_results):
        """Submit using pooled connection."""
        pool = self._get_pool()
        conn = None

        try:
            conn = pool.getconn()

            # Use connection-aware version of ship_to_database
            self._ship_with_connection(
                conn, target_info, findings_json,
                structured_findings, adoc_content, analysis_results
            )

            return {
                "status": "completed",
                "message": "Health check stored successfully (pooled)",
                "backend": "pooled"
            }
        except Exception as e:
            current_app.logger.error(f"Pooled submission failed: {e}")
            raise
        finally:
            if conn:
                pool.putconn(conn)

    def _ship_with_connection(self, conn, target_info, findings_json,
                             structured_findings, adoc_content, analysis_results):
        """
        Ship to database using provided connection from pool.
        This is a refactored version of trend_shipper.ship_to_database()
        that accepts a connection parameter.
        """
        from output_handlers.trend_shipper import (
            _extract_db_version, _parse_version_components,
            _extract_cluster_name, _extract_node_count,
            _build_infrastructure_metadata, _calculate_health_score,
            _store_triggered_rules
        )

        cursor = conn.cursor()

        try:
            # Get or create company
            company_name = target_info.get('company_name', 'Default Company')
            cursor.execute("SELECT get_or_create_company(%s);", (company_name,))
            company_id = cursor.fetchone()[0]

            # Extract target information
            db_type = target_info.get('db_type', 'unknown')
            host = target_info.get('host', 'unknown')
            port = target_info.get('port', 0)
            database = target_info.get('database', 'unknown')

            # Extract execution context
            context = structured_findings.get('execution_context', {})
            run_by_user = context.get('run_by_user', 'unknown')
            run_from_host = context.get('run_from_host', 'unknown')
            tool_version = context.get('tool_version', 'unknown')
            prompt_template_name = structured_findings.get('prompt_template_name')

            ai_context = context.get('ai_execution_metrics')
            ai_context_json = json.dumps(ai_context) if ai_context else None

            # Extract metadata
            db_version = _extract_db_version(structured_findings)
            db_version_major, db_version_minor = _parse_version_components(db_version)
            cluster_name = _extract_cluster_name(target_info, structured_findings)
            node_count = _extract_node_count(structured_findings)
            infrastructure_metadata = _build_infrastructure_metadata(target_info, structured_findings)
            health_score = _calculate_health_score(analysis_results)

            infra_json = json.dumps(infrastructure_metadata) if infrastructure_metadata else None

            # Insert health check run
            insert_query = """
            INSERT INTO health_check_runs (
                company_id, db_technology, target_host, target_port, target_db_name,
                findings, prompt_template_name, run_by_user, run_from_host, tool_version,
                report_adoc, ai_execution_context,
                db_version, db_version_major, db_version_minor, cluster_name, node_count,
                infrastructure_metadata, health_score
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """

            cursor.execute(insert_query, (
                company_id, db_type, host, port, database, findings_json,
                prompt_template_name, run_by_user, run_from_host, tool_version,
                adoc_content, ai_context_json,
                db_version, db_version_major, db_version_minor, cluster_name, node_count,
                infra_json, health_score
            ))

            run_id = cursor.fetchone()[0]

            # Store triggered rules
            if analysis_results:
                rules_stored = _store_triggered_rules(cursor, run_id, analysis_results)
                current_app.logger.info(
                    f"Stored {rules_stored} triggered rules for run {run_id}"
                )

            conn.commit()
            current_app.logger.info(f"Successfully stored health check run {run_id}")

        except Exception as e:
            conn.rollback()
            current_app.logger.error(f"Failed to store health check: {e}")
            raise

    def health_check(self):
        """Check pool health by getting a connection."""
        try:
            pool = self._get_pool()
            conn = pool.getconn()
            cursor = conn.cursor()
            cursor.execute("SELECT 1;")
            pool.putconn(conn)
            return True
        except Exception as e:
            current_app.logger.error(f"Pooled backend health check failed: {e}")
            return False

    def get_status(self):
        try:
            pool = self._get_pool()
            # Note: ThreadedConnectionPool doesn't expose pool statistics easily
            # This is a limitation of psycopg2's pool implementation
            pool_info = {
                "min_connections": self.pool_config.get('min_connections', 5),
                "max_connections": self.pool_config.get('max_connections', 20),
                "current_usage": "N/A (psycopg2 limitation)"
            }
        except:
            pool_info = {"error": "Pool not initialized"}

        return {
            "mode": "pooled",
            "healthy": self.health_check(),
            "pool_config": pool_info,
            "description": "Connection pooling enabled",
            "capabilities": {
                "immediate_consistency": True,
                "automatic_retry": False,
                "max_concurrent": self.pool_config.get('max_connections', 20)
            }
        }


class AsyncQueueBackend(SubmissionBackend):
    """
    Mode C: Asynchronous queue-based processing with Celery.

    Enqueues health check submissions to Redis/RabbitMQ for background processing.
    Best for: Large deployments, >100 concurrent submissions, variable load

    Requires: celery, redis (or rabbitmq)

    Pros:
    - API responds immediately (<100ms)
    - Automatic retries with exponential backoff
    - Can scale workers independently
    - Handles load spikes gracefully

    Cons:
    - Eventual consistency (data not immediately in DB)
    - More complex infrastructure
    - Need to monitor queue depth
    """

    def __init__(self, queue_config):
        self.queue_config = queue_config
        self._celery_app = None
        self._process_task = None

    def _get_celery_app(self):
        """Lazy initialize Celery application."""
        if self._celery_app is None:
            try:
                from celery import Celery
            except ImportError:
                raise ImportError(
                    "Celery is required for async_queue mode. "
                    "Install with: pip install celery redis"
                )

            broker_url = self.queue_config.get('broker_url')
            backend_url = self.queue_config.get('backend_url')

            if not broker_url or not backend_url:
                raise ValueError(
                    "async_queue mode requires broker_url and backend_url "
                    "in submission_config.async_queue"
                )

            current_app.logger.info(
                f"Initializing Celery with broker: {broker_url}"
            )

            self._celery_app = Celery(
                'trends_app_submissions',
                broker=broker_url,
                backend=backend_url
            )

            # Configure Celery for reliable task processing
            self._celery_app.conf.update(
                task_serializer='json',
                accept_content=['json'],
                result_serializer='json',
                timezone='UTC',
                enable_utc=True,
                task_acks_late=True,  # Acknowledge after processing
                task_reject_on_worker_lost=True,  # Retry if worker dies
                worker_prefetch_multiplier=1,  # One task per worker at a time
                task_track_started=True,
                result_expires=3600,  # Keep results for 1 hour
            )

            # Define the background task
            max_retries = self.queue_config.get('max_retries', 3)
            retry_backoff = self.queue_config.get('retry_backoff', 300)

            @self._celery_app.task(
                bind=True,
                name='trends_app.process_health_check',
                max_retries=max_retries,
                default_retry_delay=retry_backoff
            )
            def process_health_check(self, target_info, findings_json,
                                    structured_findings, adoc_content, analysis_results):
                """Background task to process health check submission."""
                from output_handlers.trend_shipper import ship_to_database
                from .utils import load_trends_config

                try:
                    config = load_trends_config()
                    db_config = config.get('database')

                    ship_to_database(
                        db_config, target_info, findings_json,
                        structured_findings, adoc_content, analysis_results
                    )

                    return {
                        "status": "completed",
                        "run_id": "stored",
                        "processed_at": datetime.utcnow().isoformat()
                    }

                except Exception as exc:
                    # Log the error
                    current_app.logger.error(
                        f"Health check processing failed (attempt {self.request.retries + 1}): {exc}"
                    )

                    # Retry with exponential backoff
                    countdown = retry_backoff * (2 ** self.request.retries)
                    raise self.retry(exc=exc, countdown=countdown)

            self._process_task = process_health_check

        return self._celery_app

    def submit(self, target_info, findings_json, structured_findings,
               adoc_content, analysis_results):
        """Enqueue task for asynchronous processing."""
        celery_app = self._get_celery_app()

        try:
            task = self._process_task.apply_async(
                args=[
                    target_info, findings_json, structured_findings,
                    adoc_content, analysis_results
                ],
                retry=True
            )

            current_app.logger.info(
                f"Health check enqueued with task_id: {task.id}"
            )

            return {
                "status": "accepted",
                "task_id": task.id,
                "message": "Health check queued for processing",
                "backend": "async_queue",
                "eta_seconds": 60
            }

        except Exception as e:
            current_app.logger.error(f"Failed to enqueue task: {e}")
            raise

    def health_check(self):
        """Check if Celery broker and workers are accessible."""
        try:
            celery_app = self._get_celery_app()

            # Try to ping the broker
            inspect = celery_app.control.inspect(timeout=1.0)
            stats = inspect.stats()

            # If we get stats back, broker and at least one worker are alive
            return stats is not None and len(stats) > 0

        except Exception as e:
            current_app.logger.error(f"Async queue health check failed: {e}")
            return False

    def get_status(self):
        """Get detailed status including worker information."""
        celery_app = self._get_celery_app()

        try:
            inspect = celery_app.control.inspect(timeout=2.0)
            stats = inspect.stats()
            active_tasks = inspect.active()

            if stats:
                active_workers = len(stats)
                total_active_tasks = sum(len(tasks) for tasks in active_tasks.values()) if active_tasks else 0
            else:
                active_workers = 0
                total_active_tasks = 0

        except Exception as e:
            current_app.logger.error(f"Failed to get Celery stats: {e}")
            active_workers = 0
            total_active_tasks = 0

        return {
            "mode": "async_queue",
            "healthy": active_workers > 0,
            "active_workers": active_workers,
            "active_tasks": total_active_tasks,
            "broker": self.queue_config.get('broker_url'),
            "max_retries": self.queue_config.get('max_retries', 3),
            "description": "Asynchronous queue processing with Celery",
            "capabilities": {
                "immediate_consistency": False,
                "automatic_retry": True,
                "max_concurrent": "unlimited (scales with workers)"
            }
        }


class DisabledBackend(SubmissionBackend):
    """
    Mode D: Submission endpoint disabled.

    Use for read-only deployments where only the web UI is needed
    for viewing existing data. All submission attempts are rejected.

    Best for: Reporting-only instances, compliance/audit scenarios
    """

    def submit(self, *args, **kwargs):
        """Reject all submissions."""
        raise NotImplementedError(
            "Health check submission is disabled. "
            "This deployment accepts data through direct database insertion only."
        )

    def health_check(self):
        """Disabled backend is always 'unhealthy' for submissions."""
        return False

    def get_status(self):
        return {
            "mode": "disabled",
            "healthy": False,
            "description": "Submission endpoint is disabled (read-only deployment)",
            "capabilities": {
                "immediate_consistency": False,
                "automatic_retry": False,
                "max_concurrent": 0
            }
        }


class BackendFactory:
    """Factory to create appropriate submission backend based on configuration."""

    @staticmethod
    def create_backend(config):
        """
        Create backend instance from configuration.

        Args:
            config (dict): Loaded trends.yaml configuration

        Returns:
            SubmissionBackend: Configured backend instance

        Raises:
            ValueError: If submission mode is invalid or required config is missing
        """
        submission_mode = config.get('submission_mode', 'direct')
        submission_config = config.get('submission_config', {})

        if submission_mode == SubmissionMode.DIRECT.value:
            mode_config = submission_config.get('direct', {})
            if not mode_config.get('enabled', True):
                return DisabledBackend()

            db_config = config.get('database')
            if not db_config:
                raise ValueError("Direct mode requires 'database' configuration")

            return DirectBackend(db_config)

        elif submission_mode == SubmissionMode.POOLED.value:
            mode_config = submission_config.get('pooled', {})
            if not mode_config.get('enabled', False):
                return DisabledBackend()

            db_config = config.get('database')
            if not db_config:
                raise ValueError("Pooled mode requires 'database' configuration")

            return PooledBackend(db_config, mode_config)

        elif submission_mode == SubmissionMode.ASYNC_QUEUE.value:
            mode_config = submission_config.get('async_queue', {})
            if not mode_config.get('enabled', False):
                return DisabledBackend()

            return AsyncQueueBackend(mode_config)

        elif submission_mode == SubmissionMode.DISABLED.value:
            return DisabledBackend()

        else:
            raise ValueError(
                f"Unknown submission mode: {submission_mode}. "
                f"Valid modes: {[m.value for m in SubmissionMode]}"
            )


# Global backend instance (initialized once per application)
_backend_instance = None


def get_submission_backend():
    """
    Get or create the submission backend singleton.

    The backend is initialized once based on config/trends.yaml and reused
    for all requests. This ensures connection pools and Celery apps are
    shared across requests.

    Returns:
        SubmissionBackend: Configured backend instance
    """
    global _backend_instance
    if _backend_instance is None:
        from .utils import load_trends_config
        config = load_trends_config()
        _backend_instance = BackendFactory.create_backend(config)
    return _backend_instance
