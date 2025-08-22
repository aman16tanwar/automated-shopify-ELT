# job_manager.py
"""
Job management for parallel processing and error logging
"""

import os
import json
import threading
import subprocess
import sys
from datetime import datetime, timezone
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import uuid
import traceback

class JobManager:
    def __init__(self, project_id=None):
        self.project_id = project_id or os.environ.get("GCP_PROJECT_ID")
        self.client = bigquery.Client(project=self.project_id)
        # Centralized dataset for all pipeline management across stores
        self.jobs_dataset = "shopify_logs"
        self.jobs_table = "pipeline_jobs"
        self.logs_table = "pipeline_logs"
        
        # Initialize tables
        self._ensure_management_tables()
    
    def _ensure_management_tables(self):
        """Create management dataset and tables if they don't exist"""
        # Create dataset
        dataset_id = f"{self.project_id}.{self.jobs_dataset}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset.description = "HappyWeb centralized pipeline management - tracks all store pipelines, jobs, and logs"
        
        try:
            dataset = self.client.create_dataset(dataset, exists_ok=True)
        except Exception as e:
            print(f"Dataset might already exist: {e}")
        
        # Create jobs table
        jobs_table_id = f"{self.project_id}.{self.jobs_dataset}.{self.jobs_table}"
        jobs_schema = [
            bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("store_url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dataset_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("job_type", "STRING", mode="REQUIRED"),  # historical_load, daily_sync
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),  # pending, running, completed, failed
            bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("completed_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("records_processed", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("duration_seconds", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("created_by", "STRING", mode="NULLABLE"),
        ]
        
        jobs_table = bigquery.Table(jobs_table_id, schema=jobs_schema)
        jobs_table = self.client.create_table(jobs_table, exists_ok=True)
        
        # Create logs table
        logs_table_id = f"{self.project_id}.{self.jobs_dataset}.{self.logs_table}"
        logs_schema = [
            bigquery.SchemaField("log_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("log_level", "STRING", mode="REQUIRED"),  # INFO, WARNING, ERROR
            bigquery.SchemaField("message", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("store_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("component", "STRING", mode="NULLABLE"),  # orders, customers, products
            bigquery.SchemaField("is_error", "BOOLEAN", mode="NULLABLE"),  # Flag for errors
            bigquery.SchemaField("error_type", "STRING", mode="NULLABLE"),  # Type of error
        ]
        
        logs_table = bigquery.Table(logs_table_id, schema=logs_schema)
        logs_table = self.client.create_table(logs_table, exists_ok=True)
    
    def create_job(self, store_url, dataset_name, job_type="historical_load", created_by=None):
        """Create a new job record"""
        job_id = str(uuid.uuid4())
        job_data = {
            "job_id": job_id,
            "store_url": store_url,
            "dataset_name": dataset_name,
            "job_type": job_type,
            "status": "pending",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "created_by": created_by or "system"
        }
        
        table_id = f"{self.project_id}.{self.jobs_dataset}.{self.jobs_table}"
        errors = self.client.insert_rows_json(table_id, [job_data])
        
        if errors:
            raise Exception(f"Failed to create job: {errors}")
        
        return job_id
    
    def update_job_status(self, job_id, status, error_message=None, records_processed=None):
        """Update job status - using insert instead of update to avoid streaming buffer issues"""
        # Instead of UPDATE, we'll insert a new status record
        # The queries will use the latest status based on timestamp
        from datetime import datetime, timezone
        
        status_data = {
            "job_id": job_id,
            "store_url": "",  # Will be filled from original job
            "dataset_name": "",  # Will be filled from original job
            "job_type": "status_update",
            "status": status,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat() if status in ['completed', 'failed'] else None,
            "error_message": error_message,
            "records_processed": records_processed,
            "created_by": "system"
        }
        
        # Get original job info to fill missing fields
        try:
            query = f"""
            SELECT store_url, dataset_name, job_type, started_at
            FROM `{self.project_id}.{self.jobs_dataset}.{self.jobs_table}`
            WHERE job_id = @job_id
            ORDER BY started_at DESC
            LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
                ]
            )
            
            result = list(self.client.query(query, job_config=job_config))
            if result:
                original = result[0]
                status_data["store_url"] = getattr(original, 'store_url', '')
                status_data["dataset_name"] = getattr(original, 'dataset_name', '')
                if hasattr(original, 'started_at') and original.started_at:
                    status_data["started_at"] = original.started_at.isoformat()
                    
                    # Calculate duration if completed
                    if status in ['completed', 'failed']:
                        duration = (datetime.now(timezone.utc) - original.started_at).total_seconds()
                        status_data["duration_seconds"] = int(duration)
        except Exception as e:
            print(f"Warning: Could not get original job info: {e}")
        
        # Insert the status update as a new record
        table_id = f"{self.project_id}.{self.jobs_dataset}.{self.jobs_table}"
        errors = self.client.insert_rows_json(table_id, [status_data])
        
        if errors:
            print(f"Failed to update job status: {errors}")
    
    def log_message(self, job_id, log_level, message, store_url=None, component=None):
        """Log a message for a job"""
        log_data = {
            "log_id": str(uuid.uuid4()),
            "job_id": job_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "log_level": log_level,
            "message": message[:1000],  # Truncate long messages
            "store_url": store_url,
            "component": component
        }
        
        table_id = f"{self.project_id}.{self.jobs_dataset}.{self.logs_table}"
        errors = self.client.insert_rows_json(table_id, [log_data])
        
        if errors:
            print(f"Failed to log message: {errors}")
    
    def get_active_jobs(self):
        """Get all active jobs - improved to handle status updates better"""
        query = f"""
        WITH job_timeline AS (
            -- Get all job records with timestamp ordering
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY started_at DESC) as rn
            FROM `{self.project_id}.{self.jobs_dataset}.{self.jobs_table}`
        ),
        latest_status AS (
            -- Get the most recent status for each job
            SELECT 
                job_id, 
                status, 
                store_url, 
                dataset_name, 
                job_type,
                started_at
            FROM job_timeline
            WHERE rn = 1
        ),
        first_record AS (
            -- Get the original job start time (exclude status updates)
            SELECT 
                job_id, 
                MIN(started_at) as original_started_at,
                MIN(store_url) as original_store_url,
                MIN(dataset_name) as original_dataset_name
            FROM `{self.project_id}.{self.jobs_dataset}.{self.jobs_table}`
            WHERE job_type != 'status_update' OR job_type IS NULL
            GROUP BY job_id
        )
        SELECT DISTINCT
            fr.job_id,
            COALESCE(ls.store_url, fr.original_store_url) as store_url,
            COALESCE(ls.dataset_name, fr.original_dataset_name) as dataset_name,
            ls.job_type,
            ls.status,
            fr.original_started_at as started_at,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), fr.original_started_at, SECOND) as running_seconds
        FROM first_record fr
        INNER JOIN latest_status ls ON fr.job_id = ls.job_id
        WHERE 
            -- Only show active statuses
            ls.status IN ('pending', 'running')
            -- Double-check: ensure not in terminal states
            AND ls.status NOT IN ('cancelled', 'completed', 'failed')
            -- Only recent jobs (last 24 hours)
            AND fr.original_started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            -- Ensure we have valid data
            AND fr.original_store_url IS NOT NULL
            AND fr.original_dataset_name IS NOT NULL
        ORDER BY fr.original_started_at DESC
        """
        
        try:
            return list(self.client.query(query))
        except Exception as e:
            print(f"[ERROR] Failed to get active jobs: {e}")
            return []
    
    def get_job_logs(self, job_id, limit=100):
        """Get logs for a specific job"""
        query = f"""
        SELECT timestamp, log_level, message, component
        FROM `{self.project_id}.{self.jobs_dataset}.{self.logs_table}`
        WHERE job_id = @job_id
        ORDER BY timestamp DESC
        LIMIT @limit
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
                bigquery.ScalarQueryParameter("limit", "INTEGER", limit),
            ]
        )
        
        return list(self.client.query(query, job_config=job_config))
    
    def get_job_error_summary(self, job_id):
        """Get error summary for a specific job"""
        query = f"""
        SELECT 
            COUNT(CASE WHEN log_level = 'ERROR' THEN 1 END) as error_count,
            COUNT(CASE WHEN log_level = 'WARNING' THEN 1 END) as warning_count,
            ARRAY_AGG(
                STRUCT(timestamp, message, component)
                ORDER BY timestamp DESC
                LIMIT 10
            ) as recent_errors
        FROM `{self.project_id}.{self.jobs_dataset}.{self.logs_table}`
        WHERE job_id = @job_id
        AND log_level IN ('ERROR', 'WARNING')
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("job_id", "STRING", job_id)
            ]
        )
        
        results = list(self.client.query(query, job_config=job_config))
        if results:
            return results[0]
        return None
    
    def get_recent_jobs(self, limit=20):
        """Get recent jobs with summary"""
        query = f"""
        WITH job_timeline AS (
            -- Get all records for each job
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY started_at DESC) as rn
            FROM `{self.project_id}.{self.jobs_dataset}.{self.jobs_table}`
        ),
        latest_record AS (
            -- Get the most recent record for each job (for latest status)
            SELECT job_id, status, error_message, completed_at, duration_seconds, records_processed
            FROM job_timeline
            WHERE rn = 1
        ),
        first_record AS (
            -- Get the first record for each job (for original data)
            -- Exclude status updates to get the actual job creation record
            SELECT job_id, started_at, store_url, dataset_name, job_type
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY started_at ASC) as rn_asc
                FROM `{self.project_id}.{self.jobs_dataset}.{self.jobs_table}`
                WHERE job_type != 'status_update' OR job_type IS NULL
            )
            WHERE rn_asc = 1
        )
        SELECT 
            f.job_id,
            f.store_url,
            f.dataset_name,
            f.job_type,
            l.status,
            f.started_at,
            l.completed_at,
            l.error_message,
            l.duration_seconds,
            l.records_processed,
            (SELECT COUNT(*) FROM `{self.project_id}.{self.jobs_dataset}.{self.logs_table}` logs 
             WHERE logs.job_id = f.job_id AND logs.log_level = 'ERROR') as error_count
        FROM first_record f
        JOIN latest_record l ON f.job_id = l.job_id
        WHERE f.store_url IS NOT NULL AND f.dataset_name IS NOT NULL
        ORDER BY f.started_at DESC
        LIMIT @limit
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("limit", "INTEGER", limit),
            ]
        )
        
        return list(self.client.query(query, job_config=job_config))
    
    def run_historical_load_async(self, store_config, job_id):
        """Run historical load using Cloud Run Job"""
        def _run_in_background():
            try:
                self.update_job_status(job_id, "running")
                merchant = store_config.get('MERCHANT', 'unknown')
                self.log_message(job_id, "INFO", f"Starting historical load for {merchant}", 
                               merchant, "main")
                
                # Check if we're in Cloud Run environment
                if os.getenv("K_SERVICE"):
                    # Running in Cloud Run - use Cloud Run Jobs
                    try:
                        from cloud_run_job_manager import CloudRunJobManager
                        job_manager = CloudRunJobManager(project_id=self.project_id)
                        
                        # Create and execute Cloud Run Job
                        result = job_manager.create_historical_job(store_config, job_id)
                        
                        if result['success']:
                            self.log_message(job_id, "INFO", 
                                           f"Created Cloud Run Job: {result['job_name']}", 
                                           merchant, "main")
                            self.log_message(job_id, "INFO", 
                                           f"Job execution started: {result.get('execution_name', 'N/A')}", 
                                           merchant, "main")
                            
                            # Store Cloud Run job name for status tracking
                            self.log_message(job_id, "INFO", 
                                           f"CLOUD_RUN_JOB_NAME:{result['job_name']}", 
                                           merchant, "system")
                            
                            # Job will update its own status when complete
                            return
                        else:
                            raise Exception(f"Failed to create Cloud Run Job: {result.get('error', 'Unknown error')}")
                    except ImportError as e:
                        import traceback
                        error_details = traceback.format_exc()
                        self.log_message(job_id, "ERROR", 
                                       f"Cloud Run Job Manager import failed: {str(e)}", 
                                       merchant, "main")
                        self.log_message(job_id, "ERROR", 
                                       f"Import error details: {error_details[:500]}", 
                                       merchant, "main")
                        self.log_message(job_id, "WARNING", 
                                       "Falling back to subprocess method", 
                                       merchant, "main")
                
                # Fallback to subprocess for local development
                # Get historical script path
                app_dir = os.path.dirname(os.path.abspath(__file__))
                historical_script = os.path.join(os.path.dirname(app_dir), "historical", "main.py")
                
                # Create environment with job_id
                env = os.environ.copy()
                env["PIPELINE_JOB_ID"] = job_id
                env["TARGET_STORE"] = store_config.get("MERCHANT", "")
                
                # Run subprocess with unbuffered output
                process = subprocess.Popen(
                    [sys.executable, "-u", historical_script],  # -u for unbuffered
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=0,  # No buffering
                    cwd=os.path.dirname(historical_script),
                    env=env
                )
                
                # Store process reference for cancellation
                self._running_processes = getattr(self, '_running_processes', {})
                self._running_processes[job_id] = process
                
                # Capture output and log
                while True:
                    line = process.stdout.readline()
                    if not line and process.poll() is not None:
                        # Process has terminated and no more output
                        break
                    if line:
                        line = line.rstrip("\n")
                        
                        # Determine log level
                        if "[ERROR]" in line or "Error" in line or "error" in line:
                            log_level = "ERROR"
                        elif "[WARNING]" in line or "Warning" in line:
                            log_level = "WARNING"
                        else:
                            log_level = "INFO"
                        
                        # Determine component
                        component = "main"
                        if "customer" in line.lower():
                            component = "customers"
                        elif "order" in line.lower() and "items" in line.lower():
                            component = "order_items"
                        elif "order" in line.lower():
                            component = "orders"
                        elif "product" in line.lower():
                            component = "products"
                        
                        self.log_message(job_id, log_level, line, store_config.get('MERCHANT', 'unknown'), component)
                
                process.wait()
                
                # Check if job status was already updated by the pipeline
                # The pipeline handles its own status updates in the finally block
                current_status = self.debug_job_status(job_id)
                latest_status = current_status[0].status if current_status else None
                
                # Only update status if pipeline didn't already update it
                if latest_status in ['pending', 'running']:
                    if process.returncode == 0:
                        self.update_job_status(job_id, "completed")
                        self.log_message(job_id, "INFO", "Historical load completed successfully", 
                                       store_config.get('MERCHANT', 'unknown'), "main")
                    else:
                        self.update_job_status(job_id, "failed", f"Process exited with code {process.returncode}")
                        self.log_message(job_id, "ERROR", f"Historical load failed with exit code {process.returncode}", 
                                       store_config.get('MERCHANT', 'unknown'), "main")
                else:
                    self.log_message(job_id, "INFO", f"Job already has terminal status: {latest_status}", 
                                   store_config.get('MERCHANT', 'unknown'), "main")
                    
            except Exception as e:
                error_msg = f"Exception during historical load: {str(e)}\n{traceback.format_exc()}"
                self.update_job_status(job_id, "failed", str(e))
                self.log_message(job_id, "ERROR", error_msg, store_config.get('MERCHANT'), "main")
        
        # Start background thread and store reference
        thread = threading.Thread(target=_run_in_background, daemon=True)
        thread.start()
        
        # Store thread reference for potential cancellation
        self._running_threads = getattr(self, '_running_threads', {})
        self._running_threads[job_id] = thread
        
        return thread
    
    def cancel_job(self, job_id):
        """Cancel a running job - simplified approach"""
        try:
            print(f"[DEBUG] Attempting to cancel job: {job_id}")
            
            # Get original job info first
            original_query = f"""
            SELECT store_url, dataset_name
            FROM `{self.project_id}.{self.jobs_dataset}.{self.jobs_table}`
            WHERE job_id = @job_id
            LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
                ]
            )
            original = list(self.client.query(original_query, job_config=job_config))
            
            store_url = original[0].store_url if original else "unknown"
            dataset_name = original[0].dataset_name if original else "unknown"
            
            # Insert a new cancelled status record
            from datetime import datetime, timezone
            cancel_data = {
                "job_id": job_id,
                "store_url": store_url,
                "dataset_name": dataset_name, 
                "job_type": "status_update",
                "status": "cancelled",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "error_message": "Cancelled by user",
                "created_by": "user_cancel"
            }
            
            table_id = f"{self.project_id}.{self.jobs_dataset}.{self.jobs_table}"
            errors = self.client.insert_rows_json(table_id, [cancel_data])
            
            if errors:
                print(f"[ERROR] Failed to insert cancel record: {errors}")
                return False
            
            # Log the cancellation
            self.log_message(job_id, "WARNING", "Job cancelled by user", component="main")
            
            # Try to kill the process if we have it tracked
            if hasattr(self, '_running_processes'):
                process = self._running_processes.get(job_id)
                if process and process.poll() is None:  # Process is still running
                    try:
                        process.terminate()
                        process.wait(timeout=2)
                        print(f"[DEBUG] Process terminated for job {job_id}")
                    except:
                        try:
                            process.kill()
                            print(f"[DEBUG] Process force killed for job {job_id}")
                        except:
                            pass
            
            print(f"[DEBUG] Job {job_id} cancelled successfully")
            return True
            
        except Exception as e:
            print(f"[ERROR] Error cancelling job {job_id}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def debug_job_status(self, job_id):
        """Debug method to see all records for a job"""
        query = f"""
        SELECT job_id, status, started_at, job_type
        FROM `{self.project_id}.{self.jobs_dataset}.{self.jobs_table}`
        WHERE job_id = @job_id
        ORDER BY started_at DESC
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
            ]
        )
        results = list(self.client.query(query, job_config=job_config))
        return results
    
    def force_clean_stuck_jobs(self, hours=1):
        """Force clean all stuck pending jobs older than X hours"""
        try:
            # Find stuck jobs
            query = f"""
            WITH job_timeline AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY started_at DESC) as rn
                FROM `{self.project_id}.{self.jobs_dataset}.{self.jobs_table}`
            ),
            latest_status AS (
                SELECT job_id, status, store_url, dataset_name, started_at
                FROM job_timeline
                WHERE rn = 1
            )
            SELECT job_id, store_url, dataset_name, started_at
            FROM latest_status
            WHERE status = 'pending'
                AND started_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
            """
            
            stuck_jobs = list(self.client.query(query))
            cleaned = 0
            
            for job in stuck_jobs:
                try:
                    # Insert failed status
                    fail_data = {
                        "job_id": job.job_id,
                        "store_url": job.store_url,
                        "dataset_name": job.dataset_name,
                        "job_type": "status_update",
                        "status": "failed",
                        "started_at": datetime.now(timezone.utc).isoformat(),
                        "error_message": f"Job timeout - stuck for over {hours} hours",
                        "created_by": "system_cleanup"
                    }
                    
                    table_id = f"{self.project_id}.{self.jobs_dataset}.{self.jobs_table}"
                    errors = self.client.insert_rows_json(table_id, [fail_data])
                    
                    if not errors:
                        cleaned += 1
                        print(f"[DEBUG] Cleaned stuck job: {job.job_id}")
                except Exception as e:
                    print(f"[ERROR] Failed to clean job {job.job_id}: {e}")
            
            return cleaned, len(stuck_jobs)
            
        except Exception as e:
            print(f"[ERROR] Error cleaning stuck jobs: {e}")
            return 0, 0