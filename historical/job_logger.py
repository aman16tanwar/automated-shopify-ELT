# job_logger.py
"""
Logger utility for pipeline jobs
"""

import os
import sys
from datetime import datetime

class JobLogger:
    def __init__(self):
        self.job_id = os.environ.get("PIPELINE_JOB_ID")
        self.job_manager = None
        
        if self.job_id:
            try:
                # Add parent directory to path to import job_manager
                parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                sys.path.insert(0, parent_dir)
                from onboarding.job_manager import JobManager
                self.job_manager = JobManager()
            except Exception as e:
                print(f"Warning: Could not initialize job manager: {e}")
    
    def log(self, level, message, store_url=None, component=None):
        """Log a message both to console and BigQuery if job_id is set"""
        # Always print to console
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{level}] {message}", flush=True)  # Force flush
        
        # Also log to BigQuery if we have a job_id
        if self.job_id and self.job_manager:
            try:
                self.job_manager.log_message(
                    job_id=self.job_id,
                    log_level=level,
                    message=message,
                    store_url=store_url,
                    component=component
                )
            except Exception as e:
                print(f"Warning: Could not log to BigQuery: {e}", flush=True)
    
    def info(self, message, store_url=None, component=None):
        self.log("INFO", message, store_url, component)
    
    def warning(self, message, store_url=None, component=None):
        self.log("WARNING", message, store_url, component)
    
    def error(self, message, store_url=None, component=None):
        self.log("ERROR", message, store_url, component)
    
    def update_job_status(self, status, error_message=None, records_processed=None):
        """Update job status if job_id is set"""
        print(f"[DEBUG] Attempting to update job status: job_id={self.job_id}, status={status}", flush=True)
        
        if not self.job_id:
            print("[WARNING] No job_id set, cannot update status", flush=True)
            return
            
        if not self.job_manager:
            print("[WARNING] Job manager not initialized, cannot update status", flush=True)
            return
            
        try:
            self.job_manager.update_job_status(
                job_id=self.job_id,
                status=status,
                error_message=error_message,
                records_processed=records_processed
            )
            print(f"[SUCCESS] Job status updated to: {status}", flush=True)
        except Exception as e:
            print(f"[ERROR] Failed to update job status: {e}", flush=True)
            import traceback
            print(f"[TRACE] {traceback.format_exc()}", flush=True)