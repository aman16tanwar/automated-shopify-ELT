"""
Add this method to JobManager class to prevent duplicate jobs
"""

def has_active_job_for_store(self, store_url):
    """Check if there's already an active job for a store"""
    query = f"""
    WITH job_timeline AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY started_at DESC) as rn
        FROM `{self.project_id}.{self.jobs_dataset}.{self.jobs_table}`
    ),
    latest_status AS (
        SELECT job_id, status, store_url
        FROM job_timeline
        WHERE rn = 1
    )
    SELECT COUNT(*) as active_count
    FROM latest_status
    WHERE store_url = @store_url
      AND status IN ('pending', 'running')
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("store_url", "STRING", store_url),
        ]
    )
    
    try:
        result = list(self.client.query(query, job_config=job_config))
        return result[0].active_count > 0 if result else False
    except Exception as e:
        print(f"[ERROR] Failed to check active jobs: {e}")
        return False

# Then update the create_job method to check for duplicates:
def create_job(self, store_url, dataset_name, job_type="historical_load", created_by=None):
    """Create a new job record (with duplicate prevention)"""
    # Check if there's already an active job for this store
    if self.has_active_job_for_store(store_url):
        raise Exception(f"There is already an active job running for {store_url}. Please wait for it to complete or cancel it first.")
    
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