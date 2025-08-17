"""
Cloud-ready API for Shopify client onboarding
Deployable to Cloud Run or Cloud Functions
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, validator
import os
import sys
import re
from typing import Optional
from datetime import datetime

# Add scripts to path
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts'))
from onboard_client import ShopifyClientOnboarding

app = FastAPI(
    title="Shopify Client Onboarding API",
    description="Automated onboarding service for Shopify clients",
    version="1.0.0"
)

class OnboardingRequest(BaseModel):
    client_id: str
    client_name: str
    merchant_url: str
    access_token: str
    contact_email: str
    memory: str = "2Gi"
    run_initial_load: bool = True
    
    @validator('client_id')
    def validate_client_id(cls, v):
        if not re.match(r'^[a-z0-9_]+$', v):
            raise ValueError('Client ID must contain only lowercase letters, numbers, and underscores')
        return v
    
    @validator('merchant_url')
    def validate_merchant_url(cls, v):
        if not v.endswith('.myshopify.com'):
            raise ValueError('Merchant URL must end with .myshopify.com')
        return v
    
    @validator('access_token')
    def validate_token(cls, v):
        if not v.startswith('shpat_'):
            raise ValueError('Access token must start with shpat_')
        return v
    
    @validator('memory')
    def validate_memory(cls, v):
        if v not in ['2Gi', '4Gi', '8Gi']:
            raise ValueError('Memory must be 2Gi, 4Gi, or 8Gi')
        return v

class OnboardingResponse(BaseModel):
    status: str
    message: str
    client_id: str
    dataset_name: str
    service_name: Optional[str] = None
    started_at: str
    completed_at: Optional[str] = None

# In-memory status tracking (use Redis or Firestore in production)
onboarding_status = {}

@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "shopify-onboarding-api",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/onboard", response_model=OnboardingResponse)
async def onboard_client(request: OnboardingRequest, background_tasks: BackgroundTasks):
    """
    Onboard a new Shopify client
    
    This endpoint validates the request and triggers the onboarding process
    in the background, returning immediately with a tracking ID.
    """
    
    # Check if client already being onboarded
    if request.client_id in onboarding_status:
        status = onboarding_status[request.client_id]
        if status['status'] == 'in_progress':
            raise HTTPException(
                status_code=409,
                detail=f"Client {request.client_id} is already being onboarded"
            )
    
    # Initialize status
    onboarding_status[request.client_id] = {
        'status': 'in_progress',
        'started_at': datetime.now().isoformat(),
        'request': request.dict()
    }
    
    # Add background task
    background_tasks.add_task(
        run_onboarding,
        request
    )
    
    return OnboardingResponse(
        status="accepted",
        message="Onboarding process started",
        client_id=request.client_id,
        dataset_name=f"shopify_{request.client_id}",
        started_at=datetime.now().isoformat()
    )

@app.get("/status/{client_id}")
async def get_onboarding_status(client_id: str):
    """Check the status of an onboarding process"""
    
    if client_id not in onboarding_status:
        raise HTTPException(
            status_code=404,
            detail=f"No onboarding found for client {client_id}"
        )
    
    return onboarding_status[client_id]

@app.get("/clients")
async def list_clients():
    """List all clients and their onboarding status"""
    
    # In production, read from database
    config_path = "../configs/store_configs.json"
    if os.path.exists(config_path):
        import json
        with open(config_path, 'r') as f:
            configs = json.load(f)
        return {
            "clients": configs,
            "total": len(configs)
        }
    
    return {"clients": [], "total": 0}

async def run_onboarding(request: OnboardingRequest):
    """Background task to run the actual onboarding"""
    
    client_id = request.client_id
    project_id = os.getenv("GCP_PROJECT_ID", "happyweb-340014")
    
    try:
        # Create onboarding instance
        onboarding = ShopifyClientOnboarding(project_id=project_id)
        
        # Run onboarding
        success = onboarding.onboard_client(
            client_id=request.client_id,
            merchant_url=request.merchant_url,
            token=request.access_token,
            memory=request.memory,
            run_initial=request.run_initial_load
        )
        
        # Update status
        onboarding_status[client_id]['status'] = 'completed' if success else 'failed'
        onboarding_status[client_id]['completed_at'] = datetime.now().isoformat()
        
        # Save client info if successful
        if success:
            save_client_info(request)
            
    except Exception as e:
        onboarding_status[client_id]['status'] = 'failed'
        onboarding_status[client_id]['error'] = str(e)
        onboarding_status[client_id]['completed_at'] = datetime.now().isoformat()

def save_client_info(request: OnboardingRequest):
    """Save client information to persistent storage"""
    
    # In production, use Firestore or CloudSQL
    config_path = "../configs/client_registry.json"
    
    client_info = {
        "client_id": request.client_id,
        "client_name": request.client_name,
        "contact_email": request.contact_email,
        "merchant_url": request.merchant_url,
        "onboarded_at": datetime.now().isoformat(),
        "dataset_name": f"shopify_{request.client_id}",
        "active": True
    }
    
    # Load existing registry
    if os.path.exists(config_path):
        import json
        with open(config_path, 'r') as f:
            registry = json.load(f)
    else:
        registry = []
    
    # Add new client
    registry.append(client_info)
    
    # Save back
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, 'w') as f:
        json.dump(registry, f, indent=2)

# For Cloud Functions deployment
def onboard_client_function(request):
    """Cloud Function entry point"""
    import asyncio
    from fastapi import Request
    
    # Parse request
    request_json = request.get_json()
    
    # Create FastAPI request object
    onboarding_request = OnboardingRequest(**request_json)
    
    # Run async function
    loop = asyncio.new_event_loop()
    response = loop.run_until_complete(
        onboard_client(onboarding_request, BackgroundTasks())
    )
    
    return response.dict()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)