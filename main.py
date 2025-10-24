from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import asyncio
from datetime import datetime, timedelta
import logging
import requests
import os
from dotenv import load_dotenv
import pandas as pd
import json
import io

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="RLM Analytics API", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create API router
api_router = APIRouter(prefix="/api")

# Initialize scheduler
scheduler = AsyncIOScheduler()

# Eastern timezone
eastern_tz = pytz.timezone('US/Eastern')

async def friday_job():
    """Job that runs every Friday at 9 AM Eastern time"""
    current_time = datetime.now(eastern_tz)
    logger.info(f"Friday job executed at {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Add your business logic here
    # For example:
    # - Pull data from external APIs
    # - Process analytics
    # - Send reports
    # - Update databases
    
    print(f"🚀 Friday Analytics Job Running at {current_time}")
    # Simulate some work
    await asyncio.sleep(1)
    print("✅ Friday job completed successfully!")

@app.on_event("startup")
async def startup_event():
    """Start the scheduler when the application starts"""
    # Schedule job for every Friday at 9 AM Eastern time
    scheduler.add_job(
        friday_job,
        trigger=CronTrigger(
            day_of_week='fri',  # Friday
            hour=9,             # 9 AM
            minute=0,           # 0 minutes
            timezone=eastern_tz
        ),
        id='friday_analytics_job',
        name='Friday Analytics Job',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("Scheduler started - Friday job scheduled for 9 AM Eastern time")

@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown the scheduler when the application stops"""
    scheduler.shutdown()
    logger.info("Scheduler stopped")

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "RLM Analytics API",
        "status": "running",
        "scheduled_jobs": len(scheduler.get_jobs())
    }

@api_router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(eastern_tz).isoformat(),
        "scheduler_running": scheduler.running
    }

@api_router.get("/jobs")
async def get_jobs():
    """Get information about scheduled jobs"""
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger)
        })
    return {"jobs": jobs}

@api_router.post("/trigger-job")
async def trigger_job():
    """Manually trigger the Friday job for testing"""
    await friday_job()
    return {"message": "Job triggered successfully"}

@api_router.get("/data")
async def get_data():
    """Get data from the database and return as flattened DataFrame"""

    limit = 3000

    completed_start_date = datetime.now(eastern_tz) - timedelta(days=7)
    completed_end_date = datetime.now(eastern_tz)

    params = {
        "limit": limit,
        "completed_start_date": completed_start_date,
        "completed_end_date": completed_end_date,
        "use_case_id": "01978ea3-2dc9-7598-8c98-3ceb357e0020"
    }

    url = "https://platform.happyrobot.ai/api/v1/runs"

    headers = {
        "authorization": f"Bearer {os.getenv("API-KEY")}",
        "x-organization-id": os.getenv("X-ORGANIZATION-ID")
    }

    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        return {"error": f"API request failed with status {response.status_code}"}
    
    data = response.json()
    
    # Flatten the nested data structure
    flattened_data = []
    
    for record in data:
        # Start with the main record fields (excluding the nested 'data' field)
        flat_record = {
            'id': record.get('id'),
            'status': record.get('status'),
            'org_id': record.get('org_id'),
            'timestamp': record.get('timestamp'),
            'use_case_id': record.get('use_case_id'),
            'version_id': record.get('version_id'),
            'annotation': record.get('annotation'),
            'completed_at': record.get('completed_at')
        }
        
        # Flatten the nested 'data' field
        if 'data' in record and record['data']:
            for key, value in record['data'].items():
                # Clean up the key names for better readability
                clean_key = key.replace('.', '_').replace('-', '_')
                flat_record[clean_key] = value
        
        flattened_data.append(flat_record)
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(flattened_data)
    
    # Filter to only include data from the last 7 days
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['completed_at'] = pd.to_datetime(df['completed_at'])
    
    # Use completed_at for filtering since that's when the call actually finished
    cutoff_date = datetime.now(eastern_tz) - timedelta(days=7)
    df_filtered = df[df['completed_at'] >= cutoff_date]
    
    print(f"📊 Original data: {len(df)} records")
    print(f"📊 Filtered data (last 7 days): {len(df_filtered)} records")
    print(f"📅 Cutoff date: {cutoff_date}")
    print(f"📅 Date range after filtering: {df_filtered['completed_at'].min()} to {df_filtered['completed_at'].max()}")
    
    # Convert DataFrame to JSON for API response
    return {
        "data": df_filtered.to_dict('records'),
        "shape": df_filtered.shape,
        "columns": df_filtered.columns.tolist(),
        "summary": {
            "total_records": len(df_filtered),
            "original_records": len(df),
            "columns_count": len(df_filtered.columns),
            "date_range": {
                "start": df_filtered['completed_at'].min().isoformat() if len(df_filtered) > 0 else None,
                "end": df_filtered['completed_at'].max().isoformat() if len(df_filtered) > 0 else None
            }
        }
    }

@api_router.get("/data/csv")
async def get_data_csv():
    """Get data as CSV download"""
    
    # Get the flattened data (reuse the logic from get_data)
    limit = 3000
    completed_start_date = datetime.now(eastern_tz) - timedelta(days=7)
    completed_end_date = datetime.now(eastern_tz)

    params = {
        "limit": limit,
        "completed_start_date": completed_start_date,
        "completed_end_date": completed_end_date,
        "use_case_id": "01978ea3-2dc9-7598-8c98-3ceb357e0020"
    }

    url = "https://platform.happyrobot.ai/api/v1/runs"
    headers = {
        "authorization": f"Bearer {os.getenv("API-KEY")}",
        "x-organization-id": os.getenv("X-ORGANIZATION-ID")
    }

    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        return {"error": f"API request failed with status {response.status_code}"}
    
    data = response.json()
    
    # Flatten the data
    flattened_data = []
    for record in data:
        flat_record = {
            'id': record.get('id'),
            'status': record.get('status'),
            'org_id': record.get('org_id'),
            'timestamp': record.get('timestamp'),
            'use_case_id': record.get('use_case_id'),
            'version_id': record.get('version_id'),
            'annotation': record.get('annotation'),
            'completed_at': record.get('completed_at')
        }
        
        if 'data' in record and record['data']:
            for key, value in record['data'].items():
                clean_key = key.replace('.', '_').replace('-', '_')
                flat_record[clean_key] = value
        
        flattened_data.append(flat_record)
    
    # Create DataFrame and filter to last 7 days
    df = pd.DataFrame(flattened_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['completed_at'] = pd.to_datetime(df['completed_at'])
    
    # Filter to only include data from the last 7 days
    cutoff_date = datetime.now(eastern_tz) - timedelta(days=7)
    df_filtered = df[df['completed_at'] >= cutoff_date]
    
    # Create CSV in memory using filtered data
    csv_buffer = io.StringIO()
    df_filtered.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()
    csv_buffer.close()
    
    # Return as downloadable CSV
    return StreamingResponse(
        io.BytesIO(csv_content.encode('utf-8')),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=rlm_analytics_data.csv"}
    )

@api_router.get("/metrics/calls-per-week")
async def get_calls_per_week():
    """Get metrics for calls per week and average per day"""
    
    print("🔍 Getting calls per week metrics...")
    
    # Get the flattened data
    data_response = await get_data()
    if "error" in data_response:
        print(f"❌ Error getting data: {data_response}")
        return data_response
    
    df = pd.DataFrame(data_response["data"])
    print(f"📊 DataFrame shape: {df.shape}")
    print(f"📅 Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['completed_at'] = pd.to_datetime(df['completed_at'])
    
    # Group by week
    df['week'] = df['timestamp'].dt.to_period('W')
    weekly_counts = df.groupby('week').size().reset_index(name='calls_count')
    
    # Calculate average per day
    total_days = (df['timestamp'].max() - df['timestamp'].min()).days + 1
    avg_per_day = len(df) / total_days if total_days > 0 else 0
    
    result = {
        "total_calls": len(df),
        "date_range": {
            "start": df['timestamp'].min().isoformat(),
            "end": df['timestamp'].max().isoformat(),
            "total_days": total_days
        },
        "average_calls_per_day": round(avg_per_day, 2),
        "weekly_breakdown": weekly_counts.to_dict('records')
    }
    
    print(f"✅ Calls per week result: {result}")
    return result

@api_router.get("/metrics/scheduling-status")
async def get_scheduling_status():
    """Get scheduling status breakdown for all calls"""
    
    print("🔍 Getting scheduling status metrics...")
    
    # Get the flattened data
    data_response = await get_data()
    if "error" in data_response:
        print(f"❌ Error getting data: {data_response}")
        return data_response
    
    df = pd.DataFrame(data_response["data"])
    print(f"📊 DataFrame shape: {df.shape}")
    
    # Use the specific scheduling status column
    scheduling_col = "0199978b_1596_7369_b3cd_0d14190e9e93_response_classification"
    print(f"📋 Using scheduling column: {scheduling_col}")
    
    if scheduling_col not in df.columns:
        print(f"❌ Scheduling column not found: {scheduling_col}")
        print(f"Available columns: {df.columns.tolist()}")
        return {"error": f"Scheduling column not found: {scheduling_col}"}
    
    # Get counts for each status
    status_counts = df[scheduling_col].value_counts().to_dict()
    print(f"📊 Raw status counts: {status_counts}")
    
    # Categorize into buckets
    buckets = {
        "scheduled": 0,
        "not_scheduled": 0,
        "exception": 0,
        "transferred": 0
    }
    
    for status, count in status_counts.items():
        status_str = str(status).lower()
        print(f"🔍 Processing status: '{status}' -> '{status_str}'")
        if 'scheduled' in status_str and 'not' not in status_str:
            buckets["scheduled"] += count
            print(f"  ✅ Categorized as 'scheduled'")
        elif 'not_scheduled' in status_str:
            buckets["not_scheduled"] += count
            print(f"  ❌ Categorized as 'not_scheduled'")
        elif 'transferred' in status_str:
            buckets["transferred"] += count
            print(f"  🔄 Categorized as 'transferred'")
        else:
            buckets["exception"] += count
            print(f"  ⚠️ Categorized as 'exception'")
    
    result = {
        "total_calls": len(df),
        "scheduling_column_used": scheduling_col,
        "raw_status_counts": status_counts,
        "categorized_buckets": buckets,
        "percentages": {
            bucket: round((count / len(df)) * 100, 2) 
            for bucket, count in buckets.items()
        }
    }
    
    print(f"✅ Scheduling status result: {result}")
    return result

@api_router.get("/metrics/scheduling-status-connected")
async def get_scheduling_status_connected():
    """Get scheduling status breakdown for calls that were NOT 'call_not_picked_up'"""
    
    # Get the flattened data
    data_response = await get_data()
    if "error" in data_response:
        return data_response
    
    df = pd.DataFrame(data_response["data"])
    print(f"📊 DataFrame shape: {df.shape}")
    
    # Use the specific call end stage column
    end_call_col = "0198e897_dff8_7049_822d_f10cbc15acb6_response_classification"
    print(f"📋 Using call end stage column: {end_call_col}")
    
    if end_call_col not in df.columns:
        print(f"❌ Call end stage column not found: {end_call_col}")
        print(f"Available columns: {df.columns.tolist()}")
        return {"error": f"Call end stage column not found: {end_call_col}"}
    
    # Filter out calls that were "call_not_picked_up"
    connected_calls = df[df[end_call_col].str.contains('call_not_picked_up', case=False, na=False) == False]
    print(f"📞 Connected calls: {len(connected_calls)} out of {len(df)} total calls")
    
    # Use the specific scheduling status column
    scheduling_col = "0199978b_1596_7369_b3cd_0d14190e9e93_response_classification"
    print(f"📋 Using scheduling column: {scheduling_col}")
    
    if scheduling_col not in connected_calls.columns:
        print(f"❌ Scheduling column not found: {scheduling_col}")
        return {"error": f"Scheduling column not found: {scheduling_col}"}
    
    # Get counts for each status
    status_counts = connected_calls[scheduling_col].value_counts().to_dict()
    
    # Categorize into buckets
    buckets = {
        "scheduled": 0,
        "not_scheduled": 0,
        "exception": 0,
        "transferred": 0
    }
    
    for status, count in status_counts.items():
        status_str = str(status).lower()
        if 'scheduled' in status_str and 'not' not in status_str:
            buckets["scheduled"] += count
        elif 'not_scheduled' in status_str:
            buckets["not_scheduled"] += count
        elif 'transferred' in status_str:
            buckets["transferred"] += count
        else:
            buckets["exception"] += count
    
    return {
        "total_calls": len(df),
        "connected_calls": len(connected_calls),
        "call_not_picked_up_calls": len(df) - len(connected_calls),
        "end_call_stage_column_used": end_call_col,
        "scheduling_column_used": scheduling_col,
        "raw_status_counts": status_counts,
        "categorized_buckets": buckets,
        "percentages": {
            bucket: round((count / len(connected_calls)) * 100, 2) 
            for bucket, count in buckets.items()
        }
    }

@api_router.get("/metrics/exceptions-by-company")
async def get_exceptions_by_company():
    """Get count of calls with 'exception' scheduling status grouped by company name"""
    
    # Get the flattened data
    data_response = await get_data()
    if "error" in data_response:
        return data_response
    
    df = pd.DataFrame(data_response["data"])
    print(f"📊 DataFrame shape: {df.shape}")
    
    # Use the specific scheduling status column
    scheduling_col = "0199978b_1596_7369_b3cd_0d14190e9e93_response_classification"
    print(f"📋 Using scheduling column: {scheduling_col}")
    
    if scheduling_col not in df.columns:
        print(f"❌ Scheduling column not found: {scheduling_col}")
        return {"error": f"Scheduling column not found: {scheduling_col}"}
    
    # Use the specific company name column
    company_col = "01987da1_cab7_777e_932b_9d36e1e5cf8a_company_name"
    print(f"🏢 Using company column: {company_col}")
    
    if company_col not in df.columns:
        print(f"❌ Company column not found: {company_col}")
        return {"error": f"Company column not found: {company_col}"}
    
    # Filter for calls with 'exception' status
    exception_calls = df[df[scheduling_col].str.contains('exception', case=False, na=False)]
    
    # Group by company name and count
    company_counts = exception_calls[company_col].value_counts().reset_index()
    company_counts.columns = ['company_name', 'exception_count']
    
    # Calculate percentages
    total_exceptions = len(exception_calls)
    company_counts['percentage'] = (company_counts['exception_count'] / total_exceptions * 100).round(2)
    
    return {
        "total_exception_calls": total_exceptions,
        "total_calls": len(df),
        "exception_rate": round((total_exceptions / len(df)) * 100, 2),
        "scheduling_column_used": scheduling_col,
        "company_column_used": company_col,
        "exceptions_by_company": company_counts.to_dict('records')
    }

@api_router.get("/metrics/call-end-stage-breakdown")
async def get_call_end_stage_breakdown():
    """Get breakdown of call end stage categories and their percentages"""
    
    print("🔍 Getting call end stage breakdown...")
    
    # Get the flattened data
    data_response = await get_data()
    if "error" in data_response:
        print(f"❌ Error getting data: {data_response}")
        return data_response
    
    df = pd.DataFrame(data_response["data"])
    print(f"📊 DataFrame shape: {df.shape}")
    
    # Use the specific call end stage column
    end_call_col = "0198e897_dff8_7049_822d_f10cbc15acb6_response_classification"
    print(f"📋 Using call end stage column: {end_call_col}")
    
    if end_call_col not in df.columns:
        print(f"❌ Call end stage column not found: {end_call_col}")
        print(f"Available columns: {df.columns.tolist()}")
        return {"error": f"Call end stage column not found: {end_call_col}"}
    
    # Get counts for each call end stage
    end_stage_counts = df[end_call_col].value_counts().to_dict()
    print(f"📊 Raw call end stage counts: {end_stage_counts}")
    
    # Calculate percentages
    total_calls = len(df)
    end_stage_percentages = {
        stage: round((count / total_calls) * 100, 2) 
        for stage, count in end_stage_counts.items()
    }
    
    # Sort by count (descending)
    sorted_stages = sorted(end_stage_counts.items(), key=lambda x: x[1], reverse=True)
    
    result = {
        "total_calls": total_calls,
        "call_end_stage_column_used": end_call_col,
        "call_end_stage_breakdown": [
            {
                "stage": stage,
                "count": count,
                "percentage": end_stage_percentages[stage]
            }
            for stage, count in sorted_stages
        ],
        "raw_counts": end_stage_counts,
        "percentages": end_stage_percentages
    }
    
    print(f"✅ Call end stage breakdown result: {result}")
    return result

@api_router.get("/metrics/summary")
async def get_all_metrics():
    """Get all metrics in one response"""
    
    calls_per_week = await get_calls_per_week()
    scheduling_status = await get_scheduling_status()
    scheduling_connected = await get_scheduling_status_connected()
    exceptions_by_company = await get_exceptions_by_company()
    call_end_stage_breakdown = await get_call_end_stage_breakdown()
    
    return {
        "calls_per_week": calls_per_week,
        "scheduling_status_all_calls": scheduling_status,
        "scheduling_status_connected_calls": scheduling_connected,
        "exceptions_by_company": exceptions_by_company,
        "call_end_stage_breakdown": call_end_stage_breakdown
    }

# Include the API router
app.include_router(api_router)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 3000))  # Default to port 3000, or use PORT env var
    uvicorn.run(app, host="0.0.0.0", port=port)

