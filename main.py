from fastapi import FastAPI, APIRouter, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import Optional
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

async def _fetch_and_process_dataframe(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """Internal helper function to fetch and process data into a DataFrame
    
    Args:
        start_date: Optional start date for filtering. If None, defaults to 7 days ago.
        end_date: Optional end date for filtering. If None, defaults to now.
    """
    limit = 3000

    # Use provided dates or default to last 7 days
    if end_date is None:
        # Default to current date/time
        completed_end_date = datetime.now(eastern_tz)
    else:
        completed_end_date = end_date
    
    if start_date is None:
        # Default to 7 days ago at start of day (00:00:00)
        seven_days_ago = datetime.now(eastern_tz) - timedelta(days=7)
        completed_start_date = seven_days_ago.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        completed_start_date = start_date

    # Convert datetime objects to ISO format strings for the API
    params = {
        "limit": limit,
        "completed_start_date": completed_start_date.isoformat() if isinstance(completed_start_date, datetime) else completed_start_date,
        "completed_end_date": completed_end_date.isoformat() if isinstance(completed_end_date, datetime) else completed_end_date,
        "use_case_id": os.getenv("USE-CASE-ID", "01978ea3-2dc9-7598-8c98-3ceb357e0020")
    }
    
    print(f"📅 API Request Date Range:")
    print(f"   Start: {params['completed_start_date']}")
    print(f"   End: {params['completed_end_date']}")

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
    
    # Filter to only include data within the specified date range
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['completed_at'] = pd.to_datetime(df['completed_at'])
    
    # Ensure completed_at is timezone-aware (assume UTC if naive)
    if df['completed_at'].dt.tz is None:
        df['completed_at'] = df['completed_at'].dt.tz_localize('UTC')
    # Convert to Eastern timezone for consistent comparison
    df['completed_at'] = df['completed_at'].dt.tz_convert(eastern_tz)
    
    # Ensure our comparison dates are also in Eastern timezone
    if completed_start_date.tzinfo is None:
        completed_start_date = eastern_tz.localize(completed_start_date)
    elif completed_start_date.tzinfo != eastern_tz:
        completed_start_date = completed_start_date.astimezone(eastern_tz)
    
    if completed_end_date.tzinfo is None:
        completed_end_date = eastern_tz.localize(completed_end_date)
    elif completed_end_date.tzinfo != eastern_tz:
        completed_end_date = completed_end_date.astimezone(eastern_tz)
    
    # Use completed_at for filtering since that's when the call actually finished
    # Filter to the date range specified (or default to last 7 days)
    # Use <= for end_date to include records up to and including the end date/time
    df_filtered = df[(df['completed_at'] >= completed_start_date) & (df['completed_at'] <= completed_end_date)]
    
    print(f"📊 Original data: {len(df)} records")
    print(f"📊 Filtered data: {len(df_filtered)} records")
    print(f"📅 Filter criteria - Start: {completed_start_date} (Eastern)")
    print(f"📅 Filter criteria - End: {completed_end_date} (Eastern)")
    if len(df) > 0:
        print(f"📅 Original data range: {df['completed_at'].min()} to {df['completed_at'].max()}")
    if len(df_filtered) > 0:
        print(f"📅 Filtered data range: {df_filtered['completed_at'].min()} to {df_filtered['completed_at'].max()}")
    else:
        print(f"⚠️ No data found in the specified date range")
    
    # Print all column names
    print(f"📋 All column names ({len(df_filtered.columns)} total):")
    for i, col in enumerate(df_filtered.columns, 1):
        print(f"  {i:2d}. {col}")
    
    return df_filtered

@api_router.get("/data")
async def get_data():
    """Get statistics about the data instead of raw records"""
    
    # Fetch and process the dataframe
    try:
        df_filtered = await _fetch_and_process_dataframe()
    except Exception as e:
        return {"error": f"Error fetching data: {str(e)}"}
    
    # Calculate statistics
    days_covered = 0
    if len(df_filtered) > 0:
        min_date = df_filtered['completed_at'].min()
        max_date = df_filtered['completed_at'].max()
        days_covered = (max_date - min_date).days + 1
    
    stats = {
        "total_records": len(df_filtered),
        "columns_count": len(df_filtered.columns),
        "date_range": {
            "start": df_filtered['completed_at'].min().isoformat() if len(df_filtered) > 0 else None,
            "end": df_filtered['completed_at'].max().isoformat() if len(df_filtered) > 0 else None,
            "days_covered": days_covered
        }
    }
    
    # Status breakdown
    if 'status' in df_filtered.columns:
        stats["status_breakdown"] = df_filtered['status'].value_counts().to_dict()
    
    # Scheduling status breakdown (if available)
    scheduling_col = "0199978b_1596_7369_b3cd_0d14190e9e93_response_classification"
    if scheduling_col in df_filtered.columns:
        scheduling_data = df_filtered[scheduling_col].dropna()
        stats["scheduling_status"] = {
            "total_with_status": len(scheduling_data),
            "breakdown": scheduling_data.value_counts().to_dict()
        }
    
    # Call end stage breakdown (if available)
    end_call_col = "0198e897_dff8_7049_822d_f10cbc15acb6_response_classification"
    if end_call_col in df_filtered.columns:
        end_stage_data = df_filtered[end_call_col].dropna()
        stats["call_end_stage"] = {
            "total_with_stage": len(end_stage_data),
            "breakdown": end_stage_data.value_counts().to_dict()
        }
    
    # Company breakdown (if available)
    company_col = "01987da1_cab7_777e_932b_9d36e1e5cf8a_company_name"
    if company_col in df_filtered.columns:
        company_data = df_filtered[company_col].dropna()
        stats["companies"] = {
            "unique_companies": len(company_data.unique()),
            "total_with_company": len(company_data),
            "top_companies": company_data.value_counts().head(10).to_dict()
        }
    
    # Client order numbers (if available)
    order_col = "01978ea3_2dda_70cb_a59a_6bac316e30db_data_orders_0_clientOrderNumber"
    if order_col in df_filtered.columns:
        order_data = df_filtered[order_col].dropna()
        stats["orders"] = {
            "total_orders": len(order_data),
            "unique_orders": len(order_data.unique()),
            "orders_with_duplicates": len(order_data) - len(order_data.unique())
        }
    
    # Calculate average calls per day
    if len(df_filtered) > 0 and days_covered > 0:
        stats["average_calls_per_day"] = round(len(df_filtered) / days_covered, 2)
    else:
        stats["average_calls_per_day"] = 0
    
    # Connected calls breakdown (scheduled vs transferred)
    end_call_col = "0198e897_dff8_7049_822d_f10cbc15acb6_response_classification"
    scheduling_col = "0199978b_1596_7369_b3cd_0d14190e9e93_response_classification"
    
    if end_call_col in df_filtered.columns and scheduling_col in df_filtered.columns:
        # Filter out calls that were "call_not_picked_up" (these are not connected)
        connected_calls = df_filtered[df_filtered[end_call_col].str.contains('call_not_picked_up', case=False, na=False) == False]
        total_connected = len(connected_calls)
        
        if total_connected > 0:
            # Get counts for each scheduling status
            status_counts = connected_calls[scheduling_col].value_counts().to_dict()
            
            # Categorize into scheduled and transferred
            scheduled_count = 0
            transferred_count = 0
            
            for status, count in status_counts.items():
                status_str = str(status).lower()
                if 'scheduled' in status_str and 'not' not in status_str:
                    scheduled_count += count
                elif 'transferred' in status_str:
                    transferred_count += count
            
            stats["connected_calls_summary"] = {
                "total_connected_calls": total_connected,
                "call_not_picked_up": len(df_filtered) - total_connected,
                "scheduled": {
                    "count": scheduled_count,
                    "percentage": round((scheduled_count / total_connected) * 100, 2)
                },
                "transferred": {
                    "count": transferred_count,
                    "percentage": round((transferred_count / total_connected) * 100, 2)
                }
            }
    
    return stats

@api_router.get("/data/raw")
async def get_data_raw():
    """Get raw data records (for use by other endpoints)"""
    
    try:
        df_filtered = await _fetch_and_process_dataframe()
    except Exception as e:
        return {"error": f"Error fetching data: {str(e)}"}
    
    # Handle NaN values for JSON serialization
    df_for_json = df_filtered.fillna("")  # Replace NaN with empty string
    
    return {
        "data": df_for_json.to_dict('records'),
        "shape": df_filtered.shape,
        "columns": df_filtered.columns.tolist()
    }

@api_router.get("/data/csv")
async def get_data_csv():
    """Get data as CSV download"""
    
    # Get the flattened data using the helper function
    try:
        df_filtered = await _fetch_and_process_dataframe()
    except Exception as e:
        return {"error": f"Error fetching data: {str(e)}"}
    
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
async def get_calls_per_week(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """Get metrics for calls per week and average per day"""
    
    print("🔍 Getting calls per week metrics...")
    
    # Get the flattened data
    try:
        df = await _fetch_and_process_dataframe(start_date, end_date)
    except Exception as e:
        error_msg = f"Error fetching data: {str(e)}"
        print(f"❌ {error_msg}")
        return {"error": error_msg}
    
    # Check if we got an error dict instead of a DataFrame
    if isinstance(df, dict) and "error" in df:
        print(f"❌ Error from data fetch: {df['error']}")
        return df
    
    print(f"📊 DataFrame shape: {df.shape}")
    print(f"📅 Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['completed_at'] = pd.to_datetime(df['completed_at'])
    
    # Group by week
    df['week'] = df['timestamp'].dt.to_period('W')
    weekly_counts = df.groupby('week').size().reset_index(name='calls_count')
    
    # Calculate average per day based on requested range or actual data range
    if start_date and end_date:
        # Use requested date range for calculation
        requested_start = start_date if start_date.tzinfo else eastern_tz.localize(start_date)
        requested_end = end_date if end_date.tzinfo else eastern_tz.localize(end_date)
        if requested_start.tzinfo != eastern_tz:
            requested_start = requested_start.astimezone(eastern_tz)
        if requested_end.tzinfo != eastern_tz:
            requested_end = requested_end.astimezone(eastern_tz)
        total_days = (requested_end - requested_start).days + 1
        date_range_start = requested_start.isoformat()
        date_range_end = requested_end.isoformat()
    else:
        # Use actual data range when no dates specified
        total_days = (df['timestamp'].max() - df['timestamp'].min()).days + 1
        date_range_start = df['timestamp'].min().isoformat()
        date_range_end = df['timestamp'].max().isoformat()
    
    avg_per_day = len(df) / total_days if total_days > 0 else 0
    
    result = {
        "total_calls": len(df),
        "date_range": {
            "start": date_range_start,
            "end": date_range_end,
            "total_days": total_days
        },
        "average_calls_per_day": round(avg_per_day, 2),
        "weekly_breakdown": weekly_counts.to_dict('records')
    }
    
    print(f"✅ Calls per week result: {result}")
    return result

@api_router.get("/metrics/scheduling-status")
async def get_scheduling_status(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """Get scheduling status breakdown for all calls"""
    
    print("🔍 Getting scheduling status metrics...")
    
    # Get the flattened data
    try:
        df = await _fetch_and_process_dataframe(start_date, end_date)
    except Exception as e:
        error_msg = f"Error fetching data: {str(e)}"
        print(f"❌ {error_msg}")
        return {"error": error_msg}
    
    # Check if we got an error dict instead of a DataFrame
    if isinstance(df, dict) and "error" in df:
        print(f"❌ Error from data fetch: {df['error']}")
        return df
    
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
async def get_scheduling_status_connected(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """Get scheduling status breakdown for calls that were NOT 'call_not_picked_up'"""
    
    # Get the flattened data
    try:
        df = await _fetch_and_process_dataframe(start_date, end_date)
    except Exception as e:
        return {"error": f"Error fetching data: {str(e)}"}
    
    # Check if we got an error dict instead of a DataFrame
    if isinstance(df, dict) and "error" in df:
        print(f"❌ Error from data fetch: {df['error']}")
        return df
    
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
async def get_exceptions_by_company(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """Get count of calls with 'exception' scheduling status grouped by company name"""
    
    # Get the flattened data
    try:
        df = await _fetch_and_process_dataframe(start_date, end_date)
    except Exception as e:
        return {"error": f"Error fetching data: {str(e)}"}
    
    # Check if we got an error dict instead of a DataFrame
    if isinstance(df, dict) and "error" in df:
        print(f"❌ Error from data fetch: {df['error']}")
        return df
    
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
async def get_call_end_stage_breakdown(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """Get breakdown of call end stage categories and their percentages"""
    
    print("🔍 Getting call end stage breakdown...")
    
    # Get the flattened data
    try:
        df = await _fetch_and_process_dataframe(start_date, end_date)
    except Exception as e:
        error_msg = f"Error fetching data: {str(e)}"
        print(f"❌ {error_msg}")
        return {"error": error_msg}
    
    # Check if we got an error dict instead of a DataFrame
    if isinstance(df, dict) and "error" in df:
        print(f"❌ Error from data fetch: {df['error']}")
        return df
    
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

@api_router.get("/columns")
async def get_column_info():
    """Get information about expected DataFrame columns without making API calls"""
    
    # These are the expected columns based on the code structure
    expected_columns = [
        "id",
        "status", 
        "org_id",
        "timestamp",
        "use_case_id",
        "version_id",
        "annotation",
        "completed_at",
        "0199978b_1596_7369_b3cd_0d14190e9e93_response_classification",  # scheduling status
        "0198e897_dff8_7049_822d_f10cbc15acb6_response_classification",  # call end stage
        "01987da1_cab7_777e_932b_9d36e1e5cf8a_company_name"  # company name
    ]
    
    print(f"📋 Expected DataFrame columns ({len(expected_columns)} total):")
    for i, col in enumerate(expected_columns, 1):
        print(f"  {i:2d}. {col}")
    
    return {
        "message": "Expected DataFrame columns",
        "total_columns": len(expected_columns),
        "columns": expected_columns,
        "note": "Additional columns may be present from the 'data' field in API response"
    }

@api_router.get("/sample-data/{column_name}")
async def get_sample_column_data(column_name: str):
    """Get sample values from a specific column"""
    
    print(f"🔍 Getting sample data for column: {column_name}")
    
    # Get the flattened data
    try:
        df = await _fetch_and_process_dataframe()
    except Exception as e:
        error_msg = f"Error fetching data: {str(e)}"
        print(f"❌ {error_msg}")
        return {"error": error_msg}
    
    # Check if we got an error dict instead of a DataFrame
    if isinstance(df, dict) and "error" in df:
        print(f"❌ Error from data fetch: {df['error']}")
        return df
    
    print(f"📊 DataFrame shape: {df.shape}")
    
    if column_name not in df.columns:
        print(f"❌ Column not found: {column_name}")
        print(f"Available columns: {df.columns.tolist()}")
        return {"error": f"Column not found: {column_name}"}
    
    # Get sample values (non-null, non-empty)
    column_data = df[column_name].dropna()
    column_data = column_data[column_data != ""]
    
    # Get unique values and their counts
    unique_values = column_data.value_counts().head(20)  # Top 20 most common values
    
    print(f"📋 Sample values from '{column_name}':")
    print(f"   Total non-null values: {len(column_data)}")
    print(f"   Unique values: {len(column_data.unique())}")
    print(f"   Top values:")
    for i, (value, count) in enumerate(unique_values.items(), 1):
        print(f"     {i:2d}. '{value}' (appears {count} times)")
    
    return {
        "column_name": column_name,
        "total_non_null_values": len(column_data),
        "unique_values_count": len(column_data.unique()),
        "sample_values": unique_values.to_dict(),
        "all_unique_values": column_data.unique().tolist()[:50]  # First 50 unique values
    }

@api_router.get("/metrics/summary")
async def get_all_metrics(
    start_date: Optional[str] = Query(None, description="Start date in ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS). Defaults to 7 days ago."),
    end_date: Optional[str] = Query(None, description="End date in ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS). Defaults to now.")
):
    """Get all metrics in one response
    
    Query Parameters:
        start_date: Optional start date (ISO format). Example: 2025-10-17 or 2025-10-17T00:00:00
        end_date: Optional end date (ISO format). Example: 2025-10-24 or 2025-10-24T23:59:59
    """
    
    # Parse dates if provided
    parsed_start_date = None
    parsed_end_date = None
    
    if start_date:
        try:
            # Try parsing with time first, then date only
            try:
                parsed_start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            except ValueError:
                # If only date provided (YYYY-MM-DD), set to start of day (00:00:00)
                parsed_start_date = datetime.strptime(start_date, '%Y-%m-%d')
                parsed_start_date = parsed_start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            # Ensure timezone awareness
            if parsed_start_date.tzinfo is None:
                parsed_start_date = eastern_tz.localize(parsed_start_date)
        except ValueError as e:
            return {"error": f"Invalid start_date format: {start_date}. Use ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"}
    
    if end_date:
        try:
            # Try parsing with time first, then date only
            try:
                parsed_end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            except ValueError:
                # If only date provided (YYYY-MM-DD), set to end of day (23:59:59)
                parsed_end_date = datetime.strptime(end_date, '%Y-%m-%d')
                parsed_end_date = parsed_end_date.replace(hour=23, minute=59, second=59, microsecond=0)
            # Ensure timezone awareness
            if parsed_end_date.tzinfo is None:
                parsed_end_date = eastern_tz.localize(parsed_end_date)
        except ValueError as e:
            return {"error": f"Invalid end_date format: {end_date}. Use ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"}
    
    # Debug: Print parsed dates
    print(f"📅 Summary endpoint date parsing:")
    print(f"   Input start_date: {start_date}")
    print(f"   Input end_date: {end_date}")
    print(f"   Parsed start_date: {parsed_start_date}")
    print(f"   Parsed end_date: {parsed_end_date}")
    
    # Call all metric functions with the parsed dates (or None for defaults)
    calls_per_week = await get_calls_per_week(parsed_start_date, parsed_end_date)
    scheduling_status = await get_scheduling_status(parsed_start_date, parsed_end_date)
    scheduling_connected = await get_scheduling_status_connected(parsed_start_date, parsed_end_date)
    exceptions_by_company = await get_exceptions_by_company(parsed_start_date, parsed_end_date)
    call_end_stage_breakdown = await get_call_end_stage_breakdown(parsed_start_date, parsed_end_date)
    
    return {
        "calls_per_week": calls_per_week,
        "scheduling_status_all_calls": scheduling_status,
        "scheduling_status_connected_calls": scheduling_connected,
        "exceptions_by_company": exceptions_by_company,
        "call_end_stage_breakdown": call_end_stage_breakdown,
        "date_range_used": {
            "start": parsed_start_date.isoformat() if parsed_start_date else None,
            "end": parsed_end_date.isoformat() if parsed_end_date else None
        }
    }

# Include the API router
app.include_router(api_router)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 3000))  # Default to port 3000, or use PORT env var
    uvicorn.run(app, host="0.0.0.0", port=port)

