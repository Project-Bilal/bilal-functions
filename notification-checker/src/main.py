from datetime import datetime, timezone
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from appwrite.services.functions import Functions
import os
import json
import requests

# Configuration - Prayer Time API
PRAYER_TIME_API_BASE_URL = "https://api-aladhan-com-1k5h.onrender.com"


# This Appwrite function will be executed every time your function is triggered
def main(context):
    # Health check - hit the API endpoint first
    health_check_url = f"{PRAYER_TIME_API_BASE_URL}/v1/timings/18-09-2025?latitude=47.618962&longitude=-122.337647"
    try:
        health_response = requests.get(health_check_url, timeout=10)
        health_response.raise_for_status()
        context.log("✅ Health check passed - API is accessible")
    except Exception as e:
        context.log(f"⚠️ Health check failed: {str(e)}")
        # Continue execution even if health check fails

    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")

    # Initialize Appwrite client
    client = (
        Client()
        .set_project(os.environ["APPWRITE_FUNCTION_PROJECT_ID"])
        .set_key(context.req.headers["x-appwrite-key"])
        .set_endpoint("https://fra.cloud.appwrite.io/v1")  # Your API Endpoint
    )

    databases = Databases(client)

    try:
        # Query for enabled notifications with timestampUTC equal to current_time
        notifications = databases.list_documents(
            database_id="projectbilal",
            collection_id="notifications",
            queries=[
                Query.equal("timestampUTC", current_time),
                Query.equal("enabled", True),
            ],
        )
        if notifications["total"] > 0:
            functions = Functions(client)
            processed_count = 0
            for notification in notifications["documents"]:
                # Skip disabled notifications (safety check)
                if not notification.get("enabled", True):
                    context.log(
                        f"Skipping disabled notification: {notification['$id']}"
                    )
                    continue

                # Prepare notification data
                notification_data = {
                    "device_id": notification["device_id"],
                    "timestampUTC": notification["timestampUTC"],
                    "ip_address": notification["ip_address"],
                    "port": notification["port"],
                    "audio_id": notification["audio_id"],
                    "volume": notification["volume"],
                }

                # Send notification to device
                try:
                    functions.create_execution(
                        function_id="invoke-notification",
                        body=json.dumps(notification_data),
                    )
                    context.log(f"Sent notification to device: {notification['$id']}")
                    processed_count += 1
                except Exception as e:
                    context.error(
                        f"Failed to send notification {notification['$id']}: {str(e)}"
                    )

            return context.res.json(
                {
                    "success": True,
                    "total_notifications": notifications["total"],
                    "processed_notifications": processed_count,
                }
            )

        return context.res.json(
            {
                "success": True,
                "current_time": current_time,
                "total_notifications": notifications["total"],
                "notifications": notifications["documents"],
            }
        )

    except Exception as e:
        return context.res.json(
            {"success": False, "error": str(e), "current_time": current_time}, 500
        )
