from datetime import datetime, timezone
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from appwrite.services.functions import Functions
import os
import json
import time


# This Appwrite function will be executed every time your function is triggered
def main(context):
    start_time = time.time()
    context.log(
        f"🚀 Notification checker started at {datetime.now(timezone.utc).isoformat()}"
    )

    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")

    # Initialize Appwrite client
    client_init_start = time.time()
    client = (
        Client()
        .set_project(os.environ["APPWRITE_FUNCTION_PROJECT_ID"])
        .set_key(context.req.headers["x-appwrite-key"])
        .set_endpoint("https://fra.cloud.appwrite.io/v1")  # Your API Endpoint
    )

    databases = Databases(client)
    client_init_time = time.time() - client_init_start
    context.log(f"⏱️ Client initialization took {client_init_time:.3f}s")

    try:
        # Query for enabled notifications with timestampUTC equal to current_time
        query_start = time.time()
        notifications = databases.list_documents(
            database_id="projectbilal",
            collection_id="notifications",
            queries=[
                Query.equal("timestampUTC", current_time),
                Query.equal("enabled", True),
            ],
        )
        query_time = time.time() - query_start
        context.log(
            f"⏱️ Database query took {query_time:.3f}s, found {notifications['total']} notifications"
        )

        if notifications["total"] > 0:
            functions = Functions(client)
            processed_count = 0
            notification_start = time.time()
            context.log(
                f"🔄 Starting to process {len(notifications['documents'])} notifications..."
            )

            for i, notification in enumerate(notifications["documents"]):
                notification_item_start = time.time()
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
                    function_start = time.time()
                    context.log(
                        f"📤 Invoking invoke-notification for notification {i+1}/{len(notifications['documents'])}: {notification['$id']}"
                    )

                    functions.create_execution(
                        function_id="invoke-notification",
                        body=json.dumps(notification_data),
                    )

                    function_time = time.time() - function_start
                    total_time = time.time() - notification_item_start
                    context.log(
                        f"✅ Notification {i+1} completed in {function_time:.3f}s (total: {total_time:.3f}s)"
                    )
                    processed_count += 1
                except Exception as e:
                    function_time = time.time() - function_start
                    context.error(
                        f"❌ Failed to send notification {notification['$id']} after {function_time:.3f}s: {str(e)}"
                    )

            notification_total_time = time.time() - notification_start
            total_execution_time = time.time() - start_time
            context.log(
                f"🏁 All notifications processed in {notification_total_time:.3f}s (total execution: {total_execution_time:.3f}s)"
            )

            return context.res.json(
                {
                    "success": True,
                    "total_notifications": notifications["total"],
                    "processed_notifications": processed_count,
                    "execution_time_seconds": total_execution_time,
                    "notification_processing_time_seconds": notification_total_time,
                }
            )

        total_execution_time = time.time() - start_time
        context.log(
            f"ℹ️ No notifications found for {current_time} (execution time: {total_execution_time:.3f}s)"
        )

        return context.res.json(
            {
                "success": True,
                "current_time": current_time,
                "total_notifications": notifications["total"],
                "notifications": notifications["documents"],
                "execution_time_seconds": total_execution_time,
            }
        )

    except Exception as e:
        return context.res.json(
            {"success": False, "error": str(e), "current_time": current_time}, 500
        )
