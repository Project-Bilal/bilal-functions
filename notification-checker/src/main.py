from datetime import datetime, timezone
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from appwrite.services.functions import Functions
import os
import json


# This Appwrite function will be executed every time your function is triggered
def main(context):
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
        # Query for notifications with timestampUTC equal to current_time
        notifications = databases.list_documents(
            database_id="projectbilal",
            collection_id="notifications",
            queries=[Query.equal("timestampUTC", current_time)],
        )
        if notifications["total"] > 0:
            functions = Functions(client)
            for notification in notifications["documents"]:
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
                except Exception as e:
                    context.error(
                        f"Failed to send notification {notification['$id']}: {str(e)}"
                    )

            return context.res.json(
                {"success": True, "total_notifications": notifications["total"]}
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
