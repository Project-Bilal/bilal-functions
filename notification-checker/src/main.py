from datetime import datetime, timezone
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
import os


# This Appwrite function will be executed every time your function is triggered
def main(context):
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")

    # Initialize Appwrite client
    client = (
        Client()
        .set_project(os.environ["APPWRITE_FUNCTION_PROJECT_ID"])
        .set_key(context.req.headers["x-appwrite-key"])
    )

    databases = Databases(client)

    try:
        # Query for notifications with timestampUTC equal to current_time
        notifications = databases.list_documents(
            database_id="projectbilal",
            collection_id="notifications",
            queries=[Query.equal("timestampUTC", current_time)],
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
