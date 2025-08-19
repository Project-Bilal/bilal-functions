from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.exception import AppwriteException
import json
import os


def main(context):
    try:
        # Parse the request body
        if not context.req.body:
            return context.res.json(
                {"success": False, "error": "Request body is required"}, 400
            )

        # Parse JSON from request body
        try:
            request_data = json.loads(context.req.body)
        except json.JSONDecodeError:
            return context.res.json(
                {"success": False, "error": "Invalid JSON in request body"}, 400
            )

        # Extract user_id and device_id from request
        user_id = request_data.get("user_id")
        device_id = request_data.get("device_id")

        if not user_id or not device_id:
            return context.res.json(
                {"success": False, "error": "Both user_id and device_id are required"},
                400,
            )

        # Initialize Appwrite client
        client = Client()
        client.set_endpoint("https://fra.cloud.appwrite.io/v1")
        client.set_project(os.environ["APPWRITE_FUNCTION_PROJECT_ID"])
        client.set_key(context.req.headers["x-appwrite-key"])

        databases = Databases(client)
        database_id = "projectbilal"

        # Delete documents from timings collection where device_id matches
        try:
            timings_response = databases.list_documents(
                database_id=database_id,
                collection_id="timings",
                queries=[f'device_id="{device_id}"'],
            )

            for document in timings_response["documents"]:
                databases.delete_document(
                    database_id=database_id,
                    collection_id="timings",
                    document_id=document["$id"],
                )
        except AppwriteException as e:
            # Log error but continue with other operations
            print(f"Error deleting from timings collection: {e}")

        # Delete documents from notifications collection where device_id matches
        try:
            notifications_response = databases.list_documents(
                database_id=database_id,
                collection_id="notifications",
                queries=[f'device_id="{device_id}"'],
            )

            for document in notifications_response["documents"]:
                databases.delete_document(
                    database_id=database_id,
                    collection_id="notifications",
                    document_id=document["$id"],
                )
        except AppwriteException as e:
            # Log error but continue with other operations
            print(f"Error deleting from notifications collection: {e}")

        # Update the device document (don't delete, just update fields)
        try:
            # First, find the device document
            device_response = databases.list_documents(
                database_id=database_id,
                collection_id="devices",
                queries=[f'device_id="{device_id}"'],
            )

            if device_response["documents"]:
                device_doc = device_response["documents"][0]

                # Update the device document with the specified values
                databases.update_document(
                    database_id=database_id,
                    collection_id="devices",
                    document_id=device_doc["$id"],
                    data={
                        "latitude": None,
                        "longitude": None,
                        "method": "2",
                        "midnight_mode": "0",
                        "school": "0",
                        "ip_address": "0.0.0.0",
                        "enabled": False,
                        "port": None,
                        "user_id": None,
                        "name": "unclaimed_device",
                    },
                )
            else:
                return context.res.json(
                    {
                        "success": False,
                        "error": f"Device with device_id {device_id} not found",
                    },
                    404,
                )

        except AppwriteException as e:
            return context.res.json(
                {"success": False, "error": f"Error updating device: {str(e)}"}, 500
            )

        return context.res.json(
            {
                "success": True,
                "message": f"Device {device_id} successfully processed",
                "deleted_timings": True,
                "deleted_notifications": True,
                "device_updated": True,
            }
        )

    except Exception as e:
        return context.res.json(
            {"success": False, "error": f"Unexpected error: {str(e)}"}, 500
        )
