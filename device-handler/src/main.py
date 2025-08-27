from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.exception import AppwriteException
from appwrite.query import Query
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

        # Extract required parameters from request
        device_id = request_data.get("device_id")
        operation = request_data.get(
            "operation", "delete"
        )  # Default to delete for backward compatibility

        if not device_id:
            return context.res.json(
                {"success": False, "error": "device_id is required"},
                400,
            )

        # Validate operation type
        if operation not in ["delete", "onboard", "status_update"]:
            return context.res.json(
                {
                    "success": False,
                    "error": "Operation must be either 'delete', 'onboard', or 'status_update'",
                },
                400,
            )

        # Initialize Appwrite client
        client = Client()
        client.set_endpoint("https://fra.cloud.appwrite.io/v1")
        client.set_project(os.environ["APPWRITE_FUNCTION_PROJECT_ID"])
        client.set_key(context.req.headers["x-appwrite-key"])

        databases = Databases(client)
        database_id = "projectbilal"

        if operation == "delete":
            return handle_device_deletion(context, databases, database_id, device_id)
        elif operation == "onboard":
            # For onboarding, user_id is still required
            user_id = request_data.get("user_id")
            if not user_id:
                return context.res.json(
                    {"success": False, "error": "user_id is required for onboarding"},
                    400,
                )
            return handle_device_onboarding(
                context, databases, database_id, device_id, user_id, request_data
            )
        elif operation == "status_update":
            return handle_device_status_update(
                context, databases, database_id, device_id, request_data
            )

    except Exception as e:
        return context.res.json(
            {"success": False, "error": f"Unexpected error: {str(e)}"}, 500
        )


def handle_device_deletion(context, databases, database_id, device_id):
    """Handle device deletion operation"""
    try:
        # Delete documents from timings collection where device_id matches
        try:
            timings_response = databases.list_documents(
                database_id=database_id,
                collection_id="timings",
                queries=[Query.equal("device_id", device_id)],
            )

            for document in timings_response["documents"]:
                databases.delete_document(
                    database_id=database_id,
                    collection_id="timings",
                    document_id=document["$id"],
                )
        except AppwriteException as e:
            # Log error but continue with other operations
            context.log(f"Error deleting from timings collection: {e}")

        # Delete documents from notifications collection where device_id matches
        try:
            notifications_response = databases.list_documents(
                database_id=database_id,
                collection_id="notifications",
                queries=[Query.equal("device_id", device_id)],
            )

            for document in notifications_response["documents"]:
                databases.delete_document(
                    database_id=database_id,
                    collection_id="notifications",
                    document_id=document["$id"],
                )
        except AppwriteException as e:
            # Log error but continue with other operations
            context.log(f"Error deleting from notifications collection: {e}")

        # Update the device document (don't delete, just update fields)
        try:
            # First, find the device document
            device_response = databases.list_documents(
                database_id=database_id,
                collection_id="devices",
                queries=[Query.equal("device_id", device_id)],
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
                        "speaker_name": "",
                        "status": "offline",
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
                "message": f"Device {device_id} successfully deleted",
                "deleted_timings": True,
                "deleted_notifications": True,
                "device_updated": True,
            }
        )

    except Exception as e:
        return context.res.json(
            {"success": False, "error": f"Error during device deletion: {str(e)}"}, 500
        )


def handle_device_onboarding(
    context, databases, database_id, device_id, user_id, request_data
):
    """Handle device onboarding operation"""
    try:
        # Extract onboarding parameters from request data
        device_name = request_data.get("device_name", "New Device")
        latitude = request_data.get("latitude")
        longitude = request_data.get("longitude")
        method = request_data.get("method", "2")
        midnight_mode = request_data.get("midnight_mode", "0")
        school = request_data.get("school", "0")
        speaker_name = request_data.get("speaker_name", "")

        # TODO: Add device validation logic here
        # - Check if device_id is valid format
        # - Verify device is not already claimed by another user
        # - Validate device capabilities/permissions

        # TODO: Add user validation logic here
        # - Check if user_id is valid
        # - Verify user has permission to claim devices
        # - Check user's device limit if applicable

        # TODO: Add device configuration logic here
        # - Set default prayer times
        # - Configure timezone settings
        # - Set up initial notification preferences

        # Check if device already exists
        try:
            device_response = databases.list_documents(
                database_id=database_id,
                collection_id="devices",
                queries=[Query.equal("device_id", device_id)],
            )

            if device_response["documents"]:
                # Device exists, update it
                device_doc = device_response["documents"][0]

                # TODO: Add conflict resolution logic here
                # - Check if device is already claimed by another user
                # - Handle device transfer scenarios

                databases.update_document(
                    database_id=database_id,
                    collection_id="devices",
                    document_id=device_doc["$id"],
                    data={
                        "user_id": user_id,
                        "name": device_name,
                        "latitude": latitude,
                        "longitude": longitude,
                        "method": method,
                        "midnight_mode": midnight_mode,
                        "school": school,
                        "speaker_name": speaker_name,
                        "enabled": True,
                        "ip_address": "0.0.0.0",  # Will be updated when device connects
                        "port": None,  # Will be updated when device connects
                    },
                )
            else:
                # Device doesn't exist, create it
                # TODO: Add device creation logic here
                # - Validate device_id format
                # - Set up initial device configuration

                databases.create_document(
                    database_id=database_id,
                    collection_id="devices",
                    document_id=device_id,
                    data={
                        "device_id": device_id,
                        "user_id": user_id,
                        "name": device_name,
                        "latitude": latitude,
                        "longitude": longitude,
                        "method": method,
                        "midnight_mode": midnight_mode,
                        "school": school,
                        "speaker_name": speaker_name,
                        "enabled": True,
                        "ip_address": "0.0.0.0",
                        "port": None,
                    },
                )

        except AppwriteException as e:
            return context.res.json(
                {
                    "success": False,
                    "error": f"Error during device onboarding: {str(e)}",
                },
                500,
            )

        # Create 6 new documents in the timings collection for each prayer
        prayer_names = ["Fajr", "Dhuhr", "Asr", "Maghrib", "Sunset", "Isha"]

        try:
            for prayer_name in prayer_names:
                databases.create_document(
                    database_id=database_id,
                    collection_id="timings",
                    data={
                        "notification": prayer_name,
                        "device_id": device_id,
                        "enabled": False,
                        "volume": "0.5",
                        "user_id": user_id,
                    },
                )
        except AppwriteException as e:
            context.log(f"Error creating timing documents: {e}")
            # Continue with onboarding even if timing creation fails

        return context.res.json(
            {
                "success": True,
                "message": f"Device {device_id} successfully onboarded",
                "device_id": device_id,
                "user_id": user_id,
                "device_name": device_name,
                "onboarded": True,
                "timings_created": True,
            }
        )

    except Exception as e:
        return context.res.json(
            {"success": False, "error": f"Error during device onboarding: {str(e)}"},
            500,
        )


def handle_device_status_update(
    context, databases, database_id, device_id, request_data
):
    """Handle device status update operation"""
    try:
        status = request_data.get("status")
        if not status:
            return context.res.json(
                {"success": False, "error": "Status is required for status update"}, 400
            )

        # Update the device document
        try:
            device_response = databases.list_documents(
                database_id=database_id,
                collection_id="devices",
                queries=[Query.equal("device_id", device_id)],
            )

            if device_response["documents"]:
                device_doc = device_response["documents"][0]
                databases.update_document(
                    database_id=database_id,
                    collection_id="devices",
                    document_id=device_doc["$id"],
                    data={"status": status},
                )
                return context.res.json(
                    {
                        "success": True,
                        "message": f"Device {device_id} status updated to {status}",
                        "device_updated": True,
                    }
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
                {"success": False, "error": f"Error updating device status: {str(e)}"},
                500,
            )

    except Exception as e:
        return context.res.json(
            {"success": False, "error": f"Error during device status update: {str(e)}"},
            500,
        )
