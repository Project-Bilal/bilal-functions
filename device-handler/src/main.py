from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.exception import AppwriteException
from appwrite.query import Query
import json
import os
import time
import urllib.request

NTFY_BASE = os.environ.get("NTFY_BASE_URL", "http://34.53.103.114")


def ntfy_alert(message: str, topic: str = "projectbilal-errors", title: str = "Project Bilal", priority: int = None, tags: str = None):
    url = f"{NTFY_BASE}/{topic}"
    try:
        req = urllib.request.Request(url, data=message.encode(), method="POST")
        req.add_header("Title", title)
        if priority:
            req.add_header("Priority", str(priority))
        if tags:
            req.add_header("Tags", tags)
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # Never break main flow


def _document_to_plain_dict(doc):
    """Normalize Appwrite Document models (Pydantic) or dicts to a flat dict."""
    if isinstance(doc, dict):
        return doc
    to_dict = getattr(doc, "to_dict", None)
    if callable(to_dict):
        full = to_dict()
        inner = full.pop("data", None)
        if isinstance(inner, dict):
            return {**inner, **full}
        return full
    return doc


def _doclist_documents(doclist):
    if hasattr(doclist, "documents"):
        raw = doclist.documents
    elif isinstance(doclist, dict):
        raw = doclist.get("documents", [])
    else:
        raw = []
    return [_document_to_plain_dict(d) for d in raw]


def _retry_appwrite(fn, *args, max_attempts=3, **kwargs):
    """Retry an Appwrite SDK call on transient errors (503, connection reset, SSL)."""
    delays = [1, 2]
    for attempt in range(max_attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            err_str = str(e).lower()
            is_transient = any(marker in err_str for marker in [
                "503", "connection reset", "ssl", "first byte timeout", "unexpected eof",
            ])
            if isinstance(e, KeyError) and "content-type" in err_str:
                is_transient = True
            if is_transient and attempt < max_attempts - 1:
                time.sleep(delays[attempt])
                continue
            raise


def _list_all_documents(databases, database_id, collection_id, queries=None):
    """Fetch all documents, paginating through Appwrite's 25-doc default limit."""
    all_docs = []
    limit = 100
    offset = 0
    while True:
        q = list(queries or [])
        q.append(Query.limit(limit))
        q.append(Query.offset(offset))
        result = _retry_appwrite(
            databases.list_documents,
            database_id=database_id,
            collection_id=collection_id,
            queries=q,
        )
        docs = _doclist_documents(result)
        all_docs.extend(docs)
        if len(docs) < limit:
            break
        offset += limit
    return all_docs


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
        if operation not in [
            "delete",
            "onboard",
            "status_update",
            "disable_with_cleanup",
            "refresh_ip",
        ]:
            return context.res.json(
                {
                    "success": False,
                    "error": "Operation must be one of: delete, onboard, status_update, disable_with_cleanup, refresh_ip",
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
            # For onboarding, user_id can be null for unclaimed devices
            user_id = request_data.get("user_id")
            # Allow null user_id for unclaimed devices
            return handle_device_onboarding(
                context, databases, database_id, device_id, user_id, request_data
            )
        elif operation == "status_update":
            return handle_device_status_update(
                context, databases, database_id, device_id, request_data
            )
        elif operation == "disable_with_cleanup":
            return handle_device_disable_with_cleanup(
                context, databases, database_id, device_id
            )
    except Exception as e:
        ntfy_alert(f"[device-handler] Unhandled error: {e}", priority=4, tags="warning")
        return context.res.json(
            {"success": False, "error": f"Unexpected error: {str(e)}"}, 500
        )


def handle_device_deletion(context, databases, database_id, device_id):
    """Handle device deletion operation"""
    device_name = device_id  # Fallback to MAC if name not found
    try:
        # Batch delete timings for this device
        try:
            _retry_appwrite(
                databases.delete_documents,
                database_id=database_id,
                collection_id="timings",
                queries=[Query.equal("device_id", device_id)],
            )
        except Exception:
            pass  # Continue with other operations

        # Batch delete notifications for this device
        try:
            _retry_appwrite(
                databases.delete_documents,
                database_id=database_id,
                collection_id="notifications",
                queries=[Query.equal("device_id", device_id)],
            )
        except Exception:
            pass  # Continue with other operations

        # Update the device document (don't delete, just update fields)
        try:
            device_documents = _list_all_documents(
                databases,
                database_id=database_id,
                collection_id="devices",
                queries=[Query.equal("device_id", device_id)],
            )
            if device_documents:
                device_doc = device_documents[0]
                device_name = device_doc.get("name", device_id)

                # Update the device document with the specified values
                _retry_appwrite(
                    databases.update_document,
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
                        "timezone": None,
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
            ntfy_alert(f"[device-handler] Delete failed for {device_id}: {e}", priority=4, tags="warning")
            return context.res.json(
                {"success": False, "error": f"Error updating device: {str(e)}"}, 500
            )

        ntfy_alert(
            f'[device-handler] Device "{device_name}" ({device_id}) removed',
            topic="projectbilal-events",
            priority=2,
            tags="wastebasket",
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
        ntfy_alert(f"[device-handler] Delete failed for {device_id}: {e}", priority=4, tags="warning")
        return context.res.json(
            {"success": False, "error": f"Error during device deletion: {str(e)}"}, 500
        )


def handle_device_onboarding(
    context, databases, database_id, device_id, user_id, request_data
):
    """Handle device onboarding operation"""
    try:
        context.log(f"=== Starting device onboarding ===")
        context.log(f"Device ID: {device_id}")
        context.log(f"User ID: {user_id}")
        context.log(f"Operation: onboard")

        # Extract onboarding parameters from request data
        device_name = request_data.get("device_name", "New Device")
        latitude = request_data.get("latitude")
        longitude = request_data.get("longitude")
        method = request_data.get("method", "2")
        midnight_mode = request_data.get("midnight_mode", "0")
        school = request_data.get("school", "0")
        speaker_name = request_data.get("speaker_name", "")

        context.log(f"Device name: {device_name}")

        # Device validation, user validation, and configuration logic
        # would be implemented here in a production environment

        # Check if device already exists
        try:
            context.log(f"Checking if device {device_id} already exists in database")
            device_documents = _list_all_documents(
                databases,
                database_id=database_id,
                collection_id="devices",
                queries=[Query.equal("device_id", device_id)],
            )
            context.log(f"Found {len(device_documents)} existing device(s)")

            if device_documents:
                # Device exists, allow re-claiming by any user
                # Physical access (BLE connection + factory reset) = ownership rights
                # This enables device transfer, family sharing, and simplified onboarding
                device_doc = device_documents[0]
                context.log(
                    f"Device exists - updating existing device document: {device_doc['$id']}"
                )
                context.log(
                    f"Current user_id: {device_doc.get('user_id')}, New user_id: {user_id}"
                )

                # Preserve existing status if it's "online", otherwise set to "pending"
                current_status = device_doc.get("status", "offline")
                new_status = "online" if current_status == "online" else "pending"
                context.log(f"Status transition: {current_status} -> {new_status}")

                _retry_appwrite(
                    databases.update_document,
                    database_id=database_id,
                    collection_id="devices",
                    document_id=device_doc["$id"],
                    data={
                        "user_id": user_id,
                        "name": device_name,
                        "status": new_status,
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
                context.log(f"Device doesn't exist - creating new device document")
                # New devices start as "pending" since they haven't connected yet
                _retry_appwrite(
                    databases.create_document,
                    database_id=database_id,
                    collection_id="devices",
                    document_id=device_id,
                    data={
                        "device_id": device_id,
                        "user_id": user_id,
                        "name": device_name,
                        "status": "pending",
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
            ntfy_alert(f"[device-handler] Onboarding failed for {device_id}: {e}", priority=4, tags="warning")
            return context.res.json(
                {
                    "success": False,
                    "error": f"Error during device onboarding: {str(e)}",
                },
                500,
            )

        # Always delete existing timing documents during re-onboarding
        # This prevents conflicts regardless of whether device is being claimed or not
        prayer_names = ["Fajr", "Dhuhr", "Asr", "Maghrib", "Isha"]
        context.log(f"Starting timing document cleanup for device: {device_id}")

        deleted_count = 0
        for prayer_name in prayer_names:
            try:
                timing_doc_id = f"{device_id}_{prayer_name.lower()}"
                _retry_appwrite(
                    databases.delete_document,
                    database_id=database_id,
                    collection_id="timings",
                    document_id=timing_doc_id,
                )
                deleted_count += 1
                context.log(f"Deleted existing timing: {timing_doc_id}")
            except AppwriteException as e:
                # Document doesn't exist, that's fine - continue
                context.log(
                    f"No existing timing for {prayer_name} (this is normal for new devices)"
                )

        context.log(f"Deleted {deleted_count} existing timing documents")

        # Only CREATE new timings if device is being claimed (user_id is not null)
        timings_created = False
        if user_id is not None:
            context.log(f"Creating fresh timing documents for user: {user_id}")
            try:
                for prayer_name in prayer_names:
                    timing_doc_id = f"{device_id}_{prayer_name.lower()}"
                    _retry_appwrite(
                        databases.create_document,
                        database_id=database_id,
                        collection_id="timings",
                        document_id=timing_doc_id,
                        data={
                            "notification": prayer_name,
                            "device_id": device_id,
                            "enabled": True,
                            "volume": "0.5",
                            "user_id": user_id,
                            "audio_id": "https://storage.googleapis.com/athans/athan_1.mp3",
                            "audio_name": "Mishary Al Afasy Kuwait",
                            "reminder": "15",
                            "reminder_audio_id": "https://storage.googleapis.com/athans/Salat_Ibrahimiyya.mp3",
                            "reminder_audio_name": "Salat Ibrahimiyya",
                            "reminder_enabled": True,
                        },
                    )
                    context.log(f"Created timing document: {timing_doc_id}")
                timings_created = True
                context.log(f"Successfully created all 5 timing documents")
            except AppwriteException as e:
                # Continue with onboarding even if timing creation fails
                context.error(f"Failed to create timing documents: {str(e)}")
                pass
        else:
            context.log(
                "Skipping timing creation - device being onboarded as unclaimed"
            )

        context.log(f"=== Onboarding completed successfully ===")
        context.log(f"Timings created: {timings_created}")

        ntfy_alert(
            f"[device-handler] Device \"{device_name}\" ({device_id}) onboarded",
            topic="projectbilal-events",
            priority=2,
            tags="electric_plug",
        )
        response_data = {
            "success": True,
            "message": f"Device {device_id} successfully onboarded",
            "device_id": device_id,
            "user_id": user_id,
            "device_name": device_name,
            "onboarded": True,
            "timings_created": timings_created,
        }
        # Include API key for ESP32 provisioning when device is being claimed
        if user_id is not None:
            appwrite_key = os.environ.get("APPWRITE_API_KEY")
            context.log(f"APPWRITE_API_KEY present: {appwrite_key is not None}")
            if appwrite_key:
                response_data["appwrite_key"] = appwrite_key
        return context.res.json(response_data)

    except Exception as e:
        context.error(f"=== Onboarding failed ===")
        context.error(f"Error: {str(e)}")
        context.error(f"Error type: {type(e).__name__}")
        import traceback

        context.error(f"Traceback: {traceback.format_exc()}")
        ntfy_alert(f"[device-handler] Onboarding failed for {device_id}: {e}", priority=4, tags="warning")

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
        firmware_version = request_data.get("firmware_version")

        if not status:
            return context.res.json(
                {"success": False, "error": "Status is required for status update"}, 400
            )

        # Prepare update data
        update_data = {"status": status}
        if firmware_version:
            update_data["firmware"] = firmware_version

        # Update the device document
        try:
            device_documents = _list_all_documents(
                databases,
                database_id=database_id,
                collection_id="devices",
                queries=[Query.equal("device_id", device_id)],
            )
            if device_documents:
                device_doc = device_documents[0]
                _retry_appwrite(
                    databases.update_document,
                    database_id=database_id,
                    collection_id="devices",
                    document_id=device_doc["$id"],
                    data=update_data,
                )

                message = f"Device {device_id} status updated to {status}"
                if firmware_version:
                    message += f" (firmware: {firmware_version})"

                return context.res.json(
                    {
                        "success": True,
                        "message": message,
                        "device_updated": True,
                        "firmware_updated": firmware_version is not None,
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



def handle_device_disable_with_cleanup(context, databases, database_id, device_id):
    """Handle device disable with notification cleanup"""
    try:
        # Batch delete notifications for this device
        try:
            _retry_appwrite(
                databases.delete_documents,
                database_id=database_id,
                collection_id="notifications",
                queries=[Query.equal("device_id", device_id)],
            )
        except Exception:
            pass  # Continue with device update

        # Set device to disabled
        try:
            device_documents = _list_all_documents(
                databases,
                database_id=database_id,
                collection_id="devices",
                queries=[Query.equal("device_id", device_id)],
            )
            if device_documents:
                device_doc = device_documents[0]
                _retry_appwrite(
                    databases.update_document,
                    database_id=database_id,
                    collection_id="devices",
                    document_id=device_doc["$id"],
                    data={"enabled": False},
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
                "message": f"Device {device_id} disabled and notifications cleaned up",
                "notifications_deleted": True,
                "device_disabled": True,
            }
        )

    except Exception as e:
        return context.res.json(
            {"success": False, "error": f"Error during device disable: {str(e)}"}, 500
        )
