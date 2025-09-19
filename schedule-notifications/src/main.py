from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from datetime import datetime, timezone, timedelta
import requests
import os
import json
from collections import defaultdict

# Configuration - Prayer Time API
PRAYER_TIME_API_BASE_URL = "https://api-aladhan-com-1k5h.onrender.com"


def get_utc_times(timestamp_str, mins=0):
    """Convert local ISO 8601 time string to UTC and compute reminder time if needed."""
    local_time = datetime.fromisoformat(timestamp_str)
    utc_time = local_time.astimezone(timezone.utc)
    main_time = utc_time.strftime("%Y-%m-%dT%H:%M")
    reminder_time = (
        (utc_time - timedelta(minutes=mins)).strftime("%Y-%m-%dT%H:%M")
        if mins > 0
        else None
    )
    return main_time, reminder_time


def init_appwrite_client(context):
    """Initialize Appwrite client from environment and context."""
    return (
        Client()
        .set_project(os.environ["APPWRITE_FUNCTION_PROJECT_ID"])
        .set_key(context.req.headers["x-appwrite-key"])
    )


def fetch_enabled_devices(databases):
    """Fetch all enabled devices."""
    return databases.list_documents(
        database_id="projectbilal",
        collection_id="devices",
        queries=[Query.equal("enabled", True)],
    )["documents"]


def fetch_enabled_timings(databases):
    """Fetch all enabled timings (for all devices)."""
    result = databases.list_documents(
        database_id="projectbilal",
        collection_id="timings",
        queries=[Query.equal("enabled", True)],
    )
    return result["documents"]


def group_timings_by_device(timings):
    """Group timing documents by device_id."""
    grouped = defaultdict(list)
    for timing in timings:
        device_id = timing.get("device_id")
        if device_id:
            grouped[device_id].append(timing)
    return grouped


def build_device_object(device, timings):
    """Build a clean dictionary representation of a device with its timings."""
    return {
        "device_id": device.get("device_id"),
        "latitude": device.get("latitude"),
        "longitude": device.get("longitude"),
        "method": device.get("method"),
        "midnight_mode": device.get("midnight_mode"),
        "school": device.get("school"),
        "ip_address": device.get("ip_address"),
        "port": device.get("port"),
        "timings": [
            {
                "timing_id": t.get("$id"),  # Include the timing document ID
                "notification": t.get("notification"),
                "audio_id": t.get("audio_id"),
                "reminder": t.get("reminder"),
                "reminder_audio_id": t.get("reminder_audio_id"),
                "reminder_enabled": t.get(
                    "reminder_enabled", True
                ),  # Include reminder_enabled field
                "volume": t.get("volume"),
                "user_id": t.get("user_id"),
            }
            for t in timings
        ],
    }


def delete_existing_notifications(databases, device_ids):
    """Delete all existing notifications for the given device IDs."""
    try:
        # Delete notifications for each device
        for device_id in device_ids:
            databases.delete_documents(
                database_id="projectbilal",
                collection_id="notifications",
                queries=[Query.equal("device_id", device_id)],
            )
        print(f"Deleted existing notifications for {len(device_ids)} devices")
    except Exception as e:
        print(f"Error deleting existing notifications: {str(e)}")


def fetch_prayer_time(date_str, lat, lon, method, context):
    """Fetch prayer timings from Prayer Time API."""
    url = f"{PRAYER_TIME_API_BASE_URL}/v1/timings/{date_str}?latitude={lat}&longitude={lon}&method={method}&school={school}&iso8601=true"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["data"]["timings"]


def build_notifications_for_device(device, date_str, context):
    """Build notification payloads for a single device."""
    notifications = []

    # Check for required fields before making API call
    required_fields = ["latitude", "longitude", "method"]
    for field in required_fields:
        if not device.get(field):
            context.log(
                f"Device {device.get('device_id', 'unknown')} missing {field}, skipping"
            )
            return notifications

    try:
        timings = fetch_prayer_time(
            date_str, device["latitude"], device["longitude"], device["method"], context
        )
    except Exception as e:
        context.error(
            f"Failed to fetch prayer times for device {device['device_id']}: {str(e)}"
        )
        return notifications  # Return empty list to skip

    for timing in device["timings"]:
        prayer_name = timing.get("notification")
        if not prayer_name or prayer_name not in timings:
            continue

        time_str = timings.get(prayer_name)
        reminder = int(timing.get("reminder") or 0)
        utc_time, utc_time_rem = get_utc_times(time_str, reminder)

        # Always calculate reminder time, even if reminder is 0
        # If reminder is 0, utc_time_rem will be the same as utc_time
        if reminder == 0:
            utc_time_rem = utc_time

        # Main notification
        notifications.append(
            {
                "device_id": device["device_id"],
                "timestampUTC": utc_time,
                "ip_address": device.get("ip_address"),
                "port": device.get("port"),
                "audio_id": timing["audio_id"],
                "volume": timing["volume"],
                "user_id": timing["user_id"],
                "timing_id": timing["timing_id"],
                "type": "notification",
                "enabled": True,
            }
        )

        # Reminder notification - always create, but set enabled based on reminder_enabled
        notifications.append(
            {
                "device_id": device["device_id"],
                "timestampUTC": utc_time_rem,
                "ip_address": device.get("ip_address"),
                "port": device.get("port"),
                "audio_id": timing["reminder_audio_id"],
                "volume": timing["volume"],
                "user_id": timing["user_id"],
                "timing_id": timing["timing_id"],
                "type": "reminder",
                "enabled": timing.get("reminder_enabled", True),
            }
        )

    return notifications


def main(context):
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%d-%m-%Y")

    client = init_appwrite_client(context)
    databases = Databases(client)

    try:
        # Check if specific device_id is provided
        request_data = context.req.body_json if context.req.body else {}
        target_device_id = request_data.get("device_id")

        if target_device_id:
            # Process only the specified device
            devices = databases.list_documents(
                database_id="projectbilal",
                collection_id="devices",
                queries=[
                    Query.equal("enabled", True),
                    Query.equal("device_id", target_device_id),
                ],
            )["documents"]
        else:
            # Process all devices (current behavior)
            devices = fetch_enabled_devices(databases)

        timings = fetch_enabled_timings(databases)
        context.log(f"Found {len(devices)} devices and {len(timings)} timings")

        # Group timings by device_id
        timings_by_device = group_timings_by_device(timings)

        devices_with_timings = []
        all_notifications = []

        for device in devices:
            device_id = device.get("device_id")
            if not device_id:
                continue

            device_timings = timings_by_device.get(device_id, [])
            if not device_timings:
                continue

            device_obj = build_device_object(device, device_timings)
            devices_with_timings.append(device_obj)

            notifications = build_notifications_for_device(
                device_obj, date_str, context
            )
            all_notifications.extend(notifications)

        context.log(f"Prepared {len(all_notifications)} notifications")

        if all_notifications:
            # Get unique device IDs from notifications
            device_ids = list(
                set(notification["device_id"] for notification in all_notifications)
            )

            # Delete existing notifications for these devices
            delete_existing_notifications(databases, device_ids)

            try:
                databases.upsert_documents(
                    database_id="projectbilal",
                    collection_id="notifications",
                    documents=all_notifications,
                )
            except Exception as e:
                context.error(f"Failed to upsert notifications: {str(e)}")

        return context.res.json(
            {
                "success": True,
                "total_devices": len(devices_with_timings),
                "all_notifications": all_notifications,
            }
        )

    except Exception as e:
        context.error(f"Unhandled error: {str(e)}")
        return context.res.json({"success": False, "error": str(e)}, 500)
