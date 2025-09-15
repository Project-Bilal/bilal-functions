from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from datetime import datetime, timezone, timedelta
import requests
import os
import json
from collections import defaultdict


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
    """Fetch prayer timings from Aladhan API."""
    url = f"https://api.aladhan.com/v1/timings/{date_str}?latitude={lat}&longitude={lon}&method={method}&iso8601=true"
    context.log(f"🌐 Making API call to: {url}")
    context.log(f"⏱️ API call started at: {datetime.now(timezone.utc).isoformat()}")
    
    try:
        response = requests.get(url, timeout=30)  # Increased timeout to 30 seconds
        context.log(f"⏱️ API call completed at: {datetime.now(timezone.utc).isoformat()}")
        context.log(f"📊 API response status: {response.status_code}")
        response.raise_for_status()
        context.log("✅ API call successful")
        return response.json()["data"]["timings"]
    except requests.exceptions.Timeout:
        context.error("⏰ API call timed out after 30 seconds")
        raise
    except requests.exceptions.RequestException as e:
        context.error(f"🌐 API request failed: {str(e)}")
        raise


def build_notifications_for_device(device, date_str, context):
    """Build notification payloads for a single device."""
    device_id = device.get('device_id', 'unknown')
    context.log(f"🔨 Building notifications for device: {device_id}")
    notifications = []

    # Check for required fields before making API call
    required_fields = ["latitude", "longitude", "method"]
    for field in required_fields:
        if not device.get(field):
            context.log(f"⚠️ Device {device_id} missing {field}, skipping")
            return notifications

    context.log(f"🌐 Fetching prayer times for device {device_id}...")
    try:
        timings = fetch_prayer_time(
            date_str, device["latitude"], device["longitude"], device["method"], context
        )
        context.log(f"✅ Successfully fetched prayer times for device {device_id}")
    except Exception as e:
        context.error(f"❌ Failed to fetch prayer times for device {device_id}: {str(e)}")
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
    context.log("🚀 Starting schedule-notifications function")
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%d-%m-%Y")
    context.log(f"📅 Processing date: {date_str}")

    context.log("🔧 Initializing Appwrite client...")
    client = init_appwrite_client(context)
    databases = Databases(client)
    context.log("✅ Appwrite client initialized")

    try:
        # Check if specific device_id is provided
        context.log("📥 Parsing request body...")
        request_data = context.req.body_json if context.req.body else {}
        target_device_id = request_data.get("device_id")
        context.log(f"🎯 Target device ID: {target_device_id}")

        if target_device_id:
            # Process only the specified device
            context.log(f"🔍 Fetching specific device: {target_device_id}")
            devices = databases.list_documents(
                database_id="projectbilal",
                collection_id="devices",
                queries=[
                    Query.equal("enabled", True),
                    Query.equal("device_id", target_device_id),
                ],
            )["documents"]
            context.log(f"✅ Found {len(devices)} specific devices")
        else:
            # Process all devices (current behavior)
            context.log("🔍 Fetching all enabled devices...")
            devices = fetch_enabled_devices(databases)
            context.log(f"✅ Found {len(devices)} enabled devices")

        context.log("🔍 Fetching enabled timings...")
        timings = fetch_enabled_timings(databases)
        context.log(f"✅ Found {len(timings)} enabled timings")

        # Group timings by device_id
        context.log("📊 Grouping timings by device...")
        timings_by_device = group_timings_by_device(timings)
        context.log(f"✅ Grouped timings for {len(timings_by_device)} devices")

        devices_with_timings = []
        all_notifications = []

        context.log("🔄 Processing devices and building notifications...")
        for i, device in enumerate(devices):
            device_id = device.get("device_id")
            context.log(f"📱 Processing device {i+1}/{len(devices)}: {device_id}")
            
            if not device_id:
                context.log(f"⚠️ Device {i+1} has no device_id, skipping")
                continue

            device_timings = timings_by_device.get(device_id, [])
            context.log(f"📋 Device {device_id} has {len(device_timings)} timings")
            
            if not device_timings:
                context.log(f"⚠️ Device {device_id} has no timings, skipping")
                continue

            context.log(f"🔨 Building device object for {device_id}...")
            device_obj = build_device_object(device, device_timings)
            devices_with_timings.append(device_obj)

            context.log(f"🕐 Building notifications for device {device_id}...")
            notifications = build_notifications_for_device(
                device_obj, date_str, context
            )
            context.log(f"✅ Device {device_id} generated {len(notifications)} notifications")
            all_notifications.extend(notifications)

        context.log(f"📊 Total prepared notifications: {len(all_notifications)}")

        if all_notifications:
            # Get unique device IDs from notifications
            device_ids = list(
                set(notification["device_id"] for notification in all_notifications)
            )
            context.log(f"🗑️ Deleting existing notifications for {len(device_ids)} devices: {device_ids}")

            # Delete existing notifications for these devices
            context.log("🗑️ Starting deletion of existing notifications...")
            delete_existing_notifications(databases, device_ids)
            context.log("✅ Finished deleting existing notifications")

            try:
                context.log(f"💾 Upserting {len(all_notifications)} notifications to database...")
                databases.upsert_documents(
                    database_id="projectbilal",
                    collection_id="notifications",
                    documents=all_notifications,
                )
                context.log("✅ Successfully upserted notifications")
            except Exception as e:
                context.error(f"❌ Failed to upsert notifications: {str(e)}")
        else:
            context.log("⚠️ No notifications to process")

        context.log("🎉 Function completed successfully")
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
