from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from datetime import datetime, timezone, timedelta
import requests
import os
import json
import time
import pytz
from collections import defaultdict
from typing import Dict, Any
from .praytime import PrayTime

# Configuration for local prayer calculation
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_TIMEZONE_BASE_URL = "https://maps.googleapis.com/maps/api/timezone/json"


def get_utc_times(timestamp_str, mins=0, target_date=None, timezone_str=None):
    """Convert local time string to UTC and compute reminder time if needed."""
    # Handle both ISO 8601 format and simple time format (HH:MM)
    if "T" in timestamp_str:
        # ISO 8601 format
        local_time = datetime.fromisoformat(timestamp_str)
    else:
        # Simple time format (HH:MM) - use the target date and timezone
        if not target_date or not timezone_str:
            raise ValueError("target_date and timezone_str are required for simple time format")
        
        time_parts = timestamp_str.split(":")
        hour = int(time_parts[0])
        minute = int(time_parts[1])
        
        # Create datetime in the target timezone
        tz = pytz.timezone(timezone_str)
        local_time = tz.localize(datetime.combine(target_date, datetime.min.time().replace(hour=hour, minute=minute)))

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


def get_timezone_from_coordinates(
    latitude: float, longitude: float, api_key: str
) -> str:
    """Get timezone from coordinates using Google Timezone API"""
    if not api_key:
        raise ValueError("Google API key is required for timezone lookup")

    try:
        timestamp = int(time.time())
        url = f"{GOOGLE_TIMEZONE_BASE_URL}?location={latitude},{longitude}&timestamp={timestamp}&key={api_key}"

        response = requests.get(url, timeout=10)
        data = response.json()

        if data.get("status") == "OK" and "timeZoneId" in data:
            return data["timeZoneId"]
        else:
            raise ValueError(
                f"Google Timezone API error: {data.get('error_message', 'Unknown error')}"
            )
    except Exception as e:
        raise ValueError(f"Failed to get timezone: {str(e)}")


def calculate_prayer_times(
    date: datetime,
    latitude: float,
    longitude: float,
    timezone_str: str,
    method: int = 2,
    school: int = 0,
    adjustment: int = 0,
    iso8601: bool = False,
) -> Dict[str, str]:
    """Calculate prayer times using our custom Python calculator"""
    try:
        print(
            f"Debug: Starting prayer calculation with method={method}, school={school}"
        )

        # Create PrayTime instance with numeric method ID
        pt = PrayTime(method)
        print(f"Debug: Created PrayTime instance")

        # Set location - ensure they are floats
        lat_float = float(latitude)
        lng_float = float(longitude)
        pt.location([lat_float, lng_float])
        print(f"Debug: Set location to [{lat_float}, {lng_float}]")

        # Set timezone
        pt.timezone(timezone_str)
        print(f"Debug: Set timezone to {timezone_str}")

        # Set school (0 = Shafi, 1 = Hanafi)
        if school == 1:
            pt.adjust({"asr": "Hanafi"})
            print("Debug: Set school to Hanafi")
        else:
            pt.adjust({"asr": "Standard"})
            print("Debug: Set school to Shafi")

        # Apply adjustments if provided
        if adjustment != 0:
            adjustments = {
                "fajr": adjustment,
                "dhuhr": adjustment,
                "asr": adjustment,
                "maghrib": adjustment,
                "isha": adjustment,
            }
            pt.tune(adjustments)
            print(f"Debug: Applied adjustments: {adjustments}")

        # Get prayer times for the specific date
        # Convert to naive datetime for the calculator
        if date.tzinfo is not None:
            naive_date = date.replace(tzinfo=None)
        else:
            naive_date = date
        date_list = [naive_date.year, naive_date.month, naive_date.day]
        print(f"Debug: Calculating times for date: {date_list}")

        try:
            times = pt.times(date_list)
            print(f"Debug: Got times from PrayTime: {times}")
        except Exception as e:
            print(f"Debug: Error in pt.times(): {type(e).__name__}: {str(e)}")
            raise e

        # Debug: Log what times are returned
        print(f"Debug: Raw times from PrayTime: {times}")

        # Convert to the expected format - PrayTime returns lowercase keys
        formatted_times = {}
        prayer_mapping = {
            "fajr": "Fajr",
            "sunrise": "Sunrise",
            "dhuhr": "Dhuhr",
            "asr": "Asr",
            "sunset": "Sunset",
            "maghrib": "Maghrib",
            "isha": "Isha",
            "midnight": "Midnight",
        }

        for key, value in times.items():
            if key in prayer_mapping:
                formatted_times[prayer_mapping[key]] = value
            else:
                # If key not in mapping, use it as-is (capitalize first letter)
                formatted_times[key.capitalize()] = value

        print(f"Debug: Formatted times: {formatted_times}")
        return formatted_times

    except Exception as e:
        # Re-raise the exception instead of returning fallback times
        raise e


def fetch_prayer_time(date_str, lat, lon, method, school, context):
    """Calculate prayer timings using local calculation only."""
    try:
        # Parse the date string (DD-MM-YYYY format)
        day, month, year = date_str.split("-")
        target_date = datetime(int(year), int(month), int(day), tzinfo=timezone.utc)

        # Determine timezone
        if GOOGLE_API_KEY:
            try:
                timezone_id = get_timezone_from_coordinates(lat, lon, GOOGLE_API_KEY)
                tz = pytz.timezone(timezone_id)
            except Exception as e:
                context.error(f"Failed to get timezone from coordinates: {str(e)}")
                raise ValueError(f"Unable to determine timezone: {str(e)}")
        else:
            context.error("Google API key is required for timezone lookup")
            raise ValueError("Google API key is required for timezone lookup")

        # Calculate prayer times using local calculation
        timings = calculate_prayer_times(
            target_date,
            lat,
            lon,
            str(tz),
            method,
            school,
            0,  # no adjustment
            True,  # iso8601 format
        )

        return timings, timezone_id

    except Exception as e:
        context.error(f"Failed to calculate prayer times: {str(e)}")
        raise e


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
        timings, timezone_id = fetch_prayer_time(
            date_str,
            device["latitude"],
            device["longitude"],
            device["method"],
            device["school"],
            context,
        )
    except Exception as e:
        context.error(
            f"Failed to fetch prayer times for device {device['device_id']}: {str(e)}"
        )
        return notifications  # Return empty list to skip

    for timing in device["timings"]:
        prayer_name = timing.get("notification")
        print(
            f"Debug: Looking for prayer '{prayer_name}' in timings: {list(timings.keys())}"
        )
        if not prayer_name or prayer_name not in timings:
            print(f"Debug: Prayer '{prayer_name}' not found in timings, skipping")
            continue

        time_str = timings.get(prayer_name)
        reminder = int(timing.get("reminder") or 0)
        
        # Parse the date string (DD-MM-YYYY format) for timezone conversion
        day, month, year = date_str.split("-")
        target_date = datetime(int(year), int(month), int(day)).date()
        
        utc_time, utc_time_rem = get_utc_times(time_str, reminder, target_date, timezone_id)

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
