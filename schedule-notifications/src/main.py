from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from datetime import datetime, timezone, timedelta
import os
import json
import requests
import time
import urllib.request
import pytz
from collections import defaultdict
from typing import Dict, Any
from .praytime import PrayTime

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


# Configuration for prayer calculation
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_TIMEZONE_BASE_URL = "https://maps.googleapis.com/maps/api/timezone/json"


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


def get_timezone_date(device_timezone):
    """Get the current date in the device's timezone"""
    if not device_timezone:
        # Fallback to UTC if no timezone
        return datetime.now(timezone.utc).date()

    try:
        # Get current UTC time
        utc_now = datetime.now(timezone.utc)

        # Convert to device timezone
        device_tz = pytz.timezone(device_timezone)
        local_time = utc_now.astimezone(device_tz)

        # Return the date in the device's timezone
        return local_time.date()
    except Exception as e:
        # Fallback to UTC if timezone conversion fails
        return datetime.now(timezone.utc).date()


def get_utc_times(timestamp_str, mins=0, target_date=None, prayer_name=None):
    """Convert UTC time string to UTC and compute reminder time if needed."""
    # Handle both ISO 8601 format and simple time format (HH:MM)
    if "T" in timestamp_str:
        # ISO 8601 format
        utc_time = datetime.fromisoformat(timestamp_str)
    else:
        # Simple time format (HH:MM) - assume UTC
        if not target_date:
            raise ValueError("target_date is required for simple time format")

        time_parts = timestamp_str.split(":")
        hour = int(time_parts[0])
        minute = int(time_parts[1])

        # Create datetime in UTC
        utc_time = datetime.combine(
            target_date, datetime.min.time().replace(hour=hour, minute=minute)
        ).replace(tzinfo=timezone.utc)

        # Check if this is a night prayer that should be on the next day in UTC
        # Night prayers (Maghrib, Isha) typically occur after sunset
        # If the time is before 12:00 UTC, it might be a night prayer from the previous day
        if prayer_name and prayer_name.lower() in ["maghrib", "isha"] and hour < 12:
            # This is a night prayer that crosses midnight in UTC
            # Add one day to the date
            utc_time = utc_time + timedelta(days=1)

    main_time = utc_time.strftime("%Y-%m-%dT%H:%M")
    reminder_time = (
        (utc_time - timedelta(minutes=mins)).strftime("%Y-%m-%dT%H:%M")
        if mins > 0
        else None
    )
    return main_time, reminder_time


def convert_utc_to_local(utc_time_str, timezone_str):
    """Convert UTC time string to local timezone"""
    # Parse UTC time
    utc_time = datetime.fromisoformat(utc_time_str + ":00+00:00")

    # Convert to local timezone
    local_tz = pytz.timezone(timezone_str)
    local_time = utc_time.astimezone(local_tz)

    return local_time.strftime("%Y-%m-%dT%H:%M")


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


def init_appwrite_client(context):
    """Initialize Appwrite client from environment and context."""
    return (
        Client()
        .set_project(os.environ["APPWRITE_FUNCTION_PROJECT_ID"])
        .set_key(context.req.headers["x-appwrite-key"])
        .set_endpoint("https://fra.cloud.appwrite.io/v1")
    )


def fetch_enabled_devices(databases):
    """Fetch all enabled devices."""
    return _list_all_documents(
        databases,
        database_id="projectbilal",
        collection_id="devices",
        queries=[Query.equal("enabled", True)],
    )


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
        "timezone": device.get("timezone"),
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
                "enabled": t.get("enabled", False),  # Include enabled field
                "reminder": t.get("reminder"),
                "reminder_audio_id": t.get("reminder_audio_id"),
                "reminder_enabled": t.get(
                    "reminder_enabled", False
                ),  # Include reminder_enabled field
                "volume": t.get("volume"),
                "user_id": t.get("user_id"),
            }
            for t in timings
        ],
    }


def delete_existing_notifications(databases, device_ids):
    """Delete all existing notifications for the given device IDs.
    Returns list of device_ids that failed deletion (should be excluded from upsert)."""
    failed_ids = []
    for device_id in device_ids:
        try:
            _retry_appwrite(
                databases.delete_documents,
                database_id="projectbilal",
                collection_id="notifications",
                queries=[Query.equal("device_id", device_id)],
            )
        except Exception as e:
            failed_ids.append(device_id)
            ntfy_alert(
                f"[schedule-notifications] Failed to delete notifications for {device_id}: {e}",
                priority=4,
                tags="warning",
            )
    return failed_ids



def calculate_prayer_times(
    date: datetime,
    latitude: float,
    longitude: float,
    method: int = 2,
    school: int = 0,
    adjustment: int = 0,
) -> Dict[str, str]:
    """Calculate prayer times using our custom Python calculator in UTC"""
    try:
        # Create PrayTime instance with numeric method ID
        pt = PrayTime(method)

        # Set location - ensure they are floats
        lat_float = float(latitude)
        lng_float = float(longitude)
        pt.location([lat_float, lng_float])

        # Set school (0 = Shafi, 1 = Hanafi)
        if school == 1:
            pt.adjust({"asr": "Hanafi"})
        else:
            pt.adjust({"asr": "Standard"})

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

        # Get prayer times for the specific date
        # Convert to naive datetime for the calculator
        if date.tzinfo is not None:
            naive_date = date.replace(tzinfo=None)
        else:
            naive_date = date
        date_list = [naive_date.year, naive_date.month, naive_date.day]

        times = pt.times(date_list)

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

        return formatted_times

    except Exception as e:
        # Re-raise the exception instead of returning fallback times
        raise e


def fetch_prayer_time(date_str, lat, lon, method, school, timezone_id, context):
    """Calculate prayer timings using UTC calculation and stored timezone."""
    try:
        # Parse the date string (DD-MM-YYYY format)
        day, month, year = date_str.split("-")
        target_date = datetime(int(year), int(month), int(day), tzinfo=timezone.utc)

        # Use stored timezone if available, otherwise fallback to API
        if not timezone_id:
            if GOOGLE_API_KEY:
                try:
                    timezone_id = get_timezone_from_coordinates(
                        lat, lon, GOOGLE_API_KEY
                    )
                    context.log(f"Retrieved timezone from API: {timezone_id}")
                except Exception as e:
                    context.error(f"Failed to get timezone from coordinates: {str(e)}")
                    raise ValueError(f"Unable to determine timezone: {str(e)}")
            else:
                context.error(
                    "No stored timezone and Google API key is required for timezone lookup"
                )
                raise ValueError("Google API key is required for timezone lookup")
        else:
            context.log(f"Using stored timezone: {timezone_id}")

        # Calculate prayer times using UTC calculation
        timings = calculate_prayer_times(
            target_date,
            lat,
            lon,
            method,
            school,
            0,  # no adjustment
        )

        return timings, timezone_id

    except Exception as e:
        context.error(f"Failed to calculate prayer times: {str(e)}")
        raise e


def build_notifications_for_device(device, target_date, context):
    """Build notification payloads for a single device."""
    notifications = []

    # Check for required fields before making API call
    required_fields = ["latitude", "longitude", "method"]
    for field in required_fields:
        if not device.get(field):
            pass
            return notifications

    # Convert target_date to date string for prayer calculation
    date_str = target_date.strftime("%d-%m-%Y")

    try:
        timings, timezone_id = fetch_prayer_time(
            date_str,
            device["latitude"],
            device["longitude"],
            device["method"],
            device["school"],
            device.get("timezone"),
            context,
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

        utc_time, utc_time_rem = get_utc_times(
            time_str, reminder, target_date, prayer_name
        )

        # Always calculate reminder time, even if reminder is 0
        # If reminder is 0, utc_time_rem will be the same as utc_time
        if reminder == 0:
            utc_time_rem = utc_time

        # Convert UTC times to local times
        local_time = convert_utc_to_local(utc_time, timezone_id)
        local_time_rem = convert_utc_to_local(utc_time_rem, timezone_id)

        # Pre-format common fields to avoid repetition
        device_id = device["device_id"]
        ip_address = device.get("ip_address")
        port = device.get("port")
        volume = timing["volume"]
        user_id = timing["user_id"]
        timing_id = timing["timing_id"]
        # Get timing enabled status - null/None should be treated as False
        timing_enabled = timing.get("enabled", False)

        # Main notification
        notifications.append(
            {
                "device_id": device_id,
                "timestampUTC": utc_time,
                "timestampLocal": local_time,
                "ip_address": ip_address,
                "port": port,
                "audio_id": timing["audio_id"],
                "volume": volume,
                "user_id": user_id,
                "timing_id": timing_id,
                "type": "notification",
                "enabled": timing_enabled,  # Use timing enabled status
            }
        )

        # Reminder notification - always create, but set enabled based on timing enabled AND reminder_enabled
        reminder_enabled = timing_enabled and timing.get("reminder_enabled", False)
        notifications.append(
            {
                "device_id": device_id,
                "timestampUTC": utc_time_rem,
                "timestampLocal": local_time_rem,
                "ip_address": ip_address,
                "port": port,
                "audio_id": timing["reminder_audio_id"],
                "volume": volume,
                "user_id": user_id,
                "timing_id": timing_id,
                "type": "reminder",
                "enabled": reminder_enabled,  # Both timing and reminder must be enabled
            }
        )

    return notifications


def main(context):
    client = init_appwrite_client(context)
    databases = Databases(client)

    try:
        # Check if specific device_id is provided
        request_data = context.req.body_json if context.req.body else {}
        target_device_id = request_data.get("device_id")

        if target_device_id:
            # Process only the specified device
            devices = _list_all_documents(
                databases,
                database_id="projectbilal",
                collection_id="devices",
                queries=[
                    Query.equal("enabled", True),
                    Query.equal("device_id", target_device_id),
                ],
            )

            # Fetch all timings for this specific device (enabled and disabled)
            timings = _list_all_documents(
                databases,
                database_id="projectbilal",
                collection_id="timings",
                queries=[
                    Query.equal("device_id", target_device_id),
                ],
            )
        else:
            # Process all devices (current behavior)
            devices = fetch_enabled_devices(databases)
            # Fetch all timings (enabled and disabled)
            timings = _list_all_documents(
                databases,
                database_id="projectbilal",
                collection_id="timings",
            )

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

            # Get the current date in the device's timezone
            device_timezone = device.get("timezone")
            device_date = get_timezone_date(device_timezone)

            context.log(
                f"Processing device {device_id} with timezone {device_timezone}, using date {device_date}"
            )

            notifications = build_notifications_for_device(
                device_obj, device_date, context
            )
            all_notifications.extend(notifications)

        if all_notifications:
            # Get unique device IDs from notifications
            device_ids = list(
                set(notification["device_id"] for notification in all_notifications)
            )

            # Delete existing notifications for these devices
            failed_ids = delete_existing_notifications(databases, device_ids)

            # Exclude devices whose old notifications couldn't be deleted (prevent duplicates)
            if failed_ids:
                all_notifications = [
                    n for n in all_notifications if n["device_id"] not in failed_ids
                ]

            try:
                _retry_appwrite(
                    databases.upsert_documents,
                    database_id="projectbilal",
                    collection_id="notifications",
                    documents=all_notifications,
                )
                ntfy_alert(
                    f"[schedule-notifications] Scheduled {len(all_notifications)} notifications for {len(device_ids) - len(failed_ids)} devices",
                    topic="projectbilal-events",
                    priority=2,
                    tags="calendar",
                )
            except Exception as e:
                context.error(f"Failed to upsert notifications: {str(e)}")
                ntfy_alert(f"[schedule-notifications] Failed to upsert: {e}", priority=4, tags="warning")

        return context.res.json(
            {
                "success": True,
                "total_devices": len(devices_with_timings),
                "all_notifications": all_notifications,
            }
        )

    except Exception as e:
        context.error(f"Unhandled error: {str(e)}")
        ntfy_alert(f"[schedule-notifications] Unhandled error: {e}", priority=4, tags="warning")
        return context.res.json({"success": False, "error": str(e)}, 500)
