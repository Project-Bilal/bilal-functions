from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from datetime import datetime, timezone, timedelta
import requests
import os


# converts a string like "2024-10-13T15:52:00-07:00" to UTC
# also formats the string like "2024-10-13T22:52"
# also calcs reminder time if present
def get_utc_times(timestamp_str, mins=0):
    local_time = datetime.fromisoformat(timestamp_str)
    utc_time = local_time.astimezone(timezone.utc)
    utc_time_str = utc_time.strftime("%Y-%m-%dT%H:%M")
    utc_time_rem_str = None
    if mins > 0:
        utc_time_rem = utc_time - timedelta(minutes=mins)
        utc_time_rem_str = utc_time_rem.strftime("%Y-%m-%dT%H:%M")
    return utc_time_str, utc_time_rem_str


# This Appwrite function will be executed every time your function is triggered
def main(context):

    # Get the current UTC time
    current_utc = datetime.now(timezone.utc)
    # Format the date as dd-mm-yyyy
    formatted_date = current_utc.strftime("%d-%m-%Y")
    # Set project and set API key
    client = (
        Client()
        .set_project(os.environ["APPWRITE_FUNCTION_PROJECT_ID"])
        .set_key(context.req.headers["x-appwrite-key"])
    )

    databases = Databases(client)

    try:
        # Query for documents where "enabled" is true
        devices_result = databases.list_documents(
            database_id="projectbilal",
            collection_id="devices",
            queries=[Query.equal("enabled", True)],
        )

        context.log(f"Found {devices_result['total']} enabled devices")

        # List to store devices with their timings
        devices_with_timings = []

        # For each enabled device, get their enabled timings
        for device in devices_result["documents"]:

            # Try to get device_id from different possible fields
            device_id = device.get("device_id")

            # Query for enabled timings for this device
            timings_result = databases.list_documents(
                database_id="projectbilal",
                collection_id="timings",
                queries=[
                    Query.equal("enabled", True),
                    Query.equal("device_id", device_id),
                ],
            )

            # Extract timing data for each timing document
            timings_list = []
            for timing in timings_result["documents"]:
                timing_data = {
                    "notification": timing.get("notification"),
                    "audio_id": timing.get("audio_id"),
                    "reminder": timing.get("reminder"),
                    "reminder_audio_id": timing.get("reminder_audio_id"),
                    "volume": timing.get("volume"),
                }
                timings_list.append(timing_data)

            # Create device object with timings
            device_with_timings = {
                "device_id": device_id,
                "latitude": device.get("latitude"),
                "longitude": device.get("longitude"),
                "method": device.get("method"),
                "midnight_mode": device.get("midnight_mode"),
                "school": device.get("school"),
                "ip_address": device.get("ip_address"),
                "port": device.get("port"),
                "timings": timings_list,
            }

            devices_with_timings.append(device_with_timings)

        all_notifications = []
        for device in devices_with_timings:
            for timing in device["timings"]:
                latitude = device["latitude"]
                longitude = device["longitude"]
                method = device["method"]
                notification = timing["notification"]
                response = requests.get(
                    f"https://api.aladhan.com/v1/timings/{formatted_date}?latitude={latitude}&longitude={longitude}&method={method}&iso8601=true"
                )
                data = response.json()["data"]["timings"]
                time_str = data.get(notification)
                reminder = int(timing.get("reminder")) if timing.get("reminder") else 0
                utc_time, utc_time_rem = get_utc_times(time_str, reminder)
                all_notifications.append(
                    {
                        "device_id": device["device_id"],
                        "timestampUTC": utc_time,
                        "ip_address": device["ip_address"],
                        "port": device["port"],
                        "audio_id": timing["audio_id"],
                        "volume": timing["volume"],
                    }
                )
                if utc_time_rem:
                    all_notifications.append(
                        {
                            "device_id": device["device_id"],
                            "timestampUTC": utc_time_rem,
                            "ip_address": device["ip_address"],
                            "port": device["port"],
                            "audio_id": timing["reminder_audio_id"],
                            "volume": timing["volume"],
                        }
                    )

        context.log(f"All notifications: {all_notifications}")

        return context.res.json(
            {
                "success": True,
                "total_devices": len(devices_with_timings),
                "devices": devices_with_timings,
                "all_notifications": all_notifications,
            }
        )

    except Exception as e:
        context.error(f"Error querying devices and timings: {str(e)}")
        return context.res.json({"success": False, "error": str(e)}, 500)
