from appwrite.client import Client
from appwrite.services.functions import Functions
from appwrite.query import Query
from appwrite.exception import AppwriteException
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
import pytz
import requests
import time
from .praytime import PrayTime
import os
import json

# Configuration
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
        # Create PrayTime instance with numeric method ID
        pt = PrayTime(method)

        # Set location
        pt.location([latitude, longitude])

        # Set timezone
        pt.timezone(timezone_str)

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

        # Convert to the expected format
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

        return formatted_times

    except Exception as e:
        # Re-raise the exception instead of returning fallback times
        raise e


def main(context):
    """Main Appwrite function handler"""
    try:
        # Parse query parameters from the request
        query_params = context.req.query if hasattr(context.req, "query") else {}

        # Extract parameters with defaults
        date = query_params.get("date")
        latitude = float(query_params.get("latitude", 0))
        longitude = float(query_params.get("longitude", 0))
        method = int(query_params.get("method", 2))
        school = int(query_params.get("school", 0))
        adjustment = int(query_params.get("adjustment", 0))
        latitudeAdjustmentMethod = int(query_params.get("latitudeAdjustmentMethod", 3))
        midnightMode = int(query_params.get("midnightMode", 0))
        timezonestring = query_params.get("timezonestring")
        iso8601 = query_params.get("iso8601", "false").lower() == "true"
        tune = query_params.get("tune")
        shafaq = query_params.get("shafaq")
        calendarMethod = int(query_params.get("calendarMethod", 0))
        methodSettings = query_params.get("methodSettings")
        # Validate coordinates
        if not (-90 <= latitude <= 90):
            return context.res.json(
                {
                    "code": 400,
                    "status": "Bad Request",
                    "data": "Invalid latitude. Must be between -90 and 90.",
                },
                400,
            )
        if not (-180 <= longitude <= 180):
            return context.res.json(
                {
                    "code": 400,
                    "status": "Bad Request",
                    "data": "Invalid longitude. Must be between -180 and 180.",
                },
                400,
            )

        # Parse date
        if date is None:
            target_date = datetime.now()
        else:
            try:
                # Try to parse as timestamp first
                if date.isdigit():
                    target_date = datetime.fromtimestamp(int(date), tz=timezone.utc)
                else:
                    # Try to parse as DD-MM-YYYY format
                    day, month, year = date.split("-")
                    target_date = datetime(
                        int(year), int(month), int(day), tzinfo=timezone.utc
                    )
            except (ValueError, IndexError):
                return context.res.json(
                    {
                        "code": 400,
                        "status": "Bad Request",
                        "data": "Invalid date format. Use DD-MM-YYYY or timestamp.",
                    },
                    400,
                )

        # Determine timezone
        if timezonestring:
            try:
                tz = pytz.timezone(timezonestring)
            except pytz.exceptions.UnknownTimeZoneError:
                return context.res.json(
                    {
                        "code": 400,
                        "status": "Bad Request",
                        "data": "Invalid timezone string.",
                    },
                    400,
                )
        else:
            # Get timezone from coordinates using Google API
            if not GOOGLE_API_KEY:
                return context.res.json(
                    {
                        "code": 400,
                        "status": "Bad Request",
                        "data": "Google API key is required for timezone lookup. Either provide timezonestring parameter or set GOOGLE_API_KEY environment variable.",
                    },
                    400,
                )

            try:
                timezone_id = get_timezone_from_coordinates(
                    latitude, longitude, GOOGLE_API_KEY
                )
                tz = pytz.timezone(timezone_id)
            except ValueError as e:
                return context.res.json(
                    {"code": 400, "status": "Bad Request", "data": str(e)}, 400
                )
            except pytz.exceptions.UnknownTimeZoneError:
                return context.res.json(
                    {
                        "code": 400,
                        "status": "Bad Request",
                        "data": "Invalid timezone returned from Google API.",
                    },
                    400,
                )

        # Calculate prayer times using our custom calculator
        try:
            timings = calculate_prayer_times(
                target_date,
                latitude,
                longitude,
                str(tz),
                method,
                school,
                adjustment,
                iso8601,
            )
        except Exception as e:
            return context.res.json(
                {
                    "code": 500,
                    "status": "Internal Server Error",
                    "data": f"Failed to calculate prayer times: {str(e)}",
                },
                500,
            )

        # Prepare date information (simplified without Hijri conversion)
        gregorian_date = {
            "date": f"{target_date.day:02d}-{target_date.month:02d}-{target_date.year:04d}",
            "format": "DD-MM-YYYY",
            "day": target_date.day,
            "weekday": {
                "en": target_date.strftime("%A"),
            },
            "month": {
                "number": target_date.month,
                "en": target_date.strftime("%B"),
            },
            "year": str(target_date.year),
        }

        # Prepare response
        response_date = {
            "readable": target_date.strftime("%d %b %Y"),
            "timestamp": str(int(target_date.timestamp())),
            "gregorian": gregorian_date,
        }

        meta = {
            "latitude": latitude,
            "longitude": longitude,
            "timezone": str(tz),
            "method": {
                "id": method,
                "name": f"Method_{method}",
                "params": {
                    "Fajr": "varies by method",
                    "Isha": "varies by method",
                },
            },
            "latitudeAdjustmentMethod": latitudeAdjustmentMethod,
            "midnightMode": midnightMode,
            "school": "Shafi" if school == 0 else "Hanafi",
            "offset": {
                "Imsak": 0,
                "Fajr": 0,
                "Sunrise": 0,
                "Dhuhr": 0,
                "Asr": 0,
                "Maghrib": 0,
                "Sunset": 0,
                "Isha": 0,
                "Midnight": 0,
            },
        }

        # Return the response in the same format as FastAPI
        return context.res.json(
            {"timings": timings, "date": response_date, "meta": meta}, 200
        )

    except Exception as e:
        return context.res.json(
            {
                "code": 500,
                "status": "Internal Server Error",
                "data": f"Unexpected error: {str(e)}",
            },
            500,
        )
