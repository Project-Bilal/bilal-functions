# praytime.py - Prayer Times Calculator (v3.2)
# Converted from JavaScript to Python
# Original Copyright (c) 2007-2025 Hamid Zarrabi-Zadeh
# Source: https://praytimes.org
# License: MIT

import math
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Union, Optional, Any


class PrayTime:
    def __init__(self, method: str = "MWL"):
        self.methods = {
            "MWL": {"fajr": 18, "isha": 17},
            "ISNA": {"fajr": 15, "isha": 15},
            "Egypt": {"fajr": 19.5, "isha": 17.5},
            "Makkah": {"fajr": 18.5, "isha": "90 min"},
            "Karachi": {"fajr": 18, "isha": 18},
            "Tehran": {"fajr": 17.7, "maghrib": 4.5, "midnight": "Jafari"},
            "France": {"fajr": 12, "isha": 12},
            "Russia": {"fajr": 16, "isha": 15},
            "Singapore": {"fajr": 20, "isha": 18},
            "defaults": {"isha": 14, "maghrib": "1 min", "midnight": "Standard"},
        }

        # Numeric ID to method name mapping
        self.method_ids = {
            1: "Karachi",
            2: "ISNA",
            3: "MWL",
            4: "Makkah",
            5: "Egypt",
            7: "Tehran",
            11: "Singapore",
            12: "France",
            14: "Russia",
        }

        # Cache for method lookups
        self._method_cache = {}

        self.settings = {
            "dhuhr": "0 min",
            "asr": "Standard",
            "highLats": "NightMiddle",
            "tune": {},
            "format": "24h",
            "rounding": "nearest",
            "utcOffset": "auto",
            "timezone": "UTC",
            "location": [0, 0],
            "iterations": 1,
        }

        self.labels = [
            "Fajr",
            "Sunrise",
            "Dhuhr",
            "Asr",
            "Sunset",
            "Maghrib",
            "Isha",
            "Midnight",
        ]

        self.method(method)

    # ---------------------- Setters ------------------------

    def method(self, method: Union[str, int]) -> "PrayTime":
        """Set calculation method by name or numeric ID"""
        # Check cache first
        cache_key = str(method)
        if cache_key in self._method_cache:
            method = self._method_cache[cache_key]
        else:
            # Handle both string and integer method IDs
            if isinstance(method, str) and method.isdigit():
                method = int(method)

            if isinstance(method, int):
                if method in self.method_ids:
                    method = self.method_ids[method]
                else:
                    raise ValueError(f"Invalid method ID: {method}")

            # Cache the result
            self._method_cache[cache_key] = method

        self.settings.update(self.methods["defaults"])

        if method in self.methods:
            self.settings.update(self.methods[method])

        return self

    def adjust(self, params: Dict[str, Any]) -> "PrayTime":
        """Set calculating parameters"""
        return self.set(params)

    def location(self, location: List[float]) -> "PrayTime":
        """Set location [latitude, longitude]"""
        return self.set({"location": location})

    def timezone(self, timezone: str) -> "PrayTime":
        """Set timezone"""
        return self.set({"timezone": timezone})

    def tune(self, tune: Dict[str, float]) -> "PrayTime":
        """Set tuning minutes"""
        return self.set({"tune": tune})

    def round(self, rounding: str = "nearest") -> "PrayTime":
        """Set rounding method"""
        return self.set({"rounding": rounding})

    def format(self, format: str) -> "PrayTime":
        """Set time format"""
        return self.set({"format": format})

    def set(self, settings: Dict[str, Any]) -> "PrayTime":
        """Set settings parameters"""
        self.settings.update(settings)
        return self

    def utcOffset(self, utcOffset: Union[str, float] = "auto") -> "PrayTime":
        """Set UTC offset"""
        if isinstance(utcOffset, (int, float)) and abs(utcOffset) < 16:
            utcOffset *= 60
        self.set({"timezone": "UTC"})
        return self.set({"utcOffset": utcOffset})

    # ---------------------- Getters ------------------------

    def times(self, date: Union[int, datetime, List[int]] = 0) -> Dict[str, str]:
        """Get prayer times"""
        if isinstance(date, int):
            if date < 1000:
                date = datetime.now(timezone.utc) + timedelta(days=date)
            else:
                date = datetime.fromtimestamp(date / 1000, tz=timezone.utc)

        if isinstance(date, datetime):
            date = [date.year, date.month, date.day]

        # Convert to UTC timestamp
        self.utcTime = (
            datetime(date[0], date[1], date[2], tzinfo=timezone.utc).timestamp() * 1000
        )

        times = self.computeTimes()
        self.formatTimes(times)
        return times

    def getTimes(
        self,
        date: Union[int, datetime, List[int]] = 0,
        location: Optional[List[float]] = None,
        timezone: str = "auto",
        dst: int = 0,
        format: str = "24h",
    ) -> Dict[str, str]:
        """Get prayer times (backward compatible)"""
        if not location:
            return self.times(date)

        utcOffset = timezone if timezone == "auto" else timezone + dst
        self.location(location).utcOffset(utcOffset).format(format)
        return self.times(date)

    # ---------------------- Compute Times -----------------------

    def computeTimes(self) -> Dict[str, float]:
        """Compute prayer times"""
        times = {
            "fajr": 5,
            "sunrise": 6,
            "dhuhr": 12,
            "asr": 13,
            "sunset": 18,
            "maghrib": 18,
            "isha": 18,
            "midnight": 24,
        }

        for i in range(self.settings["iterations"]):
            times = self.processTimes(times)

        self.adjustHighLats(times)
        self.updateTimes(times)
        self.tuneTimes(times)
        self.convertTimes(times)
        return times

    def processTimes(self, times: Dict[str, float]) -> Dict[str, float]:
        """Process prayer times"""
        params = self.settings
        horizon = 0.833

        return {
            "fajr": self.angleTime(params["fajr"], times["fajr"], -1),
            "sunrise": self.angleTime(horizon, times["sunrise"], -1),
            "dhuhr": self.midDay(times["dhuhr"]),
            "asr": self.angleTime(
                self.asrAngle(params["asr"], times["asr"]), times["asr"]
            ),
            "sunset": self.angleTime(horizon, times["sunset"]),
            "maghrib": self.angleTime(params["maghrib"], times["maghrib"]),
            "isha": self.angleTime(params["isha"], times["isha"]),
            "midnight": self.midDay(times["midnight"]) + 12,
        }

    def updateTimes(self, times: Dict[str, float]) -> None:
        """Update times"""
        params = self.settings

        if self.isMin(params["maghrib"]):
            times["maghrib"] = times["sunset"] + self.value(params["maghrib"]) / 60
        if self.isMin(params["isha"]):
            times["isha"] = times["maghrib"] + self.value(params["isha"]) / 60
        if params["midnight"] == "Jafari":
            nextFajr = self.angleTime(params["fajr"], 29, -1) + 24
            times["midnight"] = (
                times["sunset"]
                + (self.adjusted if hasattr(self, "adjusted") else times["fajr"] + 24)
            ) / 2
        times["dhuhr"] += self.value(params["dhuhr"]) / 60

    def tuneTimes(self, times: Dict[str, float]) -> None:
        """Tune times"""
        mins = self.settings["tune"]
        for key in times:
            if key in mins:
                times[key] += mins[key] / 60

    def convertTimes(self, times: Dict[str, float]) -> None:
        """Convert times"""
        lng = self.settings["location"][1]
        for key in times:
            time = times[key] - lng / 15
            timestamp = self.utcTime + math.floor(time * 3600000)
            times[key] = self.roundTime(timestamp)

    def roundTime(self, timestamp: float) -> float:
        """Round time"""
        rounding_methods = {
            "up": math.ceil,
            "down": math.floor,
            "nearest": round,
        }
        rounding = rounding_methods.get(self.settings["rounding"])
        if not rounding:
            return timestamp
        OneMinute = 60000
        return rounding(timestamp / OneMinute) * OneMinute

    # ---------------------- Calculation Functions -----------------------

    def sunPosition(self, time: float) -> Dict[str, float]:
        """Compute sun position"""
        lng = self.settings["location"][1]

        D = self.utcTime / 86400000 - 10957.5 + self.value(time) / 24 - lng / 360

        g = self.mod(357.529 + 0.98560028 * D, 360)
        q = self.mod(280.459 + 0.98564736 * D, 360)
        L = self.mod(q + 1.915 * self.sin(g) + 0.02 * self.sin(2 * g), 360)
        e = 23.439 - 0.00000036 * D
        RA = self.mod(self.arctan2(self.cos(e) * self.sin(L), self.cos(L)) / 15, 24)

        return {
            "declination": self.arcsin(self.sin(e) * self.sin(L)),
            "equation": q / 15 - RA,
        }

    def midDay(self, time: float) -> float:
        """Compute mid-day"""
        eqt = self.sunPosition(time)["equation"]
        noon = self.mod(12 - eqt, 24)
        return noon

    def angleTime(
        self, angle: Union[float, str], time: float, direction: int = 1
    ) -> float:
        """Compute the time when sun reaches a specific angle below horizon"""
        lat = self.settings["location"][0]
        decl = self.sunPosition(time)["declination"]
        angle_val = self.value(angle)
        numerator = -self.sin(angle_val) - self.sin(lat) * self.sin(decl)
        denominator = self.cos(lat) * self.cos(decl)

        # Clamp the value to prevent math domain error
        cos_val = numerator / denominator
        cos_val = max(-1.0, min(1.0, cos_val))

        diff = self.arccos(cos_val) / 15
        return self.midDay(time) + diff * direction

    def asrAngle(self, asrParam: Union[str, float], time: float) -> float:
        """Compute asr angle"""
        if isinstance(asrParam, str) and asrParam in {"Standard", "Hanafi"}:
            shadowFactor = {"Standard": 1, "Hanafi": 2}[asrParam]
        else:
            shadowFactor = self.value(asrParam)
        lat = self.settings["location"][0]
        decl = self.sunPosition(time)["declination"]
        return -self.arccot(shadowFactor + self.tan(abs(lat - decl)))

    # ---------------------- Higher Latitudes -----------------------

    def adjustHighLats(self, times: Dict[str, float]) -> None:
        """Adjust times for higher latitudes"""
        params = self.settings
        if params["highLats"] == "None":
            return

        self.adjusted = False
        night = 24 + times["sunrise"] - times["sunset"]

        times.update(
            {
                "fajr": self.adjustTime(
                    times["fajr"], times["sunrise"], params["fajr"], night, -1
                ),
                "isha": self.adjustTime(
                    times["isha"], times["sunset"], params["isha"], night
                ),
                "maghrib": self.adjustTime(
                    times["maghrib"], times["sunset"], params["maghrib"], night
                ),
            }
        )

    def adjustTime(
        self,
        time: float,
        base: float,
        angle: Union[float, str],
        night: float,
        direction: int = 1,
    ) -> float:
        """Adjust time in higher latitudes"""
        factors = {
            "NightMiddle": 1 / 2,
            "OneSeventh": 1 / 7,
            "AngleBased": (1 / 60) * self.value(angle),
        }
        portion = factors[self.settings["highLats"]] * night
        timeDiff = (time - base) * direction
        if math.isnan(time) or timeDiff > portion:
            time = base + portion * direction
            self.adjusted = True
        return time

    # ---------------------- Formatting Functions ---------------------

    def formatTimes(self, times: Dict[str, float]) -> None:
        """Format times"""
        for key in times:
            times[key] = self.formatTime(times[key])

    def formatTime(self, timestamp: float) -> str:
        """Format time"""
        format_type = self.settings["format"]
        InvalidTime = "-----"
        if math.isnan(timestamp):
            return InvalidTime
        if callable(format_type):
            return format_type(timestamp)
        if format_type.lower() == "x":
            return str(int(timestamp / (1000 if format_type == "X" else 1)))
        return self.timeToString(timestamp, format_type)

    def timeToString(self, timestamp: float, format_type: str) -> str:
        """Convert time to string"""
        utcOffset = self.settings["utcOffset"]
        offset_ms = 0 if utcOffset == "auto" else utcOffset * 60000

        # Convert timestamp to datetime
        date = datetime.fromtimestamp((timestamp + offset_ms) / 1000, tz=timezone.utc)

        # Convert to local timezone if specified
        if self.settings["timezone"] != "UTC":
            try:
                import pytz

                local_tz = pytz.timezone(self.settings["timezone"])
                date = date.astimezone(local_tz)
            except:
                pass  # Fallback to UTC if timezone conversion fails

        if format_type == "24h":
            return date.strftime("%H:%M")
        else:  # 12h format
            return date.strftime("%I:%M %p")

    # ---------------------- Misc Functions -----------------------

    def value(self, str_val: Union[str, float]) -> float:
        """Convert string to number"""
        if isinstance(str_val, (int, float)):
            return float(str_val)
        # Extract numeric part from string
        parts = str(str_val).split()
        if parts:
            try:
                return float(parts[0])
            except ValueError:
                # If no numeric part, return 0
                return 0.0
        return 0.0

    def isMin(self, str_val: Union[str, float]) -> bool:
        """Detect if input contains 'min'"""
        return "min" in str(str_val)

    def mod(self, a: float, b: float) -> float:
        """Positive modulo"""
        return ((a % b) + b) % b

    # --------------------- Degree-Based Trigonometry -----------------

    def dtr(self, d: float) -> float:
        """Degrees to radians"""
        return (d * math.pi) / 180

    def rtd(self, r: float) -> float:
        """Radians to degrees"""
        return (r * 180) / math.pi

    def sin(self, d: float) -> float:
        """Sine in degrees"""
        return math.sin(self.dtr(d))

    def cos(self, d: float) -> float:
        """Cosine in degrees"""
        return math.cos(self.dtr(d))

    def tan(self, d: float) -> float:
        """Tangent in degrees"""
        return math.tan(self.dtr(d))

    def arcsin(self, d: float) -> float:
        """Arcsine in degrees"""
        return self.rtd(math.asin(d))

    def arccos(self, d: float) -> float:
        """Arccosine in degrees"""
        return self.rtd(math.acos(d))

    def arctan(self, d: float) -> float:
        """Arctangent in degrees"""
        return self.rtd(math.atan(d))

    def arccot(self, x: float) -> float:
        """Arccotangent in degrees"""
        return self.rtd(math.atan(1 / x))

    def arctan2(self, y: float, x: float) -> float:
        """Arctangent2 in degrees"""
        return self.rtd(math.atan2(y, x))
