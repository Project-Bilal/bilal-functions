from datetime import datetime, timezone
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
import os
import json
import time
import urllib.request
import paho.mqtt.client as mqtt
import threading

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


def _doclist_total(doclist):
    if hasattr(doclist, "total"):
        return doclist.total
    if isinstance(doclist, dict):
        return doclist.get("total", 0)
    return 0


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


# This Appwrite function will be executed every time your function is triggered
def main(context):
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")

    # Initialize Appwrite client
    client = (
        Client()
        .set_project(os.environ["APPWRITE_FUNCTION_PROJECT_ID"])
        .set_key(context.req.headers["x-appwrite-key"])
        .set_endpoint("https://fra.cloud.appwrite.io/v1")
    )

    databases = Databases(client)

    try:
        # Query for enabled notifications with timestampUTC equal to current_time
        notifications = _retry_appwrite(
            databases.list_rows,
            database_id="projectbilal",
            collection_id="notifications",
            queries=[
                Query.equal("timestampUTC", current_time),
                Query.equal("enabled", True),
                Query.limit(100),
            ],
        )
        notifications_total = _doclist_total(notifications)
        notification_documents = _doclist_documents(notifications)

        if notifications_total > 0:
            processed_count = 0

            # Prepare all valid notifications first
            valid_notifications = []
            for notification in notification_documents:
                # Skip disabled notifications (safety check)
                if not notification.get("enabled", True):
                    continue

                # Validate IP address and port before proceeding
                if (
                    not notification.get("ip_address")
                    or notification.get("ip_address") == "0.0.0.0"
                    or not notification.get("port")
                ):
                    continue

                # Prepare notification data (include timing_id and type for label derivation)
                notification_data = {
                    "device_id": notification["device_id"],
                    "timestampUTC": notification["timestampUTC"],
                    "ip_address": notification["ip_address"],
                    "port": notification["port"],
                    "audio_id": notification["audio_id"],
                    "volume": notification["volume"],
                    "timing_id": notification.get("timing_id"),
                    "type": notification.get("type", "notification"),
                }
                valid_notifications.append(notification_data)

            # Look up device names for all unique device IDs
            device_names = {}
            unique_device_ids = set(n["device_id"] for n in valid_notifications)
            for did in unique_device_ids:
                try:
                    devices = _retry_appwrite(
                        databases.list_rows,
                        database_id="projectbilal",
                        collection_id="devices",
                        queries=[Query.equal("device_id", did), Query.limit(1)],
                    )
                    docs = _doclist_documents(devices)
                    if docs and docs[0].get("name"):
                        device_names[did] = docs[0]["name"]
                except Exception:
                    pass  # Fall back to device_id in logs

            if valid_notifications:
                # Send all notifications using single MQTT connection

                try:
                    # Create single MQTT client
                    mqtt_client = mqtt.Client(
                        client_id=f"bilal_checker_batch_{int(time.time())}"
                    )

                    # Set up callbacks
                    connected_event = threading.Event()
                    connection_error = None

                    def on_connect(client, userdata, flags, rc):
                        if rc == 0:
                            connected_event.set()
                        else:
                            nonlocal connection_error
                            connection_error = f"Connection failed with code {rc}"
                            connected_event.set()

                    def on_disconnect(client, userdata, rc):
                        pass

                    mqtt_client.on_connect = on_connect
                    mqtt_client.on_disconnect = on_disconnect

                    # Connect to MQTT broker
                    broker_host = os.environ.get("MQTT_BROKER_HOST")
                    if not broker_host:
                        raise Exception(
                            "MQTT_BROKER_HOST environment variable is required"
                        )
                    broker_port_str = os.environ.get("MQTT_BROKER_PORT")
                    if not broker_port_str:
                        raise Exception(
                            "MQTT_BROKER_PORT environment variable is required"
                        )
                    broker_port = int(broker_port_str)
                    mqtt_client.connect(broker_host, broker_port, 60)
                    mqtt_client.loop_start()

                    # Wait for connection
                    if not connected_event.wait(timeout=10):
                        raise Exception(
                            "Failed to connect to MQTT broker within 10 seconds"
                        )

                    if connection_error:
                        raise Exception(connection_error)

                    # Send all notifications
                    for notification_data in valid_notifications:
                        try:
                            # Derive label from timing_id (format: device_id_prayer) + type
                            timing_id = notification_data.get("timing_id") or ""
                            ntype = notification_data.get("type", "notification")
                            prayer_part = timing_id.split("_")[-1] if "_" in timing_id else "?"
                            label_base = prayer_part.capitalize() if prayer_part else "?"
                            label = f"{label_base} reminder" if ntype == "reminder" else label_base

                            # Create the MQTT message (include label for ESP32 logging)
                            message_obj = {
                                "action": "play",
                                "props": {
                                    "volume": notification_data["volume"],
                                    "url": notification_data["audio_id"],
                                    "ip": notification_data["ip_address"],
                                    "port": int(notification_data["port"]),
                                    "label": label,
                                },
                            }

                            # Convert to JSON string
                            message = json.dumps(message_obj)
                            topic = f"projectbilal/{notification_data['device_id']}"

                            # Publish message
                            result = mqtt_client.publish(topic, message, qos=1)
                            result.wait_for_publish()

                            processed_count += 1
                            device_label = device_names.get(notification_data['device_id'], notification_data['device_id'])
                            ntfy_alert(
                                f"[notification-checker] Sent play to {device_label}: {label}",
                                topic="projectbilal-events",
                                priority=2,
                                tags="speaker",
                            )

                        except Exception as e:
                            context.error(f"Failed to send notification: {str(e)}")
                            ntfy_alert(
                                f"[notification-checker] Failed to send to device {notification_data.get('device_id', '?')}: {e}",
                                priority=4,
                                tags="warning",
                            )

                    # Disconnect
                    mqtt_client.loop_stop()
                    mqtt_client.disconnect()

                except Exception as e:
                    context.error(f"MQTT batch send failed: {str(e)}")
                    ntfy_alert(f"[notification-checker] MQTT batch failed: {e}", priority=4, tags="warning")

            else:
                # Notifications due but all devices offline or invalid
                ntfy_alert(
                    f"[notification-checker] {notifications_total} notifications due, all devices offline",
                    topic="projectbilal-events",
                    priority=3,
                    tags="warning",
                )

            return context.res.json(
                {
                    "success": True,
                    "total_notifications": notifications_total,
                    "processed_notifications": processed_count,
                }
            )

        return context.res.json(
            {
                "success": True,
                "current_time": current_time,
                "total_notifications": notifications_total,
            }
        )

    except Exception as e:
        ntfy_alert(f"[notification-checker] Unhandled error: {e}", priority=4, tags="warning")
        return context.res.json(
            {"success": False, "error": str(e), "current_time": current_time}, 500
        )
