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


def ntfy_alert(message: str, topic: str = "projectbilal-errors", title: str = "Project Bilal"):
    url = f"{NTFY_BASE}/{topic}"
    try:
        req = urllib.request.Request(url, data=message.encode(), method="POST")
        req.add_header("Title", title)
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # Never break main flow


def _doclist_total(doclist):
    if hasattr(doclist, "total"):
        return doclist.total
    if isinstance(doclist, dict):
        return doclist.get("total", 0)
    return 0


def _doclist_documents(doclist):
    if hasattr(doclist, "documents"):
        return doclist.documents
    if isinstance(doclist, dict):
        return doclist.get("documents", [])
    return []


def send_mqtt_message(topic, message, broker=None, port=None):
    """
    Send a message to the MQTT broker
    """
    # Use environment variables for broker configuration
    if broker is None:
        broker = os.environ.get("MQTT_BROKER_HOST")
        if not broker:
            raise Exception("MQTT_BROKER_HOST environment variable is required")
    if port is None:
        port_str = os.environ.get("MQTT_BROKER_PORT")
        if not port_str:
            raise Exception("MQTT_BROKER_PORT environment variable is required")
        port = int(port_str)

    # Use threading events to track connection status
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

    try:
        # Create MQTT client with a unique client ID
        client = mqtt.Client(client_id=f"bilal_checker_{hash(topic)}")

        # Set up callbacks
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect

        # Set connection timeout
        client.connect(broker, port, 60)

        # Start the loop to handle the connection
        client.loop_start()

        # Wait for connection to be established with 10-second timeout
        max_wait_time = 10  # seconds

        if not connected_event.wait(timeout=max_wait_time):
            raise Exception(
                f"Failed to connect to MQTT broker within {max_wait_time} seconds"
            )

        if connection_error:
            raise Exception(connection_error)

        # Publish message with QoS 1 for broker delivery assurance
        result = client.publish(topic, message, qos=1)

        # Wait for the message to be sent
        result.wait_for_publish()

        # Stop the loop and disconnect
        client.loop_stop()
        client.disconnect()

        return True, "Message sent successfully"
    except Exception as e:
        return False, f"Failed to send message: {str(e)}"


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
        notifications = databases.list_documents(
            database_id="projectbilal",
            collection_id="notifications",
            queries=[
                Query.equal("timestampUTC", current_time),
                Query.equal("enabled", True),
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
                            ntfy_alert(
                                f"[notification-checker] Sent play to {notification_data['device_id']}: {label}",
                                topic="projectbilal-events",
                            )

                        except Exception as e:
                            context.error(f"Failed to send notification: {str(e)}")
                            ntfy_alert(
                                f"[notification-checker] Failed to send to device {notification_data.get('device_id', '?')}: {e}",
                            )

                    # Disconnect
                    mqtt_client.loop_stop()
                    mqtt_client.disconnect()

                except Exception as e:
                    context.error(f"MQTT batch send failed: {str(e)}")
                    ntfy_alert(f"[notification-checker] MQTT batch failed: {e}")

            else:
                # Notifications due but all devices offline or invalid
                ntfy_alert(
                    f"[notification-checker] {notifications_total} notifications due, all devices offline",
                    topic="projectbilal-events",
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
        ntfy_alert(f"[notification-checker] Unhandled error: {e}")
        return context.res.json(
            {"success": False, "error": str(e), "current_time": current_time}, 500
        )
