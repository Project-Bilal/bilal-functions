from datetime import datetime, timezone
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
import os
import json
import time
import paho.mqtt.client as mqtt
import threading


def send_mqtt_message(topic, message, broker="broker.hivemq.com", port=1883):
    """
    Send a message to the MQTT broker
    """
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

        if notifications["total"] > 0:
            processed_count = 0

            # Prepare all valid notifications first
            valid_notifications = []
            for notification in notifications["documents"]:
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

                # Prepare notification data
                notification_data = {
                    "device_id": notification["device_id"],
                    "timestampUTC": notification["timestampUTC"],
                    "ip_address": notification["ip_address"],
                    "port": notification["port"],
                    "audio_id": notification["audio_id"],
                    "volume": notification["volume"],
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
                    mqtt_client.connect("broker.hivemq.com", 1883, 60)
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
                            # Create the MQTT message
                            message_obj = {
                                "action": "play",
                                "props": {
                                    "volume": notification_data["volume"],
                                    "url": notification_data["audio_id"],
                                    "ip": notification_data["ip_address"],
                                    "port": int(notification_data["port"]),
                                },
                            }

                            # Convert to JSON string
                            message = json.dumps(message_obj)
                            topic = f"projectbilal/{notification_data['device_id']}"

                            # Publish message
                            result = mqtt_client.publish(topic, message, qos=1)
                            result.wait_for_publish()

                            processed_count += 1

                        except Exception as e:
                            context.error(f"Failed to send notification: {str(e)}")

                    # Disconnect
                    mqtt_client.loop_stop()
                    mqtt_client.disconnect()

                except Exception as e:
                    context.error(f"MQTT batch send failed: {str(e)}")

            return context.res.json(
                {
                    "success": True,
                    "total_notifications": notifications["total"],
                    "processed_notifications": processed_count,
                }
            )

        return context.res.json(
            {
                "success": True,
                "current_time": current_time,
                "total_notifications": notifications["total"],
            }
        )

    except Exception as e:
        return context.res.json(
            {"success": False, "error": str(e), "current_time": current_time}, 500
        )
