from datetime import datetime, timezone
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from appwrite.services.functions import Functions
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
    start_time = time.time()
    context.log(
        f"🚀 Notification checker started at {datetime.now(timezone.utc).isoformat()}"
    )

    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")

    # Initialize Appwrite client
    client_init_start = time.time()
    client = (
        Client()
        .set_project(os.environ["APPWRITE_FUNCTION_PROJECT_ID"])
        .set_key(context.req.headers["x-appwrite-key"])
        .set_endpoint("https://fra.cloud.appwrite.io/v1")  # Your API Endpoint
    )

    databases = Databases(client)
    client_init_time = time.time() - client_init_start
    context.log(f"⏱️ Client initialization took {client_init_time:.3f}s")

    try:
        # Query for enabled notifications with timestampUTC equal to current_time
        query_start = time.time()
        notifications = databases.list_documents(
            database_id="projectbilal",
            collection_id="notifications",
            queries=[
                Query.equal("timestampUTC", current_time),
                Query.equal("enabled", True),
            ],
        )
        query_time = time.time() - query_start
        context.log(
            f"⏱️ Database query took {query_time:.3f}s, found {notifications['total']} notifications"
        )

        if notifications["total"] > 0:
            functions = Functions(client)
            processed_count = 0
            notification_start = time.time()
            context.log(
                f"🔄 Starting to process {len(notifications['documents'])} notifications..."
            )

            for i, notification in enumerate(notifications["documents"]):
                notification_item_start = time.time()
                # Skip disabled notifications (safety check)
                if not notification.get("enabled", True):
                    context.log(
                        f"Skipping disabled notification: {notification['$id']}"
                    )
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

                # Send notification to device via MQTT
                try:
                    mqtt_start = time.time()
                    context.log(
                        f"📤 Sending MQTT message for notification {i+1}/{len(notifications['documents'])}: {notification['$id']}"
                    )

                    # Validate IP address and port before proceeding
                    if (
                        not notification_data.get("ip_address")
                        or notification_data.get("ip_address") == "0.0.0.0"
                        or not notification_data.get("port")
                    ):
                        context.log(
                            f"⚠️ Skipping notification {notification['$id']} - invalid IP/port (IP: {notification_data.get('ip_address')}, Port: {notification_data.get('port')})"
                        )
                        continue

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

                    # Send MQTT message
                    success, result_message = send_mqtt_message(topic, message)

                    if success:
                        mqtt_time = time.time() - mqtt_start
                        total_time = time.time() - notification_item_start
                        context.log(
                            f"✅ Notification {i+1} sent via MQTT in {mqtt_time:.3f}s (total: {total_time:.3f}s)"
                        )
                        processed_count += 1
                    else:
                        mqtt_time = time.time() - mqtt_start
                        context.error(
                            f"❌ MQTT send failed for notification {notification['$id']} after {mqtt_time:.3f}s: {result_message}"
                        )

                except Exception as e:
                    mqtt_time = time.time() - mqtt_start
                    context.error(
                        f"❌ Failed to send notification {notification['$id']} after {mqtt_time:.3f}s: {str(e)}"
                    )

            notification_total_time = time.time() - notification_start
            total_execution_time = time.time() - start_time
            context.log(
                f"🏁 All notifications processed in {notification_total_time:.3f}s (total execution: {total_execution_time:.3f}s)"
            )

            return context.res.json(
                {
                    "success": True,
                    "total_notifications": notifications["total"],
                    "processed_notifications": processed_count,
                    "execution_time_seconds": total_execution_time,
                    "notification_processing_time_seconds": notification_total_time,
                }
            )

        total_execution_time = time.time() - start_time
        context.log(
            f"ℹ️ No notifications found for {current_time} (execution time: {total_execution_time:.3f}s)"
        )

        return context.res.json(
            {
                "success": True,
                "current_time": current_time,
                "total_notifications": notifications["total"],
                "notifications": notifications["documents"],
                "execution_time_seconds": total_execution_time,
            }
        )

    except Exception as e:
        return context.res.json(
            {"success": False, "error": str(e), "current_time": current_time}, 500
        )
