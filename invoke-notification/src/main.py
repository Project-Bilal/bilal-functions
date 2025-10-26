import paho.mqtt.client as mqtt
import json
import os

"""
Appwrite Function: invoke-notification

This function handles MQTT notifications for Bilal devices. It's primarily used for:

1. BLE Action (Bluetooth Low Energy):
   - Input: {"ble": "1c-69-20-ea-c6-8c"}
   - Output: Sends MQTT message to "projectbilal/1c-69-20-ea-c6-8c"
   - Message: {"action": "ble", "props": {}}
   - Use case: Bluetooth device discovery/connection

2. Play Action (Audio Playback):
   - This is no longer used, we are using the notification-checker function instead
   - This function is only here for backward compatibility and testing
   - Input: {"volume": "...", "audio_id": "...", "ip_address": "...", "port": "...", "device_id": "..."}
   - Output: Sends MQTT message to "projectbilal/{device_id}"
   - Message: {"action": "play", "props": {"volume": "...", "url": "...", "ip": "...", "port": ...}}
   - Use case: Play prayer notifications/reminders, Chromecast testing

3. Device Validation:
   - Validates IP address and port before sending
   - Skips offline devices (IP: 0.0.0.0 or null port)
   - Returns appropriate error messages for invalid configurations

Note: The main prayer notification flow now uses notification-checker for better performance.
This function is primarily used for BLE operations, manual testing, and one-off MQTT commands.

MQTT Broker: Uses MQTT_BROKER_HOST and MQTT_BROKER_PORT environment variables (QoS 1 for delivery assurance)
"""


def send_mqtt_message(topic, message, broker=None, port=None):
    """
    Send a message to the MQTT broker
    """
    import time
    import threading
    import os

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
        client = mqtt.Client(client_id=f"bilal_function_{hash(topic)}")

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
    try:
        data = context.req.body_json

        # Check if this is a BLE action
        if "ble" in data:
            ble_address = data["ble"]

            # Validate BLE address format (basic validation)
            if not ble_address or not isinstance(ble_address, str):
                return context.res.json(
                    {
                        "success": False,
                        "error": "Invalid BLE address format. Must be a non-empty string.",
                        "received_value": ble_address,
                        "expected_format": "1c-69-20-ea-c6-8c",
                    },
                    400,
                )

            # Create the BLE message
            message_obj = {"action": "ble", "props": {}}

            # Convert to JSON string
            message = json.dumps(message_obj)
            topic = f"projectbilal/{ble_address}"

            # Send message to MQTT broker
            success, result_message = send_mqtt_message(topic, message)

            if success:
                return context.res.json(
                    {
                        "success": True,
                        "message": result_message,
                        "topic": topic,
                        "sent_message": message,
                        "broker": os.environ.get("MQTT_BROKER_HOST")
                        or "not configured",
                        "action_type": "ble",
                    }
                )
            else:
                return context.res.json(
                    {
                        "success": False,
                        "error": result_message,
                        "topic": topic,
                        "broker": os.environ.get("MQTT_BROKER_HOST")
                        or "not configured",
                        "action_type": "ble",
                    },
                    500,
                )

        # Check if all required fields are present for play action
        # btw we are no longer using this part of the function, we are using the notification-checker function instead
        # this function is only here for backward compatibility and testing
        required_fields = ["volume", "audio_id", "ip_address", "port", "device_id"]
        if all(field in data for field in required_fields):
            # Extract the required fields
            volume = data["volume"]
            audio_id = data["audio_id"]
            ip_address = data["ip_address"]
            port = data["port"]
            device_id = data["device_id"]

            # Validate IP address and port before proceeding
            if not ip_address or ip_address == "0.0.0.0" or not port:
                return context.res.json(
                    {
                        "success": False,
                        "error": "Device is offline or has invalid network configuration",
                        "device_id": device_id,
                        "ip_address": ip_address,
                        "port": port,
                        "message": "Notification skipped - device not reachable",
                    },
                    200,  # Return 200 since this is expected behavior, not an error
                )

            # Create the formatted message
            message_obj = {
                "action": "play",
                "props": {
                    "volume": volume,
                    "url": audio_id,  # Using audio_id as url
                    "ip": ip_address,
                    "port": int(port),
                },
            }

            # Convert to JSON string
            message = json.dumps(message_obj)
            topic = f"projectbilal/{device_id}"

            # Send message to MQTT broker
            success, result_message = send_mqtt_message(topic, message)

            if success:
                return context.res.json(
                    {
                        "success": True,
                        "message": result_message,
                        "topic": topic,
                        "sent_message": message,
                        "broker": os.environ.get("MQTT_BROKER_HOST")
                        or "not configured",
                        "action_type": "play",
                    }
                )
            else:
                return context.res.json(
                    {
                        "success": False,
                        "error": result_message,
                        "topic": topic,
                        "broker": os.environ.get("MQTT_BROKER_HOST")
                        or "not configured",
                        "action_type": "play",
                    },
                    500,
                )
        else:
            # Return error if neither BLE nor play action fields are present
            return context.res.json(
                {
                    "success": False,
                    "error": "Invalid request format. Must include either 'ble' field or all required play action fields",
                    "supported_actions": {
                        "ble": 'Send BLE action with format: {"ble": "device_address"}',
                        "play": 'Send play action with format: {"volume": "...", "audio_id": "...", "ip_address": "...", "port": "...", "device_id": "..."}',
                    },
                },
                400,
            )

    except json.JSONDecodeError:
        return context.res.json(
            {"success": False, "error": "Invalid JSON in request body"}, 400
        )
