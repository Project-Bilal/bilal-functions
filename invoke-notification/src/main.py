import paho.mqtt.client as mqtt
import json

"""
Appwrite Function: invoke-notification

This function handles two types of MQTT notifications:

1. BLE Action:
   - Input: {"ble": "1c-69-20-ea-c6-8c"}
   - Output: Sends MQTT message to "projectbilal/1c-69-20-ea-c6-8c"
   - Message: {"action": "ble", "props": {}}

2. Play Action:
   - Input: {"volume": "...", "audio_id": "...", "ip_address": "...", "port": "...", "device_id": "..."}
   - Output: Sends MQTT message to "projectbilal/{device_id}"
   - Message: {"action": "play", "props": {"volume": "...", "url": "...", "ip": "...", "port": ...}}

Both actions use the same MQTT broker: broker.hivemq.com:1883
"""


def send_mqtt_message(topic, message, broker="broker.hivemq.com", port=1883):
    """
    Send a message to the MQTT broker
    """
    try:
        # Create MQTT client with a unique client ID
        client = mqtt.Client(client_id=f"bilal_function_{hash(topic)}")

        # Set connection timeout
        client.connect(broker, port, 60)

        # Start the loop to handle the connection
        client.loop_start()

        # Wait a moment for connection to establish
        import time

        time.sleep(1)

        # Publish message with QoS 1 for reliability
        result = client.publish(topic, message, qos=0)

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

        # Log the incoming request for debugging
        context.log(f"Received request with data: {json.dumps(data)}")

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

            context.log(f"Processing BLE action for device {ble_address}")
            context.log(f"Topic: {topic}")
            context.log(f"Message: {message}")

            # Send message to MQTT broker
            success, result_message = send_mqtt_message(topic, message)

            if success:
                context.log(f"MQTT message sent to {topic}: {message}")
                context.log(
                    f"BLE action completed successfully for device {ble_address}"
                )
                return context.res.json(
                    {
                        "success": True,
                        "message": result_message,
                        "topic": topic,
                        "sent_message": message,
                        "broker": "broker.hivemq.com",
                        "action_type": "ble",
                    }
                )
            else:
                context.error(f"Failed to send MQTT message: {result_message}")
                return context.res.json(
                    {
                        "success": False,
                        "error": result_message,
                        "topic": topic,
                        "broker": "broker.hivemq.com",
                        "action_type": "ble",
                    },
                    500,
                )

        # Check if all required fields are present for play action
        required_fields = ["volume", "audio_id", "ip_address", "port", "device_id"]
        if all(field in data for field in required_fields):
            # Extract the required fields
            volume = data["volume"]
            audio_id = data["audio_id"]
            ip_address = data["ip_address"]
            port = data["port"]
            device_id = data["device_id"]

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

            context.log(f"Processing play action for device {device_id}")
            context.log(f"Topic: {topic}")
            context.log(f"Message: {message}")

            # Send message to MQTT broker
            success, result_message = send_mqtt_message(topic, message)

            if success:
                context.log(f"MQTT message sent to {topic}: {message}")
                context.log(
                    f"Play action completed successfully for device {device_id}"
                )
                return context.res.json(
                    {
                        "success": True,
                        "message": result_message,
                        "topic": topic,
                        "sent_message": message,
                        "broker": "broker.hivemq.com",
                        "action_type": "play",
                    }
                )
            else:
                context.error(f"Failed to send MQTT message: {result_message}")
                return context.res.json(
                    {
                        "success": False,
                        "error": result_message,
                        "topic": topic,
                        "broker": "broker.hivemq.com",
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
