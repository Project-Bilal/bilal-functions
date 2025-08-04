import paho.mqtt.client as mqtt
import json


def send_mqtt_message(topic, message, broker="broker.hivemq.com", port=1883):
    """
    Send a message to the MQTT broker
    """
    try:
        # Create MQTT client
        client = mqtt.Client()

        # Connect to broker
        client.connect(broker, port, 60)

        # Publish message
        client.publish(topic, message)

        # Wait for the message to be sent
        client.loop()

        # Disconnect
        client.disconnect()

        return True, "Message sent successfully"
    except Exception as e:
        return False, f"Failed to send message: {str(e)}"


# This Appwrite function will be executed every time your function is triggered
def main(context):
    try:
        data = context.req.body_json

        # Check if all required fields are present
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

            # Send message to MQTT broker
            success, result_message = send_mqtt_message(topic, message)

            if success:
                context.log(f"MQTT message sent to {topic}: {message}")
                return context.res.json(
                    {
                        "success": True,
                        "message": result_message,
                        "topic": topic,
                        "sent_message": message,
                        "broker": "broker.hivemq.com",
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
                    },
                    500,
                )
        else:
            # Return error if required fields are missing
            missing_fields = [field for field in required_fields if field not in data]
            return context.res.json(
                {
                    "success": False,
                    "error": f"Missing required fields: {missing_fields}",
                    "required_fields": required_fields,
                },
                400,
            )

    except json.JSONDecodeError:
        return context.res.json(
            {"success": False, "error": "Invalid JSON in request body"}, 400
        )
