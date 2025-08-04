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
        result = client.publish(topic, message)

        # Wait for the message to be sent
        client.loop()

        # Disconnect
        client.disconnect()

        return True, "Message sent successfully"
    except Exception as e:
        return False, f"Failed to send message: {str(e)}"


# This Appwrite function will be executed every time your function is triggered
def main(context):
    # Get request data
    request_data = context.req.body

    # Default topic and message
    topic = "bilal/notifications"
    message = "Hello from Bilal Functions!"

    # If request body is provided, try to parse it
    if request_data:
        try:
            data = json.loads(request_data)
            topic = data.get("topic", topic)
            message = data.get("message", message)
        except json.JSONDecodeError:
            context.log("Invalid JSON in request body, using defaults")

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
