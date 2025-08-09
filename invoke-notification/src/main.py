import paho.mqtt.client as mqtt
import json
import os
import ssl


def send_mqtt_message(topic, message):
    """
    Send a message to the MQTT broker
    """
    try:
        # Read MQTT configuration from environment variables
        broker = os.environ.get("MQTT_BROKER")
        port_str = os.environ.get("MQTT_PORT", "8883")
        mqtt_username = os.environ.get("MQTT_USER")
        mqtt_password = os.environ.get("MQTT_PASS")

        if not broker:
            return False, "MQTT_BROKER environment variable is not set"

        try:
            port = int(port_str)
        except ValueError:
            return False, f"Invalid MQTT_PORT value: {port_str}"

        # Create MQTT client with a unique client ID
        client = mqtt.Client(client_id=f"bilal_function_{hash(topic)}")

        # Configure username/password if provided
        if mqtt_username:
            client.username_pw_set(mqtt_username, mqtt_password)

        # Enable TLS/SSL. Rely on system CA certificates
        client.tls_set(
            ca_certs=None,
            certfile=None,
            keyfile=None,
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS,
            ciphers=None,
        )
        client.tls_insecure_set(False)

        # Set connection timeout and connect over TLS
        client.connect(broker, port, keepalive=60)

        # Start the loop to handle the connection
        client.loop_start()

        # Wait a moment for connection to establish
        import time

        time.sleep(1)

        # Publish message with QoS 1 for reliability
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
            context.log(f"Topic: {topic}")
            context.log(f"Message: {message}")

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
                        "broker": os.environ.get("MQTT_BROKER"),
                    }
                )
            else:
                context.error(f"Failed to send MQTT message: {result_message}")
                return context.res.json(
                    {
                        "success": False,
                        "error": result_message,
                        "topic": topic,
                        "broker": os.environ.get("MQTT_BROKER"),
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
