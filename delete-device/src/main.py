import paho.mqtt.client as mqtt


def main(context):

    return context.res.json(
        {"success": False, "error": "Invalid JSON in request body"}, 400
    )
