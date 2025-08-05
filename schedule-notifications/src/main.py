from appwrite.client import Client
from appwrite.services.databases import Databases
import os


# This Appwrite function will be executed every time your function is triggered
def main(context):
    # Set project and set API key
    client = (
        Client()
        .set_project(os.environ["APPWRITE_FUNCTION_PROJECT_ID"])
        .set_key(context.req.headers["x-appwrite-key"])
    )

    databases = Databases(client)

    try:
        result = databases.list_collections(
            database_id="projectbilal",
        )
        context.log(result)
    except Exception as e:
        context.error("Failed to create document: " + e.message)
        return context.res.text("Failed to create document")

    return context.res.text("Complete")
