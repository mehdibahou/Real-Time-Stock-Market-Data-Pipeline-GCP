import pandas as pd
from io import StringIO
import base64
import json
import requests
import os
from google.cloud import pubsub_v1
from google.cloud import storage

# Function to process Pub/Sub messages
def process_pubsub_message(event, context):
    # Decode the Pub/Sub message data
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)

    try:
        STOCK_NAME = data['GOOG']
        DURATION = data['60000']
        API_KEY = data['***********']

        # Construct the API URL
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={STOCK_NAME}&interval={DURATION}&apikey={API_KEY}&datatype=csv'

        # Make the API request
        response = requests.get(url)

        if response.status_code == 200:
            # Parse the response content as CSV and create a DataFrame
            df = pd.read_csv(StringIO(response.text))
            print(df.head())  # Print the first few rows of the DataFrame
            print("Data sent to Pub/Sub.")

            # Create a Pub/Sub publisher client
            publisher = pubsub_v1.PublisherClient()

            # Define the topic path
            project_id = "**********"
            topic_name = "real_time_data"
            topic_path = f"projects/{project_id}/topics/{topic_name}"

            # Encode the message data as bytes
            message_data = df.to_json(orient="records").encode("utf-8")

            # Create a Pub/Sub message
            message = pubsub_v1.types.PubsubMessage(data=message_data)

            # Publish the message to the topic
            future = publisher.publish(topic_path, data=message_data)

            # Wait for the message to be acknowledged by the server (optional)
            # This ensures the message was successfully published.
            future.result()

            print(f"Message published to {topic_path}")
            
            try:
            # Upload the DataFrame as a CSV file to a GCS bucket
                gcs_bucket_name = "ayoubstockdata"
                gcs_blob_name = "data.csv"  # Specify the desired path and filename
                gcs_client = storage.Client()
                gcs_bucket = gcs_client.bucket(gcs_bucket_name)
                gcs_blob = gcs_bucket.blob(gcs_blob_name)
                df.to_csv(gcs_blob.open("w"), index=False)
                print(f"DataFrame uploaded to GCS bucket: {gcs_blob_name}")
            except Exception as e:
                print(f"An error occurred during GCS upload: {str(e)}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    # This is for testing locally; the actual function will be triggered by Pub/Sub
    data = {
        'GOOG': 'GOOG',
        '60000': '1min',
        'HMD57AHWSLG5Z1E9': 'HMD57AHWSLG5Z1E9'
    }
    encoded_data = base64.b64encode(json.dumps(data).encode('utf-8'))
    process_pubsub_message({'data': encoded_data}, None)