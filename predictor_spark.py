from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
import joblib
from google.cloud import storage
from io import BytesIO

# import subprocess
# import sys

# def install_package(package_name):
#     subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])

# # Call the function with "xgboost"
# install_package("google-cloud-pubsub")

from google.cloud import pubsub_v1
import json

# Set up the Pub/Sub client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path('my-big-data-419817', 'tranx-data-sub')

# Initialize Spark session
spark = SparkSession.builder.appName('fraud_prediction').getOrCreate()

# Function to receive messages
def receive_messages():
    response = subscriber.pull(subscription=subscription_path, max_messages=10)
    ack_ids = [msg.ack_id for msg in response.received_messages]    
    print("Response:  ",response)
    # Process messages
    data_list = []
    for msg in response.received_messages:
        data = json.loads(msg.message.data)
        data_list.append(data)
        

    # Convert to Spark DataFrame
    if data_list:
        spark_df = spark.createDataFrame(data_list)
        subscriber.acknowledge(subscription=subscription_path, ack_ids=[msg.ack_id])
        print("Acknowledged all messages.")
        print("Got the data : ",data)
        return spark_df
    else:
        return None

# Use the function to get data
pubsub_df = receive_messages()

print("PubSub DF : ",pubsub_df)
if pubsub_df:
    pubsub_df.show()


dummy_data = {
    'Id': [1],
    'cc_num': [1234567890123456],
    'merchant': [0],
    'category': [0],
    'amt': [100.50],
    'gender': [1],
    'city': [0],
    'state': [0],
    'zip': [12345],
    'lat': [34.0522],
    'long': [-118.2437],
    'city_pop': [500000],
    'job': [1],
    'unix_time': [1614556800],
    'merch_lat': [34.0522],
    'merch_long': [-118.2437],
    'trans_year': [2021],
    'trans_month': [3],
    'trans_day': [1],
    'dob_year': [1990],
    'dob_month': [1],
    'dob_day': [1],
    'gender_encoded': [1],  # Assuming 'M' is encoded as 1
    'age': [31],
    'trans_hour': [15],
    'amt_zscore': [1.5],  # Assuming a z-score of 1.5 for the amount
    'unusual_amount': [0],  # Assuming this transaction is not unusual
    'uncommon_merchant': [0],
    'late_night_transaction': [0],  # Assuming this is not a late-night transaction
    'transaction_month': [3],
    'transaction_day': [1],
    'transaction_day_of_week': [1],
    'time_duration': [5]  # Assuming a duration of 5 (units not specified)
}


# Create the DataFrame using the schema defined
dummy_spark_df = spark.createDataFrame(pd.DataFrame(dummy_data))

# Show the DataFrame
dummy_spark_df.show()

def test_gcs_connectivity(bucket_name, model_path):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(model_path)
        if not blob.exists():
            print("Model file does not exist in the bucket.")
            return False
        print("Success: Connection to GCS bucket and access to the file verified.")
        return True
    except Exception as e:
        print(f"Failed to access GCS: {str(e)}")
        return False

def load_model_from_gcs(bucket_name, model_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(model_path)
    model_content = blob.download_as_bytes()
    model = joblib.load(BytesIO(model_content))  # Using BytesIO to create a file-like object
    return model

bucket_name = "big-data-bucket-abhilash"
model_path = "model_xgboost1.joblib"

if test_gcs_connectivity(bucket_name, model_path):
    loaded_model = load_model_from_gcs(bucket_name, model_path)
    prediction = loaded_model.predict(pd.DataFrame(dummy_data))
    print("Prediction: ",prediction)

# Initialize the Pub/Sub publisher client and define the topic path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('my-big-data-419817', 'prediction-results')
if prediction == [0]:
    print("Not Fraud")
    topic_path_email = publisher.topic_path('my-big-data-419817', 'prediction-results-email')

def publish_predictions(data_df):
    """
    Publish the data along with predictions to a Pub/Sub topic.

    Args:
        data_df (pandas.DataFrame): The dataframe containing the data and predictions.
    """
    # Convert each row in DataFrame to a JSON string and publish
    for index, row in data_df.iterrows():
        message_json = json.dumps(row.to_dict())
        message_bytes = message_json.encode('utf-8')
        # Publish message
        publisher.publish(topic_path, data=message_bytes)
        if prediction == [0]:
            publisher.publish(topic_path_email, data=message_bytes)
        print(f"Published {message_json} to {topic_path}")

# Convert Spark DataFrame to Pandas DataFrame for ease of processing
dummy_pandas_df = dummy_spark_df.toPandas()
dummy_pandas_df['predictions'] = prediction

# Now, publish the complete data including predictions
publish_predictions(dummy_pandas_df)

# Stop the Spark session
spark.stop()
