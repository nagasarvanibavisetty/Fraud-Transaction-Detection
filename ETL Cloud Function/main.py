import base64
import json
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import LabelEncoder
from google.cloud import pubsub_v1
import functions_framework
import numpy as np

# Setup Pub/Sub client and topic details
publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/my-big-data-419817/topics/tranx-data-1'  # Replace with your topic

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Decode the base64 encoded data from Pub/Sub
    data = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
    # Convert the JSON data to a dictionary
    data_dict = json.loads(data)
    # Load the dictionary as a DataFrame
    df = pd.DataFrame([data_dict])

    # Convert date strings to datetime objects
    df['dob'] = pd.to_datetime(df['dob'])
    df['trans_date_trans_time'] = pd.to_datetime(df['trans_date_trans_time'])

    # Derive new columns based on the datetime columns
    df['trans_year'] = df['trans_date_trans_time'].dt.year
    df['trans_month'] = df['trans_date_trans_time'].dt.month
    df['trans_day'] = df['trans_date_trans_time'].dt.day
    df['dob_year'] = df['dob'].dt.year
    df['dob_month'] = df['dob'].dt.month
    df['dob_day'] = df['dob'].dt.day

    # Initialize LabelEncoder for the gender column
    

    # Assuming df is your DataFrame and 'gender' is a column in it
    df['gender_encoded'] = np.where(df['gender'] == 'F', 0, 1)


    # Calculate age
    current_year = datetime.now().year
    df['age'] = current_year - df['dob'].dt.year

    # Calculate the transaction hour
    df['trans_hour'] = df['trans_date_trans_time'].dt.hour

    # Calculate Z-score for transaction amounts
    df['amt_zscore'] = (df['amt'] - df['amt'].mean()) / df['amt'].std()

    # Create binary flags based on conditions
    df['unusual_amount'] = (df['amt'] > df['amt'].mean() + 3 * df['amt'].std()).astype(int)
    df['uncommon_merchant'] = df['merchant'].map(df['merchant'].value_counts() < 10).astype(int)
    df['late_night_transaction'] = ((df['trans_hour'] < 6) | (df['trans_hour'] > 20)).astype(int)

    df['transaction_month'] = df['trans_date_trans_time'].dt.month
    df['transaction_day'] = df['trans_date_trans_time'].dt.day
    df['transaction_day_of_week'] = df['trans_date_trans_time'].dt.dayofweek  # Monday=0, Sunday=6
    cat_columns = ['merchant', 'category', 'gender', 'city', 'state', 'job']
    from google.cloud import storage
    from joblib import load
    import io

    # Initialize the GCS client
    storage_client = storage.Client()

    def download_blob_into_memory(bucket_name, source_blob_name):
        """Downloads a blob from the bucket into memory."""
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        return blob.download_as_bytes()

    # Load the LabelEncoder from GCS
    bucket_name = 'big-data-bucket-abhilash'

    for col in cat_columns:
        # Download the label encoder file into memory
        label_encoder_bytes = download_blob_into_memory(bucket_name, f'label_encoder{col}.joblib')
    
        # Load the label encoder from the bytes in memory
        label_encoder = load(io.BytesIO(label_encoder_bytes))
        # Get the labels
        df[col] = label_encoder.transform(df[col])
        print(f"{col} : {df[col]}")
    # Print the modified DataFrame to verify the changes
    print(df.head())

    # After processing the DataFrame, convert it to a JSON string
    df_json = df.to_json(orient='records')

    # Encode the JSON string as bytes
    message_bytes = df_json.encode('utf-8')

    # Publish the message to the other topic
    try:
        # Data must be a bytestring
        message_data = base64.b64encode(message_bytes).decode('utf-8')
        # When you publish a message, the client returns a future.
        publish_future = publisher.publish(topic_name, data=message_bytes)
        publish_future.result()  # Verify the publish succeeded, raises an exception on failure
        print(f"Message published to {topic_name}.")

        from google.cloud import dataproc_v1

    # def submit_pyspark_job(data, context):
        # Set up the Dataproc job client
        project_id = 'my-big-data-419817'
        region = 'europe-central2'
        cluster_name = 'cluster-62aa'
        job_client = dataproc_v1.JobControllerClient(client_options={
            'api_endpoint': '{}-dataproc.googleapis.com:443'.format(region)
        })

        # Configure the PySpark job
        job_config = {
            'placement': {
                'cluster_name': cluster_name
            },
            'pyspark_job': {
                'main_python_file_uri': 'gs://big-data-bucket-abhilash/predictor_spark.py'
                # Add other job properties if needed
            }
        }

        # Submit the job to the cluster
        result = job_client.submit_job(project_id=project_id, region=region, job=job_config)
        job_id = result.reference.job_id
        print(f'Submitted job "{job_id}" to cluster "{cluster_name}".')

        # Return the job ID for reference
        return job_id

    # For production use, handle errors and monitor the job status appropriately.





    except Exception as e:
        print(f"An error occurred: {e}")


