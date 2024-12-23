from google.cloud import storage
import pandas as pd
from io import StringIO

# Initialize the Cloud Storage client
storage_client = storage.Client()

# Specify the bucket name and file name
bucket_name = 'zago1'
source_blob_name = 'data.csv'  # Original file in the bucket

# Get the bucket object
bucket = storage_client.get_bucket(bucket_name)

# Get the blob (file) object
blob = bucket.blob(source_blob_name)

# Download the content of the file as a string
data = blob.download_as_text()

# Read the CSV data into a pandas DataFrame
df = pd.read_csv(StringIO(data))

# Remove duplicates
df.drop_duplicates(inplace=True)

# Fill empty or NaN values with 'null'
df.fillna('null', inplace=True)

# Print the cleaned data
print("Cleaned DataFrame:")
print(df)






