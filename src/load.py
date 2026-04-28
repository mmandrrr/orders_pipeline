"""Loading data to S3 in CSV format"""

import boto3


def load(df, bucket, key):
    data_in_csv = df.to_csv(index=False)
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=data_in_csv)
    print("Loaded succesfully")
