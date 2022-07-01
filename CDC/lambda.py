import json

import boto3


def lambda_handler(event: dict, context) -> dict:
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    print(f'BUCKET: {bucket_name}')

    file_name = event['Records'][0]['s3']['object']['key']
    print(f'FILE: {file_name}')

    glue = boto3.client('glue')
    _ = glue.start_job_run(
        JobName='glueCDC-pyspark',
        Arguments={
            '--s3_target_path_key': file_name,
            '--s3_target_path_bucket': bucket_name
        }
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
