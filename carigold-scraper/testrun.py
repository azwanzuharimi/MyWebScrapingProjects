import boto3
from io import StringIO
import scrapy
import subprocess

subprocess.run("""scrapy runspider carigold_spider.py -o carigold.csv""")

file_name = 'carigold.csv'
# print(file_name)

csv_buffer = StringIO()

s3 = boto3.resource('s3',
                    aws_access_key_id='',
                    aws_secret_access_key='')

# object = s3.Object('01dev', f'carigold/{file_name}').put(Body=file_name)

s3.meta.client.upload_file('carigold.csv', '01dev', 'carigold/carigold.csv')
