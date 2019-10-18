import boto3
import gzip
from io import BytesIO

def emptyFileCheck(bucketName, fileName):
    try:
        s3 = boto3.resource('s3')
        s3obj = s3.Object(bucketName, fileName)
        data =  s3obj.get()["Body"].read()
        gzipfile = BytesIO(data)
        gzipfile = gzip.GzipFile(fileobj=gzipfile)
        content = gzipfile.read()
        if content.decode('utf8').count > 1:
            return False
        else:
            return True
    except Exception as e:
        print("Error in Reading File: " + e)
        raise e
