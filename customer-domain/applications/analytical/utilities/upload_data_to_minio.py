from minio import Minio
import io

def upload_data_to_minio(bucket_name, data, object_name, minio_url, minio_access_key, minio_secret_key):
    minioClient = Minio(minio_url,
                        access_key=minio_access_key,
                        secret_key=minio_secret_key,
                        secure=False)
    
    # Ensure bucket exists or create
    if not minioClient.bucket_exists(bucket_name):
        minioClient.make_bucket(bucket_name)

    # Upload the data
    data_bytes = data.encode('utf-8')
    file_size = len(data_bytes)
    minioClient.put_object(bucket_name, object_name, io.BytesIO(data_bytes), file_size, content_type='application/json')

