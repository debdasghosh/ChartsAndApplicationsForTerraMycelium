from minio import Minio

def fetch_data_from_minio(distributed_storage_address, minio_access_key, minio_secret_key, bucket_name, object_name):


    minio_client = Minio(
        distributed_storage_address,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False    
    )

    data = minio_client.get_object(bucket_name, object_name)
    data_str = ''

    for d in data.stream(32*1024):
        data_str += d.decode()

    return data_str