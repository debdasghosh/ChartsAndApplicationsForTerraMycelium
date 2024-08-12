from concurrent.futures import ThreadPoolExecutor
from minio import Minio
import threading

minio_lock = threading.Lock()
MINIO_POOL_SIZE = 10
executor = ThreadPoolExecutor(max_workers=MINIO_POOL_SIZE)

def minio_fetch(storage_info):
    with minio_lock:
        minio_client = Minio(
            storage_info["distributedStorageAddress"],
            access_key=storage_info["minio_access_key"],
            secret_key=storage_info["minio_secret_key"],
            secure=False
        )

        data = minio_client.get_object(storage_info["bucket_name"], storage_info["object_name"])
        chunks = []

        for d in data.stream(32*1024):
            chunks.append(d)

        data_str = b''.join(chunks).decode('utf-8')
        return data_str

def fetch_data_from_minio(storage_info):
    return executor.submit(minio_fetch, storage_info).result()