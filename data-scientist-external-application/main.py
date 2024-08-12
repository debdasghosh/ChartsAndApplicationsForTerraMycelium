from fastapi import FastAPI, HTTPException
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from utilities import kafka_rest_proxy_exporter
from concurrent.futures import ThreadPoolExecutor
from minio import Minio
import threading
import logging
from hvac import Client

app = FastAPI()
FastAPIInstrumentor.instrument_app(app)

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables
SERVICE_ADDRESS = "http://localhost:8010"
SERVICE_NAME = "DATA_SCIENTIST_APPLICATION"
SERVICE_VERSION = "1.0.0"
ENVIRONMENT = "production"
KAFKA_REST_PROXY_URL = "http://localhost/kafka-rest-proxy"

# Setting up the trace provider
trace.set_tracer_provider(TracerProvider())
kafka_exporter = kafka_rest_proxy_exporter.KafkaRESTProxyExporter(
    topic_name="telemetry-data",
    rest_proxy_url=KAFKA_REST_PROXY_URL,
    service_name=SERVICE_NAME,
    service_address=SERVICE_ADDRESS
)
span_processor = BatchSpanProcessor(kafka_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# Setting up OpenTelemetry
tracer = trace.get_tracer(__name__)

# MinIO fetch utilities
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

        logger.info(f"Fetching object: {storage_info['object_name']} from bucket: {storage_info['bucket_name']}")

        data = minio_client.get_object(storage_info["bucket_name"], storage_info["object_name"])
        chunks = []
        chunk_count = 0
        for d in data.stream(32*1024):
            chunk_count += 1
            chunks.append(d)
            logger.info(f"Fetched chunk {chunk_count} for object: {storage_info['object_name']}")
        
        data_str = b''.join(chunks).decode('utf-8')
        return data_str

@app.get("/")
async def welcome():
    return "Welcome to the Data Scientist Query Service!"

@app.get("/query-data/{data_location}")
async def query_data(data_location: str):
    valid_data_locations = ["custom-domain-analytical-data", "weather-domain-analytical-data"]
    if data_location not in valid_data_locations:
        logger.error(f"Invalid data location provided: {data_location}")
        raise HTTPException(status_code=400, detail="Invalid data location")
    
    logger.info(f"Fetching secrets for data_location: {data_location}")
    with tracer.start_as_current_span("retrieve_secrets") as span:
        client = Client(url='http://localhost:8200', token='root')
        read_response = client.secrets.kv.read_secret_version(path='Data-Scientist-User-Pass')
        
        if 'data' not in read_response or 'data' not in read_response['data']:
            logger.error("Failed to retrieve secrets from Vault")
            raise HTTPException(status_code=500, detail="Unable to retrieve secrets from Vault")
        
        secrets = read_response['data']['data']
    
    logger.info("Fetching data from Minio")
    with tracer.start_as_current_span("query_processing") as span:
        storage_info = {
            "distributedStorageAddress": "localhost:9001",
            "minio_access_key": "minioadmin",
            "minio_secret_key": "minioadmin",
            "bucket_name": data_location
        }

        logger.info(f"Connecting to Minio with storage_info: {storage_info}")
        minio_client = Minio(
            storage_info["distributedStorageAddress"],
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )

        objects_list = []
        for obj in minio_client.list_objects(storage_info["bucket_name"]):
            object_data = minio_fetch({
                **storage_info,
                "object_name": obj.object_name
            })
            objects_list.append({
                "object_name": obj.object_name,
                "data": object_data
            })

        return {"objects": objects_list}
