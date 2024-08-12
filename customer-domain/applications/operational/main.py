from fastapi import FastAPI
import requests
from minio import Minio
from io import BytesIO
import json
import os
import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from utilities.kafka_rest_proxy_exporter import KafkaRESTProxyExporter
from opentelemetry.trace import SpanKind

app = FastAPI()
FastAPIInstrumentor.instrument_app(app)

# Global variables
SERVICE_NAME = "CUSTOMER_DOMAIN_OPERATIONAL_SERVICE"
SERVICE_ADDRESS = "http://localhost:8001"
KAFKA_REST_PROXY_URL = "http://localhost/kafka-rest-proxy"

# Setting up the trace provider
trace.set_tracer_provider(TracerProvider())

kafka_exporter = KafkaRESTProxyExporter(topic_name="telemetry-data", rest_proxy_url=KAFKA_REST_PROXY_URL, service_name=SERVICE_NAME, service_address=SERVICE_ADDRESS)
span_processor = BatchSpanProcessor(kafka_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# Setting up OpenTelemetry
tracer = trace.get_tracer(__name__)

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
cluster_id = None 
kafka_rest_proxy_base_url = "http://localhost/kafka-rest-proxy"
minio_url = "localhost:9001"
minio_acces_key = "minioadmin"
minio_secret_key = "minioadmin"

# Initialize the Minio client
minio_client = Minio(
    minio_url,
    access_key=minio_acces_key,
    secret_key=minio_secret_key,
    secure=False
)

@app.on_event('startup')
async def startup_event():
    global cluster_id
    response = requests.get(f"{kafka_rest_proxy_base_url}/v3/clusters")
    serializedData = response.json()
    cluster_id = serializedData["data"][0]["cluster_id"]
    return cluster_id

@app.post('/')
def main():
    return "Operational App"

@app.get('/get-cluster-id')
async def get_cluster_id(): 
    global cluster_id
    response = requests.get(f"{kafka_rest_proxy_base_url}/v3/clusters")
    serializedData = response.json()
    cluster_id = serializedData["data"][0]["cluster_id"]
    return cluster_id

@app.get("/")
async def main_function(): 
    return "welcome to the customer domain operational service"

def list_json_files(directory='.'):
    """Return a list of all JSON files in the directory."""
    return [f for f in os.listdir(directory) if f.endswith('.json')]

@app.get('/store-operational-data')
async def produce_to_kafka(topic: str = 'domain-customer-operational-data'):
    with tracer.start_as_current_span("store-operational-data", kind=SpanKind.SERVER) as span:

        # Kafka REST Proxy URL for producing messages to a topic
        url = f"{kafka_rest_proxy_base_url}/topics/" + topic
        headers = {
            'Content-Type': 'application/vnd.kafka.json.v2+json',
        }

        # Dispatch the "Data loading started" event
        payload_start = {
            "records": [
                {
                    "key" : "customer-domain-operational-data-stored",
                    "value": {
                        "message": "Data loading started"
                    }
                }
            ]
        }

        span.add_event("Dispatching data loading start event")
        
        response = requests.post(url, headers=headers, data=json.dumps(payload_start))
        if response.status_code != 200:
            span.set_attribute("error", True)
            span.set_attribute("error_details", response.text)
            return {"error": response.text}

        logging.info("Starting to process and store operational data...")

        # List all JSON files in current directory
        json_files = list_json_files()  # Use the previously defined function
        total_files = len(json_files)
        processed_files = 0

        print(total_files)
        print(json_files)

        minio_bucket_name = "customer-domain-operational-data"
        
        # Check bucket existence and create if not
        if not minio_client.bucket_exists(minio_bucket_name):
            minio_client.make_bucket(minio_bucket_name)

        # Loop over all JSON files, store them in Minio, and dispatch details to Kafka
        for json_file in json_files:
            with tracer.start_as_current_span(f"processing-{json_file}", kind=SpanKind.INTERNAL) as file_span:
                try:
                    # Read the entire JSON file
                    with open(json_file, 'r') as f:
                        data = f.read()

                    # Convert data to bytes and store in Minio
                    data_bytes = data.encode('utf-8')
                    data_io = BytesIO(data_bytes)
                    
                    # Put object in Minio
                    minio_client.put_object(
                        bucket_name=minio_bucket_name,
                        object_name=json_file,
                        data=data_io,
                        length=len(data_bytes),
                        content_type='application/json',
                    )

                    # Dispatch the details of the file to Kafka
                    payload_file = {
                        "records": [
                            {   
                                "key": "customer-domain-operational-data-stored",
                                "value": {
                                    "message": f"Stored {json_file}",
                                    "distributedStorageAddress": minio_url,
                                    "minio_access_key": minio_acces_key,
                                    "minio_secret_key": minio_secret_key,
                                    "bucket_name": minio_bucket_name,
                                    "object_name": json_file
                                }
                            }
                        ]
                    }

                    response = requests.post(url, headers=headers, data=json.dumps(payload_file))
                    if response.status_code != 200:
                        file_span.set_attribute("error", True)
                        file_span.set_attribute("error_details", response.text)
                        logging.error(f"Error dispatching details of {json_file} to Kafka: {response.text}")

                    processed_files += 1
                    logging.info(f"Processed {processed_files}/{total_files} files.")
                except Exception as e:
                    file_span.set_attribute("error", True)
                    file_span.set_attribute("error_details", str(e))
                    logging.error(f"Error processing file {json_file}. Details: {str(e)}")

        logging.info("Finished processing and storing operational data.")
        
    return {"status": f"Data loaded from {processed_files}/{total_files} files."}

