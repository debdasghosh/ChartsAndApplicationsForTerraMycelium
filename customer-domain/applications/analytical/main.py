from http.client import HTTPException
from xmlrpc.client import ResponseError
from fastapi import FastAPI, HTTPException, status, BackgroundTasks
from minio import Minio
import requests
import json
import base64
from typing import List, Dict
import math
import logging
import time 
from confluent_kafka import Producer
from utilities import ensure_table_exists, insert_into_db, register_metadata_to_data_lichen, upload_data_to_minio, fetch_all_customer_data_from_sqlite, kafka_utils
from utilities.kafka_rest_proxy_exporter import KafkaRESTProxyExporter
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

app = FastAPI()
FastAPIInstrumentor.instrument_app(app)

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables
SERVICE_NAME = "CUSTOMER_DOMAIN_ANALYTICAL_SERVICE"
SERVICE_ADDRESS = "http://localhost:8000"
consumer_base_url = None
KAFKA_REST_PROXY_URL = "http://localhost/kafka-rest-proxy"
KAFKA_REST_PROXY_TOPIC_ENDPOINT = f"{KAFKA_REST_PROXY_URL}/topics"


# Setting up the trace provider
trace.set_tracer_provider(TracerProvider())

kafka_exporter = KafkaRESTProxyExporter(topic_name="telemetry-data", rest_proxy_url=KAFKA_REST_PROXY_URL, service_name=SERVICE_NAME, service_address=SERVICE_ADDRESS)
span_processor = BatchSpanProcessor(kafka_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# Setting up OpenTelemetry
tracer = trace.get_tracer(__name__)

# Storage info dictionary
storage_info = {}
MINIO_BASE_URL = "localhost:9001"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Initialize the Minio client
minio_client = Minio(
    MINIO_BASE_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

@app.on_event("startup")
async def startup_event():
    url = f"{KAFKA_REST_PROXY_URL}/consumers/customer-domain-operational-data-consumer/"
    headers = {
        'Content-Type': 'application/vnd.kafka.v2+json',
    }
    data = {
        "name": "operational-data-consumer",
        "format": "binary",
        "auto.offset.reset": "earliest",
        "auto.commit.enable": "false"
    }
    try:
        # attemping to create a consumer 
        response = requests.post(url, headers=headers, data=json.dumps(data))
        # will raise an HTTPError if the status code is 4xx or 5xx
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 409:
            print("Kafka consumer already exists. Proceeding...")
        else:
            raise Exception("Failed to create Kafka consumer: " + str(e))
    else:
        print("Kafka consumer created successfully")
        print(response.json())
        consumer_info = response.json()
        print("Consumer instance URI: " + consumer_info['base_uri'])
        global consumer_base_url
        consumer_base_url = consumer_info['base_uri'].replace(
            'http://', 'http://localhost/')

        # Subscribe the consumer to the topic
        url = consumer_base_url + "/subscription"
        print(url)
        headers = {'Content-Type': 'application/vnd.kafka.v2+json'}
        data = {"topics": ["domain-customer-operational-data"]}  # Adjusted the topic name here
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code != 204:
            raise Exception(
                "Failed to subscribe consumer to topic: " + response.text)
        
@app.on_event("shutdown")
async def shutdown_event():
    url = consumer_base_url
    response = requests.delete(url)
    print(f"Consumer deleted with status code {response.status_code}")

    # Shutdown OpenTelemetry
    trace.get_tracer_provider().shutdown()

@app.get("/")
async def main_function(): 
    return "welcome to the customer domain analytical service"  

# this endpoint should only run after the startup event has succesfully run
@app.get("/subscribe-to-operational-data-and-store-addresses")
async def consume_kafka_message(background_tasks: BackgroundTasks):
    tracer = trace.get_tracer(__name__)

    # Start a new span for this endpoint
    with tracer.start_as_current_span("consume_kafka_message"):
        
        # Verify Consumer existence and status before proceeding
        if consumer_base_url:  # Check if consumer_base_url is initialized
            response_consumer = requests.get(f"{consumer_base_url}/status")
            if response_consumer.status_code != 200:
                # Recreate the consumer if it doesn't exist or is in an error state
                await startup_event()  # Reuse the startup logic to create and subscribe consumer
        
        # If after checking (or recreating) the consumer_base_url is still None, return an error message
        if consumer_base_url is None:
            return {"status": "Consumer has not been initialized. Please try again later."}

        print(consumer_base_url)
        url = consumer_base_url + "/records"
        headers = {"Accept": "application/vnd.kafka.binary.v2+json"}

        ensure_table_exists.ensure_table_exists('object_storage_address.db')

        # You can use the tracer within the consume_records function to instrument finer details.
        def consume_records():
            with tracer.start_as_current_span("consume_records"):
                global storage_info

                response = requests.get(url, headers=headers)
                if response.status_code != 200:
                    raise Exception(f"GET /records/ did not succeed: {response.text}")
                else:
                    records = response.json()
                    for record in records:
                        decoded_key = base64.b64decode(record['key']).decode('utf-8') if record['key'] else None
                        decoded_value_json = base64.b64decode(record['value']).decode('utf-8')
                        value_obj = json.loads(decoded_value_json)

                        storage_info = {
                            "distributedStorageAddress": value_obj.get('distributedStorageAddress', ''),
                            "minio_access_key": value_obj.get('minio_access_key', ''),
                            "minio_secret_key": value_obj.get('minio_secret_key', ''),
                            "bucket_name": value_obj.get('bucket_name', ''),
                            "object_name": value_obj.get('object_name', '')
                        }

                        # If distributedStorageAddress is empty, skip the storage
                        if not storage_info["distributedStorageAddress"]:
                            print("Skipping storage to SQLite since distributedStorageAddress is empty.") 
                            continue

                        # Insert the storage info into the SQLite database
                        insert_into_db.insert_into_db(storage_info)

                        print(f"Consumed record with key {decoded_key} and value {value_obj['message']} from topic {record['topic']}")
                        if 'distributedStorageAddress' in value_obj:
                            print(f"Distributed storage address: {value_obj['distributedStorageAddress']}")
                            print(f"Minio access key: {value_obj['minio_access_key']}")
                            print(f"Minio secret key: {value_obj['minio_secret_key']}")
                            print(f"Bucket name: {value_obj['bucket_name']}")
                            print(f"Object name: {value_obj['object_name']}")

        background_tasks.add_task(consume_records)
        return {"status": "Consuming records in the background"}

@app.get("/register-data-to-data-lichen")
async def retrieve_and_save_data():
    global storage_info
    print(storage_info)
 
    if not storage_info:
        raise HTTPException(404, "Storage info not found")

    try:
        register_metadata_to_data_lichen.register_metadata_to_data_lichen()
        return {"status": "Data successfully retrieved and saved to 'customer_data.db'"}
    except ResponseError as err:
        raise HTTPException(status_code=500, detail=f"An error occurred while fetching the data: {err}")

@app.get('/publish-domains-data')
async def publish_domains_data():
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("publish_domains_data_span") as span:  # start a span
        # Fetch all data from SQLite
        all_data = fetch_all_customer_data_from_sqlite.fetch_all_customer_data_from_sqlite()
        bucket_name = 'custom-domain-analytical-data'
        
        logger.info(f"Starting to process {len(all_data)} data objects.")
        
        for index, data_obj in enumerate(all_data):
            with tracer.start_as_current_span(f"process_data_object_{index}") as data_span:
                try:
                    # Convert individual data object to JSON format
                    data_json = json.dumps(data_obj)

                    # Upload to Minio
                    object_name = f"data_object_{index}.json"
                    upload_data_to_minio.upload_data_to_minio(bucket_name, data_json, object_name, MINIO_BASE_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

                    # Get the current timestamp (you can also use datetime for more granular timestamp details)
                    current_timestamp = time.time()

                    # Set custom attributes on the span
                    data_span.set_attribute("object_id", index)  # Set the object's unique identifier as an attribute
                    data_span.set_attribute("status", "data_ready")
                    data_span.set_attribute("timestamp", current_timestamp)

                    # Notify Kafka about this individual object
                    kafka_utils.post_to_kafka_topic(KAFKA_REST_PROXY_URL, 'customer-domain-data', {
                        "status": "data_ready",
                        "data_location": f"{MINIO_BASE_URL}",
                        "object_id": index,  # or any unique identifier for the data object
                        "timestamp": current_timestamp
                    })

                    logger.info(f"Processed and saved object {index} to Minio.")

                except Exception as e:
                    with tracer.start_as_current_span("error_handling") as error_span:
                        # Set custom attributes on the error span
                        error_span.set_attribute("object_id", index)
                        error_span.set_attribute("status", "processing_failed")
                        error_span.set_attribute("error", str(e))
                        error_span.set_attribute("timestamp", current_timestamp)
                        
                        # Notify Kafka of error for this specific object
                        kafka_utils.post_to_kafka_topic(KAFKA_REST_PROXY_URL, 'customer-domain-data-error', {
                            "status": "processing_failed",
                            "error": str(e),
                            "object_id": index,  # or any unique identifier for the data object
                            "timestamp": current_timestamp
                        })

                        logger.error(f"An error occurred while processing object {index}: {e}")
                        raise HTTPException(status_code=500, detail=f"An error occurred while processing object {index}: {e}")

    logger.info(f"Finished processing {len(all_data)} data objects and saving to Minio.")
    return {"status": f"Processed {len(all_data)} data objects and saved to Minio."}

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

@app.get('/stream-domains-data')
async def stream_domains_data(chunk_size: int = 1000):  # Adjust default chunk size as required
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("stream_domains_data_span") as span:
        # Fetch data from SQLite
        all_data = fetch_all_customer_data_from_sqlite.fetch_all_customer_data_from_sqlite()
        logger.info(f"Starting to stream {len(all_data)} data objects.")

        for record in all_data:
            object_id = record[0]
            data = record[1]
            data_hash = record[2]

            # Split large JSON data into smaller chunks
            chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
            total_chunks = len(chunks)

            for index, chunk in enumerate(chunks):
                with tracer.start_as_current_span(f"process_streaming_object_{object_id}_chunk_{index}") as data_span:
                    try:
                        # Convert individual chunk to JSON format
                        chunk_json = json.dumps({
                            'id': object_id,
                            'chunk': chunk,
                            'chunk_index': index,
                            'total_chunks': total_chunks,
                            'data_hash': data_hash
                        })

                        # Get current timestamp
                        current_timestamp = time.time()

                        # Set custom attributes on the span
                        data_span.set_attribute("object_id", object_id)
                        data_span.set_attribute("chunk_index", index)
                        data_span.set_attribute("status", "data_streamed")
                        data_span.set_attribute("timestamp", current_timestamp)

                        # Notify Kafka about this chunk
                        kafka_utils.post_to_kafka_topic(KAFKA_REST_PROXY_URL, 'customer-domain-stream-data', {
                            "status": "data_streamed",
                            "data": chunk_json,
                            "object_id": object_id,
                            "timestamp": current_timestamp
                        })

                        logger.info(f"Streamed object {object_id} chunk {index} to Kafka.")

                    except Exception as e:
                        with tracer.start_as_current_span("error_handling") as error_span:
                            # Error handling
                            error_span.set_attribute("object_id", object_id)
                            error_span.set_attribute("chunk_index", index)
                            error_span.set_attribute("status", "streaming_failed")
                            error_span.set_attribute("error", str(e))
                            error_span.set_attribute("timestamp", current_timestamp)

                            # Notify Kafka of error
                            kafka_utils.post_to_kafka_topic(KAFKA_REST_PROXY_URL, 'customer-domain-stream-data-error', {
                                "status": "streaming_failed",
                                "error": str(e),
                                "object_id": object_id,
                                "chunk_index": index,
                                "timestamp": current_timestamp
                            })

                            logger.error(f"An error occurred while streaming object {object_id} chunk {index}: {e}")
                            raise HTTPException(status_code=500, detail=f"An error occurred while streaming object {object_id} chunk {index}: {e}")

        logger.info(f"Finished streaming data objects to Kafka.")
