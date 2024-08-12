from http.client import HTTPException
from xmlrpc.client import ResponseError
from fastapi import FastAPI, BackgroundTasks, Response
from minio import Minio
import time
import requests
import json
import base64
from prometheus_client import Counter, start_http_server, generate_latest, CONTENT_TYPE_LATEST, Histogram, Gauge
import psutil


# Global variables
SERVICE_NAME = "TELEMETRY_PROCESSOR_SERVICE"
SERVICE_ADDRESS = "http://localhost:8008"

labels = ['service', 'version', 'address']

# Definining Metrics
kafka_records_consumed = Counter('kafka_records_consumed_total', 'Total Kafka records consumed', labels)
kafka_data_ingested_records = Counter('kafka_data_ingested_records_total', 'Number of Kafka records ingested', labels)
kafka_data_ingested_bytes = Counter('kafka_data_ingested_bytes_total', 'Number of bytes ingested from Kafka records', labels)
kafka_ingestion_errors = Counter('kafka_ingestion_errors_total', 'Number of errors while ingesting data', labels)
cpu_utilization_gauge = Gauge('service_cpu_utilization_percentage', 'CPU Utilization of the Service', labels)
memory_utilization_gauge = Gauge('service_memory_utilization_bytes', 'Memory (RAM) Utilization of the Service', labels)
KAFKA_PROCESSING_TIME = Histogram('kafka_processing_duration_seconds', 'Time taken for processing kafka messages', labels)
ingestion_latency = Histogram('kafka_ingestion_latency_seconds', 'Time taken from data creation to ingestion in seconds', labels)
secret_retrieval_latency = Histogram('secret_retrieval_duration_seconds', 'Time taken for retrieving secrets', labels)
query_processing_time = Histogram('query_processing_duration_seconds', 'Time taken for processing query', labels)

app = FastAPI()

consumer_base_url = None 

@app.on_event("startup")
async def startup_event():
    url = "http://localhost/kafka-rest-proxy/consumers/telemetry-data-consumer/"
    headers = { 
        'Content-Type': 'application/vnd.kafka.v2+json',
    }
    data = {
        "name": "telemetry-data-consumer",
        "format": "binary",
        "auto.offset.reset": "earliest",
        "auto.commit.enable": "true"
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
        headers = {'Content-Type': 'application/vnd.kafka.v2+json'}
        data = {"topics": ["telemetry-data"]} 
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code != 204:
            raise Exception(
                "Failed to subscribe consumer to topic: " + response.text)
        
    start_http_server(8001)


@app.on_event("shutdown")
async def shutdown_event():
    url = consumer_base_url
    response = requests.delete(url)
    print(f"Consumer deleted with status code {response.status_code}")


@app.get("/")
async def main_function(): 
    return "welcome to the telemetry processing service"


@app.get("/subscribe-to-telemetry-data")
async def consume_kafka_message(background_tasks: BackgroundTasks):
    if consumer_base_url is None:
        return {"status": "Consumer has not been initialized. Please try again later."}

    url = consumer_base_url + "/records"
    headers = {"Accept": "application/vnd.kafka.binary.v2+json"}

    def consume_records():
        labels_data = {
                'service': "unknown",
                'version': "unknown",
                'address': "unknown"
        }

        while True:
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                records = response.json()

                if not records:  # No more records left
                    break

                for record in records:
                    
                    start_time = time.time()  # Start the timer

                    publish_time = record.get("timestamp", time.time())  # defaulting to current time if no timestamp
                    current_time = time.time()

                    # Calculating ingestion latency
                    latency = current_time - publish_time

                    decoded_key = base64.b64decode(record['key']).decode('utf-8') if record['key'] else None
                    decoded_value_json = base64.b64decode(record['value']).decode('utf-8')
                    value_obj = json.loads(decoded_value_json)

                    service_name_from_kafka = value_obj.get("service_name", "unknown")
                    service_version_from_kafka = value_obj.get("service_version", "unknown")
                    service_address_from_kafka = value_obj.get("service_address", "unknown")

                    labels_data = {
                        'service': service_name_from_kafka,
                        'version': service_version_from_kafka,
                        'address': service_address_from_kafka
                    }

                    # Data ingestion metrics with labels
                    kafka_records_consumed.labels(**labels_data).inc()
                    kafka_data_ingested_records.labels(**labels_data).inc()
                    kafka_data_ingested_bytes.labels(**labels_data).inc(len(json.dumps(record)))
                    ingestion_latency.labels(**labels_data).observe(latency)

                    # Update the service CPU and Memory utilization from the Kafka message
                    kafka_cpu_utilization = value_obj.get("cpu_utilization", None)
                    kafka_memory_utilization = value_obj.get("memory_utilization", None)

                    if kafka_cpu_utilization is not None:
                        cpu_utilization_gauge.labels(**labels_data).set(kafka_cpu_utilization)

                    if kafka_memory_utilization is not None:
                        memory_utilization_gauge.labels(**labels_data).set(kafka_memory_utilization)

                    # Calculate duration from the event for specific event metrics
                    duration = (value_obj['end_time'] - value_obj['start_time']) / 1e9
                    if value_obj['name'] == "retrieve_secrets":
                        secret_retrieval_latency.labels(**labels_data).observe(duration)
                    elif value_obj['name'].startswith("GET "):
                        query_processing_time.labels(**labels_data).observe(duration)

                    print(f"Consumed record with key {decoded_key} and value {value_obj}")

                    end_time = time.time()  # End the timer
                    duration = end_time - start_time
                    KAFKA_PROCESSING_TIME.labels(**labels_data).observe(duration)  # Observe the duration

            except Exception as e:
                # Error metrics with labels (assuming service details can be derived from the latest processed record in case of errors)
                kafka_ingestion_errors.labels(**labels_data).inc()
                raise Exception(f"Error while consuming data: {str(e)}")

            # CPU & Memory Utilization with labels
            cpu_utilization_gauge.labels(service="TELEMETRY_PROCESSOR_SERVICE", version="1.0.0", address=SERVICE_ADDRESS).set(psutil.cpu_percent())
            memory_utilization_gauge.labels(service="TELEMETRY_PROCESSOR_SERVICE", version="1.0.0", address=SERVICE_ADDRESS).set(psutil.virtual_memory().used)

    background_tasks.add_task(consume_records)
    return {"status": "Consuming records in the background"}


@app.get("/metrics")
async def get_metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


