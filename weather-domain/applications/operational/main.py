from fastapi import FastAPI
import pandas as pd
import requests
from minio import Minio
from io import BytesIO
import json
from utilities.kafka_rest_proxy_exporter import KafkaRESTProxyExporter
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.trace import SpanKind

app = FastAPI()
FastAPIInstrumentor.instrument_app(app)

SERVICE_ADDRESS = "http://localhost:8006"
SERVICE_NAME = "WEATHER_DOMAIN_OPERATIONAL_SERVICE"
SERVICE_VERSION = "1.0.0"
ENVIRONMENT = "production"
KAFKA_REST_PROXY_URL = "http://localhost/kafka-rest-proxy"

# Setting up the trace provider
trace.set_tracer_provider(TracerProvider())

kafka_exporter = KafkaRESTProxyExporter(topic_name="telemetry-data", rest_proxy_url=KAFKA_REST_PROXY_URL, service_name=SERVICE_NAME, service_address=SERVICE_ADDRESS)
span_processor = BatchSpanProcessor(kafka_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# Setting up OpenTelemetry
tracer = trace.get_tracer(__name__)

# Load your CSV data
df1 = pd.read_csv('temperature.csv')
df2 = pd.read_csv('precipitation.csv')

# Merge the dataframes on the 'date' column
merged_df = pd.merge(df1, df2, on='date')
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

# Running this on startup 
@app.on_event('startup')
async def startup_event():
    global cluster_id
    response = requests.get(f"{kafka_rest_proxy_base_url}/v3/clusters")
    serializedData = response.json()
    # set the global cluster id
    cluster_id = serializedData["data"][0]["cluster_id"]
    # return the value to the agent
    return cluster_id


@app.post('/')
def main():
    return "Operational App"


@app.get('/get-cluster-id')
async def get_cluster_id(): 
    global cluster_id
    response = requests.get(f"{kafka_rest_proxy_base_url}/v3/clusters")
    serializedData = response.json()
    # set the global cluster id
    cluster_id = serializedData["data"][0]["cluster_id"]
    # return the value to the agent
    return cluster_id

@app.get("/")
async def main_function(): 
    return "welcome to the weather domain operational service"


@app.get('/store-operational-data')
async def produce_to_kafka(topic: str = 'domain-weather-operational-data'):
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
                    "key" : "weather-domain-operational-data-stored",
                    "value": {
                        "message": "Data loading started"
                    }
                }
            ]
        }

        span.add_event("Dispatching data loading start event")

        print(url)
        response = requests.post(url, headers=headers, data=json.dumps(payload_start))

        if response.status_code != 200:
            span.set_attribute("error", True)
            span.set_attribute("error_details", response.text)
            return {"error": response.text}

        span.add_event("Storing merged data in Minio")

        # Store the merged data in Minio
        csv_data = merged_df.to_csv(index=False).encode('utf-8')
        csv_bytes = BytesIO(csv_data)
        min_io_bucket_name = "weather-domain-operational-data"
        min_io_object_name = "merged_data-v2.5.csv"

        bucketExists = minio_client.bucket_exists(min_io_bucket_name)
        if not bucketExists:
            minio_client.make_bucket(min_io_bucket_name)
        else: 
            span.add_event("Minio bucket already exists")
            print("bucket already exist")

        minio_client.put_object(
            bucket_name=min_io_bucket_name,
            object_name=min_io_object_name,
            data=csv_bytes,
            length=len(csv_data),
            content_type='text/csv',
        )

        # Dispatch the "Data loading finished" event
        span.add_event("Dispatching data loading finished event")

        payload_end = {
            "records": [
                {   
                    "key" : "weather-domain-operational-data-stored",
                    "value": {
                        "message": "Data loading finished",
                        "distributedStorageAddress" : minio_url,
                        "minio_access_key": minio_acces_key,
                        "minio_secret_key" : minio_secret_key,
                        "bucket_name": min_io_bucket_name,
                        "object_name" : min_io_object_name
                    }
                }
            ]
        }

        response = requests.post(url, headers=headers, data=json.dumps(payload_end))
        if response.status_code != 200:
            span.set_attribute("error", True)
            span.set_attribute("error_details", response.text)
            return {"error": response.text}

    return {"status": "Data loaded and events dispatched"}
