import requests

def listen_to_kafka_topic(kafka_rest_proxy_url, topic_name):
    url = f"{kafka_rest_proxy_url}/topics/{topic_name}/records"
    headers = {"Accept": "application/vnd.kafka.binary.v2+json"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Error listening to topic {topic_name}: {response.text}")
    return response.json()

def post_to_kafka_topic(kafka_rest_proxy_url, topic_name, message):
    url = f"{kafka_rest_proxy_url}/topics/{topic_name}"
    headers = {
        "Content-Type": "application/vnd.kafka.json.v2+json"
    }
    payload = {
        "records": [
            {"value": message}
        ]
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 200:
        raise Exception(f"Error posting to topic {topic_name}: {response.text}")