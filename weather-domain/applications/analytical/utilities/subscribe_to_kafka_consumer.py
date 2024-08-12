import requests
import json

def subscribe_to_kafka_consumer(base_url, topics):
    url = f"{base_url}/subscription"
    print(url)

    headers = {'Content-Type': 'application/vnd.kafka.v2+json'}
    data = {"topics": topics}
    print(data)

    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code

        if response.status_code == 204:
            print(f"Successfully subscribed to topics: {', '.join(topics)}")
        else:
            print(f"Unexpected status code {response.status_code}: {response.text}")

    except requests.RequestException as e:
        raise Exception(f"Failed to subscribe consumer to topics {', '.join(topics)}: {str(e)}")
