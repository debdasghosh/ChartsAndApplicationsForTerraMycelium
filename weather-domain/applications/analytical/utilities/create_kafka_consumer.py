import requests
import json

def create_kafka_consumer(url, headers, data):
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    def adjust_base_uri(base_uri):
        # If 'localhost' isn't in the string, replace 'http://' with 'http://localhost/'
        if 'localhost' not in base_uri:
            return base_uri.replace('http://', 'http://localhost/')
        return base_uri

    if response.status_code == 200:  # Successfully created
        response_json = response.json()
        
        if 'base_uri' in response_json:
            base_uri = adjust_base_uri(response_json['base_uri'])
            
            return {
                'base_uri': base_uri,
                'message': 'Kafka consumer created successfully'
            }
        else:
            raise Exception("Expected 'base_uri' key in the response but not found.")
    elif response.status_code == 409:  # Already exists
        # Construct base_uri for the existing consumer
        base_uri = f"{url}/instances/{data['name']}"
        base_uri = adjust_base_uri(base_uri)
        
        return {
            'base_uri': base_uri,
            'message': 'Kafka consumer already exists.'
        }
    else:
        raise Exception(f"Failed to create Kafka consumer. Status Code: {response.status_code}. Error: {response.text}")
