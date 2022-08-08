import hashlib

from google.cloud import pubsub_v1
import schema_registry_client
import json

PROJECT = "cool project"
SCHEMA_REGISTRY_BUCKET = "bucket"
TOPIC = "topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT, TOPIC)

data = {
        "firstName": "John",
        "lastName": "Doe",
        "age": 21
}

data_str = json.dumps(data)
data_byte_str = data_str.encode("utf-8")
hash = hashlib.sha1(data_byte_str).hexdigest()
attributes = schema_registry_client.get_schema_meta_fields_dict(id="cool-person", hash=hash, version='0.0.1', data_type="JSON")
future = publisher.publish(
        topic_path, data_byte_str, **attributes
)
print(future.result())

print(f"Published messages with custom attributes to {topic_path}.")
