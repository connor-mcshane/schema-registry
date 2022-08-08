import logging
import json
import base64
import dateutil.parser

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


def json_loads(data):
    """
    :param data:
    :return:
    """
    try:
        return json.loads(data)
    except ValueError:
        logging.error(f"Decoding JSON has failed for {data}")


def parse_payload_from_envelope(envelope):
    """
    :param envelope:
    :return:
    """
    return json_loads(base64.b64decode(envelope["message"]["data"]).decode("utf-8"))


class PubSubJsonEvent:
    """ returns parsed pub sub event """
    def __init__(self, event_request):
        self.request = event_request
        raw_envelope: dict = json_loads(event_request.data.decode("utf-8"))
        self.payload: dict = parse_payload_from_envelope(raw_envelope)
        self.payload_str: str = json.dumps(self.payload, default=str)
        self.envelope_decoded = raw_envelope  # shallow copy to save memory
        self.envelope_decoded["message"]["data"] = self.payload
        self.publish_time = dateutil.parser.isoparse(
            self.envelope_decoded["message"]["publish_time"]
        ).strftime(TIMESTAMP_FORMAT)
        self.message_id = self.envelope_decoded["message"]["message_id"]
        self.subscription = self.envelope_decoded["subscription"]

        attributes = self.envelope_decoded["message"].get("attributes")
        if attributes:
            self.schema_id: str = attributes.get("schema_hash_id").lower()
            self.version: int = int(float(attributes.get("version", 1)))
