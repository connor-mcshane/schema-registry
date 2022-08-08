"""

cloud function triggered from pub sub topics

"""

import schema_registry_client

PROJECT = "cool project"
SCHEMA_REGISTRY_BUCKET = "bucket"
schema_registry = schema_registry_client.SchemaRegistry.from_gcs_bucket(
        SCHEMA_REGISTRY_BUCKET
)


def pub_sub_json_event(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    pub_sub_event = schema_registry_client.PubSubJsonEvent(
            request
    )

    # check if message validates against a schema
    schema = schema_registry.get_schema(pub_sub_event.schema_id)
    if schema:
        schema.validate(data_dict=pub_sub_event.payload)
    # else kickoff process for missing schema

    # do something with your validated message


def pub_sub_avro_event(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    pub_sub_event = schema_registry_client.PubSubJsonEvent(
            request
    )

    # check if message validates against a schema
    schema = schema_registry.get_schema(pub_sub_event.schema_id)
    if schema:
        schema.validate(data_dict=pub_sub_event.payload)