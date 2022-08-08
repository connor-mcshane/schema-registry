import unittest.mock

from google.cloud import storage
from unittest.mock import patch, MagicMock
from schema_registry_client import Schema, SchemaRegistry, SchemaValidationError


class TestSchemaRegistry(unittest.TestCase):

    def test_json_schema_simple(self):
        example_schema = {
                "id": "cool-person",
                "version": "1.0.0",
                "hash": "4fd527554f00f7742d2f03330569dcb9d2dd2979",
                "data_type": "JSON",
                "schema": {
                        "$id": "https://example.com/person.schema.json",
                        "$schema": "https://json-schema.org/draft/2020-12/schema",
                        "title": "Person",
                        "type": "object",
                        "properties": {
                                "firstName": {
                                        "type": "string",
                                        "description": "The person's first name."
                                },
                                "lastName": {
                                        "type": "string",
                                        "description": "The person's last name."
                                },
                                "age": {
                                        "description": "Age in years which must be equal to or greater than zero.",
                                        "type": "integer",
                                        "minimum": 0
                                }
                        }
                }
        }

        example_data = {
                "firstName": "John",
                "lastName": "Doe",
                "age": 21
        }

        json_schema = Schema(**example_schema)

        try:
            json_schema.validate(example_data)
        except SchemaValidationError:
            self.fail("Issue with schema package")

        example_bad_data = {
                "BAD NAME": "John",
                "lastName": "Doe",
                "age": 21
        }
        try:
            json_schema.validate(example_bad_data)
        except SchemaValidationError:
            pass  # pass test

    def test_avro_schema_simple(self):
        example_schema = {
                "id": "cool-person",
                "version": "1.0.0",
                "hash": "bb91c8f9fab6a004df95fc33df9a11ccd745290c",
                "data_type": "AVRO",
                "schema": {
                        "name": "MyClass",
                        "type": "record",
                        "namespace": "com.acme.avro",
                        "fields": [
                                {
                                        "name": "firstName",
                                        "type": "string"
                                },
                                {
                                        "name": "lastName",
                                        "type": "string"
                                },
                                {
                                        "name": "age",
                                        "type": "int"
                                }
                        ]
                }
        }

        example_data = {
                "firstName": "John",
                "lastName": "Doe",
                "age": 21
        }

        avro_schema = Schema(**example_schema)

        try:
            avro_schema.validate(example_data)
        except SchemaValidationError:
            self.fail("Issue with schema package")

        example_bad_data = {
                "BAD NAME": "John",
                "lastName": "Doe",
                "age": 21
        }
        try:
            avro_schema.validate(example_bad_data)
        except SchemaValidationError:
            pass  # pass test

    @patch("schema_registry_client.schema_registry.json.load")
    @patch("schema_registry_client.schema_registry.open")
    def test_schema_registry_init_from_gcs(self, mock_open, mock_json_load):
        # setup mocks
        mock_storage_client = MagicMock(spec=storage.Client)
        mock_blob = MagicMock()
        mock_blob.name = "COOL-SCHEMA.json"
        mock_blobs_list = [mock_blob]
        mock_storage_client.get_bucket.return_value.list_blobs.return_value = (
                mock_blobs_list
        )
        example_schema = {
                "id": "cool-person",
                "version": "1.0.0",
                "hash": "4fd527554f00f7742d2f03330569dcb9d2dd2979",
                "data_type": "JSON",
                "schema": {
                        "$id": "https://example.com/person.schema.json",
                        "$schema": "https://json-schema.org/draft/2020-12/schema",
                        "title": "Person",
                        "type": "object",
                        "properties": {
                                "firstName": {
                                        "type": "string",
                                        "description": "The person's first name."
                                },
                                "lastName": {
                                        "type": "string",
                                        "description": "The person's last name."
                                },
                                "age": {
                                        "description": "Age in years which must be equal to or greater than zero.",
                                        "type": "integer",
                                        "minimum": 0
                                }
                        }
                }
        }
        mock_json_load.return_value = example_schema

        # actual test
        schema_registry = SchemaRegistry.from_gcs_bucket("123", mock_storage_client)
        schema = schema_registry.get_schema(id="cool-person")
        self.assertIsInstance(schema, Schema)
        self.assertEqual("cool-person", schema.id)
        self.assertEqual("1.0.0", schema.version)
        self.assertEqual("4fd527554f00f7742d2f03330569dcb9d2dd2979", schema.hash)
