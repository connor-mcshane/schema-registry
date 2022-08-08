from __future__ import annotations

from typing import Dict, List
import json
from google.cloud import storage
import tempfile
import jsonschema as jsonschema
import avro_validator.schema as avro_validator
import hashlib

from jsonschema import ValidationError


class SchemaValidationError(Exception):
    pass


def get_SHA1_hash(data) -> str:
    """

    :param data:
    :return:
    """
    if isinstance(data, dict):
        return hashlib.sha1(json.dumps(data).encode()).hexdigest()
    if isinstance(data, str):
        return hashlib.sha1(data.encode()).hexdigest()


def get_schema_meta_fields_dict(version, id, hash, data_type) -> Dict:
    return {
            "id": id,
            "version": version,
            "hash": hash,
            "data_type": data_type
    }


class Schema:
    def __init__(
            self,
            id,
            version,
            schema,
            hash,
            data_type
    ):
        self.version = version
        self.id = id
        self.schema = schema
        self.hash = hash
        self.data_type = data_type

        if get_SHA1_hash(self.schema) != hash:
            raise ValueError(f"""hash for the schema does not match hash defined
            hash_calculated: {get_SHA1_hash(self.schema)}
            hash_defined_on_schema: {hash}
            """)

    def validate(self, data_dict: Dict):
        """
        :param data_dict:
        :return:
        """
        try:
            if self.data_type == "JSON":
                jsonschema.validate(instance=data_dict, schema=self.schema)
            elif self.data_type == "AVRO":
                schema_str = json.dumps(self.schema)
                avro_validator.Schema(schema_str).parse().validate(data_dict)
            else:
                print(f"{self.data_type} not supported data type")
        except (ValueError, ValidationError) as exception:
            raise SchemaValidationError(exception)

    @classmethod
    def from_file(cls, filepath):
        """

        :param filepath:
        :return:
        """
        schema_dict = json.load(open(filepath))
        return Schema(**schema_dict)


class SchemaRegistry:
    def __init__(self, schemas: Dict[str, Schema]):
        self.schemas = schemas

    def get_schema(self, id: str) -> Schema | None:
        """
        :param id:
        :return:
        """
        return self.schemas.get(id, None)

    @classmethod
    def from_gcs_bucket(cls, bucket_id: str, storage_client: storage.Client = None
                        ) -> SchemaRegistry:
        """
        Download all files from storage bucket and load into dictionary with filename as key and json as value
        expect schemas in the form
        {
          "id": "cool-object"
          "version": "1.0.0",
          "hash": "cf60cd2d12b30664050eeed09f642062d7efcf7b",
          "data_type": "JSON"
          "schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Generated schema for Root",
            "type": "object",
            "properties": { ....
        """
        if storage_client is None:
            storage_client = storage.Client()

        schemas = {}
        bucket = storage_client.get_bucket(bucket_id)
        blobs: List[storage.blob] = bucket.list_blobs()

        # write files to temp directory
        temp_dir = tempfile.mkdtemp()

        for blob in blobs:
            filename: str = str(blob.name).lower()
            filepath = temp_dir + "/" + filename
            blob.download_to_filename(filepath)
            schema = Schema.from_file(filepath)
            schemas[schema.id] = schema

        return SchemaRegistry(schemas)
