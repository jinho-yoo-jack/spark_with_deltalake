import json
from collections import namedtuple


class SchemaManager:
    def __init__(self, schemaJSON: dict):
        self.__dict__.update(schemaJSON)

    @classmethod
    def jsonFile2SchemaDto(cls, schemaJSONDict):
        return json.loads(json.dumps(schemaJSONDict), object_hook=SchemaManager)


if __name__ == '__main__':

    with open('../schema/service_db_info.json', 'r') as file:
        schema_file = json.load(file)
        sf = json.dumps(schema_file)

    print(schema_file)
    print(type(schema_file))
    print(sf)
    print(type(sf))
    obj = namedtuple("ObjectName", schema_file.keys())(*schema_file.values())
    print(type(obj))
    print(obj)


    class SchemaDto:
        # constructor
        def __init__(self, dict1):
            self.__dict__.update(dict1)


    def jsonFile2SchemaDto(schemaJSONDict):
        return json.loads(json.dumps(schemaJSONDict), object_hook=SchemaDto)


    schema_dto = jsonFile2SchemaDto(schema_file)
    print(schema_dto.user)

    sm = SchemaManager.jsonFile2SchemaDto(schema_file)
