import json

from source_data.source_info import SourceInfo


class SchemaFactory:
    @classmethod
    def createBy(cls, schemaJSONDict):
        return json.loads(json.dumps(schemaJSONDict), object_hook=SourceInfo)


if __name__ == '__main__':
    with open('schema/payment.json', 'r') as file:
        schema_file = json.load(file)
    sm = SchemaFactory.createBy(schema_file)
    print(sm)
