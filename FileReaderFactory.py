from JsonDataParser import JsonDataParser
from XmlDataParser import XmlDataParser


class FileReaderFactory:
    @staticmethod
    def get_reader(file_type):
        if file_type == "JSON":
            return JsonDataParser()
        elif file_type == "XML":
            return XmlDataParser()
        elif file_type == "HIVE_XML":
            return JsonDataParser()
        elif file_type == "CSV":
            return JsonDataParser()
        return None
