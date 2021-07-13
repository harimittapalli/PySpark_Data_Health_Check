from pyspark.sql import SparkSession
from utils.reader import dataReader
from utils.checker import healthTests


class DataHealthChecker():
    def __init__(self,
                 input_file,
                 file_format="CSV",
                 header=False,
                 multiline=False,
                 app_name=None,
                 *args, **kwargs
                 ):

        self.app_name = app_name or "Data Health Checker"
        self.input_file = input_file
        self.file_format = file_format or "CSV"
        self.header = header
        self.multiline = multiline

    def create_session(self):
        self.spark_session = SparkSession.builder.\
            master('local[*]').\
            appName(self.app_name).\
            getOrCreate()
        return self.spark_session

    def close_session(self):
        self.spark_session.stop()

    # def apply_checks(self):
    #     health_checker = healthTests(self.df)
    #     checks = [
    #         health_checker.
    #     ]
    def process(self):
        self.create_session()
        reader = dataReader()
        self.df = reader.read(self.spark_session, self.input_file,
                                   format=self.file_format, header=self.header,
                                   multiline=self.multiline)

        health_checker = healthTests(self.df)
        columns_count = health_checker.count_columns()
        print(columns_count)

        self.close_session()

if __name__ == "__main__":
    health_checker = DataHealthChecker(input_file="data/VisitorLogsData.csv", file_format="CSV")

    health_checker.process()
