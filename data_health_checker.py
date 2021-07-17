"""
TODO1: Adding plotting or reporting to this module
TODO2: enable logging
TODO3: Do the optimization
TODO4: Add more checks
TODO5: Adding documentation
"""

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
        records_count = health_checker.count_records()
        non_emptyrecords_count = health_checker.get_nonEmptyRecordsCount()
        completeness = health_checker.check_completeness()
        uniqueness = health_checker.check_uniqueness()
        variances = health_checker.check_variance()
        medians = health_checker.check_median()

        print(f"columns: {columns_count}\n"
              f"records: {records_count}\n"
              f"not empty records: {non_emptyrecords_count}\n"
              f"completeness: {completeness}\n"
              f"uniqueness: {uniqueness}\n"
              f"medians: {medians}\n"
              f"variances: {variances}"

              )

        self.close_session()


if __name__ == "__main__":
    health_checker = DataHealthChecker(input_file="data/sample_input.csv", file_format="CSV", header=True)

    health_checker.process()
