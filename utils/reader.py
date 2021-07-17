class dataReader:

    def __init__(self):
        pass

    def read_csv(self, spark, file_path, header=False, delimiter=",", schema=None, *args, **kwargs):
        if schema:
            df = spark.read.load(file_path,
                                 format='CSV',
                                 header=header,
                                 delimiter=delimiter,
                                 schema=schema)
        else:
            df = spark.read.load(file_path,
                                 format='CSV',
                                 header=header,
                                 delimiter=delimiter,
                                 inferSchema=True)
        return df

    def read_json(self, spark, file_path, header=False, multiline=False, *args, **kwargs):
        df = spark.read.load(file_path,
                             format='JSON',
                             header=header,
                             multiline=multiline)
        return df

    def read(self, spark_session, file_path, format="CSV", header=False, delimiter=",",
             multiline=False, schema=None, *args, **kwargs):
        """

        :param spark_session: spark session to use
        :param file_path: input file_path
        :param format: file format either CSV or JSON
        :param header: True or False default is False
        :param delimiter: delimiter for CSV files, default is ","
        :param multiline: True or False, used for JSON files
        :param schema: schema to read the data
        :param args:
        :param kwargs:
        :return: dataframe
        """
        if format == "CSV":
            df = self.read_csv(spark_session, file_path, header=header, delimiter=delimiter, schema=schema)

        elif format == "JSON":
            df = self.read_json(spark_session, file_path, header=header, multiline=multiline, schema=schema)

        else:
            raise Exception("the given file format is not supported by the module")

        return df