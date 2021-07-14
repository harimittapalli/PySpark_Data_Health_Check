import pyspark.sql.functions as f

class healthTests():

    def __init__(self,
                 df=None,
                 *args,
                 **kwargs):

        self.df = df
        self.emptyrecordcount = None
        self.non_emptyrecordcount = None
        self.record_count = None
        self.df_cols = None
        self.unique_recordscount = None
        self.completeness = None
        self.uniqueness = None

    def count_columns(self, expected_col_count=None, *args, **kwargs):
        self.df_cols = self.df.columns
        if expected_col_count and len(self.df_cols) != len(self.df_cols):
            raise Exception("The number of columns in the loaded dataframe is not equal to given expected count")
        else:
            print(f"Given Data Frame has {len(self.df_cols)} columns")
            return len(self.df_cols)

    def count_records(self, *args, **kwargs):
        self.record_count = self.df.count()
        return self.record_count

    def get_nonEmptyRecordsCount(self):
        build_query = ""
        for col in self.df.columns:
            build_query += col + " is null AND "
        build_query = build_query[:-4]
        self.emptyrecordcount = self.df.filter(build_query).count()
        self.non_emptyrecordcount = self.record_count - self.emptyrecordcount
        return self.non_emptyrecordcount

    def get_uniqueRecordsCount(self):
        self.unique_recordscount = self.df.distinct().count()

    def check_completeness(self):
        """
        (Total Non-Empty Records across all
        Columns)*100/(No. of Columns * No. of
        Records)
        :return:
        """
        # Assuming that Non-Empty Records means all the columns having the null values for the record
        if not self.non_emptyrecordcount:
            self.get_nonEmptyRecordsCount()
        if not self.record_count:
            self.count_records()
        self.completeness = (int(self.non_emptyrecordcount) / (len(self.df_cols) * int(self.record_count))) * 100
        return self.completeness

    def check_completeness_except_null_columns(self):
        """
        (Total Non-Empty Records for Columns
        Excluding Fully Empty Columns)*100/(No.
        of Columns * No. of Records)
        :return:
        """
        pass

    def check_uniqueness(self):
        """
        No. or Unique Rows excluding Primary Keys
        :return:
        """
        if not self.unique_recordscount:
            self.get_uniqueRecordsCount()
        self.uniqueness = (int(self.unique_recordscount) / len(self.df_cols) * int(self.record_count)) * 100
        return self.uniqueness

    def check_health_score(self):
        """
        Displays Average of Completeness &
        Uniqueness
        :return:
        """
        if not self.completeness:
            self.check_completeness()
        if not self.uniqueness:
            self.check_completeness()

        health_score = (self.completeness + self.uniqueness)/2

        return health_score

    def check_datatypes(self):
        """
        Generic Data Types as String, Int, Float,
        Double, Timestamp based on the Data
        :return:
        """
        pass

    def check_datafill_ratio(self):
        """
        (Total Non-Empty Rows for the
        Column)*100/(Total No. of Rows in the
        Table)
        :return:
        """
        return (self.non_emptyrecordcount * 100)/self.record_count

    def check_empty_count(self):
        """
        Includes Non-Empty Values count for a Column &
        Includes Empty Values count after trimming
        data along with Values as “NULL” & “null”
        for a Column
        :return:
        """
        pass

    def count_duplicates(self):
        """
        Count of Duplicate Values for the Column
        :return:
        """
        pass

    def count_uniques(self):
        """
        Count of Distinct Data Values for the Column
        :return:
        """
        pass

    def check_min_max_values(self):
        """
        Provides Minimum & Maximum Values of a specific Column
        :return:
        """
        pass

    def check_min_max_lengths(self):
        """
        Provides Minimum & Maximum Length of a specific Column
        :return:
        """
        pass

    def check_avg(self):
        """
        Provides Average Calculation only for
        Numeric Column
        :return:
        """
        pass

    def check_median(self):
        """
        Provides Median Calculation only for
        Numeric Column
        :return:
        """
        pass

    def check_variance(self):
        """
        Provides Variance Calculation only for
        Numeric Column
        :return:
        """
        pass
