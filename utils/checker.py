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
        self.null_columns = None
        self.non_emptyrecordcount_exceptnullcols = None
        self.duplicate_counts = None
        self.unique_counts = None
        self.numeric_columns = None
        self.string_columns = None

    def count_columns(self, expected_col_count=None, *args, **kwargs):
        self.df_cols = self.df.columns
        if expected_col_count and len(self.df_cols) != len(self.df_cols):
            raise Exception("The number of columns in the loaded dataframe is not equal to given expected count")
        else:
            print(f"Given Data Frame has {len(self.df_cols)} columns")

        dt = self.df.dtypes
        self.numeric_columns = [d[0] for d in dt if d[1] in ('longint', 'int', 'double', 'float')]
        self.string_columns = [d[0] for d in dt if d[1] in ('string')]
        print(dt)
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

    def check_nullcolumns(self):
        self.null_columns = []
        for col in self.df_cols:
            is_notnulldf = self.df.filter(f"{col} is not null")
            if is_notnulldf.count() == 0:
                self.null_columns.append(col)

    def get_nonemptyrecords_exceptnullcols(self):
        if self.null_columns is None:
            self.check_nullcolumns()
        if len(self.null_columns) == 0:
            self.non_emptyrecordcount_exceptnullcols = self.record_count
        else:
            self.non_emptyrecordcount_exceptnullcols = self.df.select(
                [c for c in self.df_cols if c not in self.null_columns]).count()

    def check_completeness_except_null_columns(self):
        """
        (Total Non-Empty Records for Columns
        Excluding Fully Empty Columns)*100/(No.
        of Columns * No. of Records)
        :return:
        """
        if not self.non_emptyrecordcount_exceptnullcols:
            self.get_nonemptyrecords_exceptnullcols()

        self.completeness_exceptnullcols = (int(self.non_emptyrecordcount_exceptnullcols) /
                                            (len(self.df_cols) * int(self.record_count))) * 100

        return self.completeness_exceptnullcols

    def check_uniqueness(self):
        """
        No. or Unique Rows excluding Primary Keys
        :return:
        """
        if not self.unique_recordscount:
            self.get_uniqueRecordsCount()
        self.uniqueness = (int(self.unique_recordscount)
                           / (len(self.df_cols) * int(self.record_count))) * 100
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

        empty_counts = {}
        for col in self.df_cols:
            empty_counts[col] = self.df.filter(f"{col} is null").count()
        return empty_counts

    def count_duplicates(self):
        """
        Count of Duplicate Values for the Column
        :return:
        """
        self.duplicate_counts = {}
        for col in self.df_cols:
            self.duplicate_counts[col] = self.record_count - self.df.select(col).distinct().count()

        return self.duplicate_counts

    def count_uniques(self):
        """
        Count of Distinct Data Values for the Column
        :return:
        """
        if not self.duplicate_counts:
            self.count_duplicates()
        self.unique_counts = {}
        for key in self.duplicate_counts:
            self.unique_counts[key] = self.duplicate_counts.get(key)
        return self.unique_counts

    def check_min_max_values(self):
        """
        Provides Minimum & Maximum Values of a specific Column
        :return:
        """
        min_max_values = {}
        for col in self.numeric_columns:
            min_max_values[col+"_min"] = self.df.select(col).min()
            min_max_values[col + "_max"] = self.df.select(col).max()

        return min_max_values

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
        avg_values = {}
        for col in self.numeric_columns:
            avg_values[col] = self.df.select(col).avg()

        return avg_values

    def check_median(self):
        """
        Provides Median Calculation only for
        Numeric Column
        :return:
        """
        median_values = {}
        for col in self.numeric_columns:
            median_values[col] = self.df.agg(f.expr(f"percentile({col}, 0.5)")).first()[0]

        return median_values

    def check_variance(self):
        """
        Provides Variance Calculation only for
        Numeric Column
        :return:
        """
        variance_values = {}
        for col in self.numeric_columns:
            variance_values[col] = self.df.agg({f"{col}": "variance"}).first()[0]

        return variance_values
