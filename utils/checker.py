
class healthTests():

    def __init__(self,
                 df=None,
                 *args,
                 **kwargs):

        self.df = df

    def count_columns(self, expected_col_count=None, *args, **kwargs):
        df_cols = self.df.columns
        if expected_col_count and len(df_cols) != len(df_cols):
            raise Exception("The number of columns in the loaded dataframe is not equal to given expected count")
        else:
            print(f"Given Data Frame has {len(df_cols)} columns")
            return len(df_cols)

    def count_records(self, *args, **kwargs):
        return self.df.count()

    def check_completeness(self):
        """
        (Total Non-Empty Records across all
        Columns)*100/(No. of Columns * No. of
        Records)
        :return:
        """
        pass

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
        pass

    def check_health_score(self):
        """
        Displays Average of Completeness &
        Uniqueness
        :return:
        """
        pass

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
        pass

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
