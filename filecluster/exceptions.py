"""Module with custom exceptions to be used in the filecluster."""


class DateStringNoneException(Exception):
    """Exception for case when date string is none."""

    def __init__(self):
        self.message = "date_string is None"


class MissingDfClusterColumn(Exception):
    """Exception for the case when there is missing cluster column."""

    def __init__(self, column_name):
        self.message = f"Column {column_name} is missing in data frame."
