"""Module with custom exceptions to be used in the filecluster."""


class DateStringNoneError(Exception):
    """Exception for a case when the date string is none."""

    def __init__(self):
        self.message = "date_string is None"


class MissingDfClusterColumnError(Exception):
    """Exception for the case when there is a missing cluster column."""

    def __init__(self, column_name):
        self.message = f"Column {column_name} is missing in data frame."
