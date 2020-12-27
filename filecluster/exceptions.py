"""Module with custom exceptions to be used in the filecluster."""


class DateStringNoneException(Exception):
    def __init__(self):
        self.message = "date_string is None"


class MissingDfClusterColumn(Exception):
    def __init__(self, column_name):
        self.message = f"Column {column_name} is missing in data frame."
