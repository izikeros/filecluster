"""Module with custom exceptions to be used in the filecluster."""


class DateStringNoneError(Exception):
    """Exception for a case when the date string is none."""

    def __init__(self):
        self.message = "date_string is None"


class MissingDfClusterColumnError(Exception):
    """Exception for the case when there is a missing cluster column."""

    def __init__(self, column_name):
        self.message = f"Column {column_name} is missing in data frame."


class OverlappingPathsError(ValueError):
    """Two directories that must stay separate are the same or nested.

    Reconciling a source that overlaps a library would have every file match
    itself, so a move would empty the library into the duplicates folder.
    """

    def __init__(
        self,
        first_label: str,
        first: object,
        second_label: str,
        second: object,
    ):
        self.message = (
            f"{first_label} ({first}) and {second_label} ({second}) overlap. "
            "They must be separate directories."
        )
        super().__init__(self.message)
