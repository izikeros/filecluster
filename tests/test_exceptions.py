"""Tests for custom exception classes in the filecluster package.

Verifies that exceptions carry the correct messages and can be raised/caught
as expected by consuming code.
"""

import pytest

from filecluster.exceptions import DateStringNoneError, MissingDfClusterColumnError


class TestDateStringNoneError:
    """Tests for DateStringNoneError exception."""

    def test_raise_with_correct_message(self):
        """Verify the exception carries the expected error message."""
        with pytest.raises(DateStringNoneError) as exc_info:
            raise DateStringNoneError()
        assert exc_info.value.message == "date_string is None"

    def test_is_subclass_of_exception(self):
        """Verify the error integrates with standard exception handling."""
        err = DateStringNoneError()
        assert isinstance(err, Exception)


class TestMissingDfClusterColumnError:
    """Tests for MissingDfClusterColumnError exception."""

    def test_raise_with_column_name_in_message(self):
        """Verify the column name is embedded in the error message."""
        col = "start_date"
        with pytest.raises(MissingDfClusterColumnError) as exc_info:
            raise MissingDfClusterColumnError(col)
        assert col in exc_info.value.message

    def test_different_column_names_produce_different_messages(self):
        """Verify distinct columns yield unique messages."""
        err_a = MissingDfClusterColumnError("col_a")
        err_b = MissingDfClusterColumnError("col_b")
        assert "col_a" in err_a.message
        assert "col_b" in err_b.message
        assert err_a.message != err_b.message

    def test_is_subclass_of_exception(self):
        """Verify the error integrates with standard exception handling."""
        err = MissingDfClusterColumnError("x")
        assert isinstance(err, Exception)
