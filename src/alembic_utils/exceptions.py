class AlembicUtilsException(Exception):
    """Base exception for AlembicUtils package"""


class SQLParseFailure(AlembicUtilsException):
    """An entity could not be parsed"""


class FailedToGenerateComparable(AlembicUtilsException):
    """Failed to generate a comparable entity"""


class UnreachableException(AlembicUtilsException):
    """An exception no one should ever see"""


class BadInputException(AlembicUtilsException):
    """Invalid user input"""
