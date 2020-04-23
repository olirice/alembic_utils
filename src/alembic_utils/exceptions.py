class AlembicUtilsException(Exception):
    """Base exception for AlembicUtils package"""


class DuplicateRegistration(AlembicUtilsException):
    """An entity was registered multiple times"""


class SQLParseFailure(AlembicUtilsException):
    """An entity could not be parsed"""
