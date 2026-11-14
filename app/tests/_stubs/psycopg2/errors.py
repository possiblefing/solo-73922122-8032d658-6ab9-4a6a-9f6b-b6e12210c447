"""psycopg2.errors 异常桩。"""


class InvalidTextRepresentation(Exception):
    pass


class UndefinedObject(Exception):
    pass


class DuplicateObject(Exception):
    pass
