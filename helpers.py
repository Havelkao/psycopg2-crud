import psycopg2
from functools import wraps


# https://stackoverflow.com/questions/6307761
def for_all_methods(decorator):
    def decorate(cls):
        for attr in cls.__dict__:  # there's propably a better way to do this
            if callable(getattr(cls, attr)):
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls

    return decorate


def handle_db_errors(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)

        except (Exception, psycopg2.Error) as error:
            self.conn.rollback()
            raise error

    return wrapper
