import functools

def query_to_df(func):
    """Decorated function desigend to wrap function returning a query string 
    to get resutls as Pandas DataFrame
    """
    @functools.wraps(func)
    def wrapper_decorator(db_h, *args, **kwargs):
        query = func(db_h, *args, **kwargs)
        if kwargs.get('print'):
            print(query)
        return db_h.get_df(query)
    return wrapper_decorator

def query_to_value(func):
    """Decorated function desigend to wrap function returning a query string 
    to get results a value
    """
    @functools.wraps(func)
    def wrapper_decorator(db_h, *args, **kwargs):
        query = func(db_h, *args, **kwargs)
        results = db_h.get_value(query)
        print(query)
        return results
    return wrapper_decorator

def query_to_list(func):
    """Decorated function desigend to wrap function returning a query string 
    to get results a list
    """
    @functools.wraps(func)
    def wrapper_decorator(db_h, *args, **kwargs):
        query = func(db_h, *args, **kwargs)
        results = db_h.get_list(query)
        print(query)
        return results
    return wrapper_decorator