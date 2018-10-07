import functools, itertools

def expand(func):
    '''
    Decorator to expand an (args, kwargs) tuple in function calls
    
    Intended for use with the :py:func:`collapse` function
    
    Parameters
    ----------
    func : function
        Function to have arguments expanded. Func can have any
        number of positional and keyword arguments.
    
    Returns
    -------
    wrapped : function
        Wrapped version of ``func`` which accepts a single 
        ``(args, kwargs)`` tuple.
    
    Examples
    --------
    
    .. code-block:: python

        In [1]: @expand
           ...: def my_func(a, b, exp=1):
           ...:     return (a * b)**exp
           ...:

        In [2]: my_func((2, 3))
        6
        
        In [3]: my_func((2, 3, 2))
        36
        
        In [4]: my_func(tuple([]), {'b': 4, 'c': 2, 'a': 1})
        16
        
    This function can be used in combination with the ``collapse`` helper function,
    which allows more natural parameter calls

    .. code-block:: python
        
        In [5]: my_func(collapse(2, 3, exp=2))
        36

    These can then be paired to enable many parameterized function calls:

    .. code-block:: python

        In [6]: func_calls = [collapse(a, a+1, exp=a) for a in range(5)]

        In [7]: map(my_func, func_calls)
        [1, 2, 36, 1728, 160000]

    '''

    @functools.wraps(func)
    def inner(ak):
        return func(*ak[0], **ak[1])
    return inner


def collapse(*args, **kwargs):
    '''
    Collapse positional and keyword arguments into an (args, kwargs) tuple
    
    Intended for use with the :py:func:`expand` decorator
    
    Parameters
    ----------
    *args
        Variable length argument list.
    **kwargs
        Arbitrary keyword arguments.
        
    Returns
    -------
    args : tuple
        Positional arguments tuple
    kwargs : dict
        Keyword argument dictionary
    '''
    return (args, kwargs)


def collapse_product(*args, **kwargs):
    '''
    
    Parameters
    ----------
    
    *args
        Variable length list of iterables
    **kwargs
        Keyword arguments, whose values must be iterables
        
    Returns
    -------
    iterator
        Generator with collapsed arguments
        
    See Also
    --------
    
    Function :py:func:`collapse`
    
    Examples
    --------
    
    .. code-block:: python

        In [1]: @expand
           ...: def my_func(a, b, exp=1):
           ...:     return (a * b)**exp
           ...:
    
        In [3]: product_args = collapse_product(
           ...:     [0, 1, 2],
           ...:     [0.5, 2],
           ...:     exp=[0, 1])
        
        In [4]: list(product_args)  # doctest: NORMALIZE_WHITESPACE
           [
               ((0, 0.5), {'exp': 0}),
               ((0, 0.5), {'exp': 1}),
               ((0, 2), {'exp': 0}),
               ((0, 2), {'exp': 1}),
               ((1, 0.5), {'exp': 0}),
               ((1, 0.5), {'exp': 1}),
               ((1, 2), {'exp': 0}),
               ((1, 2), {'exp': 1}),
               ((2, 0.5), {'exp': 0}),
               ((2, 0.5), {'exp': 1}),
               ((2, 2), {'exp': 0}),
               ((2, 2), {'exp': 1})]
               
        In [5]: list(map(my_func, product_args))
        [1.0, 0.0, 1, 0, 1.0, 0.5, 1, 2, 1.0, 1.0, 1, 4]
    '''
    num_args = len(args)
    kwarg_keys = list(kwargs.keys())
    kwarg_vals = [kwargs[k] for k in kwarg_keys]
    
    format_iterations = lambda x: (tuple(x[:num_args]), dict(zip(kwarg_keys, x[num_args:])))
    
    return map(format_iterations, itertools.product(*args, *kwarg_vals))
