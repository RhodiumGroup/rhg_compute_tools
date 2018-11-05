import functools
import itertools
import json
import numpy as np
import os

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

        >>> @expand
        ... def my_func(a, b, exp=1):
        ...     return (a * b)**exp
        ...

        >>> my_func(((2, 3), {}))
        6

        >>> my_func(((2, 3, 2), {}))
        36

        >>> my_func((tuple([]), {'b': 4, 'exp': 2, 'a': 1}))
        16

    This function can be used in combination with the ``collapse`` helper
    function, which allows more natural parameter calls

    .. code-block:: python

        >>> my_func(collapse(2, 3, exp=2))
        36

    These can then be paired to enable many parameterized function calls:

    .. code-block:: python

        >>> func_calls = [collapse(a, a+1, exp=a) for a in range(5)]

        >>> list(map(my_func, func_calls))
        [1, 2, 36, 1728, 160000]

    '''

    @functools.wraps(func)
    def inner(ak, *args, **kwargs):
        return func(*ak[0], *args, **ak[1], **kwargs)
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

        >>> @expand
        ... def my_func(a, b, exp=1):
        ...     return (a * b)**exp
        ...

        >>> product_args = list(collapse_product(
        ...     [0, 1, 2],
        ...     [0.5, 2],
        ...     exp=[0, 1]))

        >>> product_args  # doctest: +NORMALIZE_WHITESPACE
        [((0, 0.5), {'exp': 0}),
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

        >>> list(map(my_func, product_args))
        [1.0, 0.0, 1, 0, 1.0, 0.5, 1, 2, 1.0, 1.0, 1, 4]
    '''
    num_args = len(args)
    kwarg_keys = list(kwargs.keys())
    kwarg_vals = [kwargs[k] for k in kwarg_keys]

    format_iterations = lambda x: (
        tuple(x[:num_args]),
        dict(zip(kwarg_keys, x[num_args:])))

    return map(format_iterations, itertools.product(*args, *kwarg_vals))


class NumpyEncoder(json.JSONEncoder):
    '''
    Helper class for json.dumps to coerce numpy objects to native python
    '''
    
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.int64):
            return int(obj)
        elif isinstance(obj, np.float64):
            return float(obj)
        elif isinstance(obj, np.int32):
            return int(obj)
        elif isinstance(obj, np.float32):
            return float(obj)
        return json.JSONEncoder.default(self, obj)


def checkpoint(jobs, futures, job_name, log_dir='.', extra_pending=None, extra_errors=None, extra_others=None):
    '''
    checkpoint and save a job state to disk
    '''
    
    err_msg = (
        "lengths do not match: jobs [{}] != futures [{}]"
        .format(len(jobs), len(futures)))

    assert len(jobs) == len(futures), err_msg
    
    if extra_pending is None:
        extra_pending = []
    
    if extra_errors is None:
        extra_errors = []
        
    if extra_others is None:
        extra_others = {}

    pending_jobs = (
        [jobs[i] for i, f in enumerate(futures) if f.status == 'pending']
        + extra_pending)

    errored_jobs = (
        [jobs[i] for i, f in enumerate(futures) if f.status == 'error']
        + extra_errors)
    
    other_jobs = {}
    
    for i, f in enumerate(futures):
        if f.status in ['pending', 'error', 'finished']:
            continue

        if f.status not in other_jobs:
            other_jobs[f.status] = []
        
        other_jobs[f.status].append(jobs[i])
        
    for k, v in extra_others.items():
        if k not in other_jobs:
            other_jobs[k] = []

        other_jobs[k].append(v)

    with open(os.path.join(log_dir, '{}.pending'.format(job_name)), 'w+') as f:
        f.write(json.dumps(pending_jobs, cls=NumpyEncoder))

    with open(os.path.join(log_dir, '{}.err'.format(job_name)), 'w+') as f:
        f.write(json.dumps(errored_jobs, cls=NumpyEncoder))

    with open(os.path.join(log_dir, '{}.other'.format(job_name)), 'w+') as f:
        f.write(json.dumps(other_jobs, cls=NumpyEncoder))


def recover(job_name, log_dir='.'):
    '''
    recover pending, errored, other jobs from a checkpoint
    '''

    with open(os.path.join(log_dir, '{}.pending'.format(job_name)), 'r') as f:
        content = f.read()
        if len(content) == 0:
            pending = {}
        else:
            pending = json.loads(content)

    with open(os.path.join(log_dir, '{}.err'.format(job_name)), 'r') as f:
        content = f.read()
        if len(content) == 0:
            errored = {}
        else:
            errored = json.loads(content)

    with open(os.path.join(log_dir, '{}.other'.format(job_name)), 'r') as f:
        content = f.read()
        if len(content) == 0:
            other = {}
        else:
            other = json.loads(content)
        
    return pending, errored, other
