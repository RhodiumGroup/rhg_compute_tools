import collections.abc
import dis
import functools
import inspect
import itertools
import json
import os
import types

import numpy as np
import toolz


def expand(func):
    """
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

    """

    @functools.wraps(func)
    def inner(ak, *args, **kwargs):
        return func(*ak[0], *args, **ak[1], **kwargs)

    return inner


def collapse(*args, **kwargs):
    """
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
    """
    return (args, kwargs)


def collapse_product(*args, **kwargs):
    """

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
    """
    num_args = len(args)
    kwarg_keys = list(kwargs.keys())
    kwarg_vals = [kwargs[k] for k in kwarg_keys]

    format_iterations = lambda x: (
        tuple(x[:num_args]),
        dict(zip(kwarg_keys, x[num_args:])),
    )

    return map(format_iterations, itertools.product(*args, *kwarg_vals))


class NumpyEncoder(json.JSONEncoder):
    """
    Helper class for json.dumps to coerce numpy objects to native python
    """

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


def checkpoint(
    jobs,
    futures,
    job_name,
    log_dir=".",
    extra_pending=None,
    extra_errors=None,
    extra_others=None,
):
    """
    checkpoint and save a job state to disk
    """

    err_msg = "lengths do not match: jobs [{}] != futures [{}]".format(
        len(jobs), len(futures)
    )

    assert len(jobs) == len(futures), err_msg

    if extra_pending is None:
        extra_pending = []

    if extra_errors is None:
        extra_errors = []

    if extra_others is None:
        extra_others = {}

    pending_jobs = [
        jobs[i] for i, f in enumerate(futures) if f.status == "pending"
    ] + extra_pending

    errored_jobs = [
        jobs[i] for i, f in enumerate(futures) if f.status == "error"
    ] + extra_errors

    other_jobs = {}

    for i, f in enumerate(futures):
        if f.status in ["pending", "error", "finished"]:
            continue

        if f.status not in other_jobs:
            other_jobs[f.status] = []

        other_jobs[f.status].append(jobs[i])

    for k, v in extra_others.items():
        if k not in other_jobs:
            other_jobs[k] = []

        other_jobs[k].append(v)

    with open(os.path.join(log_dir, "{}.pending".format(job_name)), "w+") as f:
        f.write(json.dumps(pending_jobs, cls=NumpyEncoder))

    with open(os.path.join(log_dir, "{}.err".format(job_name)), "w+") as f:
        f.write(json.dumps(errored_jobs, cls=NumpyEncoder))

    with open(os.path.join(log_dir, "{}.other".format(job_name)), "w+") as f:
        f.write(json.dumps(other_jobs, cls=NumpyEncoder))


def recover(job_name, log_dir="."):
    """
    recover pending, errored, other jobs from a checkpoint
    """

    with open(os.path.join(log_dir, "{}.pending".format(job_name)), "r") as f:
        content = f.read()
        if len(content) == 0:
            pending = {}
        else:
            pending = json.loads(content)

    with open(os.path.join(log_dir, "{}.err".format(job_name)), "r") as f:
        content = f.read()
        if len(content) == 0:
            errored = {}
        else:
            errored = json.loads(content)

    with open(os.path.join(log_dir, "{}.other".format(job_name)), "r") as f:
        content = f.read()
        if len(content) == 0:
            other = {}
        else:
            other = json.loads(content)

    return pending, errored, other


class html(object):
    def __init__(self, body):
        self.body = body

    def _repr_html_(self):
        return self.body


_default_allowed_types = (
    types.FunctionType,
    types.ModuleType,
    (type if not hasattr(types, "ClassType") else types.ClassType),
    types.MethodType,
    types.BuiltinMethodType,
    types.BuiltinFunctionType,
)


@toolz.functoolz.curry
def block_globals(obj, allowed_types=None, include_defaults=True, whitelist=None):
    """
    Decorator to prevent globals and undefined closures in functions and classes

    Parameters
    ----------
    func : function
        Function to decorate. All globals not matching one of the allowed
        types will raise an AssertionError
    allowed_types : type or tuple of types, optional
        Types which are allowed as globals. By default, functions and
        modules are allowed. The full set of allowed types is drawn from
        the ``types`` module, and includes :py:class:`~types.FunctionType`,
        :py:class:`~types.ModuleType`, :py:class:`~types.MethodType`,
        :py:class:`~types.ClassType`,
        :py:class:`~types.BuiltinMethodType`, and
        :py:class:`~types.BuiltinFunctionType`.
    include_defaults : bool, optional
        If allowed_types is provided, setting ``include_defaults`` to True will
        append the default list of functions, modules, and methods to the
        user-passed list of allowed types. Default is True, in which case
        only the user-passed elements will be allowed. Setting to False will
        allow only the types passed in ``allowed_types``.
    whitelist : list of str, optional
        Optional list of variable names to whitelist. If a list is provided,
        global variables will be compared to elements of this list based on
        their string names. Default (None) is no whitelist.

    Examples
    --------

    Wrap a function to block globals:

    .. code-block:: python

        >>> my_data = 10

        >>> @block_globals
        ... def add_5(data):
        ...     ''' can you spot the global? '''
        ...     a_number = 5
        ...     result = a_number + my_data
        ...     return result  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        TypeError: Illegal <class 'int'> global found in add_5: my_data

    Wrapping a class will prevent globals from being used in all methods:

    .. code-block:: python

        >>> @block_globals
        ... class MyClass:
        ...
        ...     @staticmethod
        ...     def add_5(data):
        ...         ''' can you spot the global? '''
        ...         a_number = 5
        ...         result = a_number + my_data
        ...         return result  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        TypeError: Illegal <class 'int'> global found in add_5: my_data

    By default, functions and modules are allowed in the list of globals. You
    can modify this list with the ``allowed_types`` argument:

    .. code-block:: python

        >>> result_formatter = 'my number is {}'
        >>> @block_globals(allowed_types=str)
        ... def add_5(data):
        ...     ''' only allowed globals here! '''
        ...     a_number = 5
        ...     result = a_number + data
        ...     return result_formatter.format(result)
        ...
        >>> add_5(3)
        'my number is 8'

    block_globals will also catch undefined references:

    .. code-block:: python

        >>> @block_globals
        ... def get_mean(df):
        ...     return da.mean()  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        TypeError: Undefined global in get_mean: da
    """

    if allowed_types is None:
        allowed_types = _default_allowed_types

    if (allowed_types is not None) and include_defaults:
        if not isinstance(allowed_types, collections.abc.Sequence):
            allowed_types = [allowed_types]

        allowed_types = tuple(list(allowed_types) + list(_default_allowed_types))

    if whitelist is None:
        whitelist = []

    if isinstance(obj, type):
        for attr in obj.__dict__:
            if callable(getattr(obj, attr)):
                setattr(obj, attr, block_globals(getattr(obj, attr)))
        return obj

    closurevars = inspect.getclosurevars(obj)
    for instr in dis.get_instructions(obj):
        if instr.opname == "LOAD_GLOBAL":
            if instr.argval in closurevars.builtins:
                continue
            elif (instr.argval in closurevars.globals) or (
                instr.argval in closurevars.nonlocals
            ):
                if instr.argval in whitelist:
                    continue
                if instr.argval in closurevars.globals:
                    g = closurevars.globals[instr.argval]
                else:
                    g = closurevars.nonlocals[instr.argval]
                if not isinstance(g, allowed_types):
                    raise TypeError(
                        "Illegal {} global found in {}: {}".format(
                            type(g),
                            obj.__name__,
                            instr.argval,
                        )
                    )
            else:
                raise TypeError(
                    "Undefined global in {}: {}".format(
                        obj.__name__,
                        instr.argval,
                    )
                )

    @functools.wraps(obj)
    def inner(*args, **kwargs):
        return obj(*args, **kwargs)

    return inner
