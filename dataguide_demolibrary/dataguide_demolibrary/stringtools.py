"""String concatenation helpers."""


def concat(*args: str, sep: str = "") -> str:
    """Concatenate strings without a separator.

    >>> concat("hello", "world")
    'helloworld'
    """
    return sep.join(args)


def concat_sep(*args: str, sep: str = "_") -> str:
    """Concatenate strings with a separator (default: underscore).

    >>> concat_sep("hello", "world")
    'hello_world'
    >>> concat_sep("a", "b", "c", sep="-")
    'a-b-c'
    """
    return sep.join(args)