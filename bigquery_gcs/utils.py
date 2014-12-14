from itertools import islice


def split_every(n, iterable):
    """Split an iterator every X items

    Parameters
    ----------
    n: int
        Number of items before split

    iterable: list
        Iterable that you use for spliting

    Examples
    --------
    list(split_every(5, range(9))) will produce [[0, 1, 2, 3, 4], [5, 6, 7, 8]]

    References
    ----------
    http://stackoverflow.com/a/1915307/1446284
    """
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))
