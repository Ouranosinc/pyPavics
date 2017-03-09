"""
==============
Synthetic data
==============

 * :func:`mpairing` - Multidimensional pairing function

We have data in the shape (N1, N2, ..., Nn).
A specific value is given by (i1, i2, ..., in).

We want a method by which to fill the data array with predictable values
in multiple steps (slices).

"""

import numpy as np


def mpairing(var_shape, process_shape=None, offsets_dims=None, offsets=None,
             dtype=np.int64):
    """Multidimensional pairing function

    Parameters
    ----------
    var_shape : tuple of int
    process_shape : tuple of int
        must have the same number of dimensions as var_shape, this is used
        to process only a subset of the total data.
    offsets_dims : list of int
        the dimensions index to offset.
    offsets : list of int
        must be the same length as offsets_dims.
    dtype : numpy.dtype

    Notes
    -----
    This pairing function provides unique and easily predictable values that
    are increasing along all axes in multiple dimensions based on the ndarray
    indices. The value for indice (i1,i2,i3) is given by:
    i3 + i2*10**(len(str(N3))) + i1*10**(len(str(N3))+len(str(N2)))
    In words, starting from the last indice, we add its value plus a padding
    of a power of 10 that is large enough to contain all previous values.

    This can overflow if the sum(len(str(Nn))) > 18 with the default int64
    data type (that would be a very very large array though...).

    Examples
    --------
    >>> mpairing((2,3,4))
    array([[[  0,   1,   2,   3],
        [ 10,  11,  12,  13],
        [ 20,  21,  22,  23]],

       [[100, 101, 102, 103],
        [110, 111, 112, 113],
        [120, 121, 122, 123]]])

    >>> mpairing((2,3,4),(2,3,2),offsets_dims=[2],offsets=[2])
    array([[[  2,   3],
        [ 12,  13],
        [ 22,  23]],

       [[102, 103],
        [112, 113],
        [122, 123]]])

    """

    if offsets_dims is None:
        offsets_dims = []
    if process_shape is None:
        process_shape = var_shape
    fill_data = np.zeros(process_shape, dtype=dtype)
    dim = -1
    multiplier = 1
    while dim >= -len(var_shape):
        if len(var_shape)+dim in offsets_dims:
            j = offsets_dims.index(len(var_shape) + dim)
            elements = np.arange(offsets[j], offsets[j] + process_shape[dim])
        else:
            elements = np.arange(0, process_shape[dim])
        tile_shape = list(process_shape)
        if dim != -1:
            tile_shape[dim] = process_shape[-1]
        tile_shape[-1] = 1
        tiled = np.tile(elements, (tile_shape))
        if dim != -1:
            add = np.swapaxes(tiled, len(var_shape) + dim,
                              len(var_shape) - 1)
        else:
            add = tiled
        fill_data += add*multiplier
        multiplier *= 10**len(str(var_shape[dim]))
        dim -= 1
    return fill_data
