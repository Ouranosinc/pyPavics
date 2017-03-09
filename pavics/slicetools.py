"""
==========
SliceTools
==========

Classes:

 * SliceError - the exception raised on failure.

Functions:

 * :func:`_size_in_memory` - Size of an array in memory
 * :func:`_num_divisions_for_memory_fit` - Numbers of divisions to fit in mem.
 * :func:`divide_slice` - Divide slice
 * :func:`divide_slices` - Divide slices
 * :func:`divide_slices_for_memory_fit` - Divide slices to fit in memory

A multidimensional slice is a tuple of integer, 1d array (or any list of int?),
slice and or Ellipsis.

Slice synthax reminder: slice([start,] stop[, step])

"""

import copy
import numpy as np
import numpy.ma as ma


class SliceError(Exception):
    pass


def _size_in_memory(data_size, data_type):
    if data_type in [np.int8, np.uint8, np.bool8, np.dtype('S1')]:
        multiplier = 1
    elif data_type in [np.int16, np.uint16, np.float16]:
        multiplier = 2
    elif data_type in [np.int32, np.uint32, np.float32]:
        multiplier = 4
    elif data_dtype in [np.int64, np.uint64, np.float64, np.dtype('O')]:
        multiplier = 8
    elif data_type == np.float128:
        multiplier = 16
    return data_size * multiplier


def _num_divisions_for_memory_fit(data_size, data_type,
                                  memory_size=2000000000):
    nbytes = _size_in_memory(data_size, data_type)
    return int(np.ceil(nbytes/float(memory_size)))


def divide_slice(one_slice, divisions, length=None):
    """Divide slice.

    Parameters
    ----------
    one_slice - slice
    divisions - int
        number of slices that will be returned.
    length - int or None
        when one_slice.stop is None, the length must be provided.

    Returns
    -------
    out - list of slices

    Examples
    --------
    >>> divide_slice(slice(2,8,2),2)
    [slice(2,4,2), slice(4,8,2)]

    """

    # if slice.stop is None, we need to provide length
    if one_slice.start is None:
        initial_indice = 0
    else:
        initial_indice = one_slice.start
    if one_slice.stop is None:
        if length is None:
            raise SliceError("stop is None, must specify length.")
        final_indice = length
    else:
        final_indice = one_slice.stop
    if one_slice.step is None:
        step = 1
    else:
        step = one_slice.step
    if divisions > final_indice-initial_indice:
        raise SliceError("divisions greater than number of values in slice.")
    num_of_values = int(np.ceil((final_indice-initial_indice)/float(step)))
    divided_slices = []
    for i in reversed(range(1, divisions + 1)):
        num_in_slice = int(np.round(num_of_values/float(i)))
        stop_indice = initial_indice+num_in_slice*step
        if stop_indice > final_indice:
            stop_indice = final_indice
        next_slice = slice(initial_indice, stop_indice, step)
        divided_slices.append(next_slice)
        num_of_values -= num_in_slice
        initial_indice += num_in_slice*step
    return divided_slices


def divide_slices(shape, divisions, slices=None, dimensions=None):
    """Divide slices.

    Parameters
    ----------
    shape - tuple of int
    divisions - int or list of int or dictionary of int
        if just a number, this is the number of slices tuple that will be
        returned; if a list of numbers is provided, they correspond to the
        number of divisions in each dimension; alternatively, a dictionary
        using the name of the dimensions can be used.
    slices - list of slices or dictionary of slices or None
        slices with respect to the given shape, or None to use the whole shape;
        can also use None inside the list of slices to use all range within
        that particular dimension; alternatively, a dictionary of slices
        using the name of a the dimensions can be used.
    dimensions - list of str

    Returns
    -------
    out - list of tuple of slices

    Examples
    --------
    >>> divide_slices((365,360,179),{'lon':2},dimensions=['time','lon','lat'])
    [(slice(0, 365, 1), slice(0, 180, 1), slice(0, 179, 1)),
     (slice(0, 365, 1), slice(180, 360, 1), slice(0, 179, 1))]

    """

    # if slices is None, consider the whole shape.
    if slices is None:
        slices = []
        for i in range(len(shape)):
            slices.append(slice(0, shape[i]))
    # if slices is a dictionary, convert to list.
    if isinstance(slices, dict):
        convert_slices = []
        for i in range(len(shape)):
            if dimensions[i] in slices.keys():
                convert_slices.append(slices[dimensions[i]])
            else:
                convert_slices.append(None)
        slices = convert_slices
    # if a member of slices is None, consider the whole range.
    for i, slice1 in enumerate(slices):
        if slice1 is None:
            slices[i] = slice(0, shape[i])
    # if divisions is a dictionary, convert to list.
    if isinstance(divisions, dict):
        convert_divisions = []
        for i in range(len(shape)):
            if dimensions[i] in divisions.keys():
                convert_divisions.append(divisions[dimensions[i]])
            else:
                convert_divisions.append(1)
        divisions = convert_divisions
    # if divisions is just a number, pick largest dimension.
    if not hasattr(divisions, '__len__'):
        num_values = np.zeros([len(shape)])
        for i in range(len(shape)):
            if slices[i].stop is None:
                mystop = shape[i]
            else:
                mystop = slices[i].stop
            if slices[i].start is None:
                mystart = 0
            else:
                mystart = slices[i].start
            if slices[i].step is None:
                mystep = 1
            else:
                mystep = slices[i].step
            num_values[i] = int(np.ceil((mystop-mystart)/float(mystep)))
        indices = np.where(num_values == num_values.max())
        single_division = divisions
        divisions = []
        for i in range(len(shape)):
            if i == indices[0][0]:
                divisions.append(single_division)
            else:
                divisions.append(1)
    # independently generate slices for each dimension
    dimensions_slices = []
    for i in range(len(shape)):
        dimensions_slices.append(divide_slice(slices[i], divisions[i],
                                              shape[i]))
    divided_slices = []
    current_divisions = []
    for i in range(len(shape)):
        current_divisions.append(1)
    for i in range(np.array(divisions).prod()):
        these_slices = []
        for j in range(len(shape)):
            these_slices.append(dimensions_slices[j][current_divisions[j]-1])
        divided_slices.append(tuple(these_slices))
        for j in reversed(range(len(shape))):
            current_divisions[j] += 1
            if current_divisions[j] > divisions[j]:
                current_divisions[j] = 1
            else:
                break
    return divided_slices


def divide_slices_for_memory_fit(shape, dtype, slices=None, dimensions=None,
                                 memory_size=2000000000,
                                 divide_dimension=None):
    """Divide slices to fit in memory.

    Parameters
    ----------
    shape - tuple of int
    dtype - numpy.dtype
    slices - list of slices or dictionary of slices or None
        slices with respect to the given shape, or None to use the whole shape;
        can also use None inside the list of slices to use all range within
        that particular dimension; alternatively, a dictionary of slices
        using the name of a the dimensions can be used.
    dimensions - list of str
    memory_size - int
    divide_dimension - int or list of int/str
        if just a number, this is the index of the dimension that will be
        divided; if a list of numbers/strings is provided, they correspond to
        multiple candidate dimensions for splitting, with priority going
        to the first one provided.

    Returns
    -------
    out - list of tuple of slices

    Examples
    --------
    >>> divide_slices_for_memory_fit((365,360,179), np.float32,
                                     dimensions=['time','lon','lat'],
                                     memory_size=20000000,
                                     divide_dimension=['lon'])
    [(slice(0, 365, 1), slice(0, 72, 1), slice(0, 179, 1)),
     (slice(0, 365, 1), slice(72, 144, 1), slice(0, 179, 1)),
     (slice(0, 365, 1), slice(144, 216, 1), slice(0, 179, 1)),
     (slice(0, 365, 1), slice(216, 288, 1), slice(0, 179, 1)),
     (slice(0, 365, 1), slice(288, 360, 1), slice(0, 179, 1))]

    """

    if slices is None:
        num_divisions = _num_divisions_for_memory_fit(np.prod(shape), dtype,
                                                      memory_size)
    else:
        if isinstance(slices, dict):
            n = 0
            for key, one_slice in slices.items():
                i = dimensions.index(key)
                if one_slice:
                    if one_slice.stop is None:
                        stop = shape[i]
                    else:
                        stop = one_slice.stop
                    if one_slice.start is None:
                        start = 0
                    else:
                        start = one_slice.start
                    if one_slice.step is None:
                        step = 1
                    else:
                        step = one_slice.step
                    n += np.ceil((stop-start)/float(step))
                else:
                    n += shape[i]
            for i, key in enumerate(dimensions.keys()):
                if key not in list(slices.keys()):
                    n += shape[i]
        else:
            n = 0
            for i, one_slice in enumerate(slices):
                if one_slice:
                    if one_slice.stop is None:
                        stop = shape[i]
                    else:
                        stop = one_slice.stop
                    if one_slice.start is None:
                        start = 0
                    else:
                        start = one_slice.start
                    if one_slice.step is None:
                        step = 1
                    else:
                        step = one_slice.step
                    n += np.ceil((stop-start)/float(step))
                else:
                    n += shape[i]
        num_divisions = _num_divisions_for_memory_fit(n, dtype, memory_size)
    if divide_dimension is None:
        return divide_slices(shape, num_divisions, slices, dimensions)
    elif isinstance(divide_dimension, basestring):
        return divide_slices(shape, {divide_dimension: num_divisions}, slices,
                             dimensions)
    elif isinstance(divide_dimension, list):
        divisions_list = [1]*len(shape)
        valid_divs = []
        for div in divide_dimension:
            if isinstance(div, basestring):
                valid_divs.append(dimensions.index(div))
            else:
                valid_divs.append(div)
        for valid_div in valid_divs:
            if shape[valid_div] >= num_divisions:
                divisions_list[valid_div] = num_divisions
                break
            else:
                divisions_list[valid_div] = shape[valid_div]
                num_divisions = np.ceil(num_divisions/shape[valid_div])
        return divide_slices(shape, divisions_list, slices, dimensions)
