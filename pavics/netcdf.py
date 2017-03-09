"""
======
NetCDF
======

Classes:

 * NetCDFError - the exception raised on failure.

Functions:

 * :func:`datetimes_to_time_vectors` - convert list of datetimes to Nx6 matrix.
 * :func:`time_vectors_to_datetimes` - convert time vectors to datetimes.
 * :func:`validate_calendar` - Valide calendar string.
 * :func:`_calendar_from_ncdataset` - Get calendar from NetCDF Dataset.
 * :func:`get_calendar` - Get calendar from a NetCDF resource.
 * :func:`nc_copy_attrs` - copy multiple global attributes.
 * :func:`nc_copy_dimensions` - copy dimensions.
 * :func:`nc_copy_variables_structure` - copy variables structure.
 * :func:`nc_copy_variables_attributes` - copy variables attribute.
 * :func:`nc_copy_variables_data` - copy variables data.
 * :func:`create_dummy_netcdf` - create a dummy NetCDF file.
 * :func:`period2indices` - Find indices corresponding to a given period.

"""

import time
import datetime
import numpy as np
import numpy.ma as ma
import netCDF4


class NetCDFError(Exception):
    pass


def datetimes_to_time_vectors(datetimes):
    """Convert list of datetimes to Nx6 matrix.

    Parameters
    ----------
    datetimes - list of datetime

    Returns
    -------
    out - numpy array
        Nx6 matrix of time vectors

    Notes
    -----
    1. Does not support microsecond (datetime shape of length 7)

    """

    def datetime_timetuple(one_datetime):
        if one_datetime is None:
            return ma.masked_all([6], dtype='int32')
        return one_datetime.timetuple()[0:6]

    try:
        time_tuples = list(map(datetime_timetuple, datetimes))
        return ma.array(time_tuples)
    except (AttributeError, TypeError):
        time_tuple = datetimes.timetuple()
        return ma.array(time_tuple[0:6])


def time_vectors_to_datetimes(time_vectors):
    """Convert time vectors to a list of datetime.

    Parameters
    ----------
    time_vectors - Nx6 matrix

    Returns
    -------
    out1,out2,out3 - list of datetimes, list of masked indices,
                     list of valid indices

    """

    datetimes = []
    ncdatetimes = []
    irregular_calendar = False
    masked_datetimes = []
    valid_datetimes = []
    if len(time_vectors.shape) == 1:
        full_vector = ma.masked_all([7], dtype='int32')
        for s in range(time_vectors.shape[0]):
            full_vector[s] = time_vectors[s]
        for s in range(time_vectors.shape[0], 7):
            if s in [1, 2]:
                full_vector[s] = 1
            else:
                full_vector[s] = 0
        try:
            one_datetime = datetime.datetime(*full_vector)
        except:
            irregular_calendar = True
        else:
            datetimes.append(one_datetime)
            valid_datetimes.append(0)
        if irregular_calendar:
            try:
                ndatetime = netCDF4.netcdftime.datetime(*full_vector)
                ndatetime.strftime()
            except (ma.MaskError, ValueError):
                masked_datetimes.append(0)
            else:
                ncdatetimes.append(ndatetime)
                valid_datetimes.append(0)
    else:
        for i in range(time_vectors.shape[0]):
            full_vector = ma.masked_all([7], dtype='int32')
            for s in range(time_vectors.shape[1]):
                full_vector[s] = time_vectors[i,s]
            for s in range(time_vectors.shape[1], 7):
                if s in [1,2]:
                    full_vector[s] = 1
                else:
                    full_vector[s] = 0
            try:
                ndatetime = netCDF4.netcdftime.datetime(*full_vector)
                ndatetime.strftime()
            except (ma.MaskError, ValueError):
                masked_datetimes.append(i)
            else:
                ncdatetimes.append(ndatetime)
                valid_datetimes.append(i)
            if not irregular_calendar:
                try:
                    one_datetime = datetime.datetime(*full_vector)
                except ValueError:
                    irregular_calendar = True
                except ma.MaskError:
                    pass
                else:
                    datetimes.append(one_datetime)
    if irregular_calendar:
        return (ncdatetimes, np.array(masked_datetimes),
                np.array(valid_datetimes))
    else:
        return (datetimes, np.array(masked_datetimes),
                np.array(valid_datetimes))


def get_dimensions(nc_resource, var_names=None):
    if isinstance(var_names, basestring):
        var_names = [var_names]
    d = {}
    if isinstance(nc_resource, basestring):
        ncdataset = netCDF4.Dataset(nc_resource, 'r')
    elif isinstance(nc_resource, netCDF4._netCDF4.Dataset):
        ncdataset = nc_resource
    elif isinstance(nc_resource, (list, set, tuple)):
        ncdataset = netCDF4.Dataset(nc_resource[0], 'r')
    else:
        raise NotImplementedError()
    if var_names:
        for var_name in var_names:
            ncvar = ncdataset.variables[var_name]
            d.update({var_name: ncvar.dimensions})
        return d
    else:
        return ncdataset.dimensions.keys()


def validate_calendar(calendar):
    """Validate calendar string for CF Conventions.

    Parameters
    ----------
    calendar : str

    Returns
    -------
    out : str
        same as input if the calendar is valid

    Notes
    -----
    1. The 'none' value for the calendar attribute is not supported anywhere
       in this code presently, so NotImplementedError is raised.
    2. NetCDFError is raised for invalid calendars.

    """

    if calendar in ['gregorian', 'standard', 'proleptic_gregorian', 'noleap',
                    '365_day', 'all_leap', '366_day', '360_day', 'julian']:
        return calendar
    elif calendar == 'none':
        raise NotImplementedError("calendar is set to 'none'")
    else:
        raise NetCDFError("Unknown calendar: %s" % (calendar,))


def _calendar_from_ncdataset(ncdataset):
    """Get calendar from a netCDF4._netCDF4.Dataset object.

    Parameters
    ----------
    ncdataset : netCDF4._netCDF4.Dataset

    Returns
    -------
    out : str
        calendar attribute of the time variable

    Notes
    -----
    1. The 'none' value for the calendar attribute is not supported anywhere
       in this code presently, so NotImplementedError is raised.
    2. NetCDFError is raised for invalid calendars or if there is not time
       variable in the dataset.

    """

    if 'time' in ncdataset.variables:
        if hasattr(ncdataset.variables['time'], 'calendar'):
            return validate_calendar(ncdataset.variables['time'].calendar)
        else:
            return 'gregorian'
    else:
        raise NetCDFError("NetCDF file has no time variable")


def get_calendar(nc_resource):
    """Get calendar from a NetCDF resource.

    Parameters
    ----------
    nc_resource : str or netCDF4._netCDF4.Dataset or netCDF4._netCDF4.Variable

    Returns
    -------
    out : str
        calendar attribute of the time variable

    Notes
    -----
    1. The 'none' value for the calendar attribute is not supported anywhere
       in this code presently, so NotImplementedError is raised.
    2. NetCDFError is raised for invalid calendars or if there is not time
       variable in the dataset.

    """

    if hasattr(nc_resource, 'calendar'):
        return validate_calendar(nc_resource.calendar)
    elif (hasattr(nc_resource, 'variables') and
          hasattr(nc_resource.variables, 'keys')):
        return _calendar_from_ncdataset(nc_resource)
    else:
        try:
            nc = netCDF4.Dataset(nc_resource, 'r')
        except:
            raise NetCDFError(("Unknown NetCDF "
                               "resource: %s") % (str(nc_resource),))
        return _calendar_from_ncdataset(nc)


def _get_var_info(ncvar):
    d = {'name': ncvar.name,
         'units': getattr(ncvar, 'units', None),
         'standard_name': getattr(ncvar, 'standard_name', None),
         'long_name': getattr(ncvar, 'long_name', None),
         'dtype': str(ncvar.dtype)}
    for ncattr in ncvar.ncattrs():
        if ncattr not in ['_FillValue', 'units', 'standard_name', 'long_name']:
            my_attr = getattr(ncvar, ncattr)
            if isinstance(my_attr, np.integer):
                my_attr = int(my_attr)
            elif isinstance(my_attr, np.floating):
                my_attr = float(my_attr)
            elif isinstance(my_attr, np.ndarray):
                my_attr = my_attr.tolist()
            d.update({ncattr: my_attr})
    return d


def _get_point_from_ncvar_and_ordered_indices(ncvar, indices):
    my_value = ncvar[tuple(indices)]
    if isinstance(my_value, (np.float32, np.float64)):
        my_value = float(my_value)
    elif isinstance(my_value, (np.int16, np.uint32)):
        my_value = int(my_value)
    d = {'name': ncvar.name,
         'value': my_value,
         'units': getattr(ncvar, 'units', None),
         'standard_name': getattr(ncvar, 'standard_name', None),
         'long_name': getattr(ncvar, 'long_name', None),
         'dtype': str(ncvar.dtype)}
    for ncattr in ncvar.ncattrs():
        if ncattr not in ['_FillValue', 'units', 'standard_name', 'long_name']:
            my_attr = getattr(ncvar, ncattr)
            if isinstance(my_attr, np.integer):
                my_attr = int(my_attr)
            elif isinstance(my_attr, np.floating):
                my_attr = float(my_attr)
            elif isinstance(my_attr, np.ndarray):
                my_attr = my_attr.tolist()
            d.update({ncattr: my_attr})
    return d


def _get_point_from_ncvar_and_named_indices(ncvar, named_indices):
    indices = [named_indices[x] for x in ncvar.dimensions]
    return _get_point_from_ncvar_and_ordered_indices(ncvar, indices)


def nc_copy_attrs(nc_source, nc_destination, includes=[], excludes=[],
                  renames=None, defaults=None, appends=None):
    """Copy attributes from source file to destination file.

    Parameters
    ----------
    nc_source - netCDF4.Dataset or netCDF4.Variables
    nc_destination - netCDF4.Dataset or netCDF4.Variables
    includes - list of str
        attributes to copy.
    excludes - list of str
        attributes that will be ignored.
    renames - dict
    defaults - dict
        this is not affected by the mapping defined in 'renames'
    appends - dict
        this is not affected by the mapping defined in 'renames'

    Notes
    -----
    If includes is not empty, only those specified attributes are copied.
    To overwrite an attribute, put it in 'excludes' and set its value
    in 'defaults'.
    To append to an attribute, use 'appends'.
    To set an attribute only if it is not in the source file, use 'defaults'
    To replace an attribute and keep a backup, use 'renames' and 'defaults'

    """

    if renames is None:
        renames = {}
    if defaults is None:
        defaults = {}
    if appends is None:
        appends = {}
    attributes = nc_source.ncattrs()
    if includes:
        for include in includes:
            if include not in attributes:
                raise NotImplementedError("Attribute not found.")
        attributes = includes
    for attribute in attributes:
        if attribute not in excludes:
            if attribute not in renames:
                renames[attribute] = attribute
            if renames[attribute] == '_FillValue':
                continue
            copy_attr = getattr(nc_source, attribute)
            # hack for bypassing unicode bug
            try:
                copy_attr = copy_attr.encode('latin-1')
            except (AttributeError, UnicodeEncodeError):
                pass
            nc_destination.__setattr__(renames[attribute], copy_attr)
    for attribute in defaults:
        if not hasattr(nc_destination, attribute):
            # hack for bypassing unicode bug
            try:
                default_attr = defaults[attribute].encode('latin-1')
            except (AttributeError, UnicodeEncodeError):
                default_attr = defaults[attribute]
            nc_destination.__setattr__(attribute, default_attr)
    for attribute in appends:
        if hasattr(nc_destination, attribute):
            warp = getattr(nc_destination, attribute)
            # hack for bypassing unicode bug
            try:
                append_attr = appends[attribute].encode('latin-1')
            except (AttributeError, UnicodeEncodeError):
                append_attr = appends[attribute]
            new_attr = warp + '\n' + append_attr
            nc_destination.__setattr__(attribute, new_attr)
        else:
            # hack for bypassing unicode bug
            try:
                append_attr = appends[attribute].encode('latin-1')
            except (AttributeError, UnicodeEncodeError):
                append_attr = appends[attribute]
            nc_destination.__setattr__(attribute, append_attr)


def nc_copy_dimensions(nc_source, nc_destination, includes=[], excludes=[],
                       renames=None, defaults=None, reshapes=None):
    """Copy NetCDF dimensions.

    Parameters
    ----------
    nc_source - netCDF4.Dataset
    nc_destination - netCDF4.Dataset
    includes - list of str
        dimensions to copy.
    excludes - list of str
        dimensions that will be ignored.
    renames - dict
    defaults - dict
        this is not affected by the mapping defined in 'renames'
    reshapes - dict
        this is not affected by the mapping defined in 'renames'

    Notes
    -----
    If includes is not empty, only those specified dimensions are copied.

    """

    if renames is None:
        renames = {}
    if defaults is None:
        defaults = {}
    if reshapes is None:
        reshapes = {}
    dims_src = list(nc_source.dimensions)
    dims_dest = list(nc_destination.dimensions)
    if includes:
        for include in includes:
            if include not in dims_src:
                raise NotImplementedError("Dimension not found.")
        dims_src = includes
    for dim_src in dims_src:
        if dim_src in excludes:
            continue
        if dim_src not in renames:
            renames[dim_src] = dim_src
        if renames[dim_src] not in dims_dest:
            ncdim1 = nc_source.dimensions[dim_src]
            if renames[dim_src] not in reshapes:
                if ncdim1.isunlimited():
                    reshapes[renames[dim_src]] = None
                else:
                    reshapes[renames[dim_src]] = len(ncdim1)
            nc_destination.createDimension(renames[dim_src],
                                           reshapes[renames[dim_src]])
    for dim in defaults:
        if dim not in nc_destination.dimensions:
            nc_destination.createDimension(dim, defaults[dim])


def nc_copy_variables_structure(nc_source, nc_destination, includes=[],
                                excludes=[], renames=None, new_dtype=None,
                                new_dimensions=None, create_args=None):
    """Copy structure of multiple NetCDF variables.

    Parameters
    ----------
    nc_source - netCDF4.Dataset
    nc_destination - netCDF4.Dataset
    includes - list of str
    excludes - list of str
    renames - dict
    new_dtype - dict
    new_dimensions - dict
    create_args - dict
        Set key '_global' to apply to all variables.

    """

    if renames is None:
        renames = {}
    if new_dtype is None:
        new_dtype = {}
    if new_dimensions is None:
        new_dimensions = {}
    if create_args is None:
        create_args = {}
    vars_src = list(nc_source.variables)
    vars_dest = list(nc_destination.variables)
    if includes:
        for include in includes:
            if include not in vars_src:
                raise NotImplementedError("Variable not found.")
        vars_src = includes
    for var_src in vars_src:
        if var_src in excludes:
            continue
        if var_src not in vars_dest:
            ncvar1 = nc_source.variables[var_src]
            if var_src not in renames:
                renames[var_src] = var_src
            if var_src not in new_dtype:
                new_dtype[var_src] = ncvar1.dtype
            if var_src not in new_dimensions:
                new_dimensions[var_src] = ncvar1.dimensions
            if var_src not in create_args:
                create_args[var_src] = {}
            if '_global' in create_args:
                for key in create_args['_global']:
                    if key not in create_args[var_src]:
                        create_args[var_src][key] = create_args['_global'][key]
            warp = 'fill_value' not in create_args[var_src]
            if hasattr(ncvar1, '_FillValue') and warp:
                create_args[var_src]['fill_value'] = ncvar1._FillValue
            if 'chunksizes' not in create_args[var_src]:
                # If dimensions size have changed it is not safe to copy
                # chunksizes.
                for one_dimension in new_dimensions[var_src]:
                    source_size = len(nc_source.dimensions[one_dimension])
                    dest_size = len(nc_destination.dimensions[one_dimension])
                    if source_size != dest_size:
                        break
                else:
                    if ncvar1.chunking() == 'contiguous':
                        # This does not seem to work, why?
                        # create_args[var_src]['contiguous'] = True
                        pass
                    else:
                        create_args[var_src]['chunksizes'] = ncvar1.chunking()
            nc_destination.createVariable(renames[var_src], new_dtype[var_src],
                                          new_dimensions[var_src],
                                          **create_args[var_src])


def nc_copy_variables_attributes(nc_source, nc_destination, includes=[],
                                 excludes=[], renames=None, attr_includes=None,
                                 attr_excludes=None, attr_renames=None,
                                 attr_defaults=None, attr_appends=None):
    """Copy NetCDF variables attributes.

    Parameters
    ----------
    nc_source - netCDF4.Dataset
    nc_destination - netCDF4.Dataset
    includes - list of str
    excludes - list of str
    renames - dict
    attr_includes - dict
        for each variable, the entry will be the 'includes' in nc_copy_attr.
    attr_excludes - dict
        for each variable, the entry will be the 'excludes' in nc_copy_attr.
        Set key '_global' to apply to all variables.
    attr_renames - dict
        for each variable, the entry will be the 'renames' in nc_copy_attr.
    attr_defaults - dict
        for each variable, the entry will be the 'defaults' in nc_copy_attr.
    attr_appends - dict
        for each variable, the entry will be the 'appends' in nc_copy_attr.

    """

    if renames is None:
        renames = {}
    if attr_includes is None:
        attr_includes = {}
    if attr_excludes is None:
        attr_excludes = {}
    if attr_renames is None:
        attr_renames = {}
    if attr_defaults is None:
        attr_defaults = {}
    if attr_appends is None:
        attr_appends = {}
    vars_src = list(nc_source.variables)
    vars_dest = list(nc_destination.variables)
    if includes:
        for include in includes:
            if include not in vars_src:
                raise NotImplementedError("Variable not found in source file.")
        vars_src = includes
    for var_src in vars_src:
        if var_src in excludes:
            continue
        ncvar1 = nc_source.variables[var_src]
        if var_src not in renames:
            renames[var_src] = var_src
        if renames[var_src] not in vars_dest:
            raise NotImplementedError("Variable not found in dest. file.")
        ncvar2 = nc_destination.variables[renames[var_src]]
        if var_src not in attr_includes:
            attr_includes[var_src] = []
        if var_src not in attr_excludes:
            attr_excludes[var_src] = []
        if '_global' in attr_excludes:
            attr_excludes[var_src].extend(attr_excludes['_global'])
        if var_src not in attr_renames:
            attr_renames[var_src] = {}
        if var_src not in attr_defaults:
            attr_defaults[var_src] = {}
        if var_src not in attr_appends:
            attr_appends[var_src] = {}
        nc_copy_attrs(ncvar1, ncvar2, attr_includes[var_src],
                      attr_excludes[var_src], attr_renames[var_src],
                      attr_defaults[var_src], attr_appends[var_src])


def nc_copy_variables_data(nc_source, nc_destination, includes=[], excludes=[],
                           renames=None, source_slices=None,
                           destination_slices=None):
    """Copy NetCDF variables data.

    Parameters
    ----------
    nc_source - netCDF4.Dataset
    nc_destination - netCDF4.Dataset
    includes - list of str
    excludes - list of str
    renames - dict

    """

    if renames is None:
        renames = {}
    if source_slices is None:
        source_slices = {}
    if destination_slices is None:
        destination_slices = {}
    vars_src = list(nc_source.variables)
    vars_dest = list(nc_destination.variables)
    if includes:
        for include in includes:
            if include not in vars_src:
                raise NotImplementedError("Variable not found in source file.")
        vars_src = includes
    for var_src in vars_src:
        if var_src in excludes:
            continue
        ncvar1 = nc_source.variables[var_src]
        if var_src not in renames:
            renames[var_src] = var_src
        if renames[var_src] not in vars_dest:
            raise NotImplementedError("Variable not found in dest. file.")
        ncvar2 = nc_destination.variables[renames[var_src]]
        if var_src in source_slices:
            raise NotImplementedError("Slice copies.")
        if renames[var_src] in destination_slices:
            raise NotImplementedError("Slice copies.")
        ncvar2[...] = ncvar1[...]


def _default_fill_data(var_shape, fill_mode, data_scale_factor,
                       data_add_offset, **kwags):
    if fill_mode == 'random':
        return np.random.rand(*var_shape)*data_scale_factor + data_add_offset
    elif fill_mode == 'gradient':
        var_size = int(np.prod(var_shape))
        fill_data = np.arange(0, 1.0 + 0.5/(var_size-1), 1.0/(var_size-1))
        fill_data = fill_data*data_scale_factor + data_add_offset
        return np.reshape(fill_data, var_shape)
    elif fill_mode == 'pairing':
        fill_data = np.zeros(var_shape, dtype=int)
        dim = -1
        multiplier = 1
        while dim >= -len(var_shape):
            elements = np.arange(0, var_shape[dim])
            tile_shape = list(var_shape)
            if dim != -1:
                tile_shape[dim] = var_shape[-1]
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
        return fill_data*data_scale_factor + data_add_offset


def create_dummy_netcdf(nc_file, nc_format='NETCDF4_CLASSIC',
                        global_attributes=None, dimensions=None,
                        variables=None, time_num_values=1, time_dtype='f4',
                        time_units='days since 2001-01-01 00:00:00',
                        time_calendar='gregorian', level_dtype='f4',
                        level_units='Pa', level_positive='down',
                        level_name='height', time_values=None,
                        level_values=None, lon_values=None, lat_values=None,
                        close_nc=True):
    """
    Create a dummy NetCDF file on disk.

    Parameters
    ----------
    nc_file : str
        Name (and path) of the file to write.
    nc_format : str
    global_attributes : dict
    dimensions : list of tuple
        Tuples are (dimension_name, dimension_size).
    variables : dict
        Format is {'varname': {'create_args': {'dtype': 'f4'},
                               'attributes': {'units': 1},
                               'fill_mode': 'random',
                               'values': None,
                               'data_scale_factor': 1.0,
                               'data_add_offset': 0.0}}
    time_num_values : int
        Used for default filling of unlimited time dimension.
    time_dtype : str
    time_units : str
    time_calendar : str
    level_dtype : str
    level_units : str
    level_positive : str
    level_name : str
    time_values : numpy.ndarray
    level_values : numpy.ndarray
    lon_values : numpy.ndarray
    lat_values : numpy.ndarray

    Notes
    -----
    The 'random' fill_mode uses [0,1).
    The 'gradient' fill_mode uses [0,1].
    The 'pairing' fill_mode allows unique and easily predictable values that
    are increasing along all axes in multiple dimensions based on the ndarray
    indices. The value for indice (i1,i2,i3) is given by:
    i3 + i2*10**(len(str(N3))) + i1*10**(len(str(N3))+len(str(N2)))
    In words, starting from the last indice, we add its value plus a padding
    of a power of 10 that is large enough to contain all previous values.

    The data_scale_factor and data_add_offset are used in fill_mode, they
    are NOT traditional NetCDF scale_factor and add_offset arguments.

    Features that would make this more flexible in the future:
    1. insert_annual_cycle=True: fake an annual cycle in the data.
    2. add the yc/xc or rlat/rlon variables when the modes are activated
    3. create files larger than machine memory

    """

    # Template for CF-1.6 convention in python, using netCDF4.
    # http://cfconventions.org/

    # Aliases for default fill values
    # defi2 = netCDF4.default_fillvals['i2']
    # defi4 = netCDF4.default_fillvals['i4']
    # deff4 = netCDF4.default_fillvals['f4']

    # Create netCDF file
    # Valid formats are 'NETCDF4', 'NETCDF4_CLASSIC', 'NETCDF3_CLASSIC' and
    # 'NETCDF3_64BIT'
    now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    nc = netCDF4.Dataset(nc_file, 'w', format=nc_format)

    # 2.6.1 Identification of Conventions
    nc.Conventions = 'CF-1.6'

    # 2.6.2. Description of file contents
    nc.title = 'Dummy NetCDF file'
    nc.history = "{0}: File creation.".format(now)
    for (gattr, value) in global_attributes.items():
        setattr(nc, gattr, value)

    # Create netCDF dimensions
    if dimensions:
        for dimension in dimensions:
            nc.createDimension(*dimension)

    # Create netCDF variables
    # Compression parameters include:
    # zlib=True,complevel=9,least_significant_digit=1
    # Set the fill value (shown with the 'f4' default value here) using:
    # fill_value=netCDF4.default_fillvals['f4']
    # In order to also follow COARDS convention, it is suggested to enforce the
    # following rule (this is used, for example, in nctoolbox for MATLAB):
    #     Coordinate Variables:
    #     1-dimensional netCDF variables whose dimension names are identical to
    #     their variable names are regarded as "coordinate variables"
    #     (axes of the underlying grid structure of other variables defined on
    #     this dimension).

    if variables:
        if ('time' in nc.dimensions) and ('time' not in variables):
            # 4.4. Time Coordinate
            time = nc.createVariable('time', time_dtype, ('time',), zlib=True)
            time.units = time_units
            time.long_name = 'time'
            time.standard_name = 'time'
            # 4.4.1. Calendar
            if time_calendar:
                time.calendar = time_calendar
            if time_values is None:
                time[:] = list(range(time_num_values))
            else:
                time[:] = time_values[:]

        if ('level' in nc.dimensions) and ('level' not in variables):
            # 4.3. Vertical (Height or Depth) Coordinate
            level = nc.createVariable('level', level_dtype, ('level',),
                                      zlib=True)
            level.units = level_units
            level.positive = level_positive
            level.long_name = 'air_pressure'
            level.standard_name = 'air_pressure'
            if level_values is None:
                raise NotImplementedError()
            else:
                level[:] = level_values[:]

        if ('lat' in nc.dimensions) and ('lat' not in variables):
            # 4.1. Latitude Coordinate
            lat = nc.createVariable('lat', 'f4', ('lat',), zlib=True)
            lat.units = 'degrees_north'
            lat.long_name = 'latitude'
            lat.standard_name = 'latitude'
            if lat_values is None:
                dlat = 180.0/(lat.size+1)
                lat[:] = np.arange(-90.0 + dlat, 90.0 - dlat/2.0, dlat)
            else:
                lat[:] = lat_values[:]

        if ('lon' in nc.dimensions) and ('lon' not in variables):
            # 4.2. Longitude Coordinate
            lon = nc.createVariable('lon', 'f4', ('lon',), zlib=True)
            lon.units = 'degrees_east'
            lon.long_name = 'longitude'
            lon.standard_name = 'longitude'
            if lon_values is None:
                dlon = 360.0/lon.size
                lon[:] = np.arange(0.0, 360.0 - dlon/2.0, dlon)
            else:
                lon[:] = lon_values[:]

        if ('station' in nc.dimensions) and ('station' not in variables):
            lat = nc.createVariable('lat', 'f4', ('station',), zlib=True)
            lat.units = 'degrees_north'
            lat.long_name = 'latitude'
            lat.standard_name = 'latitude'
            if lat_values is None:
                dlat = 180.0/(lat.size+1)
                lat[:] = np.arange(-90.0 + dlat, 90.0 - dlat/2.0, dlat)
            else:
                lat[:] = lat_values[:]
            lon = nc.createVariable('lon', 'f4', ('station',), zlib=True)
            lon.units = 'degrees_east'
            lon.long_name = 'longitude'
            lon.standard_name = 'longitude'
            if lon_values is None:
                dlon = 360.0/lon.size
                lon[:] = np.arange(0.0, 360.0 - dlon/2.0, dlon)
            else:
                lon[:] = lon_values[:]

        if ('yc' in nc.dimensions) and ('yc' not in variables):
            lat = nc.createVariable('lat', 'f4', ('yc', 'xc'), zlib=True)
            lat.units = 'degrees_north'
            lat.long_name = 'latitude'
            lat.standard_name = 'latitude'
            if lat_values is None:
                raise NotImplementedError()
            else:
                lat[:,:] = lat_values[:,:]
            lon = nc.createVariable('lon', 'f4', ('yc', 'xc'), zlib=True)
            lon.units = 'degrees_east'
            lon.long_name = 'longitude'
            lon.standard_name = 'longitude'
            if lon_values is None:
                raise NotImplementedError()
            else:
                lon[:,:] = lon_values[:,:]

        if ('rlat' in nc.dimensions) and ('rlon' not in variables):
            lat = nc.createVariable('lat', 'f4', ('rlat', 'rlon'), zlib=True)
            lat.units = 'degrees_north'
            lat.long_name = 'latitude'
            lat.standard_name = 'latitude'
            if lat_values is None:
                raise NotImplementedError()
            else:
                lat[:,:] = lat_values[:,:]
            lon = nc.createVariable('lon', 'f4', ('rlat', 'rlon'), zlib=True)
            lon.units = 'degrees_east'
            lon.long_name = 'longitude'
            lon.standard_name = 'longitude'
            if lon_values is None:
                raise NotImplementedError()
            else:
                lon[:,:] = lon_values[:,:]

        # 2.3. Naming Conventions
        # 2.4 Dimensions
        #     If any or all of the dimensions of a variable have the
        #     interpretations of "date or time" (T), "height or depth" (Z),
        #     "latitude" (Y), or "longitude" (X) then we recommend, but do not
        #     require, those dimensions to appear in the relative order T,
        #     then Z, then Y, then X
        # Chunksizes should be set to the expected input/output pattern and be
        # of the order of 1000000 (that is the product of the chunksize in each
        # dimention). e.g. for daily data, this might be 30 (a month) or
        # 365 (a year) in time, and then determine the spatial chunks to obtain
        # ~1000000.
        for (var_name, var_args) in variables.items():
            ncvar = nc.createVariable(var_name, **var_args['create_args'])
            if 'attributes' in var_args:
                for (var_attr, attr_value) in var_args['attributes'].items():
                    setattr(ncvar, var_attr, attr_value)

            if 'fill_mode' in var_args:
                ncvar[...] = _default_fill_data(ncvar.shape, **var_args)
            elif ('values' in var_args) and var_args['values']:
                ncvar[...] = var_args['values']

    if close_nc:
        nc.close()
    else:
        return nc


def period2indices(initial_date, final_date, nc_file, calendar=None):
    """Find indices corresponding to a given period.

    Parameters
    ----------
    initial_date : string
        format must be %Y-%m-%dT%H:%M:%S
    final_date : string
        format must be %Y-%m-%dT%H:%M:%S
    nc_file : string
    calendar : string
        default is gregorian calendar

    Returns
    -------
    out : dict
        dict['initial_index'] is the initial index of the period
        dict['final_index'] is the final index of the period

    Notes
    -----
    1. The final index is inclusive. When using it in a slice in python,
       must add 1 to get the proper period.
    2. The input format of the date is subject to change to time.struct_time
    3. Forcing a full date & time specification avoids confusion in the
       use of date2index 'before' selection for the final index. e.g. a
       user choosing a final date of 2012-12-31 and a NetCDF file having
       a datetime of 2012-12-31T12:00:00 as its final day of the year.

    """

    initial_time = time.strptime(initial_date, '%Y-%m-%dT%H:%M:%S')
    final_time = time.strptime(final_date, '%Y-%m-%dT%H:%M:%S')

    nc = netCDF4.Dataset(nc_file, 'r')
    if 'time' not in nc.variables:
        raise NetCDFError("No time variable in the NetCDF file.")
    nctime = nc.variables['time']
    if calendar is None:
        if hasattr(nctime, 'calendar'):
            calendar = nctime.calendar
        else:
            calendar = 'gregorian'
    if calendar in ['gregorian', 'standard']:
        # The gregorian calendar requires datetime.datetime inputs.
        initial_nctime = datetime.datetime(initial_time.tm_year,
                                           initial_time.tm_mon,
                                           initial_time.tm_mday,
                                           initial_time.tm_hour,
                                           initial_time.tm_min,
                                           initial_time.tm_sec)
        final_nctime = datetime.datetime(final_time.tm_year,
                                         final_time.tm_mon,
                                         final_time.tm_mday,
                                         final_time.tm_hour,
                                         final_time.tm_min,
                                         final_time.tm_sec)
    else:
        # For the other calendars, we can use the netcdftime.datetime.
        initial_nctime = netCDF4.netcdftime.datetime(initial_time.tm_year,
                                                     initial_time.tm_mon,
                                                     initial_time.tm_mday,
                                                     initial_time.tm_hour,
                                                     initial_time.tm_min,
                                                     initial_time.tm_sec)
        final_nctime = netCDF4.netcdftime.datetime(final_time.tm_year,
                                                   final_time.tm_mon,
                                                   final_time.tm_mday,
                                                   final_time.tm_hour,
                                                   final_time.tm_min,
                                                   final_time.tm_sec)
    index_ini = netCDF4.date2index(initial_nctime, nctime, calendar=calendar,
                                   select='after')
    index_fin = netCDF4.date2index(final_nctime, nctime, calendar=calendar,
                                   select='before')
    return {'initial_index': index_ini, 'final_index': index_fin}
