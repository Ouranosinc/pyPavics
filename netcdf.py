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

    """

    def datetime_timetuple(one_datetime):
        if one_datetime is None:
            return ma.masked_all([6],dtype='int32')
        return one_datetime.timetuple()[0:6]

    try:
        time_tuples = map(datetime_timetuple,datetimes)
        return ma.array(time_tuples)
    except AttributeError, TypeError:
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
        full_vector = ma.masked_all([7],dtype='int32')
        for s in range(time_vectors.shape[0]):
            full_vector[s] = time_vectors[s]
        for s in range(time_vectors.shape[0],7):
            if s in [1,2]:
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
                datetime_str = ndatetime.strftime()
            except (ma.MaskError,ValueError):
                masked_datetimes.append(0)
            else:
                ncdatetimes.append(ndatetime)
                valid_datetimes.append(0)
    else:
        for i in range(time_vectors.shape[0]):
            full_vector = ma.masked_all([7],dtype='int32')
            for s in range(time_vectors.shape[1]):
                full_vector[s] = time_vectors[i,s]
            for s in range(time_vectors.shape[1],7):
                if s in [1,2]:
                    full_vector[s] = 1
                else:
                    full_vector[s] = 0
            try:
                ndatetime = netCDF4.netcdftime.datetime(*full_vector)
                datetime_str = ndatetime.strftime()
            except (ma.MaskError,ValueError):
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
        return ncdatetimes,np.array(masked_datetimes),np.array(valid_datetimes)
    else:
        return datetimes,np.array(masked_datetimes),np.array(valid_datetimes)

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

    if calendar in ['gregorian','standard','proleptic_gregorian','noleap',
                    '365_day','all_leap','366_day','360_day','julian']:
        return calendar
    elif calendar == 'none':
        raise NotImplementedError("calendar is set to 'none'")
    else:
        raise NetCDFError("Unknown calendar: %s" % (nc_source.calendar,))

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

    if ncdataset.variables.has_key('time'):
        if hasattr(ncdataset.variables['time'],'calendar'):
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

    if hasattr(nc_resource,'calendar'):
        return validate_calendar(nc_resource.calendar)
    elif hasattr(nc_resource,'variables') and \
         hasattr(nc_resource.variables,'has_key'):
        return _calendar_from_ncdataset(nc_resource)
    else:
        try:
            nc = netCDF4.Dataset(nc_resource,'r')
        except:
            raise NetCDFError(("Unknown NetCDF "
                               "resource: %s") % (str(nc_resource),))
        return _calendar_from_ncdataset(nc)

def nc_copy_attrs(nc_source,nc_destination,includes=[],excludes=[],renames=None,
                  defaults=None,appends=None):
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
            if attribute not in renames.keys():
                renames[attribute] = attribute
            if renames[attribute] == '_FillValue':
                continue
            copy_attr = getattr(nc_source,attribute)
            # hack for bypassing unicode bug
            try:
                copy_attr = copy_attr.encode('latin-1')
            except (AttributeError,UnicodeEncodeError):
                pass
            nc_destination.__setattr__(renames[attribute],copy_attr)
    for attribute in defaults.keys():
        if not hasattr(nc_destination,attribute):
            # hack for bypassing unicode bug
            try:
                default_attr = defaults[attribute].encode('latin-1')
            except (AttributeError,UnicodeEncodeError):
                default_attr = defaults[attribute]
            nc_destination.__setattr__(attribute,default_attr)
    for attribute in appends.keys():
        if hasattr(nc_destination,attribute):
            warp = getattr(nc_destination,attribute)
            # hack for bypassing unicode bug
            try:
                append_attr = appends[attribute].encode('latin-1')
            except (AttributeError,UnicodeEncodeError):
                append_attr = appends[attribute]
            new_attr = warp + '\n' + append_attr
            nc_destination.__setattr__(attribute,new_attr)
        else:
            # hack for bypassing unicode bug
            try:
                append_attr = appends[attribute].encode('latin-1')
            except (AttributeError,UnicodeEncodeError):
                append_attr = appends[attribute]
            nc_destination.__setattr__(attribute,append_attr)

def nc_copy_dimensions(nc_source,nc_destination,includes=[],excludes=[],
                       renames=None,defaults=None,reshapes=None):
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
    dims_src = nc_source.dimensions.keys()
    dims_dest = nc_destination.dimensions.keys()
    if includes:
        for include in includes:
            if include not in dims_src:
                raise NotImplementedError("Dimension not found.")
        dims_src = includes
    for dim_src in dims_src:
        if dim_src in excludes:
            continue
        if dim_src not in renames.keys():
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
    for dim in defaults.keys():
        if not dim in nc_destination.dimensions.keys():
            nc_destination.createDimension(dim,defaults[dim])

def nc_copy_variables_structure(nc_source,nc_destination,includes=[],
                                excludes=[],renames=None,new_dtype=None,
                                new_dimensions=None,create_args=None):
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
    vars_src = nc_source.variables.keys()
    vars_dest = nc_destination.variables.keys()
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
            if var_src not in renames.keys():
                renames[var_src] = var_src
            if var_src not in new_dtype.keys():
                new_dtype[var_src] = ncvar1.dtype
            if var_src not in new_dimensions.keys():
                new_dimensions[var_src] = ncvar1.dimensions
            if var_src not in create_args.keys():
                create_args[var_src] = {}
            if '_global' in create_args.keys():
                for key in create_args['_global'].keys():
                    if key not in create_args[var_src]:
                        create_args[var_src][key] = create_args['_global'][key]
            warp = 'fill_value' not in create_args[var_src].keys()
            if hasattr(ncvar1,'_FillValue') and warp:
                create_args[var_src]['fill_value'] = ncvar1._FillValue
            if not create_args[var_src].has_key('chunksizes'):
                # If dimensions size have changed it is not safe to copy
                # chunksizes.
                for one_dimension in new_dimensions[var_src]:
                    source_size = len(nc_source.dimensions[one_dimension])
                    dest_size = len(nc_destination.dimensions[one_dimension])
                    if source_size != dest_size:
                        break
                else:
                    create_args[var_src]['chunksizes'] = ncvar1.chunking()
            nc_destination.createVariable(renames[var_src],new_dtype[var_src],
                                          new_dimensions[var_src],
                                          **create_args[var_src])

def nc_copy_variables_attributes(nc_source,nc_destination,includes=[],
                                 excludes=[],renames=None,attr_includes=None,
                                 attr_excludes=None,attr_renames=None,
                                 attr_defaults=None,attr_appends=None):
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
    vars_src = nc_source.variables.keys()
    vars_dest = nc_destination.variables.keys()
    if includes:
        for include in includes:
            if include not in vars_src:
                raise NotImplementedError("Variable not found in source file.")
        vars_src = includes
    for var_src in vars_src:
        if var_src in excludes:
            continue
        ncvar1 = nc_source.variables[var_src]
        if var_src not in renames.keys():
            renames[var_src] = var_src
        if renames[var_src] not in vars_dest:
            raise NotImplementedError("Variable not found in destination file.")
        ncvar2 = nc_destination.variables[renames[var_src]]
        if var_src not in attr_includes.keys():
            attr_includes[var_src] = []
        if var_src not in attr_excludes.keys():
            attr_excludes[var_src] = []
        if '_global' in attr_excludes.keys():
            attr_excludes[var_src].extend(attr_excludes['_global'])
        if var_src not in attr_renames.keys():
            attr_renames[var_src] = {}
        if var_src not in attr_defaults.keys():
            attr_defaults[var_src] = {}
        if var_src not in attr_appends.keys():
            attr_appends[var_src] = {}
        nc_copy_attrs(ncvar1,ncvar2,attr_includes[var_src],
                      attr_excludes[var_src],attr_renames[var_src],
                      attr_defaults[var_src],attr_appends[var_src])

def nc_copy_variables_data(nc_source,nc_destination,includes=[],excludes=[],
                           renames=None,source_slices=None,
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
    vars_src = nc_source.variables.keys()
    vars_dest = nc_destination.variables.keys()
    if includes:
        for include in includes:
            if include not in vars_src:
                raise NotImplementedError("Variable not found in source file.")
        vars_src = includes
    for var_src in vars_src:
        if var_src in excludes:
            continue
        ncvar1 = nc_source.variables[var_src]
        if var_src not in renames.keys():
            renames[var_src] = var_src
        if renames[var_src] not in vars_dest:
            raise NotImplementedError("Variable not found in destination file.")
        ncvar2 = nc_destination.variables[renames[var_src]]
        if var_src in source_slices:
            raise NotImplementedError("Slice copies.")
        if renames[var_src] in destination_slices:
            raise NotImplementedError("Slice copies.")
        ncvar2[...] = ncvar1[...]

def create_dummy_netcdf(nc_file,nc_format='NETCDF4_CLASSIC',use_time=True,
                        use_level=False,use_lat=True,use_lon=True,
                        use_station=False,use_yc=False,use_xc=False,
                        time_size=None,time_num_values=1,level_size=1,
                        lat_size=1,lon_size=1,station_size=1,yc_size=1,
                        xc_size=1,time_dtype='i2',
                        time_units='days since 2001-01-01 00:00:00',
                        time_calendar='gregorian',level_dtype='f4',
                        level_units='Pa',level_positive='up',
                        var_name='dummy',var_dtype='f4',data_scale_factor=1.0,
                        data_add_offset=0.0,fill_mode='random',
                        time_values=None,verbose=False):
    """
    Create a dummy NetCDF file on disk.

    Parameters
    ----------
    nc_file : str
        Name (and path) of the file to write.
    nc_format : str
    use_time : bool
    use_level : bool
    use_lat : bool
    use_lon : bool
    use_station : bool
    use_yc : bool
    use_xc : bool
    time_size : int or None
    time_num_values : int
    level_size : int
    lat_size : int
    lon_size : int
    station_size : int
    yc_size : int
    xc_size : int
    time_dtype : str
    time_units : str
    time_calendar : str
    level_dtype : str
    level_units : str
    level_positive : str
    var_name : str
    var_dtype : str
    data_scale_factor : float
    data_add_offset : float
    fill_mode : str
    time_values : numpy.ndarray
    verbose : bool

    Notes
    -----
    Features that would make this more flexible in the future:
    1. insert_annual_cycle=True: fake an annual cycle in the data.
    2. chunksizes
    3. stations,yc,xc
    4. missing values
    5. consider rlat and rlon as dimensions
    6. default level values, allow user defined level_values
    7. allow user defined grids
    8. create files larger than machine memory

    """

    # Template for CF-1.6 convention in python, using netCDF4.
    # http://cfconventions.org/
    
    # Aliases for default fill values
    defi2 = netCDF4.default_fillvals['i2']
    defi4 = netCDF4.default_fillvals['i4']
    deff4 = netCDF4.default_fillvals['f4']
    
    # Create netCDF file
    # Valid formats are 'NETCDF4', 'NETCDF4_CLASSIC', 'NETCDF3_CLASSIC' and
    # 'NETCDF3_64BIT'
    now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    nc1 = netCDF4.Dataset(nc_file,'w',format=nc_format)
    
    # 2.6.1 Identification of Conventions
    nc1.Conventions = 'CF-1.6'
    
    # 2.6.2. Description of file contents
    nc1.title = 'Dummy NetCDF file'
    nc1.history = "%s: File creation." % (now,)
    
    # Create netCDF dimensions
    if use_time:
        nc1.createDimension('time',time_size)
    if use_level:
        nc1.createDimension('level',level_size)
    if use_lat:
        nc1.createDimension('lat',lat_size)
    if use_lon:
        nc1.createDimension('lon',lon_size)
    if use_station:
        nc1.createDimension('station',station_size)
    if use_yc:
        nc1.createDimension('yc',yc_size)
    if use_xc:
        nc1.createDimension('xc',xc_size)
    
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

    if use_time:
        # 4.4. Time Coordinate
        time = nc1.createVariable('time',time_dtype,('time',),zlib=True)
        time.axis = 'T'
        time.units = time_units
        time.long_name = 'time'
        time.standard_name = 'time'
        # 4.4.1. Calendar
        time.calendar = time_calendar
        if time_values is None:
            time[:] = range(time_num_values)
        else:
            time[:] = time_values[:]

    if use_level:
        # 4.3. Vertical (Height or Depth) Coordinate
        level = nc1.createVariable('level',level_dtype,('level',),zlib=True)
        level.axis = 'Z'
        level.units = level_units
        level.positive = level_positive
        #level.long_name = 'air_pressure'
        #level.standard_name = 'air_pressure'
        raise NotImplementedError() # need to fill level[:]

    if use_lat:
        # 4.1. Latitude Coordinate
        lat = nc1.createVariable('lat','f4',('lat',),zlib=True)
        lat.axis = 'Y'
        lat.units = 'degrees_north'
        lat.long_name = 'latitude'
        lat.standard_name = 'latitude'
        dlat = 180.0/(lat_size+1)
        lat[:] = np.arange(-90.0+dlat,90.0-dlat/2.0,dlat)

    if use_lon:
        # 4.2. Longitude Coordinate
        lon = nc1.createVariable('lon','f4',('lon',),zlib=True)
        lon.axis = 'X'
        lon.units = 'degrees_east'
        lon.long_name = 'longitude'
        lon.standard_name = 'longitude'
        dlon = 360.0/lon_size
        lon[:] = np.arange(0.0,360.0-dlon/2.0,dlon)

    if use_station:
        raise NotImplementedError()

    if use_yc:
        raise NotImplementedError()

    if use_xc:
        raise NotImplementedError()
    
    # 2.3. Naming Conventions
    # 2.4 Dimensions
    #     If any or all of the dimensions of a variable have the
    #     interpretations of "date or time" (T), "height or depth" (Z),
    #     "latitude" (Y), or "longitude" (X) then we recommend, but do not
    #     require, those dimensions to appear in the relative order T, then Z,
    #     then Y, then X
    # Chunksizes should be set to the expected input/output pattern and be of
    # the order of 1000000 (that is the product of the chunksize in each
    # dimention). e.g. for daily data, this might be 30 (a month) or
    # 365 (a year) in time, and then determine the spatial chunks to obtain
    # ~1000000.
    var_dims = []
    if use_time:
        var_dims.append('time')
    if use_level:
        var_dims.append('level')
    if use_lat:
        var_dims.append('lat')
    if use_lon:
        var_dims.append('lon')
    if use_station:
        var_dims.append('station')
    if use_yc:
        var_dims.append('yc')
    if use_xc:
        var_dims.append('xc')
    var1 = nc1.createVariable(var_name,var_dtype,tuple(var_dims),zlib=True,
                              chunksizes=None,
                              fill_value=netCDF4.default_fillvals[var_dtype])
    # 3.1. Units
    var1.units = '1'
    # 3.2. Long Name
    var1.long_name = 'dummy_variable'
    # 3.3. Standard Name
    var1.standard_name = 'dummy_variable'

    if fill_mode == 'random':
        data1 = np.random.rand(*var1.shape)*data_scale_factor+data_add_offset
        num_values = data1.size
        if hasattr(data1,'count'):
            masked_values = num_values-data1.count()
        else:
            masked_values = 0
        data_size_mb = data1.nbytes/1000000.0
        var1[...] = data1[...]
    elif fill_mode == 'gradient':
        data1 = np.arange(0,1.0+0.5/(var1.size-1),1.0/(var1.size-1))
        data1 = data1*data_scale_factor+data_add_offset
        if hasattr(data1,'count'):
            masked_values = num_values-data1.count()
        else:
            masked_values = 0
        data_size_mb = data1.nbytes/1000000.0
        var1[...] = ma.reshape(data1,var1.shape)
    
    nc1.close()

    if verbose:
        str1 = "%s values, %s masked, for %s Mb of uncompressed data."
        print str1 % (str(num_values),str(masked_values),str(data_size_mb))

def period2indices(initial_date,final_date,nc_file,calendar=None):
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

    initial_time = time.strptime(initial_date,'%Y-%m-%dT%H:%M:%S')
    final_time = time.strptime(final_date,'%Y-%m-%dT%H:%M:%S')

    nc = netCDF4.Dataset(nc_file,'r')
    if not nc.variables.has_key('time'):
        raise NetCDFError("No time variable in the NetCDF file.")
    nctime = nc.variables['time']
    if calendar is None:
        if hasattr(nctime,'calendar'):
            calendar = nctime.calendar
        else:
            calendar = 'gregorian'
    if calendar in ['gregorian','standard']:
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
    index_ini = netCDF4.date2index(initial_nctime,nctime,calendar=calendar,
                                   select='after')
    index_fin = netCDF4.date2index(final_nctime,nctime,calendar=calendar,
                                   select='before')
    return {'initial_index':index_ini,'final_index':index_fin}
