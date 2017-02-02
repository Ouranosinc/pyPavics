import datetime
import numbers

import numpy as np
import netCDF4


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
        # should be a better error...
        raise NotImplementedError("Unknown calendar: {0}".format(calendar))


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
        # should be a better error...
        raise NotImplementedError("NetCDF file has no time variable")


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
    elif isinstance(nc_resource, netCDF4._netCDF4.Dataset):
        return _calendar_from_ncdataset(nc_resource)
    elif isinstance(nc_resource, basestring):
        nc = netCDF4.Dataset(nc_resource, 'r')
        return _calendar_from_ncdataset(nc)
    else:
        msg = "Unknown NetCDF resource: {0}"
        raise NotImplementedError(msg.format(str(nc_resource)))


def multiple_files_time_indice(nc_files, t):
    if t < 0:
        raise NotImplementedError("Starting from the end.")
    for (i, nc_file) in enumerate(nc_files):
        ncdataset = netCDF4.Dataset(nc_file, 'r')
        if 'time' not in ncdataset.dimensions:
            raise NotImplementedError()  # should be a better error...
        nt = ncdataset.dimensions['time'].size
        if t < nt:
            return (i, t)
        t -= nt
    raise NotImplementedError("overflow.")  # should be a better error...


def _nearest_time_from_netcdf_time_units(nc_files, t, threshold=None):
    if isinstance(nc_files, basestring):
        nc_files = [nc_files]
    previous_end_time = None
    previous_nt = None
    initial_calendar = None
    initial_time_units = None
    for (i, nc_file) in enumerate(nc_files):
        ncdataset = netCDF4.Dataset(nc_file, 'r')
        if 'time' not in ncdataset.variables:
            raise NotImplementedError()  # should be a better error...
        nctime = ncdataset.variables['time']
        if initial_calendar is None:
            initial_calendar = _calendar_from_ncdataset(ncdataset)
            initial_time_units = nctime.units
        else:
            current_calendar = _calendar_from_ncdataset(ncdataset)
            # Here we should use a calendar compare that takes into account
            # aliases.
            c1 = (current_calendar != initial_calendar)
            c2 = (nctime.units != initial_time_units)
            if c1 or c2:
                datetimes = netCDF4.num2date(nctime[:], nctime.units,
                                             current_calendar)
                nctime = netCDF4.date2num(datetimes, initial_time_units,
                                          initial_calendar)
        start_time = nctime[0]
        end_time = nctime[-1]
        if (t >= start_time) and (t <= end_time):
            tn = int((np.abs(nctime[:]-t)).argmin())
            if threshold and (abs(nctime[tn]-t) > threshold):
                # should be a better error...
                raise NotImplementedError("No value below threshold.")
            return (i, tn)
        elif t < start_time:
            if previous_end_time is not None:
                pdiff = np.abs(previous_end_time-t)
                ndiff = np.abs(start_time-t)
                if pdiff <= ndiff:
                    if threshold and (pdiff > threshold):
                        # should be a better error...
                        raise NotImplementedError("No value below threshold.")
                    return (i-1, previous_nt-1)
                else:
                    if threshold and (ndiff > threshold):
                        # should be a better error...
                        raise NotImplementedError("No value below threshold.")
                    return(i, 0)
            elif threshold and ((start_time-t) > threshold):
                # should be a better error...
                raise NotImplementedError("No value below threshold.")
                return (0, 0)
        previous_end_time = end_time
        previous_nt = ncdataset.dimensions['time'].size
    if threshold and (t-end_time > threshold):
        # should be a better error...
        raise NotImplementedError("No value below threshold.")
    return (i, previous_nt-1)


def nearest_time(nc_files, t, threshold=None):
    if isinstance(nc_files, basestring):
        nc_files = [nc_files]

    if isinstance(t, (list, set, tuple)):
        if len(t) > 3:
            t = netCDF4.netcdftime.netcdftime(t[0], t[1], t[2],
                                              t[3], t[4], t[5])
        else:
            t = netCDF4.netcdftime.netcdftime(t[0], t[1], t[2], 12, 0, 0)
    elif isinstance(t, basestring):
        # Can't use time.strptime because of alternate NetCDF calendars
        decode_t = t.split('T')
        decode_date = decode_t[0].split('-')
        yyyy = int(decode_date[0])
        mm = int(decode_date[1])
        dd = int(decode_date[2])
        if len(decode_t) > 1:
            decode_time = decode_t[1].split(':')
            hh = int(decode_time[0])
            mi = int(decode_time[1])
            ss = int(decode_time[2])
        else:
            hh = 12
            mi = 0
            ss = 0
        t = netCDF4.netcdftime.datetime(yyyy, mm, dd, hh, mi, ss)

    if isinstance(t, numbers.Number):
        return _nearest_time_from_netcdf_time_units(nc_files, t, threshold)
    elif isinstance(t, (datetime.datetime,
                        netCDF4.netcdftime._datetime.datetime)):
        nc = netCDF4.Dataset(nc_files[0], 'r')
        nctime = nc.variables['time']
        t = netCDF4.date2num(t, nctime.units, _calendar_from_ncdataset(nc))
        return _nearest_time_from_netcdf_time_units(nc_files, t, threshold)
    else:
        raise NotImplementedError()
