import netCDF4


def ncplotly_from_slice(nc_file,var_name,ti=None,tf=None,levi=None,levf=None,
                        s1i=None,s1f=None,s2i=None,s2f=None):
    """Create plotly json structure from a NetCDF file slice.

    Parameters
    ----------
    nc_file : string
    var_name : string
    ti : int
    tf : int
    levi : int
    levf : int
    s1i : int
    s1f : int
    s2i : int
    s2f : int

    Returns
    -------
    out : dict

    """

    # This assumes that there are no non-spatiotemporal dimensions.
    nc = netCDF4.Dataset(nc_file,'r')
    ncvar = nc.variables[var_name]
    slices = []
    s1 = True
    s2 = True
    for dimension in ncvar.dimensions:
        if dimension == 'time':
            slices.append(slice(ti,tf))
        elif dimension == 'level':
            slices.append(slice(levi,levf))
        else:
            if s1:
                slices.append(slice(s1i,s1f))
                s1 = False
            elif s2:
                slices.append(slice(s2i,s2f))
                s2 = False
            else:
                raise NotImplementedError()
    nctime = nc.variables['time']
    # should allow default calendar
    datetimes = netCDF4.num2date(nctime[ti:tf],nctime.units,nctime.calendar)
    datestrings = []
    for one_datetime in datetimes:
        datestrings.append(one_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    # here we assume that it is safe to fallen the resulting slice
    # i.e. that the user made sure the slice result is 1 dimensional in time
    d = {'data':[{'x':datestrings,
                  'y':list(map(float,list(ncvar[slices].flatten()))),
                  'type':'scatter'}],
         'layout':{'title':nc_file,
                   'yaxis':{'title':var_name+' ('+ncvar.units+')'}}}
    nc.close()
    return d
