import numpy as np
import netCDF4

import netcdf as pavnc
import nctime
import ncgeo


def get_time_file_and_indice(nc_resource, ncvardims, ordered_indices,
                             named_indices, nearest_to, thresholds=None):
    tfi = None
    ti = ncvardims.index('time')
    if ordered_indices and (len(ordered_indices) > ti) and \
       (ordered_indices[ti] is not None):
        tfi = nctime.multiple_files_time_indice(nc_resource,
                                                ordered_indices[ti])
    elif named_indices and ('time' in named_indices):
        if tfi:
            raise NotImplementedError("Conflict of interest.")
        tfi = nctime.multiple_files_time_indice(nc_resource,
                                                named_indices['time'])
    elif nearest_to and ('time' in nearest_to):
        if tfi:
            raise NotImplementedError("Conflict of interest.")
        if thresholds and ('time' in thresholds):
            tfi = nctime.nearest_time(nc_resource, nearest_to['time'],
                                      threshold=thresholds['time'])
        else:
            tfi = nctime.nearest_time(nc_resource, nearest_to['time'])
    return tfi


def _update_with_conflicts(d1, d2):
    for (key, val) in d2.items():
        if key in d1:
            if val != d1[key]:
                raise NotImplementedError()  # This should be a better error.
        else:
            d1[key] = val


def _named_indices_from_ncvar_and_ordered_indices(ncvar, ordered_indices):
    d = {}
    if ordered_indices:
        for (i, dim) in enumerate(ncvar.dimensions):
            if (len(ordered_indices) > i) and (ordered_indices[i] is not None):
                d[dim] = ordered_indices[i]
    return d


def _named_indices_from_ncvars_and_ordered_indices(ncvars, ordered_indices):
    d1 = {}
    for ncvar in ncvars:
        d2 = _named_indices_from_ncvar_and_ordered_indices(ncvar,
                                                           ordered_indices)
        _update_with_conflicts(d1, d2)
    return d1


def get_point(nc_resource, var_names, ordered_indices=None, named_indices=None,
              nearest_to=None, thresholds=None):
    # No overlap allowed on the specified indices, but they can mix.
    # ordered_indices can have None elements and can under/overflow the
    # variable dimension (will be ignored).
    # named_indices and nearest_to are dictionary with
    # keys that are variable dimensions. keys that do not match a variable
    # dimension are ignored.

    if isinstance(var_names, basestring):
        var_names = [var_names]

    if not isinstance(nc_resource, netCDF4._netCDF4.Variable):
        vardims = pavnc.get_dimensions(nc_resource, var_names)

    if named_indices is None:
        named_indices = {}
    if thresholds is None:
        thresholds = {}

    # First determine what kind of resource we are dealing with
    tfi = None
    if isinstance(nc_resource, basestring):
        ncdataset = netCDF4.Dataset(nc_resource, 'r')
        ncvars = [ncdataset.variables[x] for x in var_names]
    elif isinstance(nc_resource, netCDF4._netCDF4.Dataset):
        ncdataset = nc_resource
        ncvars = [ncdataset.variables[x] for x in var_names]
    elif isinstance(nc_resource, netCDF4._netCDF4.Variable):
        ncdataset = None
        vardims = {nc_resource.name: nc_resource.dimensions.keys()}
        ncvars = [nc_resource]
    elif isinstance(nc_resource, (list, tuple)):
        # since there are multiple files, we assume there is a time
        # dimension, perhaps there should be a check against that.
        ncdataset = netCDF4.Dataset(nc_resource[0], 'r')
        for var_name in var_names:
            if 'time' in vardims[var_name]:
                tfi = get_time_file_and_indice(nc_resource, vardims[var_name],
                                               ordered_indices, named_indices,
                                               nearest_to, thresholds)
                _update_with_conflicts(named_indices, {'time': tfi[1]})
                break
        ncdataset.close()
        ncdataset = netCDF4.Dataset(nc_resource[tfi[0]], 'r')
        ncvars = [ncdataset.variables[x] for x in var_names]
    else:
        raise NotImplementedError()

    # Now that was have ncdataset and ncvars...

    # convert ordered_indices to named_indices
    if ordered_indices:
        d = _named_indices_from_ncvars_and_ordered_indices(ncvars,
                                                           ordered_indices)
        _update_with_conflicts(named_indices, d)

    # convert nearest_to to named_indices
    if nearest_to is None:
        nearest_to = {}
    elif isinstance(nc_resource, netCDF4._netCDF4.Variable):
        # case where the input was a Variable instance
        for dim in ncvars[0].dimensions:
            if dim in nearest_to:
                # here we have criteria on values of some dimensions,
                # which we do not have if a Variable instance was provided,
                # need to call with at least the Dataset instance.
                raise NotImplementedError()
    for dim in nearest_to:
        if dim == 'time':
            if tfi is not None:
                continue
            # Make sure at least one requested variable needs time indice
            for ncvar in ncvars:
                if 'time' in ncvar.dimensions:
                    break
            else:
                continue

            tfi = get_time_file_and_indice(nc_resource, ncvar.dimensions,
                                           {}, {}, nearest_to, thresholds)
            _update_with_conflicts(named_indices, {'time': tfi[1]})
        elif dim in ['lon', 'lat']:
            d = ncgeo.nearest_lon_lat(nc_resource, nearest_to['lon'],
                                      nearest_to['lat'],
                                      thresholds.get('_distance'))
            _update_with_conflicts(named_indices, d)
        else:
            # make sure at least one request variable needs that dimension
            for ncvar in ncvars:
                if dim in ncvar.dimensions:
                    break
            else:
                continue
            dim_data = ncdataset.variables[dim][:]
            i = np.abs(np.diff(dim_data-nearest_to[dim])).argmin()
            if dim in thresholds:
                if np.abs(dim_data[i]-nearest_to[dim]) > thresholds[dim]:
                    raise NotImplementedError("too far")
            _update_with_conflicts(named_indices, {dim: i})

    d = {}
    for ncvar in ncvars:
        f = pavnc._get_point_from_ncvar_and_named_indices
        d[ncvar.name] = f(ncvar, named_indices)
        d[ncvar.name]['_indices'] = named_indices
        d['_dimensions'] = {}
        for dim in ncvar.dimensions:
            if dim in d['_dimensions']:
                continue
            if ncdataset is None:
                continue
            ncdimvar = ncdataset.variables[dim]
            dd = pavnc._get_var_info(ncdimvar)
            dd['value'] = ncdimvar[named_indices[dim]]
            d['_dimensions'][dim] = dd
        if ncdataset is not None:
            if ('lon' in ncdataset.variables) and \
               ('lon' not in d['_dimensions']):
                nclon = ncdataset.variables['lon']
                d['_dimensions']['lon'] = f(nclon, named_indices)
            if ('lat' in ncdataset.variables) and \
               ('lat' not in d['_dimensions']):
                nclat = ncdataset.variables['lat']
                d['_dimensions']['lat'] = f(nclat, named_indices)
    if ncdataset is not None:
        ncdataset.close()
    return d
