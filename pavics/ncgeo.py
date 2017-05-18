from past.builtins import basestring
import datetime
import netCDF4
from shapely.geometry import Polygon

from . import geogrid
from . import netcdf as pavnc


def _nearest_lon_lat(nclon, nclat, lon, lat, maximum_distance=None):
    nclon_data = nclon[...]
    nclat_data = nclat[...]
    grid_type = geogrid.detect_grid(nclon_data, nclat_data,
                                    lon_dimensions=nclon.dimensions,
                                    lat_dimensions=nclat.dimensions)
    indices = geogrid.find_nearest(nclon[...], nclat[...], lon, lat,
                                   maximum_distance, grid_type)
    d = {}
    if grid_type in geogrid.list_of_points:
        d[nclon.dimensions[0]] = indices
    elif grid_type in geogrid.rectilinear_grids:
        d[nclon.dimensions[0]] = indices[0]
        d[nclat.dimensions[0]] = indices[1]
    elif grid_type in geogrid.irregular_grids:
        d[nclon.dimensions[0]] = indices[0]
        d[nclon.dimensions[1]] = indices[1]
    return d


def nearest_lon_lat(nc_resource, lon, lat, maximum_distance=None):
    if isinstance(nc_resource, basestring):
        ncdataset = netCDF4.Dataset(nc_resource, 'r')
    elif isinstance(nc_resource, netCDF4._netCDF4.Dataset):
        ncdataset = nc_resource
    elif isinstance(nc_resource, (list, tuple)):
        ncdataset = netCDF4.Dataset(nc_resource[0], 'r')
    else:
        raise NotImplementedError()

    nclon = ncdataset.variables['lon']
    nclat = ncdataset.variables['lat']
    indices = _nearest_lon_lat(nclon, nclat, lon, lat, maximum_distance)

    if not isinstance(nc_resource, netCDF4._netCDF4.Dataset):
        ncdataset.close()
    return indices


def spatial_dimensions(nc_dataset):
    # nc_dataset is an opened NetCDF4 object
    nclon = nc_dataset.variables['lon']
    nclat = nc_dataset.variables['lat']
    grid_name = geogrid.detect_grid(nclon[...], nclat[...],
                                    lon_dimensions=nclon.dimensions,
                                    lat_dimensions=nclat.dimensions)
    spatial_dims = list(set(nclon.dimensions + nclat.dimensions))
    if len(spatial_dims) == 1:
        if grid_name == 'list_of_2d_points':
            slice_order = {spatial_dims[0]: -1}
        else:
            slice_order = {spatial_dims[0]: 0}
    spatial_vars = []
    for var_name in nc_dataset.variables:
        ncvar = nc_dataset.variables[var_name]
        current_spatial_dims = []
        for spatial_dim in spatial_dims:
            if spatial_dim in ncvar.dimensions:
                current_spatial_dims.append(spatial_dim)
        if current_spatial_dims:
            spatial_vars.append(var_name)
            if len(current_spatial_dims) == 2:
                # hmmm, both conditions yield the same, really?
                if grid_name == 'rectilinear_2d_centroids':
                    slice_order = {current_spatial_dims[0]: 1,
                                   current_spatial_dims[1]: 0}
                else:
                    slice_order = {current_spatial_dims[0]: 1,
                                   current_spatial_dims[1]: 0}
    return {'spatial_dims': spatial_dims, 'spatial_vars': spatial_vars,
            'slice_order': slice_order}


def reshape_subdomain(nclon, nclat, geometry, slices=None):
    if slices is None:
        slices = geogrid.subdomain_grid_slices_or_points_indices(
            nclon[...], nclat[...], geometry, lon_dimensions=nclon.dimensions,
            lat_dimensions=nclat.dimensions)
    print slices
    reshapes = {}
    for i, lon_dimension in enumerate(nclon.dimensions):
        if isinstance(slices[i], slice):
            reshapes[lon_dimension] = slices[i].stop-slices[i].start
        else:
            reshapes[lon_dimension] = len(slices)
    i += 1
    for j, lat_dimension in enumerate(nclat.dimensions):
        if lat_dimension not in reshapes:
            reshapes[lat_dimension] = slices[j+i].stop-slices[j+i].start
    return reshapes


def subset_polygon(nc_resource, out_file, geometry, history_msg=None):
    # currently we will assume x = lon, y = lat
    nc_format = 'NETCDF4_CLASSIC'
    # maybe need to download?
    nc_reference = netCDF4.Dataset(nc_resource, 'r')

    now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    nc_output = netCDF4.Dataset(out_file, 'w', format=nc_format)

    if history_msg is None:
        history_msg = "{0}: subset_polygon.".format(now)
    pavnc.nc_copy_attrs(nc_reference, nc_output,
                        includes=[],
                        excludes=[],
                        renames={},
                        defaults={},
                        appends={'history': history_msg})

    # I need to reshape the spatial dimensions
    ncreflon = nc_reference.variables['lon']
    ncreflat = nc_reference.variables['lat']
    slices = geogrid.subdomain_grid_slices_or_points_indices(
        ncreflon[...], ncreflat[...], geometry,
        lon_dimensions=ncreflon.dimensions, lat_dimensions=ncreflat.dimensions)
    reshapes = reshape_subdomain(ncreflon, ncreflat, geometry, slices)
    pavnc.nc_copy_dimensions(nc_reference, nc_output,
                             includes=[],
                             excludes=[],
                             renames={},
                             defaults={},
                             reshapes=reshapes)

    # what to do with the chunking & compression?
    pavnc.nc_copy_variables_structure(nc_reference, nc_output,
                                      includes=[],
                                      excludes=[],
                                      renames={},
                                      new_dtype={},
                                      new_dimensions={},
                                      create_args={'_global': {'zlib':True}})

    pavnc.nc_copy_variables_attributes(nc_reference, nc_output,
                                       includes=[],
                                       excludes=[],
                                       renames={},
                                       attr_excludes={},
                                       attr_defaults={})

    spatial_info = spatial_dimensions(nc_reference)
    pavnc.nc_copy_variables_data(nc_reference, nc_output,
                                 includes=[],
                                 excludes=spatial_info['spatial_vars'],
                                 renames={})

    for var_name in spatial_info['spatial_vars']:
        ncrefvar = nc_reference[var_name]
        ncvar = nc_output.variables[var_name]
        # what if its too large for memory?
        ref_slices = []
        for dim in ncrefvar.dimensions:
            if dim in spatial_info['spatial_dims']:
                if spatial_info['slice_order'][dim] == -1:
                    ref_slices.append(slices)
                else:
                    ref_slices.append(slices[spatial_info['slice_order'][dim]])
            else:
                ref_slices.append(slice(None))
        ncvar[...] = ncrefvar[ref_slices]

    nc_reference.close()
    nc_output.close()


def subset_bbox(nc_resource, out_file, bounding_box):
    # bbox is (minx, miny, maxx, maxy)
    # currently we will assume x = lon, y = lat
    geometry = Polygon([(bounding_box[0], bounding_box[1]),
                        (bounding_box[0], bounding_box[3]),
                        (bounding_box[2], bounding_box[3]),
                        (bounding_box[2], bounding_box[1]),
                        (bounding_box[0], bounding_box[1])])
    now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    history_msg = "{0}: subset_bbox with {1}.".format(now, str(bounding_box))
    subset_polygon(nc_resource, out_file, geometry, history_msg)


def spatial_weighted_average(nc_resource, out_file, geometry):
    # currently we will assume x = lon, y = lat
    nc_format = 'NETCDF4_CLASSIC'
    # maybe need to download?
    nc_reference = netCDF4.Dataset(nc_resource, 'r')

    now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    nc_output = netCDF4.Dataset(out_file, 'w', format=nc_format)

    if history_msg is None:
        history_msg = "{0}: spatial_weighted_average.".format(now)
    pavnc.nc_copy_attrs(nc_reference, nc_output,
                        includes=[],
                        excludes=[],
                        renames={},
                        defaults={},
                        appends={'history': history_msg})

    # I need to reshape the spatial dimensions
    ncreflon = nc_reference.variables['lon']
    ncreflat = nc_reference.variables['lat']
    slices = geogrid.subdomain_grid_slices_or_points_indices(
        ncreflon[...], ncreflat[...], geometry,
        lon_dimensions=ncreflon.dimensions, lat_dimensions=ncreflat.dimensions)
    spatial_info = spatial_dimensions(nc_reference)
    reshapes = {}
    for dim in spatial_info['spatial_dims']:
        reshapes[dim] = 1
    pavnc.nc_copy_dimensions(nc_reference, nc_output,
                             includes=[],
                             excludes=[],
                             renames={},
                             defaults={},
                             reshapes=reshapes)

    # what to do with the chunking & compression?
    pavnc.nc_copy_variables_structure(nc_reference, nc_output,
                                      includes=[],
                                      excludes=[],
                                      renames={},
                                      new_dtype={},
                                      new_dimensions={},
                                      create_args={'_global': {'zlib':True}})

    pavnc.nc_copy_variables_attributes(nc_reference, nc_output,
                                       includes=[],
                                       excludes=[],
                                       renames={},
                                       attr_excludes={},
                                       attr_defaults={})

    pavnc.nc_copy_variables_data(nc_reference, nc_output,
                                 includes=[],
                                 excludes=spatial_info['spatial_vars'],
                                 renames={})

    nclon = nc_output.variables['lon']
    nclat = nc_output.variables['lat']
    nclon[...] = geometry.centroid.x
    nclat[...] = geometry.centroid.y
    for var_name in spatial_info['spatial_vars']:
        if var_name in ['lon', 'lat']:
            continue
        ncrefvar = nc_reference[var_name]
        current_spatial_dims = []
        for dim in ncrefvar.dimensions:
            if dim in spatial_info['spatial_dims']:
                current_spatial_dims.append(dim)
        if len(current_spatial_dims) == len(spatial_info['spatial_dims']):
            ncvar = nc_output.variables[var_name]
            # what if its too large for memory?
            # ncvar[...] = ncrefvar[ref_slices] some operation for weighted
            # averaging

    nc_reference.close()
    nc_output.close()
