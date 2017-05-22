import logging

import numpy as np
from shapely.geometry import Point, Polygon, LineString
from shapely import iterops
from shapely.ops import cascaded_union


class GeogridError(Exception):
    pass


def distance_lon_lat(longitudes, latitudes, longitude, latitude):
    """Distance in meters between lon/lat on Earth (sphere, 6371 km radius).

    Paramters
    ---------
    longitudes : numpy array
    latitudes : numpy array
    longitude : float
    latitude : float

    Returns
    -------
    out : numpy array
        distances of all longitudes/latitudes to the other longitude/latitude.

    """

    longitudes_rad = longitudes*(np.pi/180.)
    latitudes_rad = latitudes*(np.pi/180.)
    longitude_rad = longitude*(np.pi/180.)
    latitude_rad = latitude*(np.pi/180.)
    warp1 = np.sin(latitudes_rad)*np.sin(latitude_rad)
    warp2 = np.cos(latitudes_rad)*np.cos(latitude_rad)
    warp3 = np.cos(longitude_rad-longitudes_rad)
    warp4 = warp1+warp2*warp3
    if warp4.size == 1:
        if warp4 > 1.0:
            warp4 = 1.0
        elif warp4 < -1.0:
            warp4 = -1.0
    else:
        overflow_indices = np.where(warp4 > 1.0)
        warp4[overflow_indices] = 1.0
        overflow_indices = np.where(warp4 < -1.0)
        warp4[overflow_indices] = -1.0
    distances = 6371000.*np.arccos(warp4)
    return distances


######################
# Dealing with grids #
######################

list_of_points = ['list_of_2d_points']
rectilinear_grids = ['rectilinear_2d_centroids', 'rectilinear_2d_bounds',
                     'rectilinear_2d_vertices']
irregular_grids = ['irregular_2d_centroids', 'irregular_2d_vertices']


def detect_grid(lon, lat, lon_dimensions=None, lat_dimensions=None):
    """Determine grid type from longitudes and latitudes.

    Parameters
    ----------
    lon - numpy array
    lat - numpy array
    lon_dimensions - tuple of str
    lat_dimensions - tuple of str

    Returns
    -------
    out - str

    """

    if (len(lon.shape) == 2) and (len(lat.shape) == 2):
        if (lon_dimensions == ('lon', 'bnds')) and \
           (lat_dimensions == ('lat', 'bnds')):
            return 'rectilinear_2d_bounds'
        if (lon.shape == lat.shape):
            if (lon_dimensions == ('yc', 'xc')) and \
               (lat_dimensions == ('yc', 'xc')):
                return 'irregular_2d_centroids'
            elif (lon_dimensions == ('rlat', 'rlon')) and \
                 (lat_dimensions == ('rlat', 'rlon')):
                return 'irregular_2d_centroids'
            else:
                logging.warning("Guessing irregular 2d centroids.")
                return 'irregular_2d_centroids'

    elif (len(lon.shape) == 1) and (len(lat.shape) == 1):
        if (lon_dimensions is not None):
            if (lon_dimensions == lat_dimensions) and (lon.size == lat.size):
                return 'list_of_2d_points'
            elif (lon_dimensions != lat_dimensions):
                dlon = np.diff(lon)
                c1 = np.all(dlon < 0) or np.all(dlon > 0)
                dlat = np.diff(lat)
                c2 = np.all(dlat < 0) or np.all(dlat > 0)
                if c1 and c2:
                    return 'rectilinear_2d_centroids'
        else:
            dlon = np.diff(lon)
            c1 = np.all(dlon < 0) or np.all(dlon > 0)
            dlat = np.diff(lat)
            c2 = np.all(dlat < 0) or np.all(dlat > 0)
            if c1 and c2:
                if lon.size == lat.size:
                    logging.warning("Guessing rectilinear 2d centroids.")
                return 'rectilinear_2d_centroids'
            elif lon.size == lat.size:
                return 'list_of_2d_points'
    raise GeogridError("Unknown grid.")


def rectilinear_centroids_to_vertices(x, y, forced_bounds=False,
                                      limit_bounds=False,
                                      lower_bound_x=None, upper_bound_x=None,
                                      lower_bound_y=None, upper_bound_y=None):
    """Estimate rectlinear vertices from centroids.

    Parameters
    ----------
    x - 1d numpy array (N)
    y - 1d numpy array (M)
    forced_bounds - bool
        force the upper and lower bounds of the mesh.
    limit_bounds - bool
        limit the upper and lower bounds of the mesh within a range.
    lower_bound_x - float
    upper_bound_x - float
    lower_bound_y - float
    upper_bound_y - float

    Returns
    -------
    x,y - 1d numpy array (N+1) and (M+1)

    Notes
    -----
    For output x[1:-1],y[1:-1], the middle of 2 neighbor centroids is returned,
    the edges of the output is obtained from some basic homemade extrapolation,
    which may return garbage if the mesh if far from regular.
    Only one of forced_bounds or limit_bounds can be used and all 4 bound
    arguments must be set.

    """

    extended_x = np.zeros([x.shape[0]+2])
    extended_y = np.zeros([y.shape[0]+2])
    extended_x[1:-1] = x
    extended_y[1:-1] = y
    extended_x[0] = 2*extended_x[1]-extended_x[2]
    extended_x[-1] = 2*extended_x[-2]-extended_x[-3]
    extended_y[0] = 2*extended_y[1]-extended_y[2]
    extended_y[-1] = 2*extended_y[-2]-extended_y[-3]
    # Compute centroids of the extended field
    x = np.zeros([extended_x.shape[0]-1])
    y = np.zeros([extended_y.shape[0]-1])
    for i in range(x.shape[0]):
        x[i] = (extended_x[i]+extended_x[i+1])/2.0
    for j in range(y.shape[0]):
        y[j] = (extended_y[j]+extended_y[j+1])/2.0
    if forced_bounds:
        indices = np.where(x == x.min())[0][0]
        if indices == 0:
            x[0] = lower_bound_x
            x[-1] = upper_bound_x
        else:
            x[0] = upper_bound_x
            x[-1] = lower_bound_x
        indices = np.where(y == y.min())[0][0]
        if indices == 0:
            y[0] = lower_bound_y
            y[-1] = upper_bound_y
        else:
            y[0] = upper_bound_y
            y[-1] = lower_bound_y
    elif limit_bounds:
        indices = np.where(x == x.min())[0][0]
        if indices == 0:
            if x[0] < lower_bound_x:
                x[0] = lower_bound_x
            if x[-1] > upper_bound_x:
                x[-1] = upper_bound_x
        else:
            if x[-1] < lower_bound_x:
                x[-1] = lower_bound_x
            if x[0] > upper_bound_x:
                x[0] = upper_bound_x
        indices = np.where(y == y.min())[0][0]
        if indices == 0:
            if y[0] < lower_bound_y:
                y[0] = lower_bound_y
            if y[-1] > upper_bound_y:
                y[-1] = upper_bound_y
        else:
            if y[-1] < lower_bound_y:
                y[-1] = lower_bound_y
            if y[0] > upper_bound_y:
                y[0] = upper_bound_y
    return (x, y)


def centroids_to_quadrilaterals_mesh(x, y, forced_bounds=False,
                                     limit_bounds=False, lower_bound_x=None,
                                     upper_bound_x=None, lower_bound_y=None,
                                     upper_bound_y=None):
    # there might be a bug with the lower/upper bounds if the grid is upside
    # down....
    """Estimate a quadrilaterals mesh from centroids.

    Parameters
    ----------
    x - 2d numpy array (NxM)
    y - 2d numpy array (NxM)
    forced_bounds - bool
        force the upper and lower bounds of the mesh.
    limit_bounds - bool
        limit the upper and lower bounds of the mesh within a range.
    lower_bound_x - float
    upper_bound_x - float
    lower_bound_y - float
    upper_bound_y - float

    Returns
    -------
    x,y - 2d numpy array (N+1xM+1)

    Notes
    -----
    For output x[1:-1],y[1:-1], the centroid of 4 neighbor points is returned,
    the edges of the output is obtained from some basic homemade extrapolation,
    which may return garbage if the mesh if far from regular.
    There is currently a possible bug with the lower/upper bounds if the
    grid is upside down...

    """

    # start by extending the centroids all around
    extended_x = np.zeros([x.shape[0]+2, x.shape[1]+2])
    extended_y = np.zeros([y.shape[0]+2, y.shape[1]+2])
    extended_x[1:-1,1:-1] = x
    extended_y[1:-1,1:-1] = y
    for i in range(1, x.shape[1]+1):
        # top row
        extended_x[0,i] = 2*extended_x[1,i]-extended_x[2,i]
        extended_y[0,i] = 2*extended_y[1,i]-extended_y[2,i]
        # bottom row
        extended_x[-1,i] = 2*extended_x[-2,i]-extended_x[-3,i]
        extended_y[-1,i] = 2*extended_y[-2,i]-extended_y[-3,i]
    for j in range(1, x.shape[0]+1):
        # left column
        extended_x[j,0] = 2*extended_x[j,1]-extended_x[j,2]
        extended_y[j,0] = 2*extended_y[j,1]-extended_y[j,2]
        # right column
        extended_x[j,-1] = 2*extended_x[j,-2]-extended_x[j,-3]
        extended_y[j,-1] = 2*extended_y[j,-2]-extended_y[j,-3]
    # top left corner
    extended_x[0,0] = extended_x[0,1]-extended_x[1,1]+extended_x[1,0]
    extended_y[0,0] = extended_y[1,0]-extended_y[1,1]+extended_y[0,1]
    # top right corner
    extended_x[0,-1] = extended_x[0,-2]-extended_x[1,-2]+extended_x[1,-1]
    extended_y[0,-1] = extended_y[1,-1]-extended_y[1,-2]+extended_y[0,-2]
    # bottom right corner
    extended_x[-1,-1] = extended_x[-1,-2]-extended_x[-2,-2]+extended_x[-2,-1]
    extended_y[-1,-1] = extended_y[-2,-1]-extended_y[-2,-2]+extended_y[-1,-2]
    # bottom left corner
    extended_x[-1,0] = extended_x[-1,1]-extended_x[-2,1]+extended_x[-2,0]
    extended_y[-1,0] = extended_y[-2,0]-extended_y[-2,1]+extended_y[-1,1]
    # Compute centroids of the extended field
    x = np.zeros([extended_x.shape[0]-1, extended_x.shape[1]-1])
    y = np.zeros([extended_y.shape[0]-1, extended_y.shape[1]-1])
    for i in range(x.shape[0]):
        for j in range(x.shape[1]):
            x[i,j] = (extended_x[i,j]+extended_x[i+1,j]+extended_x[i,j+1]+
                      extended_x[i+1,j+1])/4.0
            y[i,j] = (extended_y[i,j]+extended_y[i+1,j]+extended_y[i,j+1]+
                      extended_y[i+1,j+1])/4.0
    if forced_bounds:
        for i in range(x.shape[0]):
            x[i,0] = lower_bound_x
            x[i,-1] = upper_bound_x
        for j in range(x.shape[1]):
            y[0,j] = lower_bound_y
            y[-1,j] = upper_bound_y
    elif limit_bounds:
        for i in range(x.shape[0]):
            if x[i,0] < lower_bound_x:
                x[i,0] = lower_bound_x
            if x[i,-1] > upper_bound_x:
                x[i,-1] = upper_bound_x
        for j in range(x.shape[1]):
            if y[0,j] < lower_bound_y:
                y[0,j] = lower_bound_y
            if y[-1,j] > upper_bound_y:
                y[-1,j] = upper_bound_y
    return x, y
ctoqmesh = centroids_to_quadrilaterals_mesh


def quadrilaterals_mesh_to_centroids(x_vertices, y_vertices):
    """Find the centroids of a mesh from its vertices.

    Parameters
    ----------
    x_vertices - 2d numpy array (NxM)
    y_vertices - 2d numpy array (NxM)

    Returns
    -------
    x,y - 2d numpy array (N-1xM-1)

    """

    x = (x_vertices[0:-1,0:-1] + x_vertices[1:,0:-1] + x_vertices[1:,1:] +
         x_vertices[0:-1,1:])/4.0
    y = (y_vertices[0:-1,0:-1] + y_vertices[1:,0:-1] + y_vertices[1:,1:] +
         y_vertices[0:-1,1:])/4.0
    return x, y


def rectilinear_vertices_to_centroids(lon_vertices, lat_vertices):
    """Find the centroids of a rectilinear grid from its vertices.

    Parameters
    ----------
    lon_vertices - 1d numpy array (N)
    lat_vertices - 1d numpy array (M)

    Returns
    -------
    x,y - 1d numpy array (N-1) and (M-1)

    """

    lon_centroids = (lon_vertices[0:-1]+lon_vertices[1:])/2.0
    lat_centroids = (lat_vertices[0:-1]+lat_vertices[1:])/2.0
    return (lon_centroids, lat_centroids)


def rectilinear_2d_bounds_to_vertices(lon_bnds, lat_bnds):
    """Find the vertices of a rectilinear grid from its bounds (CF convention).

    Parameters
    ----------
    lon_bnds - 2d numpy array (Nx2)
    lat_bnds - 2d numpy array (Mx2)

    Returns
    -------
    x,y - 1d numpy array (N+1) and (M+1)

    """

    lon_vertices = list(lon_bnds[:,0]) + [lon_bnds[-1,1]]
    lat_vertices = list(lat_bnds[:,0]) + [lat_bnds[-1,1]]
    return (np.array(lon_vertices), np.array(lat_vertices))


def _find_nearest_from_list_of_points(lon_points, lat_points,
                                      lon_point, lat_point,
                                      maximum_distance=None):
    """Find nearest point from list of points given a longitude/latitude.

    Parameters
    ----------
    lon_points - numpy array
    lat_points - numpy array
    lon_point - float
    lat_point - float
    maximum_distance - float
        The maximum distance tolerated for finding a nearest point (in meter).

    Returns
    -------
    out - int
        The indice in the list of points corresponding to the nearest point.

    """

    d = distance_lon_lat(lon_points, lat_points, lon_point, lat_point)
    minimum_distance = d.min()
    if maximum_distance is not None:
        if minimum_distance > maximum_distance:
            raise GeogridError("No points within provided maximum distance.")
    indices = np.where(d == d.min())
    if len(indices[0]) > 1:
        logging.warning("More than one nearest point, returning first index.")
    return indices[0][0]


def _find_nearest_from_rectilinear_centroids(lon_centroids, lat_centroids,
                                             lon_point, lat_point,
                                             maximum_distance=None):
    """Find nearest point in a rectilinear grid defined by its centroids.

    Parameters
    ----------
    lon_centroids - numpy array
    lat_centroids - numpy array
    lon_point - float
    lat_point - float
    maximum_distance - float
        The maximum distance tolerated for finding a nearest point (in meter).

    Returns
    -------
    i,j - int
        The indices of the centroid in the rectilinear grid.

    """

    # First calculate the difference in longitudes and warp overflowing
    # values back to [0, 360]
    distance_lon = np.mod(np.abs(lon_centroids-lon_point), 360)
    # For distances over 180, there's a shorter distance going the other
    # way around the globe.
    warp_indices = np.where(distance_lon > 180)
    distance_lon[warp_indices] = np.abs(distance_lon[warp_indices]-360.0)
    indices = np.where(distance_lon.min() == distance_lon)
    if len(indices[0]) > 1:
        msg = "More than one nearest meridian, returning first index."
        logging.warning(msg)
    i = indices[0][0]
    distance_lat = np.abs(lat_centroids-lat_point)
    indices = np.where(distance_lat.min() == distance_lat)
    if len(indices[0]) > 1:
        msg = "More than one nearest parallel, returning first index."
        logging.warning(msg)
    j = indices[0][0]
    minimum_distance = distance_lon_lat(lon_centroids[i], lat_centroids[j],
                                        lon_point, lat_point)
    if maximum_distance is not None:
        if minimum_distance > maximum_distance:
            raise GeogridError("No points within provided maximum distance.")
    return (i, j)


def _find_nearest_from_rectilinear_vertices(lon_vertices, lat_vertices,
                                            lon_point, lat_point,
                                            maximum_distance=None):
    """Find nearest point in a rectilinear grid defined by its vertices.

    Parameters
    ----------
    lon_vertices - numpy array
    lat_vertices - numpy array
    lon_point - float
    lat_point - float
    maximum_distance - float
        The maximum distance tolerated for finding a nearest point (in meter).

    Returns
    -------
    i,j - int
        The indices of the centroid in the rectilinear grid.

    """

    f = rectilinear_vertices_to_centroids
    (lon_centroids, lat_centroids) = f(lon_vertices, lat_vertices)
    return _find_nearest_from_rectilinear_centroids(lon_centroids,
                                                    lat_centroids)


def _find_nearest_from_irregular_centroids(lon_centroids, lat_centroids,
                                           lon_point, lat_point,
                                           maximum_distance=None):
    """Find nearest point in an irregular grid defined by its centroids.

    Parameters
    ----------
    lon_centroids - numpy array
    lat_centroids - numpy array
    lon_point - float
    lat_point - float
    maximum_distance - float
        The maximum distance tolerated for finding a nearest point (in meter).

    Returns
    -------
    i,j - int
        The indices of the centroid in the irregular grid.

    """

    d = distance_lon_lat(lon_centroids, lat_centroids, lon_point, lat_point)
    minimum_distance = d.min()
    if maximum_distance is not None:
        if minimum_distance > maximum_distance:
            raise GeogridError("No points within provided maximum distance.")
    indices = np.where(d == d.min())
    if len(indices[0]) > 1:
        logging.warning("More than one nearest point, returning first index.")
    return (indices[0][0], indices[1][0])


def _find_nearest_from_irregular_vertices(lon_vertices, lat_vertices,
                                          lon_point, lat_point,
                                          maximum_distance=None):
    """Find nearest point in an irregular grid defined by its vertices.

    Parameters
    ----------
    lon_vertices - numpy array
    lat_vertices - numpy array
    lon_point - float
    lat_point - float
    maximum_distance - float
        The maximum distance tolerated for finding a nearest point (in meter).

    Returns
    -------
    i,j - int
        The indices of the centroid in the irregular grid.

    """

    f = quadrilaterals_mesh_to_centroids
    (lon_centroids, lat_centroids) = f(lon_vertices, lat_vertices)
    f = _find_nearest_from_irregular_centroids
    return f(lon_centroids, lat_centroids, lon_point, lat_point,
             maximum_distance=maximum_distance)


def find_nearest(longitudes, latitudes, longitude, latitude,
                 maximum_distance=None, grid_type=None,
                 lon_dimensions=None, lat_dimensions=None):
    """Find nearest point in a given grid.

    Parameters
    ----------
    longitudes - numpy array
    latitudes - numpy array
    longitude - float
    latitude - float
    maximum_distance - float
        The maximum distance tolerated for finding a nearest point (in meter).
    grid_type - str (optional)
        The grid type, if None it will be found using detect_grid.
    lon_dimensions - tuple of str (optional, for detect_grid)
    lat_dimensions - tuple of str (optional, for detect_grid)

    Returns
    -------
    out - either an integer or a tuple of integer, depending on the grid
        The indice(s) of the nearest point.

    """

    if grid_type is None:
        grid_type = detect_grid(longitudes, latitudes, lon_dimensions,
                                lat_dimensions)
    if grid_type == 'list_of_2d_points':
        f = _find_nearest_from_list_of_points
        return f(longitudes, latitudes, longitude, latitude, maximum_distance)
    elif grid_type == 'rectilinear_2d_centroids':
        f = _find_nearest_from_rectilinear_centroids
        return f(longitudes, latitudes, longitude, latitude, maximum_distance)
    elif grid_type == 'rectilinear_2d_bounds':
        f = rectilinear_2d_bounds_to_vertices
        (lon_vertices, lat_vertices) = f(longitudes, latitudes)
        f = _find_nearest_from_rectilinear_vertices
        return f(lon_vertices, lat_vertices, longitude, latitude,
                 maximum_distance)
    elif grid_type == 'rectilinear_2d_vertices':
        f = _find_nearest_from_rectilinear_vertices
        return f(longitudes, latitudes, longitude, latitude, maximum_distance)
    elif grid_type == 'irregular_2d_centroids':
        f = _find_nearest_from_irregular_centroids
        return f(longitudes, latitudes, longitude, latitude, maximum_distance)
    elif grid_type == 'irregular_2d_vertices':
        f = _find_nearest_from_irregular_vertices
        return f(longitudes, latitudes, longitude, latitude, maximum_distance)


def _polygon_grid_subset_from_rectilinear_vertices(lon_vertices, lat_vertices,
                                                   geometry):
    """Find the subset of a rectilinear grid defined by its vertices such as
       to cover a given geometry.

    Parameters
    ----------
    lon_vertices - numpy array
    lat_vertices - numpy array
    geometry - Shapely geometry

    Returns
    -------
    slice_i,slice_j - slice
        The slices (with respect to the centroids) of the subset.

    """

    if geometry.bounds[2]<lon_vertices.min():
        raise GeogridError("Geometry falls outside the grid.")
    elif geometry.bounds[0]>lon_vertices.max():
        raise GeogridError("Geometry falls outside the grid.")
    elif geometry.bounds[1]<lat_vertices.min():
        raise GeogridError("Geometry falls outside the grid.")
    elif geometry.bounds[3]>lat_vertices.max():
        raise GeogridError("Geometry falls outside the grid.")
    indices = np.where(np.bitwise_and(lon_vertices>=geometry.bounds[0],
                                      lon_vertices<=geometry.bounds[2]))
    if len(indices[0]) == 0:
        distances = np.abs(lon_vertices-geometry.bounds[0])
        i1 = np.where(distances==distances.min())[0][0]
        distances[i1] = distances.max()+1
        i2 = np.where(distances==distances.min())[0][0]
        if i2<i1:
            i1 = i2
        slice_lon = slice(i1,i1+1)
    else:
        slice_lon = slice(indices[0][0]-1,indices[0][-1]+1)
    indices = np.where(np.bitwise_and(lat_vertices>=geometry.bounds[1],
                                      lat_vertices<=geometry.bounds[3]))
    if len(indices[0]) == 0:
        distances = np.abs(lat_vertices-geometry.bounds[1])
        i1 = np.where(distances==distances.min())[0][0]
        distances[i1] = distances.max()+1
        i2 = np.where(distances==distances.min())[0][0]
        if i2<i1:
            i1 = i2
        slice_lat = slice(i1,i1+1)
    else:
        slice_lat = slice(indices[0][0]-1,indices[0][-1]+1)
    return (slice_lon,slice_lat)


def _polygon_grid_subset_from_rectilinear_centroids(lon_centroids,
                                                    lat_centroids,
                                                    geometry,
                                                    lon_vertices=None,
                                                    lat_vertices=None):
    """Find the subset of a rectilinear grid defined by its centroids such as
       to cover a given geometry.

    Parameters
    ----------
    lon_centroids - numpy array
    lat_centroids - numpy array
    geometry - Shapely geometry

    Returns
    -------
    slice_i,slice_j - slice
        The slices (with respect to the centroids) of the subset.

    """

    if (lon_vertices is None) or (lat_vertices is None):
        f = rectilinear_centroids_to_vertices
        (lon_vertices,lat_vertices) = f(lon_centroids,lat_centroids)
    f = _polygon_grid_subset_from_rectilinear_vertices
    return f(lon_vertices,lat_vertices,geometry)


def _polygon_grid_subset_from_irregular_vertices(lon_vertices, lat_vertices,
                                                 geometry, lon_centroids=None,
                                                 lat_centroids=None):
    """Find the subset of an irregular grid defined by its vertices such as
       to cover a given geometry.

    Parameters
    ----------
    lon_vertices - numpy array
    lat_vertices - numpy array
    geometry - Shapely geometry

    Returns
    -------
    slice_i,slice_j - slice
        The slices (with respect to the centroids) of the subset.

    """

    c1 = lon_vertices >= geometry.bounds[0]
    c2 = lon_vertices <= geometry.bounds[2]
    c3 = lat_vertices >= geometry.bounds[1]
    c4 = lat_vertices <= geometry.bounds[3]
    indices = np.where(c1 & c2 & c3 & c4)
    if len(indices[0]) == 0:
        if (lon_centroids is None) or (lat_centroids is None):
            f = quadrilaterals_mesh_to_centroids
            lon_centroids, lat_centroids = f(lon_vertices, lat_vertices)
        distances = distance_lon_lat(lon_centroids, lat_centroids,
                                     geometry.centroid.x,
                                     geometry.centroid.y)
        indices = np.where(distances == distances.min())
        i = indices[0][0]
        j = indices[1][0]
        pol1 = Polygon([(lon_vertices[i,j], lat_vertices[i,j]),
                        (lon_vertices[i,j+1], lat_vertices[i,j+1]),
                        (lon_vertices[i+1,j+1], lat_vertices[i+1,j+1]),
                        (lon_vertices[i+1,j], lat_vertices[i+1,j])])
        if not pol1.intersects(geometry):
            raise GeogridError("Geometry falls outside the grid.")
    search_i_min = max(0, indices[0].min() - 1)
    search_i_max = min(lon_vertices.shape[0] - 2, indices[0].max())
    search_j_min = max(0, indices[1].min() - 1)
    search_j_max = min(lon_vertices.shape[1] - 2, indices[1].max())
    if indices[0].min() == 0:
        min1 = 0
    else:
        for j in range(search_j_min, search_j_max + 1):
            i = indices[0].min() - 1
            pol1 = Polygon([(lon_vertices[i,j], lat_vertices[i,j]),
                            (lon_vertices[i,j+1], lat_vertices[i,j+1]),
                            (lon_vertices[i+1,j+1], lat_vertices[i+1,j+1]),
                            (lon_vertices[i+1,j], lat_vertices[i+1,j])])
            if pol1.intersects(geometry):
                min1 = indices[0].min() - 1
                break
        else:
            min1 = indices[0].min()
    if indices[1].min() == 0:
        min2 = 0
    else:
        for i in range(search_i_min, search_i_max + 1):
            j = indices[1].min() - 1
            pol1 = Polygon([(lon_vertices[i,j], lat_vertices[i,j]),
                            (lon_vertices[i,j+1], lat_vertices[i,j+1]),
                            (lon_vertices[i+1,j+1], lat_vertices[i+1,j+1]),
                            (lon_vertices[i+1,j], lat_vertices[i+1,j])])
            if pol1.intersects(geometry):
                min2 = indices[1].min() - 1
                break
        else:
            min2 = indices[1].min()
    if indices[0].max() == lon_vertices.shape[0] - 1:
        max1 = lon_vertices.shape[0] - 1
    else:
        for j in range(search_j_min, search_j_max + 1):
            i = indices[0].max()
            pol1 = Polygon([(lon_vertices[i,j], lat_vertices[i,j]),
                            (lon_vertices[i,j+1], lat_vertices[i,j+1]),
                            (lon_vertices[i+1,j+1], lat_vertices[i+1,j+1]),
                            (lon_vertices[i+1,j], lat_vertices[i+1,j])])
            if pol1.intersects(geometry):
                max1 = indices[0].max() + 1
                break
        else:
            max1 = indices[0].max()
    if indices[1].max() == lon_vertices.shape[1] - 1:
        max2 = lon_vertices.shape[1] - 1
    else:
        for i in range(search_i_min, search_i_max + 1):
            j = indices[1].max()
            pol1 = Polygon([(lon_vertices[i,j], lat_vertices[i,j]),
                            (lon_vertices[i,j+1], lat_vertices[i,j+1]),
                            (lon_vertices[i+1,j+1], lat_vertices[i+1,j+1]),
                            (lon_vertices[i+1,j], lat_vertices[i+1,j])])
            if pol1.intersects(geometry):
                max2 = indices[1].max() + 1
                break
        else:
            max2 = indices[1].max()
    slice1 = slice(min1, max1)
    slice2 = slice(min2, max2)
    return (slice1, slice2)


def _polygon_grid_subset_from_irregular_centroids(lon_centroids, lat_centroids,
                                                  geometry, lon_vertices=None,
                                                  lat_vertices=None):
    """Find the subset of an irregular grid defined by its centroids such as
       to cover a given geometry.

    Parameters
    ----------
    lon_centroids - numpy array
    lat_centroids - numpy array
    geometry - Shapely geometry

    Returns
    -------
    slice_i,slice_j - slice
        The slices (with respect to the centroids) of the subset.

    """

    if (lon_vertices is None) or (lat_vertices is None):
        lon_vertices,lat_vertices = ctoqmesh(lon_centroids,lat_centroids)
    f = _polygon_grid_subset_from_irregular_vertices
    return f(lon_vertices,lat_vertices,geometry,lon_centroids,lat_centroids)


def _inpolygon_from_list_of_points(lon_points, lat_points, geometry):
    """Find the points from a list of poits that fall in a geometry.

    Parameters
    ----------
    lon_points - numpy array
    lat_points - numpy array
    geometry - Shapely geometry

    Returns
    -------
    out - list of int
        The indices of the points inside the geometry.

    """

    c1 = lon_points >= geometry.bounds[0]
    c2 = lon_points <= geometry.bounds[2]
    c3 = lat_points >= geometry.bounds[1]
    c4 = lat_points <= geometry.bounds[3]
    c12 = np.bitwise_and(c1,c2)
    indices = np.where(np.bitwise_and(np.bitwise_and(c12,c3),c4))
    if len(indices[0]) == 0:
        return None
    points = []
    for i in indices[0]:
        points.append(Point(lon_points[i],lat_points[i]))
    inside_points = list(iterops.contains(geometry,points))
    valid_indices = []
    for c,i in enumerate(indices[0]):
        if points[c] in inside_points:
            valid_indices.append(i)
    return valid_indices

def _inpolygon_from_rectilinear_vertices(lon_vertices, lat_vertices, geometry):
    """Find the points from a rectilinear grid defined by its vertices that 
       fall in a geometry.

    Parameters
    ----------
    lon_vertices - numpy array
    lat_vertices - numpy array
    geometry - Shapely geometry

    Returns
    -------
    out - tuple of numpy arrays
        The indices of the centroids inside the geometry.

    """

    f = _polygon_grid_subset_from_rectilinear_vertices
    (slice_lon,slice_lat) = f(lon_vertices,lat_vertices,geometry)
    inpolygon_list = [[],[]]
    for i in range(slice_lon.start,slice_lon.stop):
        for j in range(slice_lat.start,slice_lat.stop):
            grid_tile = Polygon([(lon_vertices[i],lat_vertices[j]),
                                 (lon_vertices[i],lat_vertices[j+1]),
                                 (lon_vertices[i+1],lat_vertices[j+1]),
                                 (lon_vertices[i+1],lat_vertices[j])])
            if grid_tile.intersects(geometry):
                inpolygon_list[0].append(i)
                inpolygon_list[1].append(j)
    return (np.array(inpolygon_list[0]),np.array(inpolygon_list[1]))

def _inpolygon_from_rectilinear_centroids(lon_centroids, lat_centroids,
                                          geometry, lon_vertices=None,
                                          lat_vertices=None):
    """Find the points from a rectilinear grid defined by its centroids that 
       fall in a geometry.

    Parameters
    ----------
    lon_centroids - numpy array
    lat_centroids - numpy array
    geometry - Shapely geometry
    lon_vertices - numpy array (optional)
    lat_vertices - numpy array (optional)

    Returns
    -------
    out - tuple of numpy arrays
        The indices of the centroids inside the geometry.

    """

    if (lon_vertices is None) or (lat_vertices is None):
        f = rectilinear_centroids_to_vertices
        lon_vertices,lat_vertices = f(lon_centroids,lat_centroids)
    return _inpolygon_from_rectilinear_vertices(lon_vertices,lat_vertices,
                                                geometry)


def _inpolygon_from_irregular_vertices(lon_vertices, lat_vertices, geometry):
    """Find the points from an irregular grid defined by its vertices that 
       fall in a geometry.

    Parameters
    ----------
    lon_vertices - numpy array
    lat_vertices - numpy array
    geometry - Shapely geometry

    Returns
    -------
    out - tuple of numpy arrays
        The indices of the centroids inside the geometry.

    """

    f = _polygon_grid_subset_from_irregular_vertices
    (slice1,slice2) = f(lon_vertices,lat_vertices,geometry)
    inpolygon_list = [[],[]]
    for i in range(slice1.start,slice1.stop):
        for j in range(slice2.start,slice2.stop):
            grid_tile = Polygon([(lon_vertices[i,j],lat_vertices[i,j]),
                                 (lon_vertices[i,j+1],lat_vertices[i,j+1]),
                                 (lon_vertices[i+1,j+1],lat_vertices[i+1,j+1]),
                                 (lon_vertices[i+1,j],lat_vertices[i+1,j])])
            if grid_tile.intersects(geometry):
                inpolygon_list[0].append(i)
                inpolygon_list[1].append(j)
    return (np.array(inpolygon_list[0]),np.array(inpolygon_list[1]))


def _inpolygon_from_irregular_centroids(lon_centroids, lat_centroids, geometry,
                                        lon_vertices=None, lat_vertices=None):
    """Find the points from an irregular grid defined by its centroids that 
       fall in a geometry.

    Parameters
    ----------
    lon_centroids - numpy array
    lat_centroids - numpy array
    geometry - Shapely geometry
    lon_vertices - numpy array (optional)
    lat_vertices - numpy array (optional)

    Returns
    -------
    out - tuple of numpy arrays
        The indices of the centroids inside the geometry.

    """

    if (lon_vertices is None) or (lat_vertices is None):
        lon_vertices,lat_vertices = ctoqmesh(lon_centroids,lat_centroids)
    f = _inpolygon_from_irregular_vertices
    return f(lon_vertices,lat_vertices,geometry)


def subdomain_grid_slices_or_points_indices(lon, lat, geometry, grid_type=None,
                                            lon_dimensions=None,
                                            lat_dimensions=None):
    """Find the subset of a given grid such as to cover a given geometry

    Parameters
    ----------
    lon - numpy array
    lat - numpy array
    geometry - Shapely geometry
    grid_type - str (optional)
        The grid type, if None it will be found using detect_grid.
    lon_dimensions - tuple of str (optional, for detect_grid)
    lat_dimensions - tuple of str (optional, for detect_grid)

    Returns
    -------
    out - usually a tuple of slice, depending on the grid
        The slices of the subset.

    """

    if grid_type is None:
        grid_type = detect_grid(lon,lat,lon_dimensions,
                                lat_dimensions)
    if grid_type == 'list_of_2d_points':
        return _inpolygon_from_list_of_points(lon,lat,geometry)
    elif grid_type == 'rectilinear_2d_centroids':
        f = _polygon_grid_subset_from_rectilinear_centroids
        return f(lon,lat,geometry)
    elif grid_type == 'rectilinear_2d_bounds':
        f = rectilinear_2d_bounds_to_vertices
        (lon_vertices,lat_vertices) = f(lon,lat)
        f = _polygon_grid_subset_from_rectilinear_vertices
        return f(lon_vertices,lat_vertices,geometry)
    elif grid_type == 'rectilinear_2d_vertices':
        f = _polygon_grid_subset_from_rectilinear_vertices
        return f(lon,lat,geometry)
    elif grid_type == 'irregular_2d_centroids':
        f = _polygon_grid_subset_from_irregular_centroids
        return f(lon,lat,geometry)
    elif grid_type == 'irregular_2d_vertices':
        f = _polygon_grid_subset_from_irregular_vertices
        return f(lon,lat,geometry)
subdom_slice_or_i = subdomain_grid_slices_or_points_indices


def lon_lat_polygon_area(lon_lat_polygon,lon_0=0):
    """Area of a polygon in lon/lat projection.

    Parameters
    ----------
    lon_lat_polygon - Shapely Polygon
    lon_0 - float

    Returns
    -------
    out - float
        The area of the polygon (in m**2)

    """

    from mpl_toolkits.basemap import Basemap

    m1 = Basemap(projection='moll',lon_0=lon_0)
    exterior1 = list(lon_lat_polygon.boundary.coords)

    lon1 = []
    lat1 = []
    for i in range(len(exterior1)):
        lon1.append(exterior1[i][0])
        lat1.append(exterior1[i][1])

    x1,y1 = m1(np.array(lon1),np.array(lat1))

    xys = []
    for i in range(len(exterior1)):
        xys.append((x1[i],y1[i]))
    pol2 = Polygon(xys)

    return pol2.area


def spatial_weighted_average(lon_vertices, lat_vertices, geometry, field,
                             spatial_indices=None):
    # spatial indices must be specified if field has extra dimensions
    # there's room for optimisation... iterops the intersections,
    # check lon_lat_polygon_area performance
    if len(lon_vertices.shape) == 1:
        grid_type = 'rectilinear_2d_vertices'
    else:
        grid_type = 'irregular_2d_vertices'
    slices = subdomain_grid_slices_or_points_indices(
        lon_vertices, lat_vertices, geometry, grid_type)
    if grid_type == 'rectilinear_2d_vertices':
        weights = np.zeros([slices[1].stop-slices[1].start,
                            slices[0].stop-slices[0].start])
        for i in range(slices[0].start, slices[0].stop):
            for j in range(slices[1].start, slices[1].stop):
                pol1 = Polygon([(lon_vertices[i], lat_vertices[j]),
                                (lon_vertices[i+1], lat_vertices[j]),
                                (lon_vertices[i+1], lat_vertices[j+1]),
                                (lon_vertices[i], lat_vertices[j+1])])
                cell_area = lon_lat_polygon_area(pol1)
                area = lon_lat_polygon_area(pol1.intersection(geometry))
                weights[j,i] = area/float(cell_area)
    elif grid_type == 'irregular_2d_vertices':
        weights = np.zeros([slices[0].stop-slices[0].start,
                            slices[1].stop-slices[1].start])
        for i in range(slices[0].start, slices[0].stop):
            for j in range(slices[1].start, slices[1].stop):
                pol1 = Polygon([(lon_vertices[i,j], lat_vertices[i,j]),
                                (lon_vertices[i+1,j], lat_vertices[i+1,j]),
                                (lon_vertices[i+1,j+1], lat_vertices[i+1,j+1]),
                                (lon_vertices[i,j+1], lat_vertices[i,j+1])])
                cell_area = lon_lat_polygon_area(pol1)
                intersection = pol1.intersection(geometry)
                if isinstance(intersection, LineString):
                    area = 0
                elif isinstance(intersection, Point):
                    area = 0
                else:
                    area = lon_lat_polygon_area(intersection)
                weights[i,j] = area/float(cell_area)
    weights = weights/weights.sum()
    if spatial_indices:
        if grid_type == 'rectilinear_2d_vertices':
            x = 1
        else:
            x = 0
        field_slices = []
        for i in range(len(field.shape)):
            if i in spatial_indices:
                field_slices.append(slices[x])
                if grid_type == 'rectilinear_2d_vertices':
                    x -= 1
                else:
                    x += 1
            else:
                field_slices.append(slice(None))
        field = field[field_slices]
        reps = []
        for i in range(len(field.shape)):
            if i in spatial_indices:
                reps.append(1)
            else:
                reps.append(field.shape[i])
        weights = np.tile(weights, reps)
        weighted_average_field = (field*weights)
        for i in reversed(sorted(spatial_indices)):
            weighted_average_field = weighted_average_field.sum(i)
    else:
        if grid_type == 'rectilinear_2d_vertices':
            field = field[slices[1],slices[0]]
        else:
            field = field[slices[0],slices[1]]
        weighted_average_field = (field*weights).sum()
    return weighted_average_field
