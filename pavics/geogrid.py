import logging

import numpy as np


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
