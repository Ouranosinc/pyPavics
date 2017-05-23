import requests
import geojson
from shapely.geometry import shape


def shapely_from_geoserver(wfs_server, feature_name, cql_filter=None,
                           cql_filter_value=None, feature_ids=None,
                           max_features=1000):
    # wfs_server looks like
    # http://server.org:8087/...
    # /geoserver/ows?service=WFS&version=1.0.0&request=GetFeature&typeName='
    s = "{0}{1}&maxFeatures={2}&outputFormat=application%2Fjson".format(
        wfs_server, feature_name, str(max_features))
    if cql_filter:
        s += "&CQL_FILTER={0}=%27{1}%27".format(cql_filter, cql_filter_value)
    if feature_ids:
        if not hasattr(feature_ids, '__iter__'):
            s += "&featureID={0}".format(feature_ids)
        else:
            s += "&featureID={0}".format(feature_ids[0])
            for feature_id in feature_ids[1:]:
                s += ",{0}".format(feature_id)
    r = requests.get(s)
    json_result = geojson.loads(r.text)
    geometry = shape(json_result['features'][0]['geometry'])
    # making union of additional results
    for feature in json_result['features'][1:]:
        geometry = geometry.union(shape(feature['geometry']))
    return geometry
