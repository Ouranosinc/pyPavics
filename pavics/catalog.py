"""Catalog for Solr indexing of NetCDF files on a THREDDS server.

Notes
-----
A localhost Solr index can be launched with docker::

    $ docker run --name my_solr -d -p 8983:8983 -t pavics/solr
    http://localhost:8983/solr/birdhouse/

"""

import os
import copy
import json
import requests
import logging
import urlparse

import threddsclient
import netCDF4

from . import nctime
from . import netcdfcookie

logger = logging.getLogger(__name__)


# These definitions should be moved to a config file
solr_fields_type = {'datetime_max': 'date',
                    'datetime_min': 'date',
                    'latest': 'boolean',
                    'replica': 'boolean'}

netcdf_ignored_variables = [
    'time', 'time_bnds', 'time_bounds', 'time_vectors', 'lat', 'lon', 'yc',
    'xc', 'rlat', 'rlon', 'lat_bnds', 'lat_bounds', 'lon_bnds', 'lon_bounds',
    'yc_bnds', 'xc_bnds', 'yc_bounds', 'xc_bounds', 'rlat_bnds', 'rlat_bounds',
    'rlon_bnds', 'rlon_bounds', 'level', 'level_bnds', 'level_bounds', 'plev',
    'longitude', 'latitude', 'height']

variables_default_min_max = {
    'pr': (0, 0.0001, 'seq-Blues'),
    'tas': (253.15, 293.15, 'div-BuRd')}

# In ESGF this is a config file for each node /esgf/config/facets.properties
default_facets = [
    'author', 'category', 'cf_standard_name', 'experiment', 'frequency',
    'institute', 'model', 'project', 'source', 'subject', 'title', 'units',
    'variable', 'variable_long_name']

solr_self_generating_fields = ['id', '_version_', 'keywords', 'abstract']

birdhouse_solr_attr_mapping = {'cf_standard_name': 'standard_name',
                               'variable_long_name': 'long_name',
                               'units': 'units'}

aggregate_solr_fields = ['resourcename', 'abstract', 'wms_url', 'datetime_max',
                         'id', 'opendap_url', 'title', 'datetime_min',
                         '_version_', 'catalog_url', 'last_modified',
                         'url', 'fileserver_url']

reduce_solr_fields = ['cf_standard_name', 'units', 'variable',
                      'variable_long_name', 'variable_max', 'variable_min',
                      'variable_palette']


class MissingThreddsFile(Exception):
    pass


def _modify_wms_url(thredds_dataset, wms_alternate_server=None):
    wms_url = thredds_dataset.wms_url()
    if wms_alternate_server is not None:
        thredds_id = thredds_dataset.ID
        thredds_rel_path = thredds_id[thredds_id.find('/') + 1:]
        wms_url = wms_alternate_server.replace('<DATASET>', thredds_rel_path)
    return wms_url


def _split_solr_update(solr_server, add_data, split_update=None):
    if split_update:
        previous_update = None
        for i in range(0, len(add_data), split_update):
            update_result = solr_update(
                solr_server, add_data[i:i + split_update])
            if i > 0:
                update_result = aggregate_solr_responses(
                    previous_update, update_result)
            previous_update = update_result
        return update_result
    else:
        return solr_update(solr_server, add_data)


def solr_add_field(solr_server, field_name, field_type='string',
                   multivalued=False):
    """Add a field in a Solr database.

    Parameters
    ----------
    solr_server : string
        usually of the form 'http://x.x.x.x:8983/solr/core_name/'
    field_name : string
    field_type : string
    multivalued : bool

    Returns
    -------
    out : string
        json response from the Solr server

    """

    schema_path = os.path.join(solr_server, 'schema')
    if multivalued:
        add_field = {'add-field': {'name': field_name,
                                   'type': field_type,
                                   'stored': 'true',
                                   'multiValued': 'true'}}
    else:
        add_field = {'add-field': {'name': field_name,
                                   'type': field_type,
                                   'stored': 'true'}}
    headers = {'Content-type': 'application/json'}
    r = requests.post(schema_path, data=json.dumps(add_field), headers=headers)
    return r.json()


def solr_update(solr_server, update_data):
    """Update data in a Solr database.

    Parameters
    ----------
    solr_server : string
        usually of the form 'http://x.x.x.x:8983/solr/core_name/'
    update_data : list of dict

    Returns
    -------
    out : string
        json response from the Solr server

    """

    # make sure update_data is a list
    if not hasattr(update_data, 'append'):
        update_data = [update_data]

    # search for fields that do not yet exist in Solr and add them
    solr_call = os.path.join(solr_server, 'schema', 'fields?wt=json')
    r = requests.get(solr_call)
    if not r.ok:
        r.raise_for_status()
    solr_fields = r.json()
    list_of_fields = []
    for field in solr_fields['fields']:
        list_of_fields.append(field['name'])
    for one_update in update_data:
        for field in one_update:
            if field not in list_of_fields:
                if hasattr(one_update[field], 'append'):
                    multivalued = True
                else:
                    multivalued = False
                field_type = solr_fields_type.get(field, 'string')
                solr_add_field(solr_server, field, field_type=field_type,
                               multivalued=multivalued)
                list_of_fields.append(field)

    # add data to solr
    solr_call = os.path.join(solr_server, 'update', 'json?commit=true')
    solr_json_input = json.dumps(update_data)
    headers = {'Content-type': 'application/json'}
    r = requests.post(solr_call, data=solr_json_input, headers=headers)
    if not r.ok:
        r.raise_for_status()
    update_result = r.json()
    return update_result


def solr_delete(solr_server, delete_ids):
    """Delete data in a Solr database.

    Parameters
    ----------
    solr_server : string
        usually of the form 'http://x.x.x.x:8983/solr/core_name/'
    delete_ids : list of ids to delete

    Returns
    -------
    out : string
        json response from the Solr server

    """

    # make sure delete_data is a list
    if not hasattr(delete_ids, 'append'):
        delete_ids = [delete_ids]

    # delete data in solr
    solr_call = os.path.join(solr_server, 'update?commit=true')
    solr_json_input = json.dumps({'delete': delete_ids})
    headers = {'Content-type': 'application/json'}
    r = requests.post(solr_call, data=solr_json_input, headers=headers)
    if not r.ok:
        r.raise_for_status()
    delete_result = r.json()
    return delete_result


def thredds_crawler(thredds_server, index_facets, depth=50,
                    ignored_variables=None, set_dataset_id=False,
                    overwrite_dataset_id=False,
                    wms_alternate_server=None, target_files=None,
                    ignored_files=None, headers=None, verify=True):
    """Crawl thredds server for metadata.

    Parameters
    ----------
    thredds_server : string
        usually of the form 'http://x.x.x.x:8083/thredds'
    index_facets : list of string
        global attributes from the NetCDF file that will be used as facets
    depth : int
        thredds parameter for depth of subdirectories to search
    ignored_variables : list of string
        variables that will be considered metadata and not added to the
        list of variables in the NetCDF file. Can be set to 'all' to
        ignore everything (only global attributes will be added to the
        database)
    set_dataset_id : bool
        if True, the files that are in a common directory will have
        their dataset_id set to reflect that unique relative directory
    overwrite_dataset_id : bool
        if set_dataset_id is True, this decides whether the default value
        constructed by the crawler takes precedence over the dataset_id in
        the NetCDF file.
    wms_alternate_server : string
    target_files : list of string
        only those paths will be crawled, paths are relative to thredds root
        directory. If this is provided, all target paths must be found.
        Additionally, this implementation also works in reverse, such that
        a given location suffix (e.g. a filename) will also be crawler.
    ignored_files : list of string
        same rules as target_files but those paths will be ignored.
    headers : headers adds to the thredds client requests
    verify : SSL verification

    Returns
    -------
    out : list of dict

    Notes
    -----
    1. Variables that do not have any of 'standard_name', 'long_name' or
       'units' attributes are ignored. If only one or two of those are
       missing, they are set to '_undefined' in the database.

    """

    # Add or remove dataset_id from facets list depending on parameters.
    index_facets_did = copy.deepcopy(index_facets)
    if overwrite_dataset_id and ('dataset_id' in index_facets):
        index_facets_did.remove('dataset_id')
    elif (not overwrite_dataset_id) and ('dataset_id' not in index_facets):
        index_facets_did.append('dataset_id')

    if ignored_variables is None:
        ignored_variables = netcdf_ignored_variables

    add_data = []
    targets_found = []

    opendap_hostname = urlparse.urlparse(thredds_server).hostname
    with netcdfcookie.NetCDFCookie(headers, [opendap_hostname, ],
                                   verify=verify):
        for thredds_dataset in threddsclient.crawl(thredds_server, depth=depth,
                                                   headers=headers,
                                                   verify=verify):
            if ignored_files:
                ignore_this_file = False
                for ignored_path in ignored_files:
                    ipl = len(ignored_path)
                    if thredds_dataset.url_path[:ipl] == ignored_path:
                        ignore_this_file = True
                        break
                    elif thredds_dataset.url_path[-ipl:] == ignored_path:
                        ignore_this_file = True
                        break
                if ignore_this_file:
                    continue
            if target_files:
                for target_path in target_files:
                    tpl = len(target_path)
                    if thredds_dataset.url_path[:tpl] == target_path:
                        break
                    elif thredds_dataset.url_path[-tpl:] == target_path:
                        break
                else:
                    continue
                targets_found.append(target_path)

            wms_url = _modify_wms_url(thredds_dataset, wms_alternate_server)
            urls = {'opendap_url': thredds_dataset.opendap_url(),
                    'download_url': thredds_dataset.download_url(),
                    'thredds_server': thredds_server,
                    'catalog_url': thredds_dataset.catalog.url,
                    'wms_url': wms_url}
            # Here, if opening the NetCDF file fails, we simply continue
            # to the next one. Perhaps a way to track the erroneous files
            # should be considered...
            try:
                nc = netCDF4.Dataset(urls['opendap_url'], 'r')
                (datetime_min, datetime_max) = nctime.time_start_end(nc)
            except:
                # Should have a logging mechanism
                continue

            # Default Birdhouse catalog entry
            # 'url' is used differently in the ESGF database, adding
            # 'fileserver_url' and will possibly use 'url' as in the ESGF
            # implementation
            doc = {'url': urls['download_url'],
                   'fileserver_url': urls['download_url'],
                   'source': os.path.join(urls['thredds_server'],
                                          'catalog.xml'),
                   'catalog_url': "{0}?dataset={1}".format(
                       urls['catalog_url'], thredds_dataset.ID),
                   'category': 'thredds',
                   'content_type': thredds_dataset.content_type,
                   'opendap_url': urls['opendap_url'],
                   'title': thredds_dataset.name,
                   'last_modified': thredds_dataset.modified,
                   'wms_url': urls['wms_url'],
                   'resourcename': thredds_dataset.url_path,
                   'subject': 'Birdhouse Thredds Catalog',
                   'type': 'File'}

            # Set default dataset_id
            if set_dataset_id:
                doc['dataset_id'] = '.'.join(
                    doc['resourcename'].split('/')[1:-1])

            # Add custom facets
            # In the ESGF implementation, all facets are stored in multivalued
            # fields, not sure how this is ever used... Not following this
            # convention here...
            for facet in index_facets_did:
                if hasattr(nc, facet + '_id'):
                    doc[facet] = getattr(nc, facet + '_id')
                elif hasattr(nc, facet):
                    doc[facet] = getattr(nc, facet)

            # Replica and latest
            # Setting defaults here, to be modified by other operations.
            doc['replica'] = False
            doc['latest'] = True

            # Datetime min/max
            if datetime_min:
                # Time zones are not supported by that function...
                # Plus solr only supports UTC:
                # https://lucene.apache.org/solr/guide/6_6/working-with-dates.html
                doc['datetime_min'] = nctime.nc_datetime_to_iso(
                    datetime_min, force_gregorian_date=True) + 'Z'
            if datetime_max:
                doc['datetime_max'] = nctime.nc_datetime_to_iso(
                    datetime_max, force_gregorian_date=True) + 'Z'

            if ignored_variables != 'all':
                for var_name in nc.variables:
                    if var_name in ignored_variables:
                        continue
                    ncvar = nc.variables[var_name]
                    ccf = hasattr(ncvar, 'standard_name')
                    clong = hasattr(ncvar, 'long_name')
                    cunits = hasattr(ncvar, 'units')
                    # if there is no standard name, long_name or units,
                    # ignore it
                    if not (ccf or clong or cunits):
                        continue
                    if 'variable' not in doc:
                        doc['variable'] = [var_name]
                    else:
                        doc['variable'].append(var_name)
                    for (bh_attr, attr) in birdhouse_solr_attr_mapping.items():
                        if bh_attr not in doc:
                            doc[bh_attr] = []
                        doc[bh_attr].append(
                            getattr(ncvar, attr, '_undefined'))
            nc.close()
            add_data.append(doc)

    # Check that all target files were found
    if target_files:
        if set(target_files) != set(targets_found):
            missing_files = set(target_files) - set(targets_found)
            raise MissingThreddsFile(
                "One or more target path not found: {0}".format(
                    str(list(missing_files))))
    return add_data


def pavicrawler(thredds_server, solr_server, index_facets, depth=50,
                ignored_variables=None, set_dataset_id=False,
                overwrite_dataset_id=False, wms_alternate_server=None,
                target_files=None, ignored_files=None, check_replica=True,
                split_update=100, headers=None, verify=True):
    """Crawl thredds server and output to Solr database.

    Parameters
    ----------
    thredds_server : string
        usually of the form 'http://x.x.x.x:8083/thredds'
    solr_server : string
        usually of the form 'http://x.x.x.x:8983/solr/core_name/'
    index_facets : list of string
        global attributes from the NetCDF file that will be used as facets
    depth : int
        thredds parameter for depth of subdirectories to search
    ignored_variables : list of string
        variables that will be considered metadata and not added to the
        list of variables in the NetCDF file. Can be set to 'all' to
        ignore everything (only global attributes will be added to the
        database)
    set_dataset_id : bool
        if True, the files that are in a common directory will have
        their dataset_id set to reflect that unique relative directory
    overwrite_dataset_id : bool
        if set_dataset_id is True, this decides whether the default value
        constructed by the crawler takes precedence over the dataset_id in
        the NetCDF file.
    wms_alternate_server : string
    target_files : list of string
        only those paths will be crawled, paths are relative to thredds root
        directory. If this is provided, all target paths must be found or
        nothing will be added to solr. Additionally, this implementation also
        works in reverse, such that a given location suffix (e.g. a filename)
        will also be crawler.
    ignored_files : list of string
        same rules as target_files but those paths will be ignored.
    check_replica : bool
        if True, will search for identical file names and dataset_id in solr
        and tag this instance as replica=True if it already exists on
        another thredds server.
    split_update : int
        how many datasets will be updated per solr request.
    headers : dict
        headers adds to the thredds client requests
    verify : SSL verification

    Returns
    -------
    out : string
        json response from the Solr server

    Notes
    -----
    1. Each NetCDF file served by the thredds server is scanned to extract
       metadata (CF standard vocabulary). This metadata is then added
       to the Solr database so it can index the file by its properties.
    2. Variables that do not have any of 'standard_name', 'long_name' or
       'units' attributes are ignored. If only one or two of those are
       missing, they are set to '_undefined' in the database.

    """

    add_data = thredds_crawler(thredds_server, index_facets, depth=50,
                               ignored_variables=ignored_variables,
                               set_dataset_id=set_dataset_id,
                               overwrite_dataset_id=overwrite_dataset_id,
                               wms_alternate_server=wms_alternate_server,
                               target_files=target_files,
                               ignored_files=ignored_files,
                               headers=headers, verify=verify)

    solr_response = {'responseHeader':
                     {'QTime': 0, 'status': 0, 'Nquery': 0}}
    # check for replica
    if check_replica:
        add_raw = []
        add_refresh = []
        for doc in add_data:
            (search_dict, search_url) = pavicsearch(
                solr_server, limit=1000, search_type=None,
                add_default_min_max=False, query="{0} AND {1}".format(
                    doc['title'], doc['dataset_id']))
            if search_dict['response']['docs']:
                for indexed_doc in search_dict['response']['docs']:
                    # Here, checking that the free-query above really returned
                    # exact results for title and dataset_id.
                    # if dataset_id is not already a key in solr, this will
                    # return a KeyError... but every entry should have one if
                    # it was done with the crawler...
                    if indexed_doc['title'] == doc['title'] and \
                       indexed_doc['dataset_id'] == doc['dataset_id'] and \
                       indexed_doc['source'] == doc['source']:
                        add_refresh.append(doc)
                        break
                else:
                    doc['replica'] = True
                    add_raw.append(doc)
            else:
                add_raw.append(doc)

        if add_raw:
            solr_response = _split_solr_update(solr_server, add_raw,
                                               split_update)
        for doc in add_refresh:
            update_result = pavicsupdate(solr_server, doc)
            solr_response = aggregate_solr_responses(solr_response,
                                                     update_result)
        return solr_response
    else:
        if add_data:
            solr_response = _split_solr_update(solr_server, add_data,
                                               split_update)
        return solr_response


def pavicsvalidate(solr_server, required_facets, limit_paths=None,
                   limit_files=None):
    """Query Solr database for entries with missing required facets.

    Parameters
    ----------
    solr_server : string
        usually of the form 'http://x.x.x.x:8983/solr/core_name/'
    required_facets : list of string
    limit_paths : list of string
        limit validation to certain paths relative to thredds main directory
    limit_files : list of string
        limit according to file names

    Returns
    -------
    out : list of dictionary

    Notes
    -----
    1. Empty strings or those set to '_undefined' are considered missing.

    """

    if (limit_paths is not None) and (not hasattr(limit_paths, 'append')):
        limit_paths = [limit_paths]
    if (limit_files is not None) and (not hasattr(limit_files, 'append')):
        limit_files = [limit_files]
    # Number of documents returned, this should be larger in deployed
    # version.
    nrows = 100
    n = 0
    # Note that in Solr, if indexing changes due to document modification,
    # or new documents, the pagination loop could return the same
    # document twice or miss a document.
    # https://cwiki.apache.org/confluence/display/solr/Pagination+of+Results
    incomplete_docs = []
    while True:
        limit_search = ''
        if limit_paths:
            limit_search = limit_search + '('
            for limit_path in limit_paths:
                limit_search = limit_search + limit_path + '+OR+'
            limit_search = limit_search[:-4] + ')'
        if limit_files:
            if limit_search != '':
                limit_search = limit_search + '+AND+('
            else:
                limit_search = limit_search + '('
            for limit_file in limit_files:
                limit_search = limit_search + limit_file + '+OR+'
            limit_search = limit_search[:-4] + ')'
        if limit_search != '':
            my_search = 'q={0}&start={1}&rows={2}&wt=json'.format(
                limit_search, str(n), str(n + nrows))
        else:
            my_search = 'q=*:*&start={0}&rows={1}&wt=json'.format(
                str(n), str(n + nrows))
        solr_call = os.path.join(solr_server, 'select?{0}'.format(my_search))
        r = requests.get(solr_call)
        if not r.ok:
            r.raise_for_status()
        search_dict = r.json()
        if not len(search_dict['response']['docs']):
            break
        for doc in search_dict['response']['docs']:
            if ('source' not in doc) or ('url' not in doc):
                continue
            missing_facets = []
            for required_facet in required_facets:
                if required_facet not in doc:
                    missing_facets.append(required_facet)
                    continue
                # If it's a list, it must contain values. Those set to
                # _undefined are considered missing.
                if hasattr(doc[required_facet], 'append'):
                    if len(doc[required_facet]) == 0:
                        missing_facets.append(required_facet)
                        continue
                    for value in doc[required_facet]:
                        if value in ['', '_undefined']:
                            missing_facets.append(required_facet)
                            continue
                # If it's an empty string, it's also considered missing
                if doc[required_facet] in ['', '_undefined']:
                    missing_facets.append(required_facet)
                    continue
            if missing_facets:
                incomplete_docs.append({'source': doc['source'],
                                        'url': doc['url'],
                                        'id': doc['id'],
                                        'missing_facets': missing_facets})
        n += nrows
    return incomplete_docs


def pavicsupdate(solr_server, update_dict):
    """Update a Solr entry identified by its id using (key,value) pairs.

    Parameters
    ----------
    solr_server : string
        usually of the form 'http://x.x.x.x:8983/solr/core_name/'
    update_dict : dictionary
        key:value pairs to update including the id or dataset_id key

    Returns
    -------
    out : string
        json response from the Solr server

    Notes
    -----
    1. If dataset_id is provided, all entries with that dataset_id will
       have the provided update applied to them.

    """

    # Get all the information of the current Solr document
    if 'id' in update_dict:
        my_search = "q=id:{0}&wt=json".format(update_dict['id'])
    elif 'dataset_id' in update_dict:
        my_search = "q=dataset_id:{0}&wt=json".format(
            update_dict['dataset_id'])
    solr_call = os.path.join(solr_server, 'select?{0}'.format(my_search))
    r = requests.get(solr_call)
    if not r.ok:
        r.raise_for_status()
    search_dict = r.json()
    data = search_dict['response']['docs']
    # Remove self-generated fields
    for doc in data:
        for key in solr_self_generating_fields:
            doc.pop(key, None)
        for (key, value) in update_dict.items():
            if key == 'id':
                continue
            doc[key] = value
    return solr_update(solr_server, data)


def datasets_from_solr_search(solr_search_result):
    """Convert a Solr search result on files to a Dataset search result.

    Parameters
    ----------
    solr_search_result : string
        the json structure from a Solr query

    Returns
    -------
    out : string
        restructured json response for Dataset representation

    """

    # The ESGF actually maintains a different solr table for datasets...
    search_results = copy.deepcopy(solr_search_result)
    known_datasets = []
    first_instances = []
    for i, doc in enumerate(search_results['response']['docs']):
        if doc['dataset_id'] in known_datasets:
            continue
        else:
            known_datasets.append(doc['dataset_id'])
            first_instances.append(i)
            # This is something else in ESGF...
            # doc['url'] = [doc['url']]
            doc['type'] = 'Dataset'
            for key in ['abstract', 'id', 'last_modified', 'resourcename',
                        'title', 'wms_url', 'catalog_url', 'opendap_url',
                        'fileserver_url']:
                doc.pop(key, None)
    for i in range(len(search_results['response']['docs']) - 1, -1, -1):
        if i not in first_instances:
            search_results['response']['docs'].pop(i)
    n = len(search_results['response']['docs'])
    search_results['response']['numFound'] = n
    return search_results


def aggregate_from_solr_search(solr_search_result):
    """Convert a Solr search result on files to an aggregate search result.

    Parameters
    ----------
    solr_search_result : string
        the json structure result from a Solr query

    Returns
    -------
    out : string
        restructured json response for aggregate representation

    """

    search_results = copy.deepcopy(solr_search_result)
    known_datasets = []
    first_instances = []
    for i, doc in enumerate(search_results['response']['docs']):
        # Not sure dataset_id should be used for this purpose, may be
        # changed in the future...
        if doc['dataset_id'] in known_datasets:
            refi = first_instances[known_datasets.index(doc['dataset_id'])]
            ref_doc = search_results['response']['docs'][refi]
            for attr in doc:
                if attr in aggregate_solr_fields:
                    ref_doc[attr].append(doc[attr])
        else:
            # Not sure dataset_id should be used for this purpose, may be
            # changed in the future...
            known_datasets.append(doc['dataset_id'])
            first_instances.append(i)
            for attr in doc:
                if attr in aggregate_solr_fields:
                    doc[attr] = [doc[attr]]
                elif attr in reduce_solr_fields:
                    # Validate should verify that those are all length 1
                    doc[attr] = doc[attr][0]
            doc['type'] = 'Aggregate'
            doc['aggregate_title'] = doc['dataset_id']
    for i in range(len(search_results['response']['docs']) - 1, -1, -1):
        if i not in first_instances:
            search_results['response']['docs'].pop(i)

    # reorder based on opendap_url name (or url if not possible)
    # also reduce lists with repeating values for all files
    for doc in search_results['response']['docs']:
        if 'opendap_url' in doc:
            order_attr = doc['opendap_url']
        else:
            order_attr = doc['url']
        if len(order_attr) == 1:
            doc['type'] = 'FileAsAggregate'
            doc['aggregate_title'] = doc['title'][0]
        for (attr, value) in doc.items():
            if hasattr(value, 'append') and (len(value) == len(order_attr)):
                doc[attr] = [x for (_, x) in sorted(zip(order_attr, value))]

    n = len(search_results['response']['docs'])
    search_results['response']['numFound'] = n
    return search_results


def file_as_aggregate_from_solr_search(solr_search_result):
    """Convert a Solr search result on files to Aggregate structure.

    Parameters
    ----------
    solr_search_result : string
        the json structure result from a Solr query

    Returns
    -------
    out : string
        restructured json response for Aggregate representation

    """

    search_results = copy.deepcopy(solr_search_result)
    for doc in search_results['response']['docs']:
        for attr in doc:
            if attr == 'type':
                continue
            if not hasattr(doc[attr], 'append'):
                doc[attr] = [doc[attr]]
        doc['aggregate_title'] = doc['title'][0]
    return search_results


def add_default_min_max_to_solr_search(solr_search_result):
    """Add default variables min, max and color palette.

    Parameters
    ----------
    solr_search_result : string
        the json structure result from a Solr query

    Returns
    -------
    out : string
        json response with added parameters

    """

    search_results = copy.deepcopy(solr_search_result)
    for doc in search_results['response']['docs']:
        if 'variable' not in doc:
            continue
        doc['variable_min'] = []
        doc['variable_max'] = []
        doc['variable_palette'] = []
        for i, var_name in enumerate(doc['variable']):
            min_max = variables_default_min_max.get(
                var_name, (0, 1, 'default'))
            doc['variable_min'].append(min_max[0])
            doc['variable_max'].append(min_max[1])
            doc['variable_palette'].append(min_max[2])
    return search_results


def restructure_urls(solr_search_result):
    """Split urls in multiple fields

    Parameters
    ----------
    solr_search_result : string
        the json structure result from a Solr query

    Returns
    -------
    out : string
        json response with added url fields

    """

    search_results = copy.deepcopy(solr_search_result)
    for doc in search_results['response']['docs']:
        for url in doc['url']:
            decode_url = url.split('|')
            url_type = decode_url[-1]
            if url_type == 'HTTPServer':
                http_link = decode_url[0]
                doc[u'fileserver_url'] = decode_url[0]
            elif url_type == 'OPENDAP':
                doc[u'opendap_url'] = decode_url[0]
            elif url_type == 'GridFTP':
                doc[u'gridftp_url'] = decode_url[0]
            elif url_type == 'Globus':
                doc[u'globus_url'] = decode_url[0]
            elif url_type == 'WMS':
                doc[u'wms_url'] = decode_url[0]
            else:
                raise ValueError("Unknown URL type: {0}".format(url_type))
        doc[u'url'] = http_link
        doc[u'source'] = 'ESGF'
    return search_results


def restructure_type(search_result, add_default_min_max=True,
                     search_type='File'):
    if add_default_min_max:
        search_result = add_default_min_max_to_solr_search(search_result)
    if search_type == 'Dataset':
        search_result = datasets_from_solr_search(search_result)
    elif search_type == 'Aggregate':
        search_result = aggregate_from_solr_search(search_result)
    elif search_type == 'FileAsAggregate':
        search_result = file_as_aggregate_from_solr_search(search_result)
    return search_result


def pavicsearch(solr_server, facets=None, offset=0, limit=10, fields=None,
                query=None, constraints=None, search_type='File',
                output_format='application/solr+json',
                add_default_min_max=True, **kwargs):
    """Search Solr database.

    Parameters
    ----------
    solr_server : string
        usually of the form 'http://x.x.x.x:8983/solr/core_name/'
    facets : string
        comma separated list of facets; facets are searchable indexing
        terms in the Solr database
    offset : int
        where to start in the document count of the Solr search
    limit : int
        maximum number of documents to return
    fields : string
        comma separated list of fields to return
    query : string
        direct query to the Solr database
    constraints : string
        comma separated list of facets and their search value separated
        by colon (e.g. model:CRCM4,experiment:rcp85)
    search_type : string
        one of 'Dataset', 'File', 'Aggregate' or 'FileAsAggregate'
    output_format : string
        one of 'application/solr+json' or 'application:solr+xml'
    add_default_min_max : bool
        whether to add default color palette information to search result

    Returns
    -------
    (out1, out2) : strings
        out1 - json response from the Solr server
        out2 - url used for the Solr request

    """

    solr_url = os.path.join(solr_server, 'select?')
    solr_search = ''
    if facets:
        solr_search += ("&facet=true"
                        "&facet.limit=-1"
                        "&facet.method=enum"
                        "&facet.mincount=1"
                        "&facet.sort=lex")
        if facets == '*':
            facets = default_facets
        else:
            facets = facets.split(',')
        for facet in facets:
            solr_search += "&facet.field={0}".format(facet)
    solr_search += "&start={0}".format(offset)
    solr_search += "&rows={0}".format(limit)
    if query is None:
        solr_search += "&q=*:*"
    else:
        solr_search += "&q={0}".format(query)
    if constraints:
        keys = []
        vals = []
        constraints = constraints.split(',')
        for constraint in constraints:
            keyval = constraint.split(':')
            # The constraint key has to be in the Solr index schema, here
            # the values in default_facets are used as a validation.
            if keyval[0][-1] == '!':
                if keyval[0][:-1] not in default_facets:
                    continue
                keys.append('-{0}'.format(keyval[0][:-1]))
            else:
                if keyval[0] not in default_facets:
                    continue
                keys.append(keyval[0])
            vals.append(keyval[1])
        for i, key in enumerate(keys):
            if ((keys.count(key) > 1) and (keys.index(key) != i) and
                    (key[0] != '-')):
                continue
            fq_str = '&fq={0}:"{1}"'.format(key, vals[i])
            if key[0] != '-':
                for j, other_key in enumerate(keys[i + 1:]):
                    if other_key == key:
                        fq_str += ' || {0}:"{1}"'.format(key, vals[i + j + 1])
            solr_search += fq_str
    if fields:
        solr_search += "&fl={0},score".format(fields)
    else:
        solr_search += "&fl=*,score".format(fields)
    # For now, all items in the PAVICS solr index are files.
    if search_type:
        solr_search += "&fq=type:File"
    solr_search += "&sort=id+asc"
    solr_search += "&wt=json"
    solr_search += "&indent=true"
    solr_search_url = solr_url + solr_search.lstrip('&')
    r = requests.get(solr_search_url)
    if not r.ok:
        r.raise_for_status()
    solr_result = r.json()
    solr_result = restructure_type(solr_result, add_default_min_max,
                                   search_type)
    return (solr_result, solr_search_url)


def esgf_search(esgf_server, facets=None, offset=0, limit=10, fields=None,
                query=None, constraints=None, search_type='Dataset',
                distrib=True, **kwargs):
    """Search ESGF server.

    Parameters
    ----------
    esgf_server : string
        e.g. https://esgf-node.llnl.gov/esg-search
    facets : string
        comma separated list of facets; facets are searchable indexing
        terms in the Solr database
    offset : int
        where to start in the document count of the Solr search
    limit : int
        maximum number of documents to return
    fields : string
        comma separated list of fields to return
    query : string
        direct query to the Solr database
    constraints : string
        comma separated list of facets and their search value separated
        by colon (e.g. model:CRCM4,experiment:rcp85)
    search_type : string
        one of 'Dataset', 'File', 'Aggregate'
    distrib : bool
        distributed search
    add_default_min_max : bool
        whether to add default color palette information to search result

    Returns
    -------
    (out1, out2) : strings
        out1 - json response from the ESGF server
        out2 - url used for the ESGF request

    """

    esgf_url = os.path.join(esgf_server, 'search?')
    esgf_search = ''
    # facets (default to not provided)
    if facets:
        esgf_search += '&facets={0}'.format(facets)
    # shards (default to all available)
    # offset
    esgf_search += '&offset={0}'.format(str(offset))
    # limit (Solr: rows=0)
    esgf_search += '&limit={0}'.format(str(limit))
    # fields (default fl=*%2Cscore)
    if fields:
        esgf_search += '&fields={0}'.format(fields)
    # format (force json output, Solr: wt=json)
    esgf_search += '&format=application%2Fsolr%2Bjson'
    # query (default *:*)
    if query:
        esgf_search += '&query={0}'.format(query)
    # Facet Queries
    if constraints:
        constraints = constraints.split(',')
        for constraint in constraints:
            keyval = constraint.split(':')
            esgf_search += '&{0}={1}'.format(keyval[0], keyval[1])
    # distrib (default true)
    if distrib is False:
        esgf_search += '&distrib=false'
    # id
    # master_id
    # instance_id
    # title
    # description
    # type (default Dataset, Solr: fq=type:Dataset)
    if search_type:
        esgf_search += '&type={0}'.format(search_type)
    # replica
    # latest
    # data_node
    # index_node
    # version
    # timestamp
    # url
    # access
    # xlink
    # size
    # checksum
    # checksum_type
    # number_of_files
    # number_of_aggregations
    # dataset_id
    # tracking_id
    # drs_id
    # start
    # end
    # bbox
    # from
    # to
    esgf_search_url = esgf_url + esgf_search.lstrip('&')
    r = requests.get(esgf_search_url)
    if not r.ok:
        r.raise_for_status()
    return (r.json(), esgf_search_url)


def aggregate_solr_responses(solr_r1, solr_r2):
    solr_agg = copy.deepcopy(solr_r1)
    # responseHeader
    rheader1 = solr_agg['responseHeader']
    rheader2 = solr_r2['responseHeader']
    if 'Nquery' in rheader1:
        rheader1['Nquery'] += 1
    else:
        rheader1['Nquery'] = 2
    rheader1['QTime'] += rheader2['QTime']
    # responseHeader/params
    # here we would like some uniformity across the responses, otherwise
    # we might be mixing apples and oranges.
    # response
    if 'response' in solr_agg:
        response1 = solr_agg['response']
        response2 = solr_r2['response']
        response1['numFound'] += response2['numFound']
        response1['docs'].extend(response2['docs'])
    # facet_counts
    if 'facet_counts' in solr_agg:
        facet_counts1 = solr_agg['facet_counts']
        facet_counts2 = solr_r2['facet_counts']
        # facet_counts/facet_fields
        facet_fields1 = facet_counts1['facet_fields']
        facet_fields2 = facet_counts2['facet_fields']
        for key, val in facet_fields2.items():
            if key in facet_fields1:
                # assume the value, count pair are always there, is that really
                # the case?
                for i, item in enumerate(val[::2]):
                    if item in facet_fields1[key]:
                        facet_fields1[key][2 * i + 1] += val[2 * i + 1]
                    else:
                        facet_fields1[key].extend([item, val[2 * i + 1]])
            else:
                facet_fields1[key] = val
    return solr_agg


def pavics_and_esgf_search(solr_servers, esgf_servers,
                           add_default_min_max=True, **kwargs):
    """Combine PAVICS search with ESGF search

    Parameters
    ----------
    solr_servers : list of strings
        e.g. http://x.x.x.x:8983/solr/core_name/
    esgf_servers : list of strings
        e.g. https://esgf-node.llnl.gov/esg-search
    add_default_min_max : bool
        whether to add default color palette information to search result

    Returns
    -------
    (out1, out2) : strings
        out1 - json response from the ESGF server
        out2 - url used for the ESGF request

    Notes
    -----
    When providing multiple esgf_servers, should use distrib=False since
    their index should be synchronized.

    """

    combined_solr = None
    for solr_server in solr_servers:
        if combined_solr is None:
            (combined_solr, search_url) = pavicsearch(
                solr_server, add_default_min_max=add_default_min_max, **kwargs)
        else:
            (solr_result, search_url) = pavicsearch(
                solr_server, add_default_min_max=add_default_min_max, **kwargs)
            combined_solr = aggregate_solr_responses(combined_solr,
                                                     solr_result)
    # Forcing search_type to File and reconstructing search type. Should check
    # whether Dataset and Aggregate actually work...
    if 'search_type' in kwargs:
        search_type = kwargs['search_type']
        kwargs['search_type'] = 'File'
    else:
        kwargs['search_type'] = 'File'
        search_type = 'File'
    for esgf_server in esgf_servers:
        if combined_solr is None:
            (combined_solr, search_url) = esgf_search(esgf_server, **kwargs)
            combined_solr = restructure_urls(combined_solr)
            combined_solr = restructure_type(
                combined_solr, add_default_min_max, search_type)
        else:
            (solr_result, search_url) = esgf_search(esgf_server, **kwargs)
            solr_result = restructure_urls(solr_result)
            solr_result = restructure_type(
                solr_result, add_default_min_max, search_type)
            combined_solr = aggregate_solr_responses(combined_solr,
                                                     solr_result)
    return combined_solr


def list_of_files_from_pavicsearch(search_result, output_type='opendap_url'):
    """List of file from a pavicsearch result.

    Parameters
    ----------
    search_result : string
        output from pavicsearch

    Returns
    -------
    out : string
        json of list of opendap urls from the search result

    """

    list_of_files = []
    for doc in search_result['response']['docs']:
        if output_type not in doc:
            continue
        if hasattr(doc[output_type], 'append'):
            list_of_files.extend(doc[output_type])
        else:
            list_of_files.append(doc[output_type])
    return list_of_files
