"""
=======
Catalog
=======

Functions:

 * :func:`pavicrawler` - Crawl thredds server and output to Solr.
 * :func:`pavicsvalidate` - Validate Solr database for required facets.
 * :func:`pavicsupdate` - Update Solr database.
 * :func:`pavicsearch` - Search Solr database.

"""

import os
import copy
import json
import calendar
import requests
import logging

import threddsclient
import netCDF4

from . import nctime

logger = logging.getLogger(__name__)


# These definitions should be moved to a config file
solr_fields_type = {'datetime_max': 'long',
                    'datetime_min': 'long',
                    'latest': 'boolean',
                    'replica': 'boolean'}

netcdf_ignored_variables = [
    'time', 'time_bnds', 'time_bounds', 'time_vectors', 'lat', 'lon', 'yc',
    'xc', 'rlat', 'rlon', 'lat_bnds', 'lat_bounds', 'lon_bnds', 'lon_bounds',
    'yc_bnds', 'xc_bnds', 'yc_bounds', 'xc_bounds', 'rlat_bnds', 'rlat_bounds',
    'rlon_bnds', 'rlon_bounds', 'level', 'level_bnds', 'level_bounds', 'plev']

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


class MissingThreddsFile(Exception):
    pass


def _modify_wms_url(thredds_dataset, wms_alternate_server=None):
    wms_url = thredds_dataset.wms_url()
    if wms_alternate_server is not None:
        thredds_id = thredds_dataset.ID
        thredds_rel_path = thredds_id[thredds_id.find('/') + 1:]
        wms_url = wms_alternate_server.replace('<DATASET>', thredds_rel_path)
    return wms_url


def aggregate_solr_responses(solr_response1, solr_response2):
    solr_response_agg = copy.deepcopy(solr_response1)
    qtime2 = solr_response2['responseHeader']['QTime']
    solr_response_agg['responseHeader']['QTime'] += qtime2
    if 'Nquery' in solr_response_agg['responseHeader']:
        solr_response_agg['responseHeader']['Nquery'] += 1
    else:
        solr_response_agg['responseHeader']['Nquery'] = 2
    return solr_response_agg


def _split_solr_update(solr_server, add_data, split_update=None):
    if split_update:
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


def thredds_crawler(thredds_server, index_facets, depth=50,
                    ignored_variables=None, set_dataset_id=False,
                    overwrite_dataset_id=False,
                    wms_alternate_server=None, target_files=None):
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
        only those file names will be crawled, can be urls or full paths,
        but only the file name will be used (if two files have the same name
        in different folders of the thredds catalog, they are both picked up
        by the crawler). If this is provided, all target files must be
        found.

    Returns
    -------
    out : list of dict

    Notes
    -----
    1. Variables that do not have any of 'standard_name', 'long_name' or
       'units' attributes are ignored. If only one or two of those are
       missing, they are set to '_undefined' in the database.

    """

    if ignored_variables is None:
        ignored_variables = netcdf_ignored_variables

    add_data = []
    # Get only filenames of target_files
    target_file_names = []
    if target_files:
        for target_file in target_files:
            target_file_names.append(os.path.basename(target_file))

    targets_found = []
    for thredds_dataset in threddsclient.crawl(thredds_server, depth=depth):
        if target_files:
            thredds_file_name = os.path.basename(thredds_dataset.ID)
            if thredds_file_name not in target_file_names:
                continue
            targets_found.append(thredds_file_name)

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
               'source': os.path.join(urls['thredds_server'], 'catalog.xml'),
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
            ci = urls['catalog_url'].find('/catalog.xml')
            did = '.'.join(urls['catalog_url'][:ci].split('/')[6:])
            doc['dataset_id'] = did
            if overwrite_dataset_id and ('dataset_id' in index_facets):
                index_facets.remove('dataset_id')
            elif (not overwrite_dataset_id) and \
                 ('dataset_id' not in index_facets):
                index_facets.append('dataset_id')

        # Add custom facets
        # In the ESGF implementation, all facets are stored in multivalued
        # fields, not sure how this is ever used... Not following this
        # convention here...
        for facet in index_facets:
            if hasattr(nc, facet):
                doc[facet] = getattr(nc, facet)
            elif hasattr(nc, facet + '_id'):
                doc[facet] = getattr(nc, facet + '_id')

        # Replica and latest
        # Setting defaults here, to be modified by other operations.
        doc['replica'] = False
        doc['latest'] = True

        # Datetime min/max
        if datetime_min:
            # Apparently, calendar.timegm can take dates from irregular
            # calendars. 2003-03-01 & 2003-02-29 (not a valid gregorian date)
            # both return the same result...
            # Not sure what happens to time zones here...
            doc['datetime_min'] = calendar.timegm(datetime_min.timetuple())
        if datetime_max:
            doc['datetime_max'] = calendar.timegm(datetime_max.timetuple())

        if ignored_variables != 'all':
            for var_name in nc.variables:
                if var_name in ignored_variables:
                    continue
                ncvar = nc.variables[var_name]
                ccf = hasattr(ncvar, 'standard_name')
                clong = hasattr(ncvar, 'long_name')
                cunits = hasattr(ncvar, 'units')
                # if there is no standard name, long_name or units, ignore it
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
        if set(target_file_names) != set(targets_found):
            missing_files = set(target_file_names) - set(targets_found)
            raise MissingThreddsFile(
                "One or more target file not found: {0}".format(
                    str(list(missing_files))))
    return add_data


def pavicrawler(thredds_server, solr_server, index_facets, depth=50,
                ignored_variables=None, set_dataset_id=False,
                overwrite_dataset_id=False, wms_alternate_server=None,
                target_files=None, check_replica=True, split_update=100):
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
        only those file names will be crawled. If this is provided, all target
        files must be found or nothing will be added to solr.
    check_replica : bool
        if True, will search for identical file names and dataset_id in solr
        and tag this instance as replica=True if it already exists on
        another thredds server.
    split_update : int
        how many datasets will be updated per solr request.

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
                               target_files=target_files)

    solr_response = {'responseHeader':
                     {'QTime': 0, 'status': 0, 'Nquery': 0}}
    # check for replica
    if check_replica:
        add_raw = []
        add_refresh = []
        for doc in add_data:
            search_dict = pavicsearch(
                solr_server, limit=1000, search_type='File',
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
                if hasattr(ref_doc[attr], 'append'):
                    if not hasattr(doc[attr], 'append'):
                        ref_doc[attr].append(doc[attr])
                    else:
                        for attr_value in doc[attr]:
                            if attr_value not in ref_doc[attr]:
                                ref_doc[attr].append(attr_value)
        else:
            # Not sure dataset_id should be used for this purpose, may be
            # changed in the future...
            known_datasets.append(doc['dataset_id'])
            first_instances.append(i)
            for attr in doc:
                if not hasattr(doc[attr], 'append'):
                    doc[attr] = [doc[attr]]
            doc['type'] = 'Aggregate'
            doc['aggregate_title'] = doc['dataset_id'][0]
    for i in range(len(search_results['response']['docs']) - 1, -1, -1):
        if i not in first_instances:
            search_results['response']['docs'].pop(i)

    # reorder based on opendap_url name
    for doc in search_results['response']['docs']:
        order_attr = doc['opendap_url']
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


def pavicsearch(solr_server, facets=None, limit=10, offset=0,
                search_type='Dataset', output_format='application/solr+json',
                fields=None, constraints=None, query=None,
                add_default_min_max=True):
    """Search Solr database.

    Parameters
    ----------
    solr_server : string
        usually of the form 'http://x.x.x.x:8983/solr/core_name/'
    facets : string
        comma separated list of facets; facets are searchable indexing
        terms in the Solr database
    limit : int
        maximum number of documents to return
    offset : int
        where to start in the document count of the Solr search
    search_type : string
        one of 'Dataset', 'File', 'Aggregate' or 'FileAsAggregate'
    output_format : string
        one of 'application/solr+json' or 'application:solr+xml'
    fields : string
        comma separated list of fields to return
    constraints : string
        comma separated list of facets and their search value separated
        by colon (e.g. model:CRCM4,experiment:rcp85)
    query : string
        direct query to the Solr database
    add_default_min_max : bool
        whether to add default color palette information to search result

    Returns
    -------
    out : string
        json response from the Solr server

    """

    my_search = ""
    my_query = "&q="
    if facets:
        my_search += "&facet=true"
        if facets != '*':
            facets = facets.split(',')
        else:
            facets = default_facets
        for facet in facets:
            my_search += "&facet.field=%s" % (facet,)
    my_search += "&rows=%s" % (limit,)
    my_search += "&start=%s" % (offset,)
    if output_format == 'application/solr+json':
        my_search += "&wt=json"
    else:
        # No longer supporting application:solr+xml (&wt=xml)
        raise NotImplementedError()
    if fields is not None:
        my_search += "&fl={0}".format(fields)
    if constraints is not None:
        constraints = constraints.split(',')
        for constraint in constraints:
            keyval = constraint.split(':')
            # According to the ESGF API, if a key appears more than
            # once, it is an OR statement, this should be added
            # eventually. Similarly, there is support for not equal
            # != ...
            my_query += '+{0}:{1}'.format(keyval[0], keyval[1])
    if query is not None:
        my_query += '+{0}'.format(query)
    my_search += "&indent=true"
    if my_query != '&q=':
        my_search += my_query
    my_search = my_search.lstrip('&')
    solr_call = os.path.join(solr_server, 'select?{0}'.format(my_search))
    r = requests.get(solr_call)
    if not r.ok:
        r.raise_for_status()
    search_result = r.json()
    if add_default_min_max:
        search_result = add_default_min_max_to_solr_search(search_result)
    if search_type == 'Dataset':
        search_result = datasets_from_solr_search(search_result)
    elif search_type == 'Aggregate':
        search_result = aggregate_from_solr_search(search_result)
    elif search_type == 'FileAsAggregate':
        search_result = file_as_aggregate_from_solr_search(search_result)
    return search_result


def list_of_files_from_pavicsearch(search_result):
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
        if hasattr(doc['opendap_url'], 'append'):
            list_of_files.extend(doc['opendap_url'])
        else:
            list_of_files.append(doc['opendap_url'])
    return list_of_files
