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
import json
import urllib2
import requests

import threddsclient
import netCDF4


def solr_add_field(solr_server,field_name,field_type='string'):
    """Add a field in a Solr database.

    Parameters
    ----------
    solr_server : string
        usually of the form 'http://x.x.x.x:8983/solr/core_name/'
    field_name : string
    field_type : string

    Returns
    -------
    out : string
        json response from the Solr server

    """

    schema_path = os.path.join(solr_server,'schema')
    add_field = {'add-field':{'name':field_name,
                              'type':field_type,
                              'stored':'true'}}
    headers = {'Content-type':'application/json'}
    r = requests.post(schema_path,data=json.dumps(add_field),headers=headers)
    return r._content


def solr_update(solr_server,update_data):
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

    solr_json_input = json.dumps(update_data)
    # Send Solr update request
    solr_method = 'update/json?commit=true'
    url_request = urllib2.Request(url=solr_server+solr_method,
                                  data=solr_json_input)
    url_request.add_header('Content-type','application/json')
    try:
        url_response = urllib2.urlopen(url_request)
    except urllib2.HTTPError as err:
        if err.msg == 'Bad Request':
            # One of the most likely reason for this is trying to add
            # a field that is not part of the Solr Schema.
            fields_path = os.path.join(solr_server,'schema','fields?wt=json')
            fields_request = urllib2.urlopen(fields_path)
            fields_response = json.loads(fields_request.read())
            list_of_fields = []
            for one_field in fields_response['fields']:
                list_of_fields.append(one_field['name'])
            if not hasattr(update_data,'append'):
                update_data = [update_data]
            for one_update in update_data:
                for one_key in one_update.keys():
                    if one_key not in list_of_fields:
                        # New fields are added as string type (default)
                        solr_add_field(solr_server,one_key)
                        list_of_fields.append(one_key)
            url_response = urllib2.urlopen(url_request)
            #return 'Unknown field'
        else:
            raise err
    update_result = url_response.read()
    url_response.close()

    return update_result


def thredds_crawler(thredds_server,index_facets,depth=50,
                    ignored_variables=None,set_dataset_id=False,
                    overwrite_dataset_id=False,internal_ip=None,
                    external_ip=None,output_internal_ip=False,
                    wms_original_server=None,wms_alternate_server=None):
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
        if true, the files that are in a common directory will have
        their dataset_id set to reflect that unique relative directory
    overwrite_dataset_id : bool
        if set_dataset_id is True, this decides whether the default value
        constructed by the crawler takes precedence over the dataset_id in
        the NetCDF file.
    internal_ip : string
    external_ip : string
    output_internal_ip : bool
    wms_original_server : string
    wms_alternate_server : string

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
        ignored_variables = ['time','time_bnds','time_bounds',
                             'time_vectors','lat','lon','yc','xc',
                             'rlat','rlon','lat_bnds','lat_bounds',
                             'lon_bnds','lon_bounds','yc_bnds','xc_bnds',
                             'yc_bounds','xc_bounds','rlat_bnds',
                             'rlat_bounds','rlon_bnds','rlon_bounds',
                             'level','level_bnds','level_bounds']

    add_data = []
    for thredds_dataset in threddsclient.crawl(thredds_server,depth=depth):
        wms_url = thredds_dataset.wms_url()
        # Change wms_url server if requested
        if wms_alternate_server is not None:
            wms_url = wms_url.replace(wms_original_server,
                                      wms_alternate_server,1)
        urls = {'opendap_url':thredds_dataset.opendap_url(),
                'download_url':thredds_dataset.download_url(),
                'thredds_server':thredds_server,
                'catalog_url':thredds_dataset.catalog.url,
                'wms_url':wms_url}
        # Here, if opening the NetCDF file fails, we simply continue
        # to the next one. Perhaps a way to track the erroneous files
        # should be considered...
        try:
            nc = netCDF4.Dataset(urls['opendap_url'],'r')
        except:
            continue
        # Modifying all the ip addresses if there is an issue of
        # internal/external ips (e.g. OpenStack)
        if internal_ip is not None:
            for key in urls:
                if output_internal_ip:
                    urls[key] = urls[key].replace(external_ip,internal_ip)
                else:
                    urls[key] = urls[key].replace(internal_ip,external_ip)
        # Default Birdhouse catalog entry
        doc = {'url':urls['download_url'],
               'source':os.path.join(urls['thredds_server'],'catalog.xml'),
               'catalog_url':urls['catalog_url']+'?dataset='+
                             thredds_dataset.ID,
               'category':'thredds',
               'content_type':thredds_dataset.content_type,
               'opendap_url':urls['opendap_url'],
               'title':thredds_dataset.name,
               'last_modified':thredds_dataset.modified,
               'wms_url':urls['wms_url'],
               'resourcename':thredds_dataset.url_path,
               'subject':'Birdhouse Thredds Catalog'}
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
        for facet in index_facets:
            if hasattr(nc,facet):
                doc[facet] = getattr(nc,facet)
            elif hasattr(nc,facet+'_id'):
                doc[facet] = getattr(nc,facet+'_id')
        if ignored_variables != 'all':
            for var_name in nc.variables:
                if var_name in ignored_variables:
                    continue
                ncvar = nc.variables[var_name]
                ccf = hasattr(ncvar,'standard_name')
                clong = hasattr(ncvar,'long_name')
                cunits = hasattr(ncvar,'units')
                # if there is no standard name, long_name or units, ignore it
                if not (ccf or clong or cunits):
                    continue
                for parameter_name in ['variable','cf_standard_name',
                                       'variable_long_name','units']:
                    if parameter_name not in doc:
                        doc[parameter_name] = []
                doc['variable'].append(var_name)
                if ccf:
                    value = getattr(ncvar,'standard_name')
                    doc['cf_standard_name'].append(value)
                else:
                    doc['cf_standard_name'].append('_undefined')
                if clong:
                    value = getattr(ncvar,'long_name')
                    doc['variable_long_name'].append(value)
                else:
                    doc['variable_long_name'].append('_undefined')
                if ccf:
                    doc['units'].append(getattr(ncvar,'units'))
                else:
                    doc['units'].append('_undefined')
        nc.close()
        add_data.append(doc)
    return add_data


def pavicrawler(thredds_server,solr_server,index_facets,depth=50,
                ignored_variables=None,set_dataset_id=False,
                overwrite_dataset_id=False,internal_ip=None,
                external_ip=None,output_internal_ip=False,
                wms_original_server=None,wms_alternate_server=None):
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
    internal_ip : string
    external_ip : string
    output_internal_ip : bool
    wms_original_server : string
    wms_alternate_server : string

    Returns
    -------
    out : string
        json response from the Solr server

    Notes
    -----
    1. Each NetCDF file served bu the thredds server is scanned to extract
       metadata (CF standard vocabulary). This metadata is then added
       to the Solr database so it can index the file by its properties.
    2. Currently, the index facets must be part of the default Birdhouse
       Solr Schema.
    3. Variables that do not have any of 'standard_name', 'long_name' or
       'units' attributes are ignoreds. If only one or two of those are
       missing, they are set to '_undefined' in the database.

    """

    add_data = thredds_crawler(thredds_server,index_facets,depth=50,
                               ignored_variables=ignored_variables,
                               set_dataset_id=set_dataset_id,
                               overwrite_dataset_id=overwrite_dataset_id,
                               internal_ip=internal_ip,external_ip=external_ip,
                               output_internal_ip=output_internal_ip
                               wms_original_server=wms_original_server,
                               wms_alternate_server=wms_alternate_server)
    return solr_update(solr_server,add_data)


def pavicsvalidate(solr_server,required_facets,limit_paths=None,
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

    if (limit_paths is not None) and (not hasattr(limit_paths,'append')):
        limit_paths = [limit_paths]
    if (limit_files is not None) and (not hasattr(limit_files,'append')):
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
            limit_search = limit_search[:-4]+')'
        if limit_files:
            if limit_search != '':
                limit_search = limit_search + '+AND+('
            else:
                limit_search = limit_search + '('
            for limit_file in limit_files:
                limit_search = limit_search + limit_file + '+OR+'
            limit_search = limit_search[:-4]+')'
        if limit_search != '':
            warp = (limit_search,str(n),str(n+nrows))
            my_search = 'q=%s&start=%s&rows=%s&wt=json' % warp
        else:
            warp = (str(n),str(n+nrows))
            my_search = 'q=*:*&start=%s&rows=%s&wt=json' % warp
        my_url = solr_server+"select?%s" % (my_search,)
        url_request = urllib2.Request(url=my_url)
        url_response = urllib2.urlopen(url_request)
        search_result = url_response.read()
        url_response.close()
        search_dict = json.loads(search_result)
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
                if hasattr(doc[required_facet],'append'):
                    if len(doc[required_facet]) == 0:
                        missing_facets.append(required_facet)
                        continue
                    for value in doc[required_facet]:
                        if value in ['','_undefined']:
                            missing_facets.append(required_facet)
                            continue
                # If it's an empty string, it's also considered missing
                if doc[required_facet] in ['','_undefined']:
                    missing_facets.append(required_facet)
                    continue
            if missing_facets:
                incomplete_docs.append({'source':doc['source'],
                                        'url':doc['url'],
                                        'id':doc['id'],
                                        'missing_facets':missing_facets})
        n += nrows
    return incomplete_docs


def pavicsupdate(solr_server,update_dict):
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
        my_search = "q=id:%s&wt=json" % (update_dict['id'])
    elif 'dataset_id' in update_dict:
        my_search = "q=dataset_id:%s&wt=json" % (update_dict['dataset_id'])
    url_request = urllib2.Request(url=solr_server+"select?%s" % (my_search,))
    url_response = urllib2.urlopen(url_request)
    search_result = url_response.read()
    url_response.close()
    search_dict = json.loads(search_result)
    data = search_dict['response']['docs']
    # Remove self-generated fields
    for doc in data:
        for key in ['id','_version_','keywords','abstract']:
            doc.pop(key,None)
        for (key, value) in update_dict.items():
            if key == 'id':
                continue
            doc[key] = value
    return solr_update(solr_server,data)


def datasets_from_solr_search(solr_search_result):
    """Convert a Solr search result on files to a dataset search result.

    Parameters
    ----------
    solr_search_result : string
        the json text result from a Solr query

    Returns
    -------
    out : string
        restructured json response for dataset representation

    """

    search_results = json.loads(solr_search_result)
    known_datasets = []
    first_instances = []
    for i,doc in enumerate(search_results['response']['docs']):
        if doc['dataset_id'] in known_datasets:
            refi = first_instances[known_datasets.index(doc['dataset_id'])]
            ref_doc = search_results['response']['docs'][refi]
            ref_doc['catalog_urls'].append(doc['catalog_url'])
            ref_doc['opendap_urls'].append(doc['opendap_url'])
            ref_doc['urls'].append(doc['url'])
            ref_doc['wms_urls'].append(doc['wms_url'])
            for key in ['variable','variable_long_name','units',
                        'cf_standard_name']:
                for var_name in doc[key]:
                    if var_name not in ref_doc[key]:
                        ref_doc[key].append(var_name)
            for var_name in doc['cf_standard_name']:
                if var_name not in ref_doc['keywords']:
                    ref_doc['keywords'].append(var_name)
        else:
            known_datasets.append(doc['dataset_id'])
            first_instances.append(i)
            doc['catalog_urls'] = [doc['catalog_url']]
            doc['opendap_urls'] = [doc['opendap_url']]
            doc['urls'] = [doc['url']]
            doc['wms_urls'] = [doc['wms_url']]
            for key in ['abstract','catalog_url','id','last_modified',
                        'opendap_url','resourcename','title','url','wms_url']:
                doc.pop(key,None)
    for i in range(len(search_results['response']['docs'])-1,-1,-1):
        if i not in first_instances:
            search_results['response']['docs'].pop(i)
    for doc in search_results['response']['docs']:
        for key in ['catalog_urls','opendap_urls','urls','wms_urls']:
            doc[key].sort()
    n = len(search_results['response']['docs'])
    search_results['response']['numFound'] = n
    return json.dumps(search_results)


def pavicsearch(solr_server,facets=None,limit=10,offset=0,
                search_type='Dataset',output_format='application/solr+json',
                fields=None,constraints=None,query=None,):
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
        one of 'Dataset' or 'File'
    output_format : string
        one of 'application/solr+json' or 'application:solr+xml'
    fields : string
        comma separated list of fields to return
    constraints : string
        comma separated list of facets and their search value separated
        by colon (e.g. model:CRCM4,experiment:rcp85)
    query : string
        direct query to the Solr database

    Returns
    -------
    out : string
        json (default) or xml response from the Solr server

    """

    my_search = ""
    my_query = "&q="
    if facets:
        my_search += "&facet=true"
        if facets != '*':
            facets = facets.split(',')
        else:
            # how do we know what all the fields are?
            # manual input for now...
            # In ESGF this is is a config file for each node
            # /esgf/config/facets.propertires
            facets = ['author','category','cf_standard_name','experiment',
                      'frequency','institute','model','project','source',
                      'subject','title','units','variable',
                      'variable_long_name']
        for facet in facets:
            my_search += "&facet.field=%s" % (facet,)
    my_search += "&rows=%s" % (limit,)
    my_search += "&start=%s" % (offset,)
    if output_format == 'application:solr+xml':
        my_search += "&wt=xml"
    elif output_format == 'application/solr+json':
        my_search += "&wt=json"
    else:
        raise NotImplementedError()
    if fields is not None:
        my_search += "&fl=%s" % (fields,)
    if constraints is not None:
        constraints = constraints.split(',')
        for constraint in constraints:
            keyval = constraint.split(':')
            # According to the ESGF API, if a key appears more than
            # once, it is an OR statement, this should be added
            # eventually. Similarly, there is support for not equal
            # != ...
            my_query += '+%s:%s' % (keyval[0],keyval[1])
    if query is not None:
        my_query += '+%s' % (query,)
    my_search += "&indent=true"
    if my_query != '&q=':
        my_search += my_query
    my_search = my_search.lstrip('&')
    url_request = urllib2.Request(url=solr_server+'select?'+my_search)
    url_request.add_header('Content-type','application/json')
    url_response = urllib2.urlopen(url_request)
    search_result = url_response.read()
    url_response.close()
    if search_type == 'Dataset':
        if output_format == 'application:solr+xml':
            raise NotImplementedError()
        search_result = datasets_from_solr_search(search_result)
    return search_result
