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

import threddsclient
import netCDF4

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
            return 'Unknown field'
        else:
            raise err
    update_result = url_response.read()
    url_response.close()

    return update_result

def thredds_crawler(thredds_server,index_facets,depth=50,
                    ignored_variables=None,
                    internal_ip=None,external_ip=None,
                    output_internal_ip=False):
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
    internal_ip : string
    external_ip : string
    output_internal_ip : bool

    Returns
    -------
    out : list of dict

    Notes
    -----
    1. Variables that do not have any of 'standard_name', 'long_name' or
       'units' attributes are ignoreds. If only one or two of those are
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
        urls = {'opendap_url':thredds_dataset.opendap_url(),
                'download_url':thredds_dataset.download_url(),
                'thredds_server':thredds_server,
                'catalog_url':thredds_dataset.catalog.url,
                'wms_url':thredds_dataset.wms_url()}
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
            for key in urls.keys():
                if output_internal_ip:
                    urls[key] = urls[key].replace(external_ip,internal_ip)
                else:
                    urls[key] = urls[key].replace(internal_ip,external_ip)
        # Default Birdhouse catalog entry
        doc = {'url':urls['download_url'],
               'source':os.path.join(urls['thredds_server'],'catalog.xml'),
               'catalog_url':urls['catalog_url']+'?dataset='+\
                             thredds_dataset.ID,
               'category':'thredds',
               'content_type':thredds_dataset.content_type,
               'opendap_url':urls['opendap_url'],
               'title':thredds_dataset.name,
               'last_modified':thredds_dataset.modified,
               'wms_url':urls['wms_url'],
               'resourcename':thredds_dataset.url_path,
               'subject':'Birdhouse Thredds Catalog'}
        # Add custom facets
        for facet in index_facets:
            if hasattr(nc,facet):
                doc[facet] = getattr(nc,facet)
            elif hasattr(nc,facet+'_id'):
                doc[facet] = getattr(nc,facet+'_id')
        if ignored_variables != 'all':
            for var_name in nc.variables.keys():
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
                    if not doc.has_key(parameter_name):
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
                ignored_variables=None,
                internal_ip=None,external_ip=None,output_internal_ip=False):
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
    internal_ip : string
    external_ip : string
    output_internal_ip : bool

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
                               internal_ip=internal_ip,external_ip=external_ip,
                               output_internal_ip=output_internal_ip)
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
    #https://cwiki.apache.org/confluence/display/solr/Pagination+of+Results
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
            if (not doc.has_key('source')) or (not doc.has_key('url')):
                continue
            missing_facets = []
            for required_facet in required_facets:
                if not doc.has_key(required_facet):
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
        key:value pairs to update including the id key for reference

    Returns
    -------
    out : string
        json response from the Solr server

    Notes
    -----
    1. The current implementation only updates one entry at a time.

    """

    # Get all the information of the current Solr document
    my_search = "q=id:%s&wt=json" % (update_dict['id'])
    url_request = urllib2.Request(url=solr_server+"select?%s" % (my_search,))
    url_response = urllib2.urlopen(url_request)
    search_result = url_response.read()
    url_response.close()
    search_dict = json.loads(search_result)
    data = search_dict['response']['docs']
    # Remove self-generated fields
    for key in ['id','_version_','keywords','abstract']:
        data[0].pop(key,None)
    for key,value in update_dict.items():
        if key == 'id':
            continue
        data[0][key] = value
    return solr_update(data)

def pavicsearch(solr_server,facets=None,limit=10,offset=0,
                output_format='application/solr+json',fields=None,
                constraints=None,query=None,):
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
    return search_result
