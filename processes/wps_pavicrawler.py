import os
import time
import random
import hashlib
import json
import urllib2
from pywps import Process,Format
from pywps import LiteralInput,LiteralOutput

import netCDF4

# Example usage:
#localhost/pywps?service=WPS&request=execute&version=1.0.0&\
#identifier=pavicrawler

# Current behaviour: values in the NetCDF files take precedence over the
# values in the Solr database. This could be an option as an input...

# The list of metadata to scan should be in a config file, let's input
# it manually for now:
my_facets = ['experiment','frequency','institute','model','project']
# variable, variable_long_name and cf_standard_name, are not necessarily
# in the global attributes, need to come back for this later...

# base_search_URL in the ESGF Search API is now a solr database URL,
# this is provided as the environment variable SOLR_SERVER.
solr_server = "http://%s:8983/solr/birdhouse/" % (os.environ['SOLR_SERVER'],)
# The place where we save the resulting json files should also be in
# a config file, the user under which apache is running must be able
# to write to that directory.
json_output_path = '/var/www/html/wps_results'
json_output_url = 'http://localhost/wps_results/'

# Fix for OpenStack internal/external ip:
# (the internal ip is the environment variable OPENSTACK_INTERNAL_IP)
if os.environ.has_key('OPENSTACK_INTERNAL_IP'):
    internal_ip = os.environ['OPENSTACK_INTERNAL_IP']
else:
    internal_ip = os.environ['SOLR_SERVER']
external_ip = os.environ['SOLR_SERVER']

class PavicsCrawler(Process):
    def __init__(self):
        # The combination of the 'source' and 'url' fields provide the 'id'
        # in the Solr database, they both must be provided.
        inputs = []
        outputs = [LiteralOutput('crawler_result',
                                 'PAVICS Crawler Result',
                                 data_type='string')]

        super(PavicsCrawler,self).__init__(
            self._handler,
            identifier='pavicrawler',
            title='PAVICS Crawler',
            version='0.1',
            inputs=inputs,
            outputs=outputs,
            store_supported=True,
            status_supported=True)

    def _handler(self,request,response):
        # Number of documents returned, this should be larger in deployed
        # version.
        nrows = 10
        n = 0
        # Note that in Solr, if indexing changes due to document modification,
        # or new documents, the pagination loop could return the same
        # document twice or miss a document.
        #https://cwiki.apache.org/confluence/display/solr/Pagination+of+Results
        update_data = []
        while True:
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
                c1 = not doc.has_key('source') 
                c2 = not doc.has_key('url')
                c3 = not doc.has_key('opendap_url')
                if c1 or c2 or c3:
                    continue
                # Here things could go wrong if opening the file fails
                # Need to handle the error if that's the case (ToDo)...
                # First thing: the Solr URLs use internal ip address, but from
                # outside we need the external ip address.
                try:
                    warp = doc['opendap_url'].replace(internal_ip,
                                                      external_ip)
                    nc = netCDF4.Dataset(warp,'r')
                except:
                    response.outputs['crawler_result'].data = warp+str(n)
                    return response
                new_facet = False
                for facet in my_facets:
                    if hasattr(nc,facet):
                        new_value = getattr(nc,facet)
                        if doc.has_key(facet) and doc[facet] == new_value:
                            continue
                        doc[facet] = new_value
                        new_facet = True
                    elif hasattr(nc,facet+'_id'):
                        new_value = getattr(nc,facet+'_id')
                        if doc.has_key(facet) and doc[facet] == new_value:
                            continue
                        doc[facet] = new_value
                        new_facet = True
                if new_facet:
                    # These are the fields in the birdhouse-solr schema
                    # that are reserved or built from other fields.
                    doc.pop('id')
                    doc.pop('_version_')
                    doc.pop('keywords',None)
                    doc.pop('abstract',None)
                    update_data.append(doc)
            n += nrows
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
                msg1 = 'Unknown field'
                # Should this be a different output field?
                response.outputs['update_result'].data = msg1
                return response
            else:
                raise err
        update_result = url_response.read()
        url_response.close()

        # Here we construct a unique filename
        md5_str = hashlib.md5(update_result+str(random.random())).hexdigest()
        time_str = time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())
        output_file_name = "json_result_%s_%s.json" % (time_str,md5_str[0:8])
        f1 = open(os.path.join(json_output_path,output_file_name),'w')
        f1.write(update_result)
        f1.close()
        json_url = os.path.join(json_output_url,output_file_name)
        response.outputs['crawler_result'].data = json_url
        return response
