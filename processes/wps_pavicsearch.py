import os
import time
import random
import hashlib
import json
import urllib2
from pywps import Process,Format,FORMATS
from pywps import LiteralInput,LiteralOutput,ComplexOutput

# Example usage:
#
# List facets values:
#localhost/pywps?service=WPS&request=execute&version=1.0.0&\
#identifier=pavicsearch&DataInputs=facets=*
#
# Search by facet:
#localhost/pywps?service=WPS&request=execute&version=1.0.0&\
#identifier=pavicsearch&DataInputs=constraints=model:CRCM4,experiment:rcp85

# base_search_URL in the ESGF Search API is now a solr database URL,
# this is provided as the environment variable SOLR_SERVER.
solr_server = "http://%s:8983/solr/birdhouse/" % (os.environ['SOLR_SERVER'],)
# The place where we save the resulting json files should also be in
# a config file, the user under which apache is running must be able
# to write to that directory.
json_output_path = '/var/www/html/wps_results'
json_output_url = "http://%s:8009/wps_results/" % (os.environ['SOLR_SERVER'],)

json_format = Format('application/json')
gmlxml_format = Format('application/gml+xml')

class PavicsSearch(Process):
    def __init__(self):
        inputs = [LiteralInput('facets',
                               'Facet values and counts',
                               data_type='string',
                               default='',
                               min_occurs=0,
                               mode=None),
                  LiteralInput('shards',
                               'Shards to be queried',
                               data_type='string',
                               default='*',
                               min_occurs=0,
                               mode=None),
                  LiteralInput('offset',
                               'Pagination offset',
                               data_type='integer',
                               default=0,
                               min_occurs=0,
                               mode=None),
                  LiteralInput('limit',
                               'Pagination limit',
                               data_type='integer',
                               default=0,
                               min_occurs=0,
                               mode=None),
                  LiteralInput('fields',
                               'Metadata fields to return',
                               data_type='string',
                               default='*',
                               min_occurs=0,
                               mode=None),
                  LiteralInput('format',
                               'Output Format',
                               data_type='string',
                               default='application/solr+json',
                               min_occurs=0,
                               mode=None),
                  LiteralInput('query',
                               'Free text search',
                               data_type='string',
                               default='*',
                               min_occurs=0,
                               mode=None),
                  LiteralInput('distrib',
                               'Distributed query',
                               data_type='boolean',
                               default=False,
                               min_occurs=0,
                               mode=None),
                  LiteralInput('type',
                               'Type of the record',
                               data_type='string',
                               default='Dataset',
                               min_occurs=0,
                               mode=None),
                  LiteralInput('constraints',
                               'Search constraints',
                               data_type='string',
                               default='',
                               min_occurs=0,
                               mode=None),]
        outputs = [LiteralOutput('search_result',
                                 'PAVICS Catalogue Search Result',
                                 data_type='string')]

        super(PavicsSearch,self).__init__(
            self._handler,
            identifier='pavicsearch',
            title='PAVICS Catalogue Search',
            version='0.1',
            inputs=inputs,
            outputs=outputs,
            store_supported=True,
            status_supported=True)

    def _handler(self,request,response):
        my_search = ""
        my_query = "&q="
        facets = request.inputs['facets'][0].data
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
        limit = request.inputs['limit'][0].data
        if limit is None:
            limit = request.inputs['limit'][0].default
        my_search += "&rows=%s" % (limit,)
        offset = request.inputs['offset'][0].data
        if offset is None:
            offset = request.inputs['offset'][0].default
        my_search += "&start=%s" % (offset,)
        output_format = request.inputs['format'][0].data
        if output_format is None:
            output_format = request.inputs['format'][0].default
        if output_format == 'application:solr/xml':
            my_search += "&wt=xml"
        elif output_format == 'application/solr+json':
            my_search += "&wt=json"
        else:
            raise NotImplementedError()
        fields = request.inputs['fields'][0].data
        if fields is not None:
            my_search += "&fl=%s" % (fields,)
        constraints = request.inputs['constraints'][0].data
        if constraints is not None:
            constraints = constraints.split(',')
            for constraint in constraints:
                keyval = constraint.split(':')
                # According to the ESGF API, if a key appears more than
                # once, it is an OR statement, this should be added
                # eventually. Similarly, there is support for not equal
                # != ...
                my_query += '+%s:%s' % (keyval[0],keyval[1])
        query = request.inputs['query'][0].data
        if query is not None:
            my_query += '+%s' % (query,)
        my_search += "&indent=true"
        if my_query != '&q=':
            my_search += my_query
        #my_search = 'q=aet&wt=json&indent=true'
        my_search = my_search.lstrip('&')
        #response.outputs['search_result'].data = solr_server+'select?'+my_search
        #return response
        url_request = urllib2.Request(url=solr_server+'select?'+my_search)
        url_request.add_header('Content-type','application/json')
        url_response = urllib2.urlopen(url_request)
        search_result = url_response.read()
        # Here we construct a unique filename
        md5_str = hashlib.md5(search_result+str(random.random())).hexdigest()
        time_str = time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())
        output_file_name = "json_result_%s_%s.json" % (time_str,md5_str[0:8])
        f1 = open(os.path.join(json_output_path,output_file_name),'w')
        f1.write(search_result)
        f1.close()
        if output_format == 'application/solr+json':
            json_url = os.path.join(json_output_url,output_file_name)
            response.outputs['search_result'].data = json_url
        elif output_format == 'application/solr+xml':
            # Output as xml...
            raise NotImplementedError()
        else:
            # Unsupported format
            raise NotImplementedError()
        return response
