import os
import json
import urllib2
from pywps import Process,Format,FORMATS
from pywps import LiteralInput,LiteralOutput,ComplexOutput

# base_search_URL in the ESGF Search API is now a solr database URL,
# probably from a config file in the platform rather than an input?
# For now let's set it manually here:
solr_server = 'http://132.217.140.31:8983/solr/birdhouse/'
# The place where we save the resulting json files should also be in
# a config file, the user under which apache is running must be able
# to write to that directory.
json_output_path = '/var/www/html/wps_results'
json_output_url = 'http://localhost/wps_results/'

json_format = Format('application/json')
gmlxml_format = Format('application/gml+xml')

class PavicsSearch(Process):
    def __init__(self):
        inputs = [LiteralInput('facets',
                               'Facet values and counts',
                               data_type='string',
                               default='*',
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
                               default=10,
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
                               data_type='string'),
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
        my_search = 'q=aet&wt=json&indent=true'
        url_request = urllib2.Request(url=solr_server+'select?'+my_search)
        url_request.add_header('Content-type','application/json')
        url_response = urllib2.urlopen(url_request)
        solr_result = url_response.read()
        # This should make use of the current time or random number or
        # md5 of such things to create unique filenames.
        output_file_name = 'json_result.json'
        f1 = open(os.path.join(json_output_path,output_file_name),'w')
        f1.write(solr_result)
        f1.close()
        if request.inputs['format'][0].data == 'application/solr+json':
            json_url = os.path.join(json_output_url,output_file_name)
            response.outputs['search_result'].data = json_url
        elif request.inputs['format'][0].data == 'application/solr+xml':
            # Output as xml...
            raise NotImplementedError()
        else:
            # Unsupported format
            raise NotImplementedError()
        return response
