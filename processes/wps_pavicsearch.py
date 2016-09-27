import os
import time
import random
import hashlib
import json
import urllib2
from pywps import Process,get_format
from pywps import LiteralInput,LiteralOutput,ComplexOutput

from pavics import catalog

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

#json_format = Format('application/json')
#gmlxml_format = Format('application/gml+xml')
json_format = get_format('JSON')
gmlxml_format = get_format('GML')

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
                               default=0, # Unable to set to 10, pywps bug.
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

        outputs = [ComplexOutput('search_result',
                                 'PAVICS Catalogue Search Result',
                                 supported_formats=[json_format,
                                                    gmlxml_format])]
        outputs[0].as_reference=True

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
        facets = request.inputs['facets'][0].data
        limit = request.inputs['limit'][0].data
        if limit is None:
            # Bypass pywps bug where we are unable to set default limit
            limit = 10
            #limit = request.inputs['limit'][0].default
        offset = request.inputs['offset'][0].data
        if offset is None:
            offset = request.inputs['offset'][0].default
        output_format = request.inputs['format'][0].data
        if output_format is None:
            output_format = request.inputs['format'][0].default
        fields = request.inputs['fields'][0].data
        constraints = request.inputs['constraints'][0].data
        query = request.inputs['query'][0].data

        search_result = catalog.pavicsearch(solr_server,facets,limit,offset,
                                            output_format,fields,constraints,
                                            query)

        # Here we construct a unique filename
        md5_str = hashlib.md5(search_result+str(random.random())).hexdigest()
        time_str = time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())
        output_file_name = "json_result_%s_%s." % (time_str,md5_str[0:8])
        if output_format == 'application/solr+json':
            output_file_name += 'json'
        elif output_format == 'application/solr+xml':
            output_file_name += 'xml'
        else:
            # Unsupported format
            raise NotImplementedError()
        f1 = open(os.path.join(json_output_path,output_file_name),'w')
        f1.write(search_result)
        f1.close()
        result_url = os.path.join(json_output_url,output_file_name)
        response.outputs['search_result'].data = result_url
        if output_format == 'application/solr+xml':
            response.outputs['search_result'].data_format = gmlxml_format
        return response
