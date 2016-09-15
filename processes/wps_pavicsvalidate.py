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
# Check whether certain facets exist for all entries:
#localhost/pywps?service=WPS&request=execute&version=1.0.0&\
#identifier=pavicsvalidate&DataInputs=facets=model,variable
#
# Check whether certain facets exist for particular files:
#localhost/pywps?service=WPS&request=execute&version=1.0.0&\
#identifier=pavicsvalidate&DataInputs=facets=model,variable;\
#paths=sio%2Fsrtm30_plus_v6,ouranos%2Fsubdaily;\
#files=srtm30_plus.nc,aev_shum_1961.nc

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

class PavicsValidate(Process):
    def __init__(self):
        inputs = [LiteralInput('facets',
                               'Required facets',
                               data_type='string'),
                  LiteralInput('paths',
                               'Search paths',
                               data_type='string',
                               default='',
                               min_occurs=0,
                               mode=None),
                  LiteralInput('files',
                               'Search files',
                               data_type='string',
                               default='',
                               min_occurs=0,
                               mode=None),]
        outputs = [LiteralOutput('validate_result',
                                 'Validation result',
                                 data_type='string')]

        super(PavicsValidate,self).__init__(
            self._handler,
            identifier='pavicsvalidate',
            title='PAVICS Catalogue Validation',
            version='0.1',
            inputs=inputs,
            outputs=outputs,
            store_supported=True,
            status_supported=True)

    def _handler(self,request,response):
        facets = request.inputs['facets'][0].data
        paths = request.inputs['paths'][0].data
        if paths is not None:
            paths = paths.split(',')
        files = request.inputs['files'][0].data
        if files is not None:
            files = files.split(',')

        validate_result = catalog.pavicsvalidate(solr_server,facets,
                                                 paths,files)

        # Here we construct a unique filename
        md5_str = hashlib.md5(validate_result+str(random.random())).hexdigest()
        time_str = time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())
        output_file_name = "json_result_%s_%s.json" % (time_str,md5_str[0:8])
        
        f1 = open(os.path.join(json_output_path,output_file_name),'w')
        f1.write(json.dumps(validate_result))
        f1.close()
        result_url = os.path.join(json_output_url,output_file_name)
        response.outputs['validation_result'].data = result_url
        return response
