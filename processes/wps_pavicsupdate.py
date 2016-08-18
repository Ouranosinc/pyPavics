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

class PavicsUpdate(Process):
    def __init__(self):
        inputs = [LiteralInput('id',
                               'Unique id of the dataset or file',
                               data_type='string',),
                  LiteralInput('DataInputs',
                               'Semicolon separated fields to update',
                               data_type='string'),]
        outputs = [LiteralOutput('update_result',
                                 'PAVICS Catalogue Update Result',
                                 data_type='string')]

        super(PavicsUpdate,self).__init__(
            self._handler,
            identifier='pavicsupdate',
            title='PAVICS Catalogue Update',
            version='0.1',
            inputs=inputs,
            outputs=outputs,
            store_supported=True,
            status_supported=True)

    def _handler(self,request,response):
        update_id = request.inputs['id'][0].data
        update_dict = {'id':update_id}
        data_inputs = request.inputs['DataInputs'][0].data
        key_value_pairs = data_inputs.split(';')
        for key_value_pair in key_value_pairs:
            kv = key_value_pair.split('=')
            data_inputs.update({kv[0]:{'set':kv[1]}})
        update_data = [data_inputs]
        solr_json_input = json.dumps(update_data)
        solr_method = 'update/json?commit=true'
        url_request = urllib2.Request(url=solr_server+solr_method,
                                      data=solr_json_input)
        url_request.add_header('Content-type','application/json')
        try:
            url_response = urllib2.urlopen(url_request)
        except urllib2.HTTPError as err:
            # Probably an unknown field?
            if err.msg == 'Bad Request':
                raise NotImplementedError()
            else:
                raise err
        update_result = url_response.read()
        url_response.close()

        # This should make use of the current time or random number or
        # md5 of such things to create unique filenames.
        output_file_name = 'json_result.json'
        f1 = open(os.path.join(json_output_path,output_file_name),'w')
        f1.write(update_result)
        f1.close()
        json_url = os.path.join(json_output_url,output_file_name)
        response.outputs['update_result'].data = json_url
        return response
