import os
import time
import random
import hashlib
import json
import urllib2
from pywps import Process,Format
from pywps import LiteralInput,LiteralOutput

from pavics import catalog

# Example usage:
#localhost/pywps?service=WPS&request=execute&version=1.0.0&\
#identifier=pavicsupdate&DataInputs=source=source_string;url=url_string;\
#updates=subject:new_subject,units:m

# Still need to perhaps validate the inputs, and consider whether we want
# to do updates that involve list of entries (not tested yet)

# base_search_URL in the ESGF Search API is now a solr database URL,
# this is provided as the environment variable SOLR_SERVER.
solr_server = "http://%s:8983/solr/birdhouse/" % (os.environ['SOLR_SERVER'],)
# The place where we save the resulting json files should also be in
# a config file, the user under which apache is running must be able
# to write to that directory.
json_output_path = '/var/www/html/wps_results'
json_output_url = "http://%s:8009/wps_results/" % (os.environ['SOLR_SERVER'],)

class PavicsUpdate(Process):
    def __init__(self):
        # The combination of the 'source' and 'url' fields provide the 'id'
        # in the Solr database, they both must be provided.
        inputs = [LiteralInput('id',
                               'id field of the dataset or file',
                               data_type='string',),
                  LiteralInput('updates',
                               'Fields to update with their new values',
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
        # Get the source and url to setup the update dictionary.
        update_id = request.inputs['id'][0].data
        update_dict = {'id':update_id}
        # Get updates, which are the facets to add/modify.
        data_inputs = request.inputs['updates'][0].data
        # Split using comma & colon as separator
        key_value_pairs = data_inputs.split(',')
        for key_value_pair in key_value_pairs:
            kv = key_value_pair.split(':')
            # Here, we do not use the {'set':kv[1]} syntax, it does not work
            # in birdhouse-solr. Instead it's like adding a new entry, but
            # since the source and url already exist, it will update the other
            # fields.
            update_dict.update({kv[0]:kv[1]})

        update_result = catalog.pavicsupdate(solr_server,update_dict)

        # Here we construct a unique filename
        md5_str = hashlib.md5(update_result+str(random.random())).hexdigest()
        time_str = time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())
        output_file_name = "json_result_%s_%s.json" % (time_str,md5_str[0:8])
        f1 = open(os.path.join(json_output_path,output_file_name),'w')
        f1.write(update_result)
        f1.close()
        json_url = os.path.join(json_output_url,output_file_name)
        response.outputs['update_result'].data = json_url
        return response
