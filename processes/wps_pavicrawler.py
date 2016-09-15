import os
import time
import random
import hashlib
from pywps import Process,Format
from pywps import LiteralInput,LiteralOutput

from pavics import catalog

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

thredds_server = 'http://132.217.140.45:8083/thredds'
# base_search_URL in the ESGF Search API is now a solr database URL,
# this is provided as the environment variable SOLR_SERVER.
solr_server = "http://%s:8983/solr/birdhouse/" % (os.environ['SOLR_SERVER'],)
# The place where we save the resulting json files should also be in
# a config file, the user under which apache is running must be able
# to write to that directory.
json_output_path = '/var/www/html/wps_results'
json_output_url = "http://%s:8009/wps_results/" % (os.environ['SOLR_SERVER'],)

# Fix for OpenStack internal/external ip:
# (the internal ip is the environment variable OPENSTACK_INTERNAL_IP)
if os.environ.has_key('OPENSTACK_INTERNAL_IP'):
    internal_ip = os.environ['OPENSTACK_INTERNAL_IP']
else:
    internal_ip = os.environ['SOLR_SERVER']
external_ip = os.environ['SOLR_SERVER']

class PavicsCrawler(Process):
    def __init__(self):
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
        update_result = catalog.pavicrawler(thredds_server,solr_server,
                                            my_facets,
                                            internal_ip=internal_ip,
                                            external_ip=external_ip,
                                            output_internal_ip=True)

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
