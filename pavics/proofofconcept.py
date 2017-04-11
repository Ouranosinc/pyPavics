import os
import esgf
import json
import copy

esgf_server = 'https://esgf-node.llnl.gov'
esgf_search_url = 'https://esgf-node.llnl.gov/esg-search'

search_paths = ['/dmf2/scenario/external_data/CMIP5',
                '/dmf2/scenario/broken/cmip5_download']

# type is one of Dataset, File or Aggregation
# distrib will determine if searching only one the selected node or all
# the federated nodes.
search_dict = esgf.ESGFSearchDict({'type':'Dataset',
                                   'distrib':'true',
                                   'project':'CMIP5',
                                   'institute':None,
                                   'model':['BNU-ESM','CanESM2','CESM1-BGC','HadGEM2-ES','inmcm4','IPSL-CM5A-LR','IPSL-CM5A-MR','IPSL-CM5B-LR','MIROC-ESM','MPI-ESM-LR','MPI-ESM-MR','NorESM1-ME','GFDL-ESM2G','GFDL-ESM2M'],
                                   'experiment':['1pctCO2','historical','rcp45','rcp85'],
                                   'time_frequency':'mon',
                                   'variable':['nbp','fgco2','tas','pr','tpf','lai','sic','sss','sst'],
                                   'ensemble':None,
                                   'latest':'true',
                                   'replica':'false',
                                   'data_node':None,
                                  })

#myurl = search_dict.search_url(esgf_search_url)

json_res = search_dict.dataset_search_with_split_retries(esgf_search_url,
                                                         limit=1000)

data_nodes = []
for i in range(j1['response']['numFound']):
    if j1['response']['docs'][i]['data_node'] not in data_nodes:
        data_nodes.append(j1['response']['docs'][i]['data_node'])

print(data_nodes)

split_ds = search_dict.split_by_elements(['data_node'],
                                         {'data_node':data_nodes})

json_results = []
n = 0
for split_d in split_ds:
    split_d.search['type'] = 'File'
    fn = split_d.dataset_search_with_split_retries
    json_results.append(fn(esgf_search_url,limit=1000))
    n += len(json_results[-1]['response']['docs'])

print("Found {0} files.".format(str(n)))

# If numFound > len, we need to split the request, since this is Solr and
# there are concurrency issues with pagination
# Note that Solr default sort order is oldest document first (editing a
# document renews it completely)



#for data_node in data_nodes:
    #sub_search_dict = copy.deepcopy(search_dict)
    #sub_search_dict.search['data_node'] = data_node
    #sub_search_dict.search['type'] = 'File'
    #json_res = sub_search_dict.dataset_search(esgf_search_url, limit=1000,
                                              #output_format='json')
    #subj1 = json.loads(json_res)
    
myfiles = {}
for search_path in search_paths:
    for root, dirs, files in os.walk(search_path):
        for one_file in files:
            if one_file in myfiles:
                myfiles[one_file].append(root)
            else:
                myfiles[one_file] = [root]


def checksum_large_file(file1, block_size=2**20, hashlib_fn=hashlib.sha256):
    f1 = open(file1)
    checksum = hashlib_fn()
    while True:
        data1 = f1.read(block_size)
        if not data1:
            break
        checksum.update(data1)
    f1.close()
    return checksum.hexdigest()

class FileOnDisk():
    def __init__(self, path):
        self.path = path
        self.basename = os.path.basename(path)
        self.size = os.path.getsize(path)
    def md5():
        return checksum_large_file(self.path, hashlib_fn=hashlib.md5)
    def sha256():
        return checksum_large_file(self.path, hashlib_fn=hashlib.sha256)        

class ESGFFile():
    def __init__(self, esgf_doc):
        self.doc = esgf_doc
        self.data_node = esgf_doc['data_node']
        self.title = esgf_doc['title']
        self.size = esgf_doc['size']
        if 'checksum' in esgf_doc:
            self.checksum = esgf_doc['checksum']
        else:
            self.checksum = None
        if 'checksum_type' in esgf_doc:
            self.checksum_type = esgf_doc['checksum_type'].upper()
        else:
            # Assuming default checksum type is SHA256
            self.checksum_type = 'SHA256'

def file_check(json_results, files_on_disk):
    files_status = {}
    for json_result in json_results:
        for one_doc in json_result['response']['docs']:
            file_title = one_doc['title']
            if file_title not in files_status:
                files_status[file_title] = {}
            if file_title in files_on_disk:
                warp = copy.deepcopy(files_on_disk[file_title])
                files_status[file_title]['local_paths'] = warp
            if 'data_nodes' in files_status[file_title]:
                warp = one_doc['data_node']
                files_status[file_title]['data_nodes'].append(warp)
            else:
                files_status[file_title]['data_nodes'] = [one_doc['data_node']]
    for one_file, file_args in files_status.items():
        
        

for json_result in json_results:
    for one_doc in json_result['response']['docs']:
        one_doc['title']
        ond_doc['checksum']
        one_doc['checksum_type']
