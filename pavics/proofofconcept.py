import esgf
import json

esgf_server = 'https://esgf-node.llnl.gov'
esgf_search_url = 'https://esgf-node.llnl.gov/esg-search'

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

myurl = search_dict.search_url(esgf_search_url)

json_res = search_dict.dataset_search(esgf_search_url,limit=1000,
                                      output_format='json')

j1 = json.loads(json_res)

j1['response']['numFound']
len(j1['response']['docs'])


