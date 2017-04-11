import os
import sys
import copy
import json
import itertools
import pexpect
try:
    from http.client import HTTPConnection
except ImportError:
    from httplib import HTTPConnection
try:
    from urllib.parse import quote_plus, urlparse
except ImportError:
    from urlparse import urlparse
    from urllib import quote_plus


class InvalidSearchDict(Exception):
    pass


class LimitExceeded(Exception):
    pass


class WgetStuck(Exception):
    pass


class ESGFSearchDict:
    """ESGF Search Dictionary definition."""

    def __init__(self, esgf_search_dict, esgf_omission_search_dict=None):
        """Initialize ESGFSearchDict.

        Parameters
        ----------
        esgf_search_dict : dictionary
        esgf_omission_search_dict : dictionary

        """

        self.search = esgf_search_dict
        if esgf_omission_search_dict is None:
            self.omission = {}
        else:
            self.omission = esgf_omission_search_dict
        self.validate()

    def validate(self):
        """Check for contradiction in search dictionaries."""

        msg = "Constradiction in search."
        for k, v in self.search.items():
            if v is None:
                continue
            if k in self.omission:
                if self.omission[k] is None:
                    continue
                elif hasattr(self.omission[k], '__iter__'):
                    for element in self.omission[k]:
                        if hasattr(v, '__iter__'):
                            if element in v:
                                raise InvalidSearchDict(msg)
                        else:
                            if element == v:
                                raise InvalidSearchDict(msg)
                else:
                    if hasattr(v, '__iter__'):
                        if self.omission[k] in v:
                            raise InvalidSearchDict(msg)
                    else:
                        if self.omission[k] == v:
                            raise InvalidSearchDict(msg)

    def list_loose_components(self, loose_defaults=[]):
        """List components that are allowed more than one value.

        Parameters
        ----------
        loose_defaults : list of string
            These will be added to the results.

        """

        loose_components = []
        for k, v in self.search.items():
            if (v is None) or hasattr(v, '__iter__'):
                loose_components.append(k)
        for loose_default in loose_defaults:
            if loose_default not in self.search:
                loose_components.append(loose_default)
        return loose_components

    def list_split_candidates(self):
        candidates = []
        ns = []
        for k, v in self.search.items():
            if hasattr(v, '__iter__'):
                candidates.append(k)
                ns.append(len(v))
        return [x for (y, x) in sorted(zip(ns, candidates))]

    def split_by_elements(self, split_components, elements_defaults=None):
        """Split search dictionary into multiple search dictionaries.

        Parameters
        ----------
        split_components : list of string
        elements_defaults : dictionary of list of string
            Manually add split along additional components.

        """

        if not split_components:
            return [copy.deepcopy(self)]
        if elements_defaults is None:
            elements_defaults = {}
        elements_lists = []
        for component in split_components:
            elements_lists.append([])
            c1 = component in self.search
            if c1 and self.search[component] is not None:
                if hasattr(self.search[component], '__iter__'):
                    elements_lists[-1].extend(self.search[component])
                else:
                    elements_lists[-1].append(self.search[component])
            elif component in elements_defaults:
                elements_lists[-1].extend(elements_defaults[component])

        sub_ds = []
        for elements_tuple in itertools.product(*elements_lists):
            sub_d = copy.deepcopy(self)
            for i, component in enumerate(split_components):
                sub_d.search[component] = elements_tuple[i]
            sub_ds.append(sub_d)
        return sub_ds

    def _add_single_omission(self, component, element):
        """Add an omission to the search dictionary.

        Parameters
        ----------
        component : string
        element : string

        """

        if element is None:
            return
        if component in self.search:
            if self.search[component] is None:
                pass
            elif hasattr(self.search[component], '__iter__'):
                if element in self.search[component]:
                    self.search[component].remove(element)
            else:
                if self.search[component] == element:
                    self.search[component] = None
        if component in self.omission:
            if self.omission[component] is None:
                self.omission[component] = element
            elif hasattr(self.omission[component], '__iter__'):
                if element not in self.omission[component]:
                    self.omission[component].append(element)
            else:
                if self.omission[component] != element:
                    self.omission[component] = [self.omission[component],
                                                element]
        else:
            self.omission[component] = element

    def add_omissions(self, omission_dict):
        """Add omissions to search dictionary.

        Parameters
        ----------
        omission_dict : dictionary

        """

        for component, elements in omission_dict.items():
            if elements is None:
                continue
            elif hasattr(elements, '__iter__'):
                for element in elements:
                    self._add_single_omission(component, element)
            else:
                self._add_single_omission(component, elements)

    def search_string(self):
        """Convert facets dictionary into a search string.

        Parameters
        ----------
        d : dictionary
        not_d : dictionary
            search by omission.

        Returns
        -------
        out : string

        """

        search_string = ""
        for key, value in self.search.items():
            if value is None:
                continue
            elif hasattr(value, '__iter__'):
                for value1 in value:
                    search_string = "{0}&{1}={2}".format(search_string, key,
                                                         quote_plus(value1))
            else:
                search_string = "{0}&{1}={2}".format(search_string, key,
                                                     quote_plus(value))
        for key, value in self.omission.items():
            if value is None:
                continue
            elif hasattr(value, '__iter__'):
                for value1 in value:
                    search_string = "{0}&{1}!={2}".format(search_string, key,
                                                          quote_plus(value1))
            else:
                search_string = "{0}&{1}!={2}".format(search_string, key,
                                                      quote_plus(value))
        return search_string[1:]

    def search_url(self, esgf_search_url, facets=None, shards=None,
                   offset=None, limit=None, fields=None, output_format=None):
        """ESGF search URL.

        Parameters
        ----------
        esgf_search_url : string
        facets : list of strings or '*'
        shards : list of strings
        offset : int
        limit : int
        fields : list of strings
        output_format : string

        Returns
        -------
        out : string

        """

        search_path = ''
        if facets is not None:
            if facets == '*':
                search_path = search_path + "&facets=*"
            else:
                search_path += "&facets="
                for facet in facets:
                    search_path += facet + ','
                search_path = search_path.rstrip(',')
        if shards is not None:
            search_path += "&shards="
            for shard in shards:
                search_path += shard + ','
            search_path = search_path.rstrip(',')
        if offset is not None:
            search_path = "{0}&offset={1}".format(search_path, str(offset))
        if limit is not None:
            search_path = "{0}&limit={1}".format(search_path, str(limit))
        if fields is not None:
            search_path += "&fields="
            for field in fields:
                search_path += field + ','
            search_path = search_path.rstrip(',')
        if output_format is not None:
            if output_format.lower() == 'json':
                search_path = search_path + "&format=application%2Fsolr%2Bjson"
            elif output_format.lower() == 'xml':
                search_path = search_path + "&format=application%2Fsolr%2Bxml"
            else:
                search_path = "{0}&format={1}".format(
                    search_path, quote_plus(output_format))
        search_path = "{0}&{1}".format(search_path, self.search_string())
        return esgf_search_url.rstrip('/') + '/search?' + \
            search_path.lstrip('&')

    def wget_url(self, esgf_server, esgf_path="/esg-search/", limit=None):
        """ESGF wget request URL.

        Parameters
        ----------
        esgf_server : string
        d : dictionary
            elements can be string or None or a list of string.
        esgf_path : string
        limit : int

        Returns
        -------
        out : string

        """

        wget_path = "/esg-search/wget?{0}".format(self.search_string())
        # wget_path = "/esg-search/wget?"
        # wget_path = wget_path + self.search_string()
        if limit is not None:
            wget_path = "{0}&limit={1}".format(wget_path, str(limit))
            # wget_path = wget_path + "&limit=%s" % (str(limit),)
        return esgf_server + wget_path

    def dataset_search(self, esgf_search_url, facets=None, shards=None,
                       offset=None, limit=None, fields=None,
                       output_format=None):
        """ESGF search.

        Parameters
        ----------
        esgf_server : string
        facets : list of strings or '*'
        shards : list of strings
        offset : int
        limit : int
        fields : list of strings
        output_format : string

        Returns
        -------
        out : xml or json string

        """

        one_search_url = self.search_url(esgf_search_url, facets, shards,
                                         offset, limit, fields, output_format)
        parsed_url = urlparse(one_search_url)
        conn = HTTPConnection(parsed_url.hostname)
        search_path_start = one_search_url.find(parsed_url.hostname) + \
            len(parsed_url.hostname)
        conn.request("GET", one_search_url[search_path_start:])
        r1 = conn.getresponse()
        esgf_response = r1.read()
        conn.close()
        return esgf_response

    def dataset_search_with_split_retries(self, esgf_search_url, facets=None,
                                          shards=None, offset=None, limit=None,
                                          fields=None):
        json_responses = [None]
        overflow = [-1]
        sub_ds = [self]
        while True:
            for i, oflag in enumerate(overflow):
                if oflag == -1:
                    r = sub_ds[i].dataset_search(esgf_search_url,
                                                 facets=facets,
                                                 shards=shards, offset=offset,
                                                 limit=limit, fields=fields,
                                                 output_format='json')
                    json_responses[i] = json.loads(r)
                    num_found = json_responses[i]['response']['numFound']
                    num_returned = len(json_responses[i]['response']['docs'])
                    if num_found > num_returned:
                        overflow[i] = 1
                    else:
                        overflow[i] = 0

            overflow_pop = []
            for i, oflag in enumerate(overflow):
                if oflag == 1:
                    overflow_pop.append(i)
                    split_candidates = sub_ds[i].list_split_candidates()
                    if not split_candidates:
                        raise LimitExceeded()
                    sc = split_candidates[0]
                    split_ds = sub_ds[i].split_by_elements([sc])
                    sub_ds.extend(split_ds)
                    json_responses.extend([None] * len(split_ds))
                    overflow.extend([-1] * len(split_ds))
            for i in overflow_pop[::-1]:
                dummy = sub_ds.pop(i)
                dummy = json_responses.pop(i)
                dummy = overflow.pop(i)

            if -1 not in overflow:
                break

        n = 0
        for json_response in json_responses[1:]:
            add_docs = json_response['response']['docs']
            # Unfortunately, need to check for duplicates since separate
            # variable searches can lead to the same dataset...
            for add_doc in add_docs:
                if add_doc not in json_responses[0]['response']['docs']:
                    json_responses[0]['response']['docs'].append(add_doc)
                    n += 1
        json_responses[0]['response']['numFound'] += n
        return json_responses[0], sub_ds

    def datasets_from_d(self, esgf_server, search_type='Dataset', limit=None,
                        verbose=True, log_file=None):
        """Get datasets from a search dictionary.

        Parameters
        ----------
        esgf_server : string
        d : dictionary
        not_d : dictionary
        search_type : string
        limit : int or None
        verbose : bool
        log_file : string or None

        Returns
        -------
        out : list of dictionary

        """

        xml_response = self.dataset_search(esgf_server,
                                           search_type=search_type,
                                           limit=limit)
        datasets = parse_xml_response(xml_response)
        n = len(datasets.ds)
        if verbose:
            logging.info("{0} file(s) returned.".format(str(n)))
        # tp("%s file(s) returned." % (str(n),), log_file=log_file,
        #                              verbose=verbose)
        if limit and (n >= limit):
            raise LimitExceeded("Files returned >= %s." % (str(n),))
        return datasets

    def dataset_wget(self, esgf_server, wget_output, esgf_path="/esg-search/",
                     limit=None):
        """ESGF wget file request.

        Parameters
        ----------
        esgf_server : string
        d : dictionary
            elements can be string or None, no list allowed.
        wget_output : string
            filename to save the wget script, if an existing path is given,
            a file named my_wget.sh will be created at that location.
        esgf_path : string
        limit : int

        """

        wget_path = "/esg-search/wget?{0}".format(self.search_string())
        # wget_path = "/esg-search/wget?"
        # wget_path = wget_path + self.search_string()
        if limit is not None:
            wget_path = "{0}&limit={1}".format(wget_path, str(limit))
            # wget_path = wget_path + "&limit=%s" % (str(limit),)
        # conn = httplib.HTTPConnection(esgf_server)
        conn = HTTPConnection(esgf_server)
        conn.request("GET", wget_path)
        r1 = conn.getresponse()
        wget_script = r1.read()
        conn.close()
        if os.path.isdir(wget_output):
            wget_file = os.path.join(wget_output, 'my_wget.sh')
        else:
            wget_file = wget_output
        f1 = open(wget_file, 'w')
        f1.write(wget_script)
        f1.close()


def keyword_string_or_file(string_or_file):
    """Get string from either a string or file.

    Parameters
    ----------
    string_or_file : string or file

    Returns
    -------
    out : string
        the input string, or the text found in the given file.

    """

    if os.path.isfile(string_or_file):
        f1 = open(string_or_file, 'r')
        string_or_file = f1.read().rstrip('\n')
        f1.close()
    return string_or_file


def wget_execution(wget_script, myopenid, myopenpw, wget_timeout=172800,
                   chdir=None, wget_args=[]):
    """wget script execution.

    Parameters
    ----------
    wget_script : string
    myopenid : string
    myopenpw : string or file
    wget_timeout : int
    chdir : string

    """

    os.chmod(wget_script, 0744)
    if chdir is not None:
        cwd = os.getcwd()
        os.chdir(chdir)
    child = pexpect.spawn(wget_script, wget_args, logfile=sys.stdout)
    if chdir is not None:
        os.chdir(cwd)
    if '-H' in wget_args:
        i = child.expect(["Enter your openid*\?*", pexpect.EOF,
                          pexpect.TIMEOUT],
                         timeout=120)
        if i == 0:
            child.sendline(myopenid)
            j = child.expect(["Enter password\?*", pexpect.EOF,
                              pexpect.TIMEOUT],
                             timeout=60)
            if j == 0:
                child.logfile = None
                myopenpw = keyword_string_or_file(myopenpw)
                child.sendline(myopenpw)
                del myopenpw
                child.logfile = sys.stdout
            else:
                raise NotImplementedError("Gave a username but no password?")
        elif i == 1:
            return
    else:
        i = child.expect(["Please give your OpenID*\?*", pexpect.EOF,
                          pexpect.TIMEOUT],
                         timeout=120)
        if i == 0:
            child.sendline(myopenid)
            j = child.expect(["MyProxy Password\?*", pexpect.EOF,
                              pexpect.TIMEOUT],
                             timeout=60)
            if j == 0:
                child.logfile = None
                myopenpw = genfunc.keyword_string_or_file(myopenpw)
                child.sendline(myopenpw)
                del myopenpw
                child.logfile = sys.stdout
            else:
                raise NotImplementedError("Gave a username but no password?")
        elif i == 1:
            return
    i = child.expect([pexpect.EOF, pexpect.TIMEOUT], timeout=60)
    t = 60
    while i == 1:
        i = child.expect([pexpect.EOF, pexpect.TIMEOUT], timeout=60)
        t += 60
        if t > wget_timeout:
            raise WgetStuck()
