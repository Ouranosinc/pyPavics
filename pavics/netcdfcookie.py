import os


class NetCDFCookie:
    def __init__(self, headers, opendap_hostnames=[], verify=True):
        self.headers = headers
        self.cookie = None
        self.daprc_fn = '.daprc'
        self.auth_cookie_fn = 'auth_cookie'
        self.opendap_hostnames = opendap_hostnames
        self.verify = verify

    def get_auth_cookie(self, headers):
        try:
            return dict(cookie.split('=') for cookie in headers['Cookie'].split(';'))
        except (KeyError, TypeError):
            # No token... will be anonymous
            return None

    def __enter__(self):
        self.cookie = self.get_auth_cookie(self.headers)

        if self.cookie:
            with open(self.daprc_fn, 'w') as f:
                f.write('HTTP.COOKIEJAR = {0}'.format(self.auth_cookie_fn))
                if not self.verify:
                    f.write('\nHTTP.SSL_VALIDATE = 0')

            with open(self.auth_cookie_fn, 'w') as f:
                for opendap_hostname in self.opendap_hostnames:
                    for key, value in self.cookie.items():
                        f.write('{domain}\t{access_flag}\t{path}\t{secure}\t{expiration}\t{name}\t{value}\n'.format(
                            domain=opendap_hostname,
                            access_flag='FALSE',
                            path='/',
                            secure='FALSE',
                            expiration=0,
                            name=key,
                            value=value))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cookie:
            os.remove(self.daprc_fn)
            os.remove(self.auth_cookie_fn)
