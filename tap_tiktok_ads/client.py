from urllib.parse import urlencode

import requests
import singer

from singer import metrics

LOGGER = singer.get_logger()

ENDPOINT_BASE = "https://{api}.tiktok.com/open_api/v1.2"
# SANDBOX_ENDPOINT_BASE = "https://sandbox-ads.tiktok.com/open_api/v1.2"
TOKEN_URL = 'https://{api}.tiktok.com/open_api/v1.2/user/info'
# SANDBOX_TOKEN_URL = 'https://sandbox-ads.tiktok.com/open_api/v1.2/user/info'

class TikTokClient:
    def __init__(self,
                 access_token,
                 sandbox=False,
                 user_agent=None):
        self.__access_token = access_token
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.__base_url = None
        self.__verified = False
        self.sandbox = False
        if sandbox in ['true', 'True', True]:
            self.sandbox = True

    def __enter__(self):
        self.__verified = self.check_access_token()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__session.close()

    def check_access_token(self):
        if self.__access_token is None:
            raise Exception('Error: Missing access_token.')
        headers = {}
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['Access-Token'] = self.__access_token
        headers['Accept'] = 'application/json'
        if self.sandbox:
            url = TOKEN_URL.format(api='sandbox-ads')
        else:
            url = TOKEN_URL.format(api='business-api')
        response = self.__session.get(
            url=url,
            headers=headers)
        if response.status_code != 200:
            LOGGER.error('Error status_code = %s', response.status_code)
            return False
        else:
            resp = response.json()
            return bool(resp['message'] == 'OK')

    def request(self, method, url=None, path=None, **kwargs):
        if not self.__verified:
            self.__verified = self.check_access_token()

        if not url and self.__base_url is None:
            if self.sandbox:
                self.__base_url = ENDPOINT_BASE.format(api='sandbox-ads')
            else:
                self.__base_url = ENDPOINT_BASE.format(api='business-api')

        if not url and path:
            url = f'{self.__base_url}/{path}'
        else:
            url = f'{url}/{path}'

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['Access-Token'] = self.__access_token
        kwargs['headers']['Accept'] = 'application/json'

        query = ''
        if 'params' in kwargs:
            query = '?' + urlencode(kwargs['params'])
            kwargs['params'] = {}

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url + query, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code != 200:
            raise Exception(f'Error code: {response.status_code}')

        return response.json()

    def get(self, url=None, path=None, **kwargs):
        return self.request('GET', url=url, path=path, **kwargs)

    def post(self, url=None, path=None, **kwargs):
        return self.request('POST', url=url, path=path, **kwargs)
