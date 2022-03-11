from urllib.parse import urlencode

import requests
import singer

from singer import metrics

LOGGER = singer.get_logger()

class TikTokClient:
    def __init__(self,
                 access_token,
                 user_agent=None,
                 sandbox=False):
        self.__access_token = access_token
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.__base_url = None
        self.__verified = False
        # base URL prefix
        self.__base_url_prefix = 'business-api'
        # if the account is sandbox, change the URL prefix
        if str(sandbox).lower() == 'true':
            self.__base_url_prefix = 'sandbox-ads'

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
        response = self.__session.get(
            url='https://{}.tiktok.com/open_api/v1.2/user/info'.format(self.__base_url_prefix),
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
            self.__base_url = 'https://{}.tiktok.com/open_api/v1.2'.format(self.__base_url_prefix)

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
