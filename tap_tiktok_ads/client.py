from urllib.parse import urlencode

import requests
import backoff
import singer

from singer import metrics

LOGGER = singer.get_logger()

# pylint: disable=missing-class-docstring
class TikTokAdsClientError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response

def should_retry(e):
    """ Return true if exception is required to retry otherwise return false """
    response = e.response
    error_code = response.json().get("code")
    if error_code == 50000:
        return True

class TikTokClient:
    def __init__(self,
                 access_token,
                 user_agent=None):
        self.__access_token = access_token
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.__base_url = None
        self.__verified = False

    @backoff.on_exception(backoff.constant,
                          (TikTokAdsClientError),
                          max_time=600, # 10 minutes
                          interval=300, # 5 minutes
                          giveup=lambda e: not should_retry(e),
                          jitter=None)
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
            url='https://business-api.tiktok.com/open_api/v1.2/user/info',
            headers=headers)
        if response.status_code != 200:
            LOGGER.error('Error status_code = %s', response.status_code)
            return False
        resp = response.json()
        error_code = resp.get('code')
        message = resp.get('message', 'Unknown Error occurred.')

        if error_code != 0:
            raise TikTokAdsClientError(message, response)
        return bool(resp.get('message') == 'OK')

    @backoff.on_exception(backoff.constant,
                          (TikTokAdsClientError),
                          max_time=600, # 10 minutes
                          interval=300, # 5 minutes
                          giveup=lambda e: not should_retry(e),
                          jitter=None)
    def request(self, method, url=None, path=None, **kwargs):
        if not self.__verified:
            self.__verified = self.check_access_token()

        if not url and self.__base_url is None:
            self.__base_url = 'https://business-api.tiktok.com/open_api/v1.2'

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

        try:
            json_response = response.json()
        except:
            json_response = {}
        error_code = json_response.get("code")
        message = json_response.get('message', 'Unknown Error occurred.')
        if error_code != 0:
            raise TikTokAdsClientError(message, response)
        return json_response

    def get(self, url=None, path=None, **kwargs):
        return self.request('GET', url=url, path=path, **kwargs)

    def post(self, url=None, path=None, **kwargs):
        return self.request('POST', url=url, path=path, **kwargs)
