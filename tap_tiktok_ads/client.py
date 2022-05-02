from urllib.parse import urlencode
import backoff

import requests
import backoff
import singer

from singer import metrics

# max tries for backoff
MAX_TRIES = 5
# default timeout for requests
REQUEST_TIMEOUT = 300

LOGGER = singer.get_logger()

ENDPOINT_BASE = "https://{api}.tiktok.com/open_api/v1.2"
TOKEN_URL = 'https://{api}.tiktok.com/open_api/v1.2/user/info'

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
    # Backoff in case of 50000 error code. Refer doc: https://ads.tiktok.com/marketing_api/docs?id=1714940022762498 
    # for more information.
    if error_code == 50000:
        return True

class TikTokClient:
    def __init__(self,
                 access_token,
                 advertiser_id,
                 sandbox=False,
                 user_agent=None,
                 request_timeout=REQUEST_TIMEOUT):
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
        self.__advertiser_id = advertiser_id

        # set request timeout from config param "request_timeout" value
        # If value is 0,"0","" or not passed then it set default to 300 seconds.
        if request_timeout and float(request_timeout):
            self.__request_timeout = float(request_timeout)
        else:
            self.__request_timeout = REQUEST_TIMEOUT

    # Backoff the request after 5 minutes in case of 50000 error code
    @backoff.on_exception(backoff.constant,
                          (TikTokAdsClientError),
                          max_time=600, # 10 minutes
                          interval=300, # 5 minutes
                          giveup=lambda e: not should_retry(e),
                          jitter=None)
    @backoff.on_exception(backoff.expo,
                          requests.Timeout, # backoff for "Timeout" error
                          max_tries=MAX_TRIES,
                          factor=2)
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
            headers=headers,
            timeout=self.__request_timeout)

        if response.status_code != 200:
            raise Exception('Error status_code = %s', response.status_code)
        resp = response.json()
        error_code = resp.get('code')
        message = resp.get('message', 'Unknown Error occurred.')

        if error_code != 0: # `0` error code indicates successful request
            raise TikTokAdsClientError(message, response) # raise the exception with the message retrieved
        # if the check_access_token() succeeds, then check the account access with the account ids provided in config.
        if resp['message'] == 'OK':
            self.__verified = True
            headers = {
                "Access-Token": self.__access_token
            }
            params = {
                "advertiser_ids": self.__advertiser_id
            }
            # Call the advertisers API with the account ids to check whether the accounts are valid or not.
            adv_response = self.get(path='advertiser/info/', headers=headers,
                                        params=params)
            return bool(adv_response.get('message') == 'OK')

    # Backoff the request after 5 minutes in case of 50000 error code
    @backoff.on_exception(backoff.constant,
                          (TikTokAdsClientError),
                          max_time=600, # 10 minutes
                          interval=300, # 5 minutes
                          giveup=lambda e: not should_retry(e),
                          jitter=None)
    @backoff.on_exception(backoff.expo,
                          requests.Timeout, # backoff for "Timeout" error
                          max_tries=MAX_TRIES,
                          factor=2)
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
            response = self.__session.request(method, url + query, timeout=self.__request_timeout, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code != 200:
            raise Exception(f'Error code: {response.status_code}')

        try:
            json_response = response.json()
        except:
            json_response = {}
        error_code = json_response.get("code")
        message = json_response.get('message', 'Unknown Error occurred.')
        if "Service error:" in message:
            message = "Error encountered accessing the accounts with the given account ids. Kindly check your account ids."

        if error_code != 0: # `0` error code indicates successful request
            raise TikTokAdsClientError(message, response) # raise the exception with the message retrieved
        return json_response

    def get(self, url=None, path=None, **kwargs):
        return self.request('GET', url=url, path=path, **kwargs)

    def post(self, url=None, path=None, **kwargs):
        return self.request('POST', url=url, path=path, **kwargs)
