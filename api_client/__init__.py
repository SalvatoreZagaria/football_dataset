import requests
import typing as t
from pathlib import Path

import logger
from api_client import utils

LOGGER = logger.get_logger('api_client')

LEAGUES = (('Serie A', 'Italy'), ('Premier League', 'England'))


class APILimitReached(Exception):
    pass


class APIFootballClient:
    def __init__(self, enable_cache=True, api_version='v3', cache_folder='.rapid_api_cache',
                 requests_block=None):
        self._api_key = utils.get_api_key()
        if not self._api_key:
            raise Exception('API Football key not found in the environment vars set')
        self._rapid_api_host = utils.get_api_host()
        self._enable_cache = enable_cache
        if self._enable_cache:
            cache_folder = Path(cache_folder)
            cache_folder.mkdir(exist_ok=True)
            utils.CACHE_FOLDER = cache_folder
        self._url = f'https://{self._rapid_api_host}/{api_version}'
        self._headers = {
            'X-RapidAPI-Key': self._api_key,
            'X-RapidAPI-Host': self._rapid_api_host
        }
        self._requests_so_far = 0

        self._requests_block = requests_block

    def send_request(self, partial_url: str, params: dict = None) -> t.Optional[t.Any]:
        url = f'{self._url}/{partial_url}'
        if self._enable_cache:
            cached_response = utils.read_from_cache(url, params=params)
            if cached_response:
                return cached_response

        if self._requests_block is not None and self._requests_so_far >= self._requests_block:
            msg = f'API limit reached (requests_n: {self._requests_so_far}, block: {self._requests_block})'
            raise APILimitReached(msg)
        self._requests_so_far += 1
        response = requests.get(url, params=params, headers=self._headers)
        if response.status_code != 200:
            LOGGER.info(f'Received a non 200 status code: {response.status_code} : {response.text}')
            return None
        if self._enable_cache:
            utils.cache_result(utils.prepare_for_caching(url, params=params), response)

        return response.json()

    def get_leagues(self, filter_by: t.Tuple[t.Tuple[str, str]] = LEAGUES):
        response = self.send_request('leagues')
        if response:
            response = response.get('response', [])
            response = [r for r in response if (r['league']['name'], r['country']['name']) in filter_by]

        return response
