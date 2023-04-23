import time
import requests
import typing as t
from pathlib import Path

import logger
import api_client
from api_client import utils

LOGGER = logger.get_logger('api_client')


class APIFootballClient:
    def __init__(self, enable_cache=True, api_version='v3', cache_folder='.rapid_api_cache',
                 requests_block=None):
        self._api_key = utils.get_api_key()
        if not self._api_key:
            raise Exception('API Football key not found in the environment vars set')
        self._rapid_api_host = utils.get_api_football_host()
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

    def send_request(self, partial_url: str, params: dict = None) -> t.Optional[t.Dict]:
        url = f'{self._url}/{partial_url}'

        if self._enable_cache:
            cached_response = utils.read_from_cache(url, params=params)
            if cached_response:
                LOGGER.info(f'cache hit - {url}; params: {str(params)}')
                return cached_response

        if self._requests_block is not None and self._requests_so_far >= self._requests_block:
            msg = f'API limit reached (requests_n: {self._requests_so_far}, block: {self._requests_block})'
            raise api_client.APILimitReached(msg)
        self._requests_so_far += 1
        LOGGER.info(f'starting request - {url}; params: {str(params)}')
        retried = False
        while True:
            response = requests.get(url, params=params, headers=self._headers)
            remaining_requests = response.headers.get('x-ratelimit-requests-remaining', 0)
            if int(remaining_requests) < 10:
                msg = f'API limit reached ({remaining_requests} remaining)'
                if retried:
                    raise api_client.APILimitReached(msg)
                LOGGER.warning(msg)
                retried = True
                time.sleep(15)
                continue
            if response.status_code == 429:
                msg = f'Rate limit: {response.status_code} : {response.text}'
                if retried:
                    raise api_client.APILimitReached(msg)
                LOGGER.warning(msg)
                retried = True
                time.sleep(15)
                continue
            break

        if response.status_code != 200:
            LOGGER.warning(f'Received a non 200 status code: {response.status_code} : {response.text}')
            return None
        if response.json().get('errors'):
            LOGGER.warning(
                f'Received one or more errors in the response: {"; ".join(response.json().get("errors", []))}')
            return None
        if self._enable_cache:
            utils.cache_result(utils.prepare_for_caching(url, params=params), response)

        return response.json()

    def get_clean_response(self, partial_url: str, params: dict = None, pagination=False) -> t.Optional[t.List]:
        if pagination:
            response = self.send_request_with_pagination(partial_url, params)
        else:
            response = self.send_request(partial_url, params)
            response = response.get('response', []) if response else None
        return response

    def send_request_with_pagination(self, partial_url: str, params: dict = None) -> t.Optional[t.List[t.Dict]]:
        res = []
        current_page = 1
        while True:
            params['page'] = current_page
            current_response = self.send_request(partial_url, params=params)
            if current_response is None:
                LOGGER.warning(f'Returning partial result for pagination - url: {partial_url}, params: {str(params)}')
                return res
            total_pages = current_response.get('paging', {}).get('total', 1)
            current_page = current_response.get('paging', {}).get('current', 1)
            LOGGER.info(f'\tpagination {current_page}/{total_pages}')

            res.extend(current_response.get('response', []))

            if current_page == total_pages:
                return res
            current_page += 1

    def get_leagues(self, filter_by: t.Tuple = None) -> t.Optional[t.List[t.Dict]]:
        LOGGER.info('requesting leagues')
        response = self.get_clean_response('leagues')
        if response:
            response = [r for r in response if r['league']['type'] == 'League']
            if filter_by:
                response = [r for r in response if (r['league']['name'], r['country']['name']) in filter_by]

        return response

    def get_league_players(self, league_id, year) -> t.Optional[t.List[t.Dict]]:
        LOGGER.info(f'requesting league players - {league_id}, year {year}')
        response = self.get_clean_response('players', params={'league': league_id, 'season': year}, pagination=True)

        return response

    def get_player_stats(self, player_id, year):
        LOGGER.info(f'requesting player stats - {player_id}')
        response = self.get_clean_response('players', params={'id': player_id, 'season': year})

        return response

    def get_team_transfers(self, team_id):
        LOGGER.info(f'requesting team transfers - {team_id}')
        response = self.get_clean_response('transfers', params={'team': team_id})

        return response
