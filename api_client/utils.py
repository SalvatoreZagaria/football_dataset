import os
import json
import hashlib
import requests
import typing as t

from pathlib import Path

import logger

LOGGER = logger.get_logger('api_client')
CACHE_FOLDER: Path = None


def get_api_key() -> t.Optional[str]:
    key = os.getenv('RAPID_API_KEY')
    return key or None


def get_api_host() -> str:
    return os.getenv('RAPID_API_HOST', 'api-football-v1.p.rapidapi.com')


def prepare_for_caching(request_url: str, params: dict = None) -> str:
    h = request_url
    if params:
        h += '_' + '_'.join([f'{key}_{value}' for key, value in sorted(params.items())])

    return hashlib.md5(h.encode("utf-8")).hexdigest()


def cache_result(hashed_url: str, response: requests.Response):
    if response.status_code != 200:
        return
    path_to_obj = Path(CACHE_FOLDER, hashed_url)
    if path_to_obj.exists():
        return
    try:
        obj = response.json()
    except Exception as e:
        LOGGER.error(f'CACHE - Exception occurred while parsing response for cache: {e}')
        return

    with open(path_to_obj, 'w') as f:
        json.dump(obj, f, indent=4)


def read_from_cache(url, params: dict = None) -> t.Optional[t.Any]:
    path_to_obj = Path(CACHE_FOLDER, prepare_for_caching(url, params=params))
    if not path_to_obj.exists():
        return None

    with open(path_to_obj, 'r') as f:
        try:
            return json.load(f)
        except Exception as e:
            LOGGER.error(f'CACHE - Exception occurred while parsing json file for cache: {e}')
            return None
