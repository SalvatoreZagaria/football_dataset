import re
import time
import json
import datetime
import traceback
import typing as t
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import logger

LOGGER = logger.get_logger('transfermarkt')

MAX_RETRIES = 3
HEADERS = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
}
RESULTS_FOLDER = Path('.transfermarkt_results')
RESULTS_FOLDER.mkdir(exist_ok=True)

MILLION_FINDER = re.compile(r'[^0-9]([0-9]+\.[0-9]+)m')
BILLION_FINDER = re.compile(r'[^0-9]([0-9]+\.[0-9]+)bn')


class MaxRetriesReached(Exception):
    pass


def _extract_value(v: str):
    value = MILLION_FINDER.findall(v)
    if value:
        return int(float(value[0]))
    value = BILLION_FINDER.findall(v)
    if value:
        return int(float(value[0]) * 1000)

    return None


def _extract_teams(r_text: str) -> t.List[t.Dict]:
    ret = []
    soup = BeautifulSoup(r_text, 'html.parser')
    t_body = soup.find('div', attrs={'id': 'yw1'}).find('table', attrs={'class': 'items'}).find('tbody')
    for tr in t_body.findChildren('tr', recursive=False):
        try:
            tds = [td for td in tr.findChildren('td', recursive=False)]
            team_name = tds[2].find('a')['title']
            league_name = tds[3].find('a')['title']
            value = tds[4].find('b').text
            value = _extract_value(value)
            if not all((league_name, team_name, value)):
                LOGGER.warning(f'Skipping row {tr}: {(league_name, team_name, value)}')
                continue
            ret.append({
                'team': team_name,
                'league': league_name,
                'value': value
            })
        except Exception as e:
            LOGGER.error(f'Skipping row {tr}')
            LOGGER.error(f'{e}\n{traceback.format_exc()}')
    return ret


def collect_valuable_teams(up_to_page: int):
    page = 1
    res = []
    current_retries = 0
    try:
        for i in range(page, up_to_page + 1):
            LOGGER.info(f'Page {i}')
            r = requests.get(
                f'https://www.transfermarkt.co.uk/spieler-statistik/wertvollstemannschaften/marktwertetop?ajax=yw1&page={i}',
                headers=HEADERS)
            if r.status_code != 200:
                LOGGER.warning(f'{r.status_code}: {r.text}')
                current_retries += 1
                if current_retries == MAX_RETRIES:
                    raise MaxRetriesReached('Max retries reached')
                time.sleep(5)
            current_retries = 0
            res.extend(_extract_teams(r.text))
            time.sleep(.2)
    except MaxRetriesReached:
        LOGGER.warning('Max retries reached')
    except Exception as e:
        stack = traceback.format_exc()
        LOGGER.error(f'Unhandled exception: {e}\n{stack}')

    now = datetime.datetime.now().strftime("%m_%d_%Y__%H_%M_%S")
    with open(Path(RESULTS_FOLDER, f'teams_{now}.json'), 'w') as f:
        json.dump(res, f, indent=4, ensure_ascii=False)


def _extract_players(r_text: str) -> t.List[t.Dict]:
    ret = []
    soup = BeautifulSoup(r_text, 'html.parser')
    t_body = soup.find('div', attrs={'id': 'yw1'}).find('table', attrs={'class': 'items'}).find('tbody')
    for tr in t_body.findChildren('tr', recursive=False):
        try:
            tds = [td for td in tr.findChildren('td', recursive=False)]
            player_name = tds[1].find('td', attrs={'class': 'hauptlink'}).find('a')['title']
            team_name = tds[4].find('a')['title']
            value = tds[5].find('a').text
            value = _extract_value(value)
            if not all((player_name, team_name, value)):
                LOGGER.warning(f'Skipping row {tr}: {(player_name, team_name, value)}')
                continue
            ret.append({
                'player': player_name,
                'team': team_name,
                'value': value
            })
        except Exception as e:
            LOGGER.error(f'Skipping row {tr}')
            LOGGER.error(f'{e}\n{traceback.format_exc()}')
    return ret


def collect_valuable_players(up_to_page: int):
    page = 1
    res = []
    current_retries = 0
    try:
        for i in range(page, up_to_page + 1):
            LOGGER.info(f'Page {i}')
            r = requests.get(
                f'https://www.transfermarkt.co.uk/spieler-statistik/wertvollstespieler/marktwertetop?ajax=yw1&page={i}',
                headers=HEADERS)
            if r.status_code != 200:
                LOGGER.warning(f'{r.status_code}: {r.text}')
                current_retries += 1
                if current_retries == MAX_RETRIES:
                    raise MaxRetriesReached('Max retries reached')
                time.sleep(5)
            current_retries = 0
            res.extend(_extract_players(r.text))
            time.sleep(.2)
    except MaxRetriesReached:
        LOGGER.warning('Max retries reached')
    except Exception as e:
        stack = traceback.format_exc()
        LOGGER.error(f'Unhandled exception: {e}\n{stack}')

    now = datetime.datetime.now().strftime("%m_%d_%Y__%H_%M_%S")
    with open(Path(RESULTS_FOLDER, f'players_{now}.json'), 'w') as f:
        json.dump(res, f, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    # collect_valuable_players(1000)
    collect_valuable_teams(100)
