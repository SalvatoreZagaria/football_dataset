import aiohttp
from aiohttp import client_exceptions
import typing as t

import data_collector
import logger

from understat import Understat

LOGGER = logger.init_logs('dataset')

SESSION: aiohttp.ClientSession = None
UNDERSTAT_CLIENT: Understat = None


async def init_session(force=False):
    LOGGER.info('init session...', extra={'force': force})
    global SESSION, UNDERSTAT_CLIENT
    if force:
        await close_session()
    SESSION = aiohttp.ClientSession()
    UNDERSTAT_CLIENT = Understat(SESSION)


async def close_session():
    global SESSION
    if not SESSION.closed:
        await SESSION.close()


async def get_league_teams_in_year(league_name: str, year: int) -> t.List[t.Dict[str, str]]:
    try:
        teams = await UNDERSTAT_CLIENT.get_teams(league_name, year)
    except client_exceptions.ServerDisconnectedError:
        await init_session()
        teams = await UNDERSTAT_CLIENT.get_teams(league_name, year)
    if not teams:
        return []
    return [{'id': team['id'], 'name': team['title']} for team in teams]


async def get_team_players_in_year(team_name: str, year: int) -> t.List[t.Dict[str, t.Union[str, bool]]]:
    try:
        players = await UNDERSTAT_CLIENT.get_team_players(team_name, year)
    except client_exceptions.ServerDisconnectedError:
        await init_session()
        players = await UNDERSTAT_CLIENT.get_team_players(team_name, year)
    if not players:
        return []
    return [{'id': player['id'], 'name': player['player_name'], 'first_half': True, 'second_half': True}
            for player in players]


async def fix_half_season_player(player_obj: t.Dict[str, str], teams: t.List[t.Dict[str, t.Union[str, bool]]],
                                 year: int):
    year = str(year)
    player_id = player_obj['id']
    try:
        player_stats = await UNDERSTAT_CLIENT.get_player_grouped_stats(player_id)
    except client_exceptions.ServerDisconnectedError:
        await init_session()
        player_stats = await UNDERSTAT_CLIENT.get_player_grouped_stats(player_id)
    seasons = [s for s in player_stats.get('season') or [] if s['season'] == year]
    if len(seasons) != 2:
        LOGGER.error('Unhandled case', extra={'error': 'seasons obj must be len=2', 'seasons': seasons,
                                              'player_id': player_id})
        return
    first_team = seasons[1]['team']
    second_team = seasons[0]['team']

    for team_name, key in zip((second_team, first_team), ('first_half', 'second_half')):
        team_obj = [team for team in teams if team['name'] == team_name]
        if len(team_obj) != 1:
            LOGGER.error('Unhandled case', extra={'error': 'team obj must be len=1', 'team_obj': team_obj,
                                                  'player_id': player_id, 'team_name': team_name})
            return
        team_obj = team_obj[0]
        player_obj = [player for player in team_obj['players'] if player['id'] == player_id]
        if len(player_obj) != 1:
            LOGGER.error('Unhandled case', extra={'error': 'player obj must be len=1', 'player_obj': player_obj,
                                                  'player_id': player_id, 'team_name': team_name})
            return
        player_obj = player_obj[0]
        player_obj[key] = False


async def fix_half_season_transfers(teams: t.List[t.Dict[str, t.Union[str, bool]]], year: int):
    LOGGER.info('fixing half season transfers...')
    all_players_to_teams = {}  # player_id: [teams]
    for team in teams:
        for player in team['players']:
            player_id = player['id']
            all_players_to_teams.setdefault(player_id, {'player_obj': player, 'teams': []})
            all_players_to_teams[player_id]['teams'].append(team)

    for player_id, info in all_players_to_teams.items():
        teams = info['teams']
        if len(teams) == 1:
            continue
        await fix_half_season_player(info['player_obj'], teams, year)


async def main(year_from: int, year_to: int, leagues=data_collector.LEAGUES):
    res = {}
    await init_session()

    if isinstance(leagues, str):
        leagues = (leagues,)
    for league in leagues:
        LOGGER.info(f'retrieving {league}...')
        res[league] = {}
        for year in range(year_from, year_to + 1):
            LOGGER.info(f'year {year}...')
            res[league][year] = {'teams': await get_league_teams_in_year(league, year)}
            for team in res[league][year]['teams']:
                LOGGER.info(f'team {team}...')
                team['players'] = await get_team_players_in_year(team['name'], year)
            await fix_half_season_transfers(res[league][year]['teams'], year)
            await init_session(force=True)

    await close_session()
    return res
