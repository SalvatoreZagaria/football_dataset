import re
import math
import json
import datetime
import typing as t
from pathlib import Path
from multiprocessing import Pool

import sqlalchemy
from sqlalchemy.orm import Session
from fuzzywuzzy import fuzz

import logger
import db_interactor
from db_interactor import model as m


LOGGER = logger.get_logger('market_values')

SIMILARITY_THRESHOLD = 70
NAMES_FIXER = re.compile(r'[a-z][A-Z]')
LEAGUE_NAME_FIXER = {
    'Série A': 'Serie A',
    'Liga Portugal': 'Primeira Liga',
    'Premier Liga': 'Premier League',
}
TEAM_NAME_FIXER = {

}


def initializer():
    m.engine.dispose(close=False)


def fuzz_similar(a: str, b: str):
    return fuzz.partial_ratio(a, b)


def fix_name(s: str) -> str:
    to_fix = NAMES_FIXER.findall(s)
    for f in to_fix:
        s = s.replace(f, f'{f[0]} {f[1]}', 1)
    s = s.replace('-', ' ')
    return s


def fix_league_name(s: str) -> str:
    s = fix_name(s)
    return LEAGUE_NAME_FIXER.get(s, s)


def fix_team_name(s: str) -> str:
    s = fix_name(s)
    return TEAM_NAME_FIXER.get(s, s)


def process_team(team):
    team = team[0]
    team_name = fix_team_name(team['team'])
    league_name = fix_league_name(team['league'])

    with db_interactor.get_session() as session:
        teams_records = session.query(m.Team).filter(m.Team.name == team_name).all()
        if teams_records:
            teams_records = {(team.id, mi.league.display_name, mi.league_id)
                             for team in teams_records for mi in team.militancy}
            teams_records = {(a[0], a[1], a[2], fuzz_similar(a[1], league_name)) for a in teams_records}
            teams_records = [tr for tr in teams_records if tr[-1] >= SIMILARITY_THRESHOLD]
            if teams_records:
                team_record = max(teams_records, key=lambda tr: tr[-1])
                return {
                    'team': {'name': team_name, 'id': team_record[0]},
                    'league': {'name': team_record[1], 'id': team_record[2]},
                    'value': team['value']
                }
        league_records = session.query(m.League).filter(m.League.display_name == league_name).all()
        if league_records:
            league_records = {(league.id, mi.team.name, mi.team.id)
                              for league in league_records for mi in league.militancy}
            league_records = {(a[0], a[1], a[2], fuzz_similar(a[1], team_name)) for a in league_records}
            league_records = [lr for lr in league_records if lr[-1] >= SIMILARITY_THRESHOLD]
            if league_records:
                league_record = max(league_records, key=lambda lr: lr[-1])
                return {
                    'team': {'name': league_record[1], 'id': league_record[2]},
                    'league': {'name': league_name, 'id': league_record[0]},
                    'value': team['value']
                }

    LOGGER.warning(f'Nothing found for team value - league: {league_name}, team: {team_name}')
    return None


def get_potential_players(name: str, session: Session):
    player = session.query(m.Player.id.label('player_id'),
                           sqlalchemy.func.concat(m.Player.name, ' ', m.Player.surname
                                                  ).label('full_name')).cte('player')

    player_sim = session.query(player.columns.player_id.label('player_id')).order_by(
        sqlalchemy.desc(sqlalchemy.func.similarity(name, player.columns.full_name))).limit(10).cte()

    query = session.query(player_sim, m.Militancy).outerjoin(m.Militancy)

    players_records = {(r[0], r[1].team.name, r[1].team.id, r[1].appearences) for r in query.all() if r[1]}

    return players_records


def get_potential_teams(name: str, session: Session):
    team_sim = session.query(m.Team.id).order_by(
        sqlalchemy.desc(sqlalchemy.func.similarity(name, m.Team.name))).limit(5).cte()

    query = session.query(team_sim, m.Militancy).outerjoin(m.Militancy)

    teams_records = {(r[0], f'{r[1].player.name} {r[1].player.surname}', r[1].player_id, r[1].appearences)
                     for r in query.all() if r[1]}

    return teams_records


def process_player(player):
    player = player[0]
    player_name = player['player']
    team_name = player['team']

    with db_interactor.get_session() as session:
        players_records = get_potential_players(player_name, session)
        if players_records:
            players_records = {(a[0], a[1], a[2], a[3], fuzz_similar(a[1], team_name)) for a in players_records}
            players_records = [pr for pr in players_records if pr[-1] >= SIMILARITY_THRESHOLD]
            if players_records:
                max_similarity = max([pr[-1] for pr in players_records])
                players_records = [pr for pr in players_records if pr[-1] == max_similarity]
                player_record = max(players_records, key=lambda pr: pr[-2])
                return {
                    'player': {'name': player_name, 'id': player_record[0]},
                    'team': {'name': player_record[1], 'id': player_record[2]},
                    'value': player['value']
                }

        teams_records = get_potential_teams(team_name, session)
        if teams_records:
            teams_records = {(a[0], a[1], a[2], a[3], fuzz_similar(a[1], player_name)) for a in teams_records}
            teams_records = [tr for tr in teams_records if tr[-1] >= SIMILARITY_THRESHOLD]
            if teams_records:
                max_similarity = max([tr[-1] for tr in teams_records])
                teams_records = [tr for tr in teams_records if tr[-1] == max_similarity]
                team_record = max(teams_records, key=lambda tr: tr[-2])
                return {
                    'player': {'name': team_record[1], 'id': team_record[2]},
                    'team': {'name': team_name, 'id': team_record[0]},
                    'value': player['value']
                }

    LOGGER.warning(f'Nothing found for player value - player: {player_name}, team: {team_name}')
    return None


def find_ids(teams: t.List[t.Dict], players: t.List[t.Dict]) -> t.Tuple[t.List, t.List]:
    teams = list({(team['team'], team['league']): team for team in teams}.values())
    args = [(team,) for team in teams]
    with Pool(12, initializer=initializer) as p:
        teams_res = p.map(process_team, args)

    teams_not_found = [team for team, res in zip(teams, teams_res) if not res]
    LOGGER.warning(f'{len(teams_not_found)} teams could not be identified')

    players = list({(player['player'], player['team']): player for player in players}.values())
    args = [(player,) for player in players]
    with Pool(12, initializer=initializer) as p:
        players_res = p.map(process_player, args)

    players_not_found = [p for p, res in zip(players, players_res) if not res]
    LOGGER.warning(f'{len(players_not_found)} players could not be identified')

    now = datetime.datetime.now().strftime("%m_%d_%Y__%H_%M_%S")
    log_dir = Path('.not_found')
    log_dir.mkdir(exist_ok=True)
    with open(Path(log_dir, f'teams_not_found_{now}.json'), 'w') as f:
        json.dump(teams_not_found, f, indent=4, ensure_ascii=False)

    with open(Path(log_dir, f'players_not_found_{now}.json'), 'w') as f:
        json.dump(players_not_found, f, indent=4, ensure_ascii=False)

    return [team for team in teams_res if team], [player for player in players_res if player]


def sort_data(teams: t.List[t.Dict], players: t.List[t.Dict]) -> t.Tuple[t.Set, t.Dict, t.Dict]:
    all_leagues = {team['league']['id'] for team in teams}
    all_teams = {team['team']['id']: team['value'] for team in teams}
    for player in players:
        team_id = player['team']['id']
        if team_id in all_teams:
            continue
        all_teams[team_id] = 100     # assigning a default value

    all_players = {player['player']['id']: player['value'] for player in players}

    return all_leagues, all_teams, all_players


def apply_formula(leagues, teams, players):
    # for all leagues get all teams and from teams all players (most recent year) and assign a base value
    with db_interactor.get_session() as session:
        for l_id in leagues:
            league = session.query(m.League).get(l_id)
            max_year = max([lm.year for lm in league.militancy])
            this_teams = [lm.team.id for lm in league.militancy if lm.year == max_year]
            for team_id in this_teams:
                sub_query = session.query(m.Militancy.player_id).filter_by(team_id=team_id, year=max_year).subquery()
                query = session.query(m.Player).filter(m.Player.id.in_(sub_query))
                for player in query.all():
                    player.value = 1
                    session.add(player)

        # teams with big value
        # get the average value of a player for a team and weight it in relation with a player's appearences
        # I have chosen a logarithmic function, the formula would be
        '''
        team_value / n_players (I assume this is 10) = player_average_value -> 
        appearences_weight(appearences, player_average_value)
    
        appearences_weight is a logarithmic function like this: log[b](x) = y, with x being the number of appearences 
        and y being one player's average
        . To calculate x, I need to know that for a maximum number of appearences I would have the player average value
        as a result, so, converting the logarithm, x=\sqrt[player_average_value]{maximum_possible_appearences}
        '''

        for team_id, team_value in teams.items():
            player_average_value = int(team_value / 10)
            militancies = session.query(m.Militancy).filter(m.Militancy.team_id == team_id).all()

            min_year = min([mi.year for mi in militancies])
            year = max([mi.year for mi in militancies])
            while year >= min_year:
                cur_militancies = [mi for mi in militancies if mi.year == year]
                max_appearences = max([mi.appearences for mi in cur_militancies]) if cur_militancies else 0
                if max_appearences > 10:    # picking a long enough season
                    break
                year -= 1
            if max_appearences <= 10:
                continue

            log_base = max_appearences ** (1 / player_average_value)
            for mi in cur_militancies:
                player = session.query(m.Player).get(mi.player_id)
                new_player_value = int(math.log(mi.appearences + 1, log_base))
                player.value = max((player.value, new_player_value))
                session.add(player)

        for player_id, player_value in players.items():
            player = session.query(m.Player).get(player_id)
            player.value = max((player.value, player_value))
            session.add(player)

        session.commit()


def main(teams_path: str, players_path: str, cut_players: int = None):
    teams_path = Path(teams_path).absolute()
    players_path = Path(players_path).absolute()
    assert teams_path.exists() and players_path.exists(), 'File(s) not found'

    with open(teams_path, 'r') as f:
        teams = json.load(f)
    with open(players_path, 'r') as f:
        players = json.load(f)
    if cut_players:
        players = players[:cut_players]

    teams, players = find_ids(teams, players)

    leagues, teams, players = sort_data(teams, players)

    apply_formula(leagues, teams, players)


if __name__ == '__main__':
    main(
        '/Users/salvo/dev/repos/football_dataset/api_client/.transfermarkt_results/teams_04_29_2023__16_20_06.json',
        '/Users/salvo/dev/repos/football_dataset/api_client/.transfermarkt_results/players_04_29_2023__16_10_45.json'
    )

