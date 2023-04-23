import typing as t
from multiprocessing import Pool

from sqlalchemy.dialects.postgresql import insert

import logger
import api_client
from api_client import api_football_client, utils
import db_interactor
from db_interactor import model as m


LOGGER = logger.get_logger('api_client')


def get_insert_do_nothing_stmt(table, values, no_constraint=False):
    index_elements = None if no_constraint else ['id']
    insert_stmt = insert(table).values(**values)
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(
        index_elements=index_elements
    )
    return do_nothing_stmt


def store_leagues(leagues: t.List[t.Dict]) -> t.List[t.Dict]:
    ret = []
    with m.engine.connect() as conn:
        for l in leagues:
            seasons = []
            for s in l.get('seasons') or []:
                year = s.get('year')
                if year not in api_client.YEARS:
                    continue
                start_date = utils.convert_to_date(s.get('start'))
                end_date = utils.convert_to_date(s.get('end'))
                s_values = {'league_id': l['league']['id'], 'year': s.get('year'), 'start_date': start_date,
                            'end_date': end_date}
                if not all(s_values.values()):
                    seasons = []
                    break
                seasons.append(s_values)
            if not seasons:
                continue

            values = {'id': l['league']['id'], 'img_url': l['league']['logo'], 'display_name': l['league']['name'],
                      'country_code': l['country']['code']}
            do_nothing_stmt = get_insert_do_nothing_stmt(m.League, values)
            conn.execute(do_nothing_stmt)

            for s_values in seasons:
                s_do_nothing_stmt = get_insert_do_nothing_stmt(m.LeagueSeasons, s_values, no_constraint=True)
                conn.execute(s_do_nothing_stmt)

            ret.append({'id': l['league']['id'], 'seasons': seasons})

        conn.commit()

    return ret


def process_players_batch(players_batch: t.List[t.Dict], season: t.Dict):
    teams = []
    players = []
    militancies = []
    if not players_batch:
        return teams, players, militancies

    for p in players_batch:
        if not p.get('player', {}).get('id') or not p.get('statistics'):
            continue
        player_values = {'id': p['player']['id'], 'name': p['player']['firstname'],
                         'surname': p['player']['lastname'], 'position': p['statistics'][0]['games']['position'],
                         'img_url': p['player']['photo']}
        players.append(player_values)

        for s in p['statistics']:
            if not s.get('team', {}).get('id'):
                continue
            team_values = {'id': s['team']['id'], 'name': s['team']['name'], 'img_url': s['team']['logo'],
                           'league_id': s['league']['id']}
            teams.append(team_values)

            militancy_values = {'player_id': player_values['id'], 'team_id': team_values['id'],
                                'year': season['year'], 'start_date': season['start_date'],
                                'end_date': season['end_date'], 'rating': s['games']['rating'],
                                'appearences': s['games']['appearences'] or 0}
            militancies.append(militancy_values)

    teams = list({team['id']: team for team in teams}.values())
    militancies = list({(mi['player_id'], mi['team_id'], mi['year']): mi for mi in militancies}.values())

    return teams, players, militancies


def initializer():
    m.engine.dispose(close=False)


def process_league_year_players(*args):
    l_id, season = args[0]
    client = api_football_client.APIFootballClient()
    players = client.get_league_players(l_id, season['year'])
    teams, players, militancies = process_players_batch(players, season)
    return teams, players, militancies


def process_teams(teams):
    teams = {team['id']: team for team in teams}
    LOGGER.info(f'TEAMS - storing {len(teams)} teams')
    with db_interactor.get_session() as session:
        teams_objs = [m.Team(**team) for team in teams.values()]
        session.bulk_save_objects(teams_objs)
        session.commit()


def process_players(players):
    players = {player['id']: player for player in players}
    LOGGER.info(f'PLAYERS - storing {len(players)} players')
    with db_interactor.get_session() as session:
        players_objs = [m.Player(**player) for player in players.values()]
        session.bulk_save_objects(players_objs)
        session.commit()


def process_militancies(militancies):
    militancies = {(mi['player_id'], mi['team_id'], mi['year']): mi for mi in militancies}
    LOGGER.info(f'MILITANCIES - storing {len(militancies)} militancies')
    with db_interactor.get_session() as session:
        m_objs = [m.Militancy(**m_obj) for m_obj in militancies.values()]
        session.bulk_save_objects(m_objs)
        session.commit()


def main():
    db_interactor.init_db()
    client = api_football_client.APIFootballClient(requests_block=5)
    all_leagues = client.get_leagues()
    all_leagues = store_leagues(all_leagues)

    args = [(league['id'], s) for league in all_leagues for s in league['seasons']]
    LOGGER.info(f'LEAGUES - Starting multiprocessing ({len(args)}) processes')
    with Pool(10, initializer=initializer) as p:
        data = p.map(process_league_year_players, args)

    teams = [team for d in data for team in d[0]]
    players = [p for d in data for p in d[1]]
    militancies = [mi for d in data for mi in d[2]]
    del data

    LOGGER.info('Storing teams')
    process_teams(teams)
    LOGGER.info('Storing players')
    process_players(players)
    LOGGER.info('Storing militancies')
    process_militancies(militancies)


if __name__ == '__main__':
    main()
