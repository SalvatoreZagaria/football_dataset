import typing as t
import datetime

from sqlalchemy.dialects.postgresql import insert

import logger
from api_client import api_football_client, utils
import db_interactor
from db_interactor import model as m


LOGGER = logger.get_logger('api_client')

starting_year = 2015
current_year = datetime.datetime.now().year
YEARS = [y for y in range(starting_year, current_year + 1)]


def get_insert_do_nothing_stmt(table, values, no_constraint=False):
    index_elements = None if no_constraint else ['id']
    insert_stmt = insert(table).values(**values)
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(
        index_elements=index_elements
    )
    return do_nothing_stmt


def store_leagues(leagues: t.List[t.Dict]):
    with m.engine.connect() as conn:
        for l in leagues:
            values = {'id': l['league']['id'], 'img_url': l['league']['logo'], 'display_name': l['league']['name'],
                      'country_code': l['country']['code']}
            do_nothing_stmt = get_insert_do_nothing_stmt(m.League, values)
            conn.execute(do_nothing_stmt)

            for s in l.get('seasons') or []:
                start_date = utils.convert_to_datetime(s.get('start'))
                end_date = utils.convert_to_datetime(s.get('end'))
                s_values = {'league_id': values['id'], 'year': s.get('year'), 'start_date': start_date,
                            'end_date': end_date}
                if not all(s_values.values()):
                    continue
                s_do_nothing_stmt = get_insert_do_nothing_stmt(m.LeagueSeasons, s_values, no_constraint=True)
                conn.execute(s_do_nothing_stmt)
        conn.commit()


def process_players_batch(players: t.List[t.Dict], year: int):
    with m.engine.connect() as conn:
        for p in players:
            if not p.get('player', {}).get('id') or not p.get('statistics'):
                continue
            player_values = {'id': p['player']['id'], 'name': p['player']['firstname'],
                             'surname': p['player']['lastname'], 'position': p['statistics'][0]['games']['position'],
                             'img_url': p['player']['photo']}
            player_do_nothing_stmt = get_insert_do_nothing_stmt(m.Player, player_values)
            conn.execute(player_do_nothing_stmt)

            for s in p['statistics']:
                if not s.get('team', {}).get('id'):
                    continue
                team_values = {'id': s['team']['id'], 'name': s['team']['name'], 'img_url': s['team']['logo'],
                               'league_id': s['league']['id']}
                team_do_nothing_stmt = get_insert_do_nothing_stmt(m.Team, team_values)
                conn.execute(team_do_nothing_stmt)

                militancy_values = {'player_id': player_values['id'], 'team_id': team_values['id'],
                                    'year': year, 'rating': s['games']['rating']}
                conn.execute(insert(m.Militancy).values(**militancy_values))
        conn.commit()


def main():
    db_interactor.init_db()
    client = api_football_client.APIFootballClient(requests_block=10000)
    all_leagues = client.get_leagues()
    store_leagues(all_leagues)
    # save all leagues info (including start and end)
    for league in all_leagues:
        league_id = league['league']['id']
        for year in YEARS:
            players = client.get_league_players(league_id, year)
            process_players_batch(players, year)


if __name__ == '__main__':
    main()
