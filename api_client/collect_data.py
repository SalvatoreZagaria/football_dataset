import typing as t
import datetime

from sqlalchemy.dialects.postgresql import insert

from api_client import api_football_client
import db_interactor
from db_interactor import model as m


LEAGUES = (('Serie A', 'Italy'), ('Premier League', 'England'))

starting_year = 2015
current_year = datetime.datetime.now().year
YEARS = [y for y in range(starting_year, current_year + 1)]


def get_insert_do_nothing_stmt(table, values):
    insert_stmt = insert(table).values(**values)
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(
        index_elements=['id']
    )
    return do_nothing_stmt


def store_leagues(leagues: t.List[t.Dict]):
    with m.engine.connect() as conn:
        for l in leagues:
            values = {'id': l['league']['id'], 'img_url': l['league']['logo'], 'display_name': l['league']['name']}
            do_nothing_stmt = get_insert_do_nothing_stmt(m.League, values)
            conn.execute(do_nothing_stmt)
        conn.commit()


def process_players_batch(players: t.List[t.Dict]):
    with m.engine.connect() as conn:
        for p in players:
            player_values = {'id': p['player']['id'], 'name': p['player']['firstname'],
                             'surname': p['player']['lastname'], 'position': p['statistics'][0]['games']['position'],
                             'img_url': p['player']['photo']}
            player_do_nothing_stmt = get_insert_do_nothing_stmt(m.Player, player_values)
            conn.execute(player_do_nothing_stmt)

            for s in p['statistics']:
                team_values = {'id': s['team']['id'], 'name': s['team']['name'], 'img_url': s['team']['logo'],
                               'league_id': s['league']['id']}
                team_do_nothing_stmt = get_insert_do_nothing_stmt(m.Team, team_values)
                conn.execute(team_do_nothing_stmt)

                militancy_values = {'player_id': player_values['id'], 'team_id': team_values['id'],
                                    'year': s['league']['season'], 'rating': s['games']['rating']}
                conn.execute(insert(m.Militancy).values(**militancy_values))
        conn.commit()


def main():
    db_interactor.init_db()
    client = api_football_client.APIFootballClient(requests_block=50000)
    all_leagues = client.get_leagues()
    store_leagues(all_leagues)
    # save all leagues info (including start and end)
    for league in all_leagues:
        league_id = league['league']['id']
        for year in YEARS:
            players = client.get_league_players(league_id, year)    # here I can get players, teams
            process_players_batch(players)
            print()

    # stats are in players (for each year you get list of players and their stats (be careful, each year has different
    # leagues. Just keep the ones that we already have in the set))
    # if, for a year, I get more than a team, call transfer


if __name__ == '__main__':
    main()
