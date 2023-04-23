import typing as t
import sqlalchemy
from sqlalchemy.orm import Session
import traceback
from multiprocessing import Pool

import logger
from api_client import api_football_client
from data_generator import utils
import db_interactor
from db_interactor import model as m


LOGGER = logger.get_logger('api_client')


def initializer():
    m.engine.dispose(close=False)


def download_images():
    pass


def get_all_teams(session: Session):
    query = session.query(m.Team.id)
    return [row[0] for row in query.all()]


def create_militancy_if_possible(player_id, team_id, transfer_date, session) -> t.Optional[m.Militancy]:
    team = session.query(m.Team).filter_by(id=team_id).first()
    if not team:
        return None
    if session.query(m.Player.id).filter_by(id=player_id).first() is None:
        return None
    team_seasons = session.query(m.LeagueSeasons).filter_by(league_id=team.league_id).all()
    if not team_seasons:
        return None
    seasons = [ts for ts in team_seasons if ts.start_date < transfer_date < ts.end_date]
    if seasons:
        season = seasons[0]
    else:
        seasons = [ts for ts in team_seasons if transfer_date > ts.end_date]
        seasons = sorted(seasons, key=lambda ts: ts.end_date)
        season = seasons[-1] if seasons else None

    if not season:
        return None

    militancy = m.Militancy(player_id=player_id, team_id=team_id, year=season.year, start_date=season.start_date,
                            end_date=season.end_date, appearences=0)
    return militancy


def fix_player_transfer(player_transfer: t.Dict, session: sqlalchemy.orm.Session):
    if not player_transfer.get('transfers') or not player_transfer.get('player', {}).get('id'):
        return
    player_id = player_transfer['player']['id']
    player_militancy = session.query(m.Militancy).filter_by(player_id=player_id).all()
    for transfer in player_transfer['transfers']:
        transfer_date = utils.convert_to_date(transfer['date'])
        if not transfer_date:
            LOGGER.info(f'Skipping transfer, player_id: {player_id} (Reason: date={transfer["date"]})')
            continue

        for team, is_out in zip((transfer['teams']['out'], transfer['teams']['in']), (True, False)):
            if team['id'] not in (pm.team_id for pm in player_militancy):
                new_militancy = create_militancy_if_possible(player_id, team['id'], transfer_date, session)
                if not new_militancy:
                    continue
                session.add(new_militancy)
                player_militancy.append(new_militancy)

            this_militancy = [pm for pm in player_militancy
                              if pm.team_id == team['id'] and pm.start_date < transfer_date < pm.end_date]

            if not this_militancy:
                LOGGER.info(
                    f'Skipping transfer, player_id: {player_id}, team_id: {team["id"]} (Reason: militancy not found)')
                continue

            if is_out:
                this_militancy[0].end_date = transfer_date
            else:
                this_militancy[0].start_date = transfer_date
            session.add(this_militancy[0])


def get_team_transfer(t_id):
    t_id = t_id[0]
    LOGGER.info(f'Processing team {t_id}')
    client = api_football_client.APIFootballClient(requests_block=500)
    transfers = client.get_team_transfers(t_id)
    return transfers or []


def fix_transfers():
    with db_interactor.get_session() as session:
        team_ids = get_all_teams(session)

    args = [(t_id,) for t_id in team_ids]
    try:
        with Pool(4, initializer=initializer) as p:
            data = p.map(get_team_transfer, args)

        transfers = [tr for d in data for tr in d]
        LOGGER.info(f'Processing {len(transfers)} player transfers')
        with db_interactor.get_session() as session:
            for i, player_transfers in enumerate(transfers):
                if i % 5000 == 0:
                    print(f'TRANSFER {i+1}/{len(transfers)}')
                fix_player_transfer(player_transfers, session)
            session.commit()
    except Exception as e:
        LOGGER.error(f'Exception occurred: {e}')
        traceback.print_stack()
        session.rollback()


if __name__ == '__main__':
    import time
    start = time.time()
    fix_transfers()
    print(f'Time taken: {time.time() - start}')
