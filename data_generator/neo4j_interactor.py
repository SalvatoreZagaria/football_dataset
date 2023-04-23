import os
import csv
import time
import shutil
import logger
import subprocess
import typing as t
from pathlib import Path
from multiprocessing import Pool

import db_interactor
from db_interactor import model as m


LOGGER = logger.get_logger('data_generator')


def initializer():
    m.engine.dispose(close=False)


def get_all_player_ids():
    with db_interactor.get_session() as session:
        query = session.query(m.Player.id)
        return [row[0] for row in query.all()]


def generate_player_relationships(*args):
    p_id, i, tot = args[0]
    LOGGER.info(f'Player {i+1} of {tot}')
    militancies = {p_id: set()}
    with db_interactor.get_session() as session:
        this_player_militancies = session.query(m.Militancy).filter_by(player_id=p_id).all()

        for mi in this_player_militancies:
            other_players_militancies = session.query(m.Militancy.player_id).filter(
                m.Militancy.team_id == mi.team_id, m.Militancy.start_date >= mi.start_date,
                m.Militancy.end_date <= mi.end_date)
            militancies[p_id].update([mi[0] for mi in other_players_militancies])
    if p_id in militancies[p_id]:
        militancies[p_id].remove(p_id)

    return militancies


def dump_csvs(data: t.Dict[str, t.List]):
    # players nodes
    shutil.rmtree('csv_files', ignore_errors=True)
    os.mkdir('csv_files')
    with open('csv_files/players-header.csv', 'w', encoding='UTF8') as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(('playerId:ID', ':LABEL'))

    LOGGER.info(f'Players csv...')
    with open('csv_files/players.csv', 'w', encoding='UTF8') as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerows([(p, 'Player') for p in data])

    # relationships
    with open('csv_files/played-with-header.csv', 'w', encoding='UTF8') as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow((':START_ID', ':END_ID', ':TYPE'))
    relationships = []
    for p, rels in data.items():
        for rp in rels:
            relationships.append((p, rp, 'PLAYED_WITH'))

    i = 1
    estimated = int(len(relationships) / 100000) + 1
    LOGGER.info(f'Relationships csv...')
    while relationships:
        LOGGER.info(f'{i}/{estimated} (estimated)...')
        with open(f'csv_files/played-with-part{i}.csv', 'w', encoding='UTF8') as f:
            writer = csv.writer(f, delimiter=",")
            writer.writerows(relationships[:100000])
        relationships = relationships[100000:]
        i += 1


def generate_relationships():
    all_player_ids = get_all_player_ids()
    all_player_ids = [(p_id, i, len(all_player_ids)) for i, p_id in enumerate(all_player_ids)]

    LOGGER.info(f'Generating relationships for {len(all_player_ids)} players...')
    with Pool(14, initializer=initializer) as p:
        data = p.map(generate_player_relationships, all_player_ids)

    LOGGER.info(f'Relationships generated')
    LOGGER.info(f'Dumping csvs...')
    data = {p: r for d in data for p, r in d.items()}
    dump_csvs(data)


def import_csv_command_line(csv_files_root=None):
    if not csv_files_root:
        csv_files_root = Path('csv_files')
    if not csv_files_root.exists():
        raise FileNotFoundError(f'{csv_files_root} does not exist')

    try:
        neo4j_home = Path(os.getenv("NEO4J_HOME"))
        assert neo4j_home.exists()
    except:
        raise EnvironmentError('NEO4J_HOME env variable must be properly set')

    relationships_parts = ','.join([str(f.name) for f in csv_files_root.glob('played-with-part*')])
    cmd = f'{neo4j_home.absolute()}/bin/neo4j-admin database import full --nodes=players-header.csv,players.csv ' \
          f'--relationships=played-with-header.csv,{relationships_parts} ' \
          f'neo4j --overwrite-destination --skip-bad-relationships --verbose'

    process = subprocess.Popen(cmd, shell=True, cwd=str(csv_files_root.absolute()), stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    stdout = stdout.decode("utf-8")
    stderr = stderr.decode("utf-8")

    print(stdout)
    if stderr:
        print('ERROR')
        print(stderr)


if __name__ == '__main__':
    start = time.time()
    generate_relationships()
    import_csv_command_line()
    print(f'Time taken: {time.time() - start}')
