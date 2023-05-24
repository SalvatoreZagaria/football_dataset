import psycopg2
from sqlalchemy.orm import Session

from shared import db as db_utils


def get_session():
    from db_interactor import model
    return Session(model.engine)


def init_db():
    from db_interactor import model
    model.metadata_obj.create_all(model.engine)
    with psycopg2.connect(db_utils.get_db_url()) as con:
        cursor = con.cursor()
        cursor.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm;')
        con.commit()
