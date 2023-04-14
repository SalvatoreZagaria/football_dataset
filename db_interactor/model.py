import os

from sqlalchemy import ForeignKey, String, Column, Integer, LargeBinary, PrimaryKeyConstraint, DateTime, Float, \
    MetaData, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from shared import db as db_utils

metadata_obj = MetaData()
base = declarative_base(metadata=metadata_obj)
engine = create_engine(db_utils.get_db_url())


class Player(base):
    __tablename__ = 'player'

    id = Column(String, primary_key=True)
    name = Column(String)
    surname = Column(String)
    position = Column(String)
    img = Column(LargeBinary)
    img_url = Column(String)
    militancy = relationship('militancy', backref='player')


class Team(base):
    __tablename__ = 'team'

    id = Column(String, primary_key=True)
    name = Column(String)
    img = Column(LargeBinary)
    img_url = Column(String)
    league_id = Column(String, ForeignKey('league.id'))
    militancy = relationship('militancy', backref='team')


class League(base):
    __tablename__ = 'league'

    id = Column(String, primary_key=True)
    display_name = Column(String)
    img = Column(LargeBinary)
    img_url = Column(String)
    teams = relationship('team', backref='league')


class Militancy(base):
    __tablename__ = 'militancy'

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(String, ForeignKey('player.id'))
    team_id = Column(String, ForeignKey('team.id'))
    year = Column(Integer)
    start_date = Column(DateTime, default=None)
    end_date = Column(DateTime, default=None)
    rating = Column(Float)
