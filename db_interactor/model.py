import os

from sqlalchemy import ForeignKey, String, Column, Integer, LargeBinary, Boolean, PrimaryKeyConstraint, \
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
    img = Column(LargeBinary)
    militancy = relationship('militancy', backref='player')


class Team(base):
    __tablename__ = 'team'

    id = Column(String, primary_key=True)
    name = Column(String)
    img = Column(LargeBinary)
    league_id = Column(String, ForeignKey('league.id'))
    militancy = relationship('militancy', backref='team')


class League(base):
    __tablename__ = 'league'

    id = Column(String, primary_key=True)
    name = Column(String)
    display_name = Column(String)
    img = Column(LargeBinary)
    teams = relationship('team', backref='league')


class Militancy(base):
    __tablename__ = 'militancy'
    __table_args__ = (
        PrimaryKeyConstraint('player_id', 'team_id'),
    )

    player_id = Column(String, ForeignKey('player.id'))
    team_id = Column(String, ForeignKey('team.id'))
    year = Column(Integer)
    first_half = Column(Boolean)
    second_half = Column(Boolean)
