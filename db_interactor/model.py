from sqlalchemy import ForeignKey, String, Column, Integer, LargeBinary, Date, Float, MetaData, create_engine, \
    PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from shared import db as db_utils

metadata_obj = MetaData()
base = declarative_base(metadata=metadata_obj)
engine = create_engine(db_utils.get_db_url())


class TeamMilitancy(base):
    __tablename__ = 'teammilitancy'
    __table_args__ = (
        PrimaryKeyConstraint('team_id', 'league_id', 'year'),
    )
    team_id = Column(Integer, ForeignKey('team.id'))
    league_id = Column(Integer, ForeignKey('league.id'))
    year = Column(Integer)


class Team(base):
    __tablename__ = 'team'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    img = Column(LargeBinary)
    img_url = Column(String)
    militancy = relationship(TeamMilitancy, backref='team')


class Militancy(base):
    __tablename__ = 'militancy'
    __table_args__ = (
        PrimaryKeyConstraint('player_id', 'team_id', 'year'),
    )

    player_id = Column(Integer, ForeignKey('player.id'))
    team_id = Column(Integer, ForeignKey('team.id'))
    year = Column(Integer)
    start_date = Column(Date, default=None)
    end_date = Column(Date, default=None)
    appearences = Column(Integer)
    team = relationship(Team, backref='player_militancy')


class Player(base):
    __tablename__ = 'player'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    surname = Column(String)
    position = Column(String)
    img = Column(LargeBinary)
    img_url = Column(String)
    value = Column(Float, default=0)
    militancy = relationship(Militancy, backref='player')


class League(base):
    __tablename__ = 'league'

    id = Column(Integer, primary_key=True)
    display_name = Column(String)
    img = Column(LargeBinary)
    img_url = Column(String)
    country_code = Column(String)
    militancy = relationship(TeamMilitancy, backref='league')


class LeagueSeasons(base):
    __tablename__ = 'leagueseasons'
    __table_args__ = (
        PrimaryKeyConstraint('league_id', 'year'),
    )

    league_id = Column(Integer, ForeignKey('league.id'))
    year = Column(Integer)
    start_date = Column(Date, default=None)
    end_date = Column(Date, default=None)
