from sqlalchemy import create_engine, ForeignKey, Column, Integer, String, CHAR, DateTime, Boolean, Enum
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database

BASE_URL = 'postgresql://postgres:postgres@localhost:5433/movies_db'


def get_engine(url):
    "Get the database engine"
    if not database_exists(url):
        create_database(url)

    engine = create_engine(url)
    return engine


def get_session(engine):
    "Get the database session"
    make_session = sessionmaker(bind=engine)
    session = make_session()
    return session


db_engine = get_engine(BASE_URL)

Base = declarative_base()



Base.metadata.create_all(bind=db_engine)

db_session = get_session(db_engine)
