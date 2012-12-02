import sqlalchemy
from sqlalchemy import Column

from pp.db import Base

class TestTable(Base):
    __tablename__ = 'test'

    id = Column(sqlalchemy.types.String(36), primary_key=True, nullable=False, unique=True)
    foo = Column(sqlalchemy.types.String(200), nullable=False)

def init():
    """Called to do the initial metadata set up.

    Returns a list of the tables, mappers and declarative base classes this
    module implements.

    """
    declarative_bases = [TestTable]
    tables = []
    mappers = []
    return (declarative_bases, tables, mappers)
