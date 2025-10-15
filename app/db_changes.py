from sqlalchemy import Column, Integer
from flask_sqlalchemy import SQLAlchemy


def apply_db_changes(db: SQLAlchemy):
    from dapytains.app.database import Collection

    # Define the new column
    nb_children = Column('nb_children', Integer)
    # Attach it to the table
    Collection.__table__.append_column(nb_children)
    # Also attach it to the ORM mapper
    setattr(Collection, 'nb_children', nb_children)