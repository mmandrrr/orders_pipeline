"""Extract data from DB with SQLAlchemy"""

import pandas as pd


def extract(engine, query):
    """Executes SQL query and returns DataFrame."""

    return pd.read_sql_query(query, engine)
