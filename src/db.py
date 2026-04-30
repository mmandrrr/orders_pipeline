import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
db_password = os.getenv("DATABASE_PASSWORD")


def get_engine():
    """Creates and returns SQLAlchemy engine for PostgreSQL."""
    engine = create_engine(
        f"postgresql://postgres:{db_password}@host.docker.internal:5432/music_db"
    )
    return engine
