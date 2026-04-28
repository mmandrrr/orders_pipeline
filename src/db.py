import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
db_password = os.getenv("DATABASE_PASSWORD")


def get_engine():
    engine = create_engine(
        f"postgresql://postgres:{db_password}@localhost:5432/music_db"
    )
    return engine
