"""ETL combined"""

from src.db import get_engine
from src.extract import extract
from src.transform import transform
from src.load import load


def main():
    engine = get_engine()
    query = "SELECT * FROM orders"
    bucket = "orders-pipeline-my-mini-proj"
    key = "orders/orders_transformed.csv"

    df = extract(engine, query)
    df = transform(df)
    load(df, bucket, key)


if __name__ == "__main__":
    main()
