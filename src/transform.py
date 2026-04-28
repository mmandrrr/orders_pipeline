"""Adds column with amount in UAH and filters orders with amount > 100"""


def transform(df):
    df["amount_uah"] = df["amount"] * 43.5
    df = df[df["amount"] > 100]
    df["date"] = df["created_at"].dt.date
    return df
