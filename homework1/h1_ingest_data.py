#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
import click


def ingest_df(
    df: pd.DataFrame,
    engine,
    target_table: str,
    chunksize: int = 100_000,
):
    # 1) create table
    df.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace",
        index=False
    )
    print(f"Table {target_table} created")

    # 2) add data
    total = len(df)
    for i in range(0, total, chunksize):
        chunk = df.iloc[i:i+chunksize]
        chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False
        )
        print(f"Inserted rows: {min(i+chunksize, total)}/{total}")

    print(f"Done ingesting to {target_table}")


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='green_tripdata_2025-11_merged', help='Target table name')
@click.option('--trip-parquet', default='green_tripdata_2025-11.parquet', help='Path to trip parquet file (optional)')
@click.option('--zone-lookup', default='taxi_zone_lookup.csv', help='Path to taxi zone lookup csv')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, target_table, trip_parquet, zone_lookup):

    engine = create_engine(f'postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_lkp = pd.read_csv(zone_lookup)
    df_trip = pd.read_parquet(trip_parquet)

    df_trip = df_trip.merge(df_lkp, how='inner', left_on='PULocationID', right_on='LocationID')
    df_trip = df_trip.merge(df_lkp, how='inner', left_on='DOLocationID', right_on='LocationID',
                            suffixes=('_pu', '_do'))

    ingest_df(
        df=df_trip,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )

if __name__ == '__main__':
    main()