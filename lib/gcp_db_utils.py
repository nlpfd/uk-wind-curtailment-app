import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

from lib import constants

logger = logging.getLogger(__name__)


def get_db_connection():
    """
    Create a SQLAlchemy engine for connecting to the PostgreSQL database.
    This version includes:
    - The database name in the connection URL (important!)
    - Forces the search_path to 'public' schema so unqualified table names work.
    - Enforces SSL connection with sslmode=require for Render PostgreSQL.
    """

    if cloud_sql_instance := os.environ.get("CLOUD_SQL_INSTANCE"):
        # For Google Cloud SQL environment (leave as is)
        address = (
            f"postgresql+psycopg2://{constants.DB_USERNAME}:"
            f"{constants.DB_PASSWORD}@{constants.DB_NAME}"
            f"?host={constants.HOST}:{cloud_sql_instance}"
        )
    else:
        # Local or Render connection: add DB name, force search_path=public, and require SSL
        address = (
            f"postgresql+psycopg2://{constants.DB_USERNAME}:"
            f"{constants.DB_PASSWORD}@{constants.DB_IP}:5432/"
            f"{constants.DB_NAME}?sslmode=require&options=-csearch_path%3Dpublic"
        )
    print("Connecting to DB with URL:", address)  # Optional debug print
    return create_engine(url=address)


def write_curtailment_data(df: pd.DataFrame):

    if len(df) == 0:
        logger.debug("There was not data to write to the database")
    else:
        engine = get_db_connection()
        if "local_datetime" in df.columns:
            df = df.rename(columns={"local_datetime": "time"})

        logger.info(f"Adding curtailment to database ({len(df)})")

        with engine.connect() as conn:
            df.to_sql("curtailment", conn, if_exists="append", index=False)


def write_sbp_data(df: pd.DataFrame):
    df = df.rename(columns={"local_datetime": "time", "systemSellPrice": "system_buy_price"})

    df = df[["time", "system_buy_price"]]

    if len(df) == 0:
        logger.debug("There was not data to write to the database")
    else:
        engine = get_db_connection()
        logger.info(f"Adding sbp to database ({len(df)})")

        with engine.connect() as conn:
            try:
                df.to_sql("sbp", conn, if_exists="append", index=False)
            except IntegrityError:
                logger.warning(f"Failed to write df from {df['time'].min()} to {df['time'].max()}")


def read_data(start_time="2022-01-01", end_time="2023-01-01"):
    engine = get_db_connection()

    # Make sure your SQL query file uses schema-qualified table names like public.curtailment
    with open(constants.SQL_DIR / "read_data.sql") as f:
        query = f.read()

    with engine.connect() as conn:
        df_curtailment = pd.read_sql(
            query,
            conn,
            params=dict(start_time=start_time, end_time=end_time),
            parse_dates=["timeFrom", "timeTo"],
        )

    return df_curtailment


def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, index_col=0)

    columns = ["time", "level_fpn", "level_boal", "level_after_boal", "delta_mw", "cost_gbp"]

    if len(df) == 0:
        logger.debug("No data to load")
        return pd.DataFrame(columns=columns)

    df = df.rename(
        columns={
            "Time": "time",
            "Level_FPN": "level_fpn",
            "Level_BOAL": "level_boal",
            "Level_After_BOAL": "level_after_boal",
            "delta": "delta_mw",
        }
    )
    return df[columns]


def read_scottish_boa_volumes():
    """
    Reads Scottish BOA volumes data from Postgres table 'boa_volumes_scotland'.
    Returns a DataFrame sorted by Date and Settlement_Period.
    """
    engine = get_db_connection()
    query = """
        SELECT *
        FROM public.boa_volumes_scotland
        ORDER BY "Date", "Settlement_Period"
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, parse_dates=["Date"])
    return df


def prepare_scottish_data_for_plot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare Scottish BOA data for the app’s plotting function.
    Renames columns, sets dummy cost columns, and formats as needed.
    """
    # Rename date column for consistency with other data
    df = df.rename(columns={"Date": "local_datetime"})

    # Ensure datetime type
    df["local_datetime"] = pd.to_datetime(df["local_datetime"])

    # Dummy columns to fit expected plot input
    df["level_fpn_mw"] = 0  # No wind potential info in BOA data; set to zero or estimate if available
    df["level_after_boal_mw"] = df["BOA_Volume"].abs()  # Use absolute BOA volume as proxy for delivered wind

    # Costs columns - no data so set zero
    df["cost_gbp"] = 0
    df["turnup_cost_gbp"] = 0

    return df
