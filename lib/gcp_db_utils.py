import logging
import os
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from lib import constants

logger = logging.getLogger(__name__)

@st.experimental_singleton
def get_engine():
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_IP = os.getenv("DB_IP")
    DB_NAME = os.getenv("DB_NAME")
    DB_PORT = os.getenv("DB_PORT", "5432")

    if DB_IP in ("127.0.0.1", "localhost"):
        ssl_mode = ""
    else:
        ssl_mode = "?sslmode=require&options=-csearch_path%3Dpublic"

    DB_URL = (
        f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_IP}:{DB_PORT}/{DB_NAME}{ssl_mode}"
    )

    engine = create_engine(
        DB_URL,
        pool_pre_ping=True,
        pool_recycle=300,
    )
    return engine

# Use the singleton engine instance everywhere
engine = get_engine()

def get_db_connection():
    """
    Return the persistent SQLAlchemy engine with pool management.
    """
    return engine


def write_curtailment_data(df: pd.DataFrame):
    if len(df) == 0:
        logger.debug("There was no data to write to the database")
        return

    if "local_datetime" in df.columns:
        df = df.rename(columns={"local_datetime": "time"})

    logger.info(f"Adding curtailment to database ({len(df)})")

    engine = get_db_connection()
    with engine.connect() as conn:
        df.to_sql("curtailment", conn, if_exists="append", index=False)


def write_sbp_data(df: pd.DataFrame):
    df = df.rename(columns={"local_datetime": "time", "systemSellPrice": "system_buy_price"})
    df = df[["time", "system_buy_price"]]

    if len(df) == 0:
        logger.debug("There was no data to write to the database")
        return

    logger.info(f"Adding sbp to database ({len(df)})")

    engine = get_db_connection()
    with engine.connect() as conn:
        try:
            df.to_sql("sbp", conn, if_exists="append", index=False)
        except IntegrityError:
            logger.warning(
                f"Failed to write sbp df from {df['time'].min()} to {df['time'].max()}"
            )


def read_data(start_time="2022-01-01", end_time="2023-01-01"):
    engine = get_db_connection()

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
    df = df.rename(columns={"Date": "local_datetime"})
    df["local_datetime"] = pd.to_datetime(df["local_datetime"])

    df["level_fpn_mw"] = 0  # No wind potential info in BOA data; set to zero or estimate if available
    df["level_after_boal_mw"] = df["BOA_Volume"].abs()  # Use absolute BOA volume as proxy for delivered wind

    # Costs columns - no data so set zero
    df["cost_gbp"] = 0
    df["turnup_cost_gbp"] = 0

    return df
