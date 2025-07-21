from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine

# Load environment variables from .env.local
load_dotenv(".env.local")

# Load DB credentials from environment variables (with defaults just in case)
DB_IP = os.getenv("DB_IP", "127.0.0.1")
DB_NAME = os.getenv("DB_NAME", "wind_curtailment")
DB_USER = os.getenv("DB_USERNAME", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "password")

# Create DB connection engine with search_path=public
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_IP}:5432/{DB_NAME}?options=-csearch_path%3Dpublic"
)

# Load filtered Scotland data CSV
df = pd.read_csv("boa_data_scotland.csv")

# Write to Postgres (replace table if exists)
df.to_sql("boa_volumes_scotland", engine, if_exists="replace", index=False)

print("Uploaded 'boa_data_scotland.csv' to PostgreSQL table 'boa_volumes_scotland'.")
