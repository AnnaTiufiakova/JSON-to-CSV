import pandas as pd
from sqlalchemy import create_engine, text

# PostgreSQL connection details
db_url = "postgresql://airflow:airflow@postgres:5432/airflow"
engine = create_engine(db_url)

# Define table creation SQL
create_table_sql = """
CREATE TABLE IF NOT EXISTS nasa_asteroids (
    date DATE,
    id TEXT PRIMARY KEY,
    name TEXT,
    magnitude FLOAT,
    diameter_min FLOAT,
    diameter_max FLOAT,
    hazardous BOOLEAN, 
    close_approach_date DATE,
    velocity_km_per_s FLOAT,
    miss_distance_km FLOAT, 
    orbiting_body TEXT
);
"""

# Execute table creation
with engine.connect() as conn:
    conn.execute(text(create_table_sql))

# Read CSV and load into PostgreSQL
csv_path = "/opt/airflow/data/nasa_neo_data.csv"
df = pd.read_csv(csv_path, dtype={"id": str})

# Ensure hazardous column is boolean
if "hazardous" in df.columns:
    df["hazardous"] = df["hazardous"].astype(bool)

df.to_sql("nasa_asteroids", engine, if_exists="append", index=False)

print("Data successfully loaded into PostgreSQL")
