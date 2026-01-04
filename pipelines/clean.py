import pandas as pd
import duckdb
from pathlib import Path

# Connect to DuckDB warehouse
con = duckdb.connect("db/warehouse.duckdb")

INGESTED_DIR = Path("data/raw/ingested")
print("Looking in:", INGESTED_DIR.resolve())
print("Files found:", list(INGESTED_DIR.iterdir()))

def load_latest(prefix):
    files = sorted(INGESTED_DIR.glob(f"{prefix}_*.csv"))
    if not files:
        raise FileNotFoundError(f"No ingested files found for {prefix}")
    return pd.read_csv(files[-1])

# ---- Load latest ingested files ----
patients = load_latest("patients")
encounters = load_latest("encounters")
procedures = load_latest("procedures")
organizations = load_latest("organizations")

# ---- Normalize column names ----
for df in [patients, encounters, procedures, organizations]:
    df.columns = df.columns.str.lower()

# ---- Parse dates (only if columns exist) ----
def parse_dates(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

patients = parse_dates(patients, ["birthdate", "deathdate"])
encounters = parse_dates(encounters, ["start", "stop"])
procedures = parse_dates(procedures, ["start", "stop"])

# ---- Basic data quality checks ----
assert patients["id"].notnull().all()
assert encounters["id"].notnull().all()

# Remove encounters without patients
if "patient" in encounters.columns:
    encounters = encounters[encounters["patient"].notnull()]

# ---- Load staging tables into DuckDB ----
con.execute("CREATE OR REPLACE TABLE staging_patients AS SELECT * FROM patients")
con.execute("CREATE OR REPLACE TABLE staging_encounters AS SELECT * FROM encounters")
con.execute("CREATE OR REPLACE TABLE staging_procedures AS SELECT * FROM procedures")
con.execute("CREATE OR REPLACE TABLE staging_organizations AS SELECT * FROM organizations")


print("Staging tables created successfully")