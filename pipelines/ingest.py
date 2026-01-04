import shutil
from datetime import datetime
from pathlib import Path

RAW_DIR = Path("data/raw")
INGESTED_DIR = RAW_DIR / "ingested"
INGESTED_DIR.mkdir(parents=True, exist_ok=True)

def ingest(file_name):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    src = RAW_DIR / file_name
    dest = INGESTED_DIR / f"{file_name.replace('.csv','')}_{timestamp}.csv"
    shutil.copy(src, dest)
    print(f"Ingested {file_name} → {dest.name}")

if __name__ == "__main__":
    for file in ["encounters.csv", "organizations.csv", "patients.csv", "procedures.csv"]:
        ingest(file)
