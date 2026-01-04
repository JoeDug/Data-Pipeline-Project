import duckdb

con = duckdb.connect("db/warehouse.duckdb")

with open("db/fact_encounters.sql") as f:
    con.execute(f.read())

print("fact encounters created")
