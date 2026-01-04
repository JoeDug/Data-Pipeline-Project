import duckdb

con = duckdb.connect("db/warehouse.duckdb")

with open("db/dim_patients.sql") as f:
    con.execute(f.read())

print("dim_patients created")
