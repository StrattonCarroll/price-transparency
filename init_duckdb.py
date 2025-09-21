import duckdb, os
os.makedirs("data", exist_ok=True)
con = duckdb.connect("data/hpt.duckdb")
con.execute("CREATE SCHEMA IF NOT EXISTS hpt;")
print(con.execute("SELECT 'duckdb ready' as status;").fetchdf())
con.close()
