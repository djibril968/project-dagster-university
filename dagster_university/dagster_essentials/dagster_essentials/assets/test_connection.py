
#This file is created to test asset connection within our working environment and directory

import os
import duckdb
import glob

# Check current working directory
print("Current working directory:", os.getcwd())

# Look for .duckdb files
print("\nLooking for .duckdb files...")
duckdb_files = glob.glob("**/*.duckdb", recursive=True)
if duckdb_files:
    print("Found DuckDB files:")
    for file in duckdb_files:
        print(f"  - {os.path.abspath(file)}")
else:
    print("No .duckdb files found")

# Check if the environment variable is set
env_db = os.getenv("DUCKDB_DATABASE")
print(f"\nDUCKDB_DATABASE environment variable: {env_db}")



# Connect to your database
conn = duckdb.connect(os.getenv("DUCKDB_DATABASE", "data/staging/data.duckdb")) 

# Show all tables
tables = conn.execute("SHOW TABLES").fetchall()
print("Tables in database:")
for table in tables:
    print(f"  - {table[0]}")

# Or get more detailed info
table_info = conn.execute("SELECT * FROM information_schema.tables").fetchdf()
print(table_info)

conn.close()