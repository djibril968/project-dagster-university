from dagster_duckdb import DuckDBResource 
import dagster as dg


db_resource = DuckDBResource(database=dg.EnvVar("DuckDB_DATABASE"))


#@dg.definition
#def resources():
 #   return dg.Definitions(resources={"database": db_resource})





