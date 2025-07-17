
import dagster as dg
import matplotlib.pyplot as plt
import geopandas as gpd
from dagster_duckdb import DuckDBResource
import duckdb
import os
from dagster_essentials.assets import constants

from dagster import Definitions, load_asset_checks_from_package_module





@dg.asset(
    deps=["taxi_trips_file", "taxi_zones_file"]
    #required_resource_keys={"database"}
)

def manhattan_stats(database: DuckDBResource) -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips
        from tax_trips as trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry

    """

    with database.get_connection()as conn:
        conn.execute(query)
        trips_by_zone = conn.fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())


@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90]) #type: ignore  
    ax.set_ylim([40.70, 40.82])  # #type: ignore 
    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)



@dg.asset(
    deps=['taxi_trips']
    #required_resource_keys={"database"}
)
def trips_by_week (database: DuckDBResource) -> None:
    query = """
        select
            date_trunc('week', pickup_datetime + INTERVAL 1 DAY) ::DATE - INTERVAL 1 DAY as period,
            count(1) as num_trips,
            sum(passenger_count) as passenger_count,
            sum(total_amount) as rev_by_week,
            sum(trip_distance) as tot_trip_by_week
        from tax_trips
        group by period
        order by period
    """

    with database.get_connection()as conn:
        conn.execute(query)
        trips_by_week = conn.fetch_df()
    with open(constants.TRIPS_BY_WEEK_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_week.to_csv())  