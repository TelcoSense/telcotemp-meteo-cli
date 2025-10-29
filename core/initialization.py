from sqlalchemy import create_engine
from data.sql_manager import DatabaseOperations
from geo.geographical_processing import GeographicalProcessing
from datetime import datetime, timedelta
from time import time, sleep


def wait_for_next_hour():
    """Wait until the start of the next hour."""
    current = datetime.datetime.now()
    next_hour = (current + datetime.timedelta(hours=1)).replace(
        minute=0, second=0, microsecond=0
    )
    wait_seconds = (next_hour - current).total_seconds()
    time.sleep(wait_seconds)


def initialize(config):
    db_config = config.get_mysql_config()
    paths = config.get_paths()

    engine = create_engine(
        f"mysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}"
    )
    db_ops = DatabaseOperations(engine)
    geo_proc = GeographicalProcessing()
    state = geo_proc.load_country_data(paths["country_file"])
    czech_rep = geo_proc.json_to_geodataframe(state)
    czech_rep = czech_rep.to_crs("EPSG:3857")
    elevation_data, transform_matrix, crs = geo_proc.load_elevation_data(
        paths["dem_tif"]
    )
    return db_ops, geo_proc, czech_rep, elevation_data, transform_matrix, crs
