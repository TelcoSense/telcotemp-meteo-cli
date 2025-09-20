from core.initialization import initialize, wait_for_next_hour
from data.data_processing import processing_loop
from core.log import setup_logger
from core.config import AppConfig

config = AppConfig()
logging_config = config.get_logging_config()

backend_logger = setup_logger(
    "backend_logger",
    logging_config["backend_log"],
    level=logging_config["level"],
    max_bytes=logging_config["max_bytes"],
    backups=logging_config["backups"],
    fmt=logging_config["fmt"]
)


def data_processing_loop():
    db_ops, geo_proc, czech_rep, elevation_data, transform_matrix, crs = initialize(config)
    while True:
        processing_loop(db_ops, geo_proc, czech_rep, elevation_data, transform_matrix, crs, config)
        wait_for_next_hour()


if __name__ == "__main__":
    backend_logger.info("Backend processing started")
    data_processing_loop()