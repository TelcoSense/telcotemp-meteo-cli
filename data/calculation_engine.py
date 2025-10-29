import datetime
from data.data_processing import processing_loop
from core.initialization import wait_for_next_hour, initialize
from core.log import setup_logger


class CalculationEngine:
    def __init__(self, config, logger_manager):
        self.config = config
        self.logger_manager = logger_manager
        self.backend_logger = self.logger_manager.get_logger("backend_logger")
        (
            self.db_ops,
            self.geo_proc,
            self.czech_rep,
            self.elevation_data,
            self.transform_matrix,
            self.crs,
        ) = initialize(config)
        self.backend_logger = setup_logger("backend_logger", config.get_logging_config().get("backend_log"),
                                           level=config.get_logging_config().get("level"))

    def process_historical_data(self, start_time, end_time, stations=None):
        """
        Processes historical data for the given time range.
        """
        current_time = start_time
        while current_time < end_time:
            self.backend_logger.info(
                f"Processing historical data for hour: {current_time}"
            )
            processing_loop(
                self.db_ops,
                self.geo_proc,
                self.czech_rep,
                self.elevation_data,
                self.transform_matrix,
                self.crs,
                self.config,
                current_time,
                stations,
            )
            current_time += datetime.timedelta(hours=1)

    def data_processing_loop(
            self, first_run=False, start_time=None, end_time=None, stations=None
    ):
        """
        Main data processing loop.
        """
        historical_processed = False

        if first_run:
            self.backend_logger.info("First run: Processing data for the last week.")
            historical_end_time = datetime.datetime.now().replace(
                minute=0, second=0, microsecond=0
            )
            historical_start_time = historical_end_time - datetime.timedelta(days=7)
            self.process_historical_data(
                historical_start_time, historical_end_time, stations
            )
            historical_processed = True

        if start_time and end_time:
            self.backend_logger.info(
                f"Processing data from {start_time} to {end_time}."
            )
            self.process_historical_data(start_time, end_time, stations)
            historical_processed = True

        if not historical_processed:
            while True:
                processing_loop(
                    self.db_ops,
                    self.geo_proc,
                    self.czech_rep,
                    self.elevation_data,
                    self.transform_matrix,
                    self.crs,
                    self.config,
                )
                wait_for_next_hour()