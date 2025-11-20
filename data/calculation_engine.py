import datetime
import time
from data.data_processing import DataProcessor
from core.initialization import initialize
from core.log import setup_logger


class CalculationEngine:
    """
    Main engine for orchestrating data processing, including initialization,
    historical data processing, and regular hourly map generation.
    """

    def __init__(self, config, logger_manager):
        """
        Initializes CalculationEngine with configuration and logger manager.
        Sets up database, geo processing, and data processor.
        """
        self.config = config
        self.logger_manager = logger_manager
        self.backend_logger = logger_manager.get_logger("backend_logger")
        (
            self.db_ops,
            self.geo_proc,
            self.czech_rep,
            self.elevation_data,
            self.transform_matrix,
            self.crs,
        ) = initialize(config)

        self.data_processor = DataProcessor(
            config,
            self.db_ops,
            self.geo_proc,
            self.czech_rep,
            self.elevation_data,
            self.transform_matrix,
            self.crs,
            self.backend_logger,
        )

    def process_historical_data(self, start_time, end_time, stations=None):
        """
        Processes historical data for the given time range.
        Calls DataProcessor to generate maps for each hour in the interval.
        """
        self.backend_logger.info(
            f"Processing historical data from {start_time} to {end_time}."
        )
        self.data_processor.process_time_range(start_time, end_time, stations)

    def data_processing_loop(
        self, first_run=False, start_time=None, end_time=None, stations=None
    ):
        """
        Main data processing loop.
        Modes:
        1. Regular hourly calculation: processes last complete hour, then each new hour.
        2. First run: processes last week, then switches to regular mode.
        3. Specific range: processes given interval and exits.
        """
        # Mode 3: Specific time range
        if start_time and end_time:
            self.backend_logger.info(
                f"Processing historical time range from {start_time} to {end_time}."
            )
            self.process_historical_data(start_time, end_time, stations)
            return

        # Determine last complete hour (data available at hh:30)
        now = datetime.datetime.now()
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        current_minutes = now.minute

        if current_minutes >= 30:
            last_complete_hour = current_hour
        else:
            last_complete_hour = current_hour - datetime.timedelta(hours=1)

        # Mode 2: First run - process last week
        if first_run:
            self.backend_logger.info("First run: Processing data for the last week.")
            historical_start = last_complete_hour - datetime.timedelta(days=7)
            historical_end = last_complete_hour

            self.backend_logger.info(
                f"Processing historical range: {historical_start} to {historical_end}"
            )
            self.process_historical_data(historical_start, historical_end, stations)
            self.backend_logger.info("Historical data processed. Switching to real-time mode.")

        # Modes 1 & 2: Regular hourly calculation
        self.backend_logger.info(
            f"Processing last complete hour immediately: {last_complete_hour}"
        )
        try:
            self.data_processor.process_time_range(
                last_complete_hour,
                last_complete_hour + datetime.timedelta(hours=1),
                stations
            )
        except Exception as e:
            self.backend_logger.error(
                f"Error processing initial hour {last_complete_hour}: {e}"
            )

        # Next map hour is the hour after last complete
        next_map_hour = last_complete_hour + datetime.timedelta(hours=1)

        self.backend_logger.info(
            f"Starting regular hourly processing. Next hour to process: {next_map_hour}"
        )

        while True:
            now = datetime.datetime.now()
            current_hour = now.replace(minute=0, second=0, microsecond=0)
            current_minutes = now.minute

            # Determine last complete hour
            if current_minutes >= 30:
                last_complete_hour = current_hour
            else:
                last_complete_hour = current_hour - datetime.timedelta(hours=1)

            # Catch-up: process all missed hours
            while next_map_hour <= last_complete_hour:
                self.backend_logger.info(
                    f"Processing map for hour: {next_map_hour}"
                )
                try:
                    self.data_processor.process_time_range(
                        next_map_hour,
                        next_map_hour + datetime.timedelta(hours=1),
                        stations
                    )
                except Exception as e:
                    self.backend_logger.error(
                        f"Error processing hour {next_map_hour}: {e}"
                    )
                next_map_hour += datetime.timedelta(hours=1)

            # Wait until next complete hour's data is available (hh:30)
            wait_until = next_map_hour + datetime.timedelta(minutes=30)
            wait_seconds = (wait_until - now).total_seconds()

            if wait_seconds > 0:
                self.backend_logger.info(
                    f"Waiting {wait_seconds:.0f}s until {wait_until} for data for hour {next_map_hour}."
                )
                time.sleep(wait_seconds)
            else:
                self.backend_logger.info(
                    f"Data for hour {next_map_hour} should already be available."
                )