import pandas as pd
from data.influx_manager import get_data
import gc
import datetime
import traceback
from pyproj import Transformer
from geo.interpolation import spatial_interpolation
from visualization.visualization import map_plotting


class DataProcessor:
    """
    Handles the data processing pipeline for temperature mapping.
    Responsible for fetching, preparing, filtering, transforming, and visualizing data.
    """

    def __init__(
        self,
        config,
        db_ops,
        geo_proc,
        czech_rep,
        elevation_data,
        transform_matrix,
        crs,
        logger,
    ):
        """
        Initializes the DataProcessor with configuration, database operations,
        geographical processing, country shape, elevation data, transformation matrix,
        coordinate reference system, and logger.
        """
        self.config = config
        self.db_ops = db_ops
        self.geo_proc = geo_proc
        self.czech_rep = czech_rep
        self.elevation_data = elevation_data
        self.transform_matrix = transform_matrix
        self.crs = crs
        self.logger = logger

    def process_time_range(self, target_time=None, end_time=None, stations=None):
        """
        Main processing loop for generating temperature maps for each hour in the given range.
        Fetches, prepares, filters, transforms, interpolates, and visualizes data.
        """
        current_time = target_time

        while current_time < end_time: 
            try:
                self.logger.info(f"Processing map for hour: {current_time}")
                df = self._fetch_data(current_time)
                
                if df.empty:
                    self.logger.warning(
                        f"No data fetched for hour {current_time}. Skipping."
                    )
                    current_time += datetime.timedelta(hours=1)
                    continue

                df = self._prepare_data(df)
                
                if stations:
                    df = self._filter_by_stations(df, stations)
                    if df.empty:
                        self.logger.warning(
                            f"No data after station filtering for {current_time}. Skipping."
                        )
                        current_time += datetime.timedelta(hours=1)
                        continue

                self._transform_coordinates(df)
                image_name, image_time = self._collect_data_summary(df)
                
                self._interpolate_and_visualize(df, image_name)

            except Exception as e:
                self.logger.error(
                    f"Error processing hour {current_time}: {e}\n{traceback.format_exc()}"
                )
            finally:
                current_time += datetime.timedelta(hours=1)
                gc.collect()

            end_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            self.logger.info(
                f"Calculation ended on {end_datetime}. Waiting for another round..."
            )

    def _fetch_data(self, target_hour):
        """
        Fetches data for the hour BEFORE target_hour.
        E.g., for target_hour 12:00, fetches data from 11:00-11:59.
        Returns a DataFrame with columns: ['Time', 'Temperature', 'ID']
        """
        data_start = target_hour - datetime.timedelta(hours=1)
        data_end = target_hour
        return get_data(self.config, data_start, data_end)

    def _prepare_data(self, df):
        """
        Prepares the data by adding metadata and elevation information.
        Ensures columns: Time, Temperature, ID, Latitude, Longitude, Elevation.
        """
        cols_out = ["Time", "Temperature", "ID", "Latitude", "Longitude", "Elevation"]

        if df is None or df.empty:
            self.logger.info("prepare_data: Empty input.")
            return pd.DataFrame(columns=cols_out)

        df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")

        try:
            self.db_ops.get_metadata(df)
        except Exception as e:
            self.logger.error(f"prepare_data: Failed to load metadata: {e}")
            for c in ("Latitude", "Longitude", "Elevation"):
                if c not in df.columns:
                    df[c] = pd.NA

        for c in ("Temperature", "Latitude", "Longitude", "Elevation"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        return df[cols_out].copy()

    def _filter_by_stations(self, df, stations):
        """
        Filters the data by the specified station IDs.
        """
        mask = df["ID"].isin(stations)
        df = df[mask].reset_index(drop=True)
        self.logger.info(
            f"Filtered dataset to {len(df)} rows based on Weatherstation IDs."
        )
        if df.empty:
            self.logger.warning(
                "No data available for the specified Weatherstation IDs."
            )
        return df

    def _transform_coordinates(self, df):
        """
        Transforms coordinates to Mercator projection.
        """
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        xs, ys = transformer.transform(
            df["Longitude"].to_numpy(), df["Latitude"].to_numpy()
        )
        df["X"] = xs
        df["Y"] = ys

    def _collect_data_summary(self, df):
        """
        Collects summary information for the data.
        """
        image_time = pd.to_datetime(df["Time"].iloc[0]).ceil("h")
        image_hour = image_time.strftime("%Y-%m-%d_%H%M")
        image_name = f"{image_hour}.png"
        return image_name, image_time

    def _interpolate_and_visualize(self, df, image_name):
        """
        Performs spatial interpolation and generates a visualization.
        """
        compute_config = self.config.get_grid_config()
        interpolation_config = self.config.get_interpolation_config()

        grid_x, grid_y, grid_z = spatial_interpolation(
            df,
            self.czech_rep,
            self.geo_proc,
            self.elevation_data,
            self.transform_matrix,
            self.crs,
            variogram_model=interpolation_config["variogram_model"],
            nlags=interpolation_config["nlags"],
            regression_model_type=interpolation_config["regression_model"],
            grid_x_points=compute_config["x_points"],
            grid_y_points=compute_config["y_points"],
        )

        map_plotting(grid_x, grid_y, grid_z, self.czech_rep, image_name, self.config)

    