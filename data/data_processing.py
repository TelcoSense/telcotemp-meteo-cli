# data_processing.py
import pandas as pd
import logging
from .influx_manager import get_data
from geo.interpolation import spatial_interpolation
from visualization.visualization import map_plotting
import gc
import datetime
import traceback
from pyproj import Transformer

backend_logger = logging.getLogger("backend_logger")


def collect_data_summary(df):

    image_time = pd.to_datetime(df["Time"].iloc[0]).ceil("h")

    image_hour = image_time.strftime("%Y-%m-%d_%H%M")
    image_name = f"{image_hour}.png"

    return image_name, image_time


def prepare_data(df: pd.DataFrame, db_ops) -> pd.DataFrame:
    """
    Vstup:  DF s (Timestamp, Temperature, ID)
    Přidá:  Latitude, Longitude, Elevation přes db_ops.get_meteo_latlon_elev(df)
    Výstup: DF se sloupci (Timestamp, Temperature, ID, Latitude, Longitude, Elevation)
    """
    cols_out = ["Time", "Temperature", "ID", "Latitude", "Longitude", "Elevation"]

    if df is None or df.empty:
        backend_logger.info("prepare_data: prázdný vstup.")
        return pd.DataFrame(columns=cols_out)

    df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")

    try:
        db_ops.get_metadata(df)
    except Exception as e:
        backend_logger.error(f"prepare_data: selhalo načtení metadat: {e}")
        for c in ("Latitude", "Longitude", "Elevation"):
            if c not in df.columns:
                df[c] = pd.NA

    for c in ("Temperature", "Latitude", "Longitude", "Elevation"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    out = df[cols_out].copy()

    backend_logger.info("prepare_data: hotovo – %d řádků.", len(out))
    return out


def processing_loop(
    db_ops, geo_proc, czech_rep, elevation_data, transform_matrix, crs, config
):
    start_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    backend_logger.info(f"Calculation started on {start_datetime}")
    try:
        df = get_data(config)
        df = prepare_data(df, db_ops)

        # Mercator for web -- transformation
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        xs, ys = transformer.transform(
            df["Longitude"].to_numpy(), df["Latitude"].to_numpy()
        )
        df["X"] = xs
        df["Y"] = ys

        image_name, image_time = collect_data_summary(df)
        compute_config = config.get_grid_config()
        interpolation_config = config.get_interpolation_config()

        # interpolation now uses X, Y instead of Lon, Lat
        grid_x, grid_y, grid_z = spatial_interpolation(
            df,
            czech_rep,
            geo_proc,
            elevation_data,
            transform_matrix,
            crs,  # ideally set to EPSG:3857
            variogram_model=interpolation_config["variogram_model"],
            nlags=interpolation_config["nlags"],
            regression_model_type=interpolation_config["regression_model"],
            grid_x_points=compute_config["x_points"],
            grid_y_points=compute_config["y_points"],
        )

        map_plotting(grid_x, grid_y, grid_z, czech_rep, image_name, config)

    except Exception as e:
        backend_logger.error(
            f"Error during data processing round: {e}\n{traceback.format_exc()}"
        )

    finally:
        if "df" in locals():
            del df
        gc.collect()
    end_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    backend_logger.info(
        f"Calculation ended on {end_datetime}. Waiting for another round.."
    )
