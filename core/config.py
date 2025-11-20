import os
import configparser


class AppConfig:
    """
    Loads and provides access to application, database, and compute configuration.
    Reads .ini files from the configs/ directory.
    """

    def __init__(self, config_dir="configs"):
        self.config_dir = config_dir
        self.app = self._load("app.ini")
        self.database = self._load("database.ini")
        self.compute = self._load("compute.ini")

    def _load(self, filename):
        """
        Loads a configuration file and returns a ConfigParser object.
        """
        cfg = configparser.ConfigParser()
        path = os.path.join(self.config_dir, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Configuration file {path} does not exist.")
        cfg.read(path, encoding="utf-8")
        return cfg

    # --- APP ---
    def get_logging_config(self):
        """
        Returns logging configuration as a dictionary.
        """
        lg = self.app["logging"]
        return {
            "level": lg.get("level", "INFO"),
            "backend_log": lg.get("backend_log", "app.log"),
            "max_bytes": lg.getint("max_bytes", 10 * 1024 * 1024),
            "backups": lg.getint("backups", 1),
            "fmt": lg.get("fmt", raw=True, fallback="%(asctime)s -%(funcName)s - %(levelname)s - %(message)s"),
        }

    def get_paths(self):
        """
        Returns paths configuration as a dictionary.
        """
        p = self.app["paths"]
        return {
            "country_file": p.get("country_file"),
            "dem_tif": p.get("dem_tif"),
            "images_dir": p.get("images_dir", "images"),
            "saved_grids_dir": p.get("saved_grids_dir", "saved_grids"),
        }

    def get_visualization(self):
        """
        Returns visualization configuration as a dictionary.
        """
        vis = self.app["visualization"] if "visualization" in self.app else {}
        colormap = vis.get("colormap", "[]")
        return {
            "n_levels": int(vis.get("n_levels", "15")),
            "colormap": eval(colormap) if colormap else [],
        }

    # --- DATABASE ---
    def get_mysql_config(self):
        """
        Returns MySQL database configuration as a dictionary.
        """
        mysql = self.database["mysql"]
        return {
            "user": mysql.get("user"),
            "password": mysql.get("password"),
            "host": mysql.get("host"),
            "port": mysql.getint("port"),
        }

    def get_influx_config(self):
        """
        Returns InfluxDB configuration as a dictionary.
        """
        influx = self.database["influx"]
        return {
            "org": influx.get("org"),
            "url": influx.get("url"),
            "token": influx.get("token"),
            "bucket": influx.get("bucket"),
            "measurements": influx.get("measurements", "").split(","),
            "fields": influx.get("fields", "").split(","),
            "tag_device": influx.get("tag_device"),
            "field_temperature": influx.get("field_temperature"),
            "field_signal": influx.get("field_signal"),
            "window": influx.get("window"),
            "range": influx.get("range"),
        }

    # --- COMPUTE ---
    def get_grid_config(self):
        """
        Returns grid configuration for interpolation as a dictionary.
        """
        g = self.compute["grid"]
        return {
            "x_points": g.getint("x_points", 500),
            "y_points": g.getint("y_points", 500),
            "mask_resolution_safe": g.getboolean("mask_resolution_safe", True),
        }

    def get_interpolation_config(self):
        """
        Returns interpolation configuration as a dictionary.
        """
        itp = self.compute["interpolation"]
        return {
            "variogram_model": itp.get("variogram_model", "spherical"),
            "nlags": itp.getint("nlags", 40),
            "regression_model": itp.get("regression_model", "linear"),
        }

    def get_location(self):
        """
        Returns location configuration as a dictionary.
        """
        loc = self.compute["location"]
        return {
            "lat": loc.getfloat("lat", 49.8175),
            "lng": loc.getfloat("lng", 15.4730),
            "tz": loc.get("tz", "Europe/Prague"),
        }