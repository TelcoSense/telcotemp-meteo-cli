import geopandas as gpd
from shapely.geometry import Polygon, Point
import numpy as np
import json
import rasterio
from pyproj import Transformer

class GeographicalProcessing:
    """
    Provides geographical utilities for country shape, mask creation, and elevation data.
    """

    def json_to_geodataframe(self, json_data):
        """
        Converts GeoJSON data to a GeoDataFrame.
        """
        geometries = []
        for feature in json_data["features"]:
            poly = Polygon(feature["geometry"]["coordinates"][0])
            geometries.append(poly)
        gdf = gpd.GeoDataFrame(geometry=geometries, crs="EPSG:4326")
        return gdf

    def create_mask(self, czech_rep, grid_x, grid_y):
        """
        Creates a boolean mask for grid points inside the country polygon.
        """
        mask = np.zeros_like(grid_x, dtype=bool)
        for i in range(grid_x.shape[0]):
            for j in range(grid_x.shape[1]):
                point = Point(grid_x[i, j], grid_y[i, j])
                mask[i, j] = czech_rep.contains(point).any()
        return mask

    def load_country_data(self, country_file_path):
        """
        Loads country shape data from a GeoJSON file.
        """
        with open(country_file_path, "r", encoding="utf-8") as file:
            return json.load(file)

    def load_elevation_data(self, tif_path):
        """
        Loads elevation data from a GeoTIFF file.
        Returns: elevation_data (2D np.ndarray), transform (Affine), crs (CRS)
        """
        with rasterio.open(tif_path) as src:
            elevation_data = src.read(1)
            nodata = src.nodata
            if nodata is not None:
                elevation_data = np.where(elevation_data == nodata, np.nan, elevation_data)
            transform_matrix = src.transform  
            crs = src.crs
        return elevation_data, transform_matrix, crs