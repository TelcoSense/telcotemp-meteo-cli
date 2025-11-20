import time
import pandas as pd
from sqlalchemy import text, bindparam
import logging
from sqlalchemy.orm import sessionmaker

backend_logger = logging.getLogger("backend_logger")

class DatabaseOperations:
    """
    Handles database operations for metadata enrichment of weather station data.
    Provides caching and efficient bulk fetching of station coordinates and elevation.
    """

    def __init__(self, engine):
        """
        Initializes DatabaseOperations with SQLAlchemy engine.
        Sets up session factory and metadata caches.
        """
        self.engine = engine
        self.Session = sessionmaker(bind=self.engine)
        self._ip_meta_cache = {}
        self._station_meta_cache = {}

    def get_metadata(self, df: pd.DataFrame):
        """
        Enriches DataFrame with Latitude, Longitude, Elevation from DB table chmi_metadata.weather_stations.
        - Uses cache for already fetched stations.
        - Bulk fetches missing station metadata.
        - Drops rows without metadata.
        - Adds columns: Longitude, Latitude, Elevation.
        Returns lists of latitudes, longitudes, elevations in the order of resulting DataFrame.
        """
        t0 = time.perf_counter()

        if df.empty or "ID" not in df.columns:
            backend_logger.info("get_meteo_latlon_elev: prázdný DF nebo chybí sloupec 'ID'.")
            df["Longitude"] = pd.NA
            df["Latitude"] = pd.NA
            df["Elevation"] = pd.NA
            return [], [], []

        # Prepare unique station IDs
        ids_series = df["ID"].astype(str).str.strip()
        unique_ids = sorted({sid for sid in ids_series if sid})
        backend_logger.debug(
            "get_meteo_latlon_elev: %d řádků, %d unikátních ID.",
            len(df), len(unique_ids)
        )

        # 1) Use cache for already fetched stations
        cached = {sid: self._station_meta_cache[sid] for sid in unique_ids if sid in self._station_meta_cache}
        missing = [sid for sid in unique_ids if sid not in cached]

        # 2) Bulk fetch missing stations from DB
        fetched = {}
        if missing:
            try:
                with self.Session() as session:
                    stmt = text("""
                        SELECT
                            gh_id     AS station_id,
                            X         AS lon,     -- Longitude
                            Y         AS lat,     -- Latitude
                            elevation AS elev
                        FROM chmi_metadata.weather_stations
                        WHERE gh_id IN :ids
                    """).bindparams(bindparam("ids", expanding=True))

                    rows = session.execute(stmt, {"ids": missing}).all()

                    def _to_float(v):
                        s = None if v is None else str(v).strip().replace(",", ".")
                        try:
                            return float(s) if s not in (None, "", "None") else None
                        except Exception:
                            return None

                    for r in rows:
                        m = r._mapping
                        rec = {
                            "id": str(m["station_id"]),
                            "lon": _to_float(m["lon"]),
                            "lat": _to_float(m["lat"]),
                            "elev": _to_float(m["elev"]),
                        }
                        fetched[rec["id"]] = rec
                        self._station_meta_cache[rec["id"]] = rec
            except Exception as e:
                backend_logger.error(f"get_meteo_latlon_elev bulk fetch failed: {e}")

        lookup = {**cached, **fetched}

        # 3) Traverse DataFrame in row order, drop missing, collect metadata
        latitudes, longitudes, elevations = [], [], []
        to_drop = []
        have = 0

        for idx, sid in enumerate(ids_series):
            if not sid:
                to_drop.append(idx)
                continue
            meta = lookup.get(sid)
            if (meta is None) or (meta["lon"] is None) or (meta["lat"] is None):
                backend_logger.warning(f"No station coords found for ID: {sid}")
                to_drop.append(idx)
                continue

            latitudes.append(meta["lat"])
            longitudes.append(meta["lon"])
            elevations.append(meta["elev"])
            have += 1

        # 4) Drop rows without metadata and assign columns
        if to_drop:
            df.drop(index=to_drop, inplace=True)
            df.reset_index(drop=True, inplace=True)

        # Assign columns
        if len(df) == len(latitudes):
            df["Latitude"] = latitudes
            df["Longitude"] = longitudes
            df["Elevation"] = elevations
        else:
            # Fallback mapping (should not happen)
            map_lat = {rec["id"]: rec["lat"] for rec in lookup.values()}
            map_lon = {rec["id"]: rec["lon"] for rec in lookup.values()}
            map_elev = {rec["id"]: rec["elev"] for rec in lookup.values()}
            df["Latitude"] = df["ID"].map(map_lat)
            df["Longitude"] = df["ID"].map(map_lon)
            df["Elevation"] = df["ID"].map(map_elev)

        backend_logger.info(f"Completed get_meteo_latlon_elev for {have} station rows.")
        backend_logger.debug(
            "get_meteo_latlon_elev: cache_hit=%d, fetched=%d, elapsed=%.3fs",
            len(cached), len(fetched), time.perf_counter() - t0
        )

        return latitudes, longitudes, elevations
