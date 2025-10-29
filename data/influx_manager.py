from influxdb_client import InfluxDBClient
import pandas as pd
import logging
from datetime import timezone

backend_logger = logging.getLogger("backend_logger")


def get_data(config, start_time, end_time):
    """
    Reads data from InfluxDB within the given UTC time range.
    Returns DataFrame with columns: ['Time', 'Temperature', 'ID']
    """

    influx_config = config.get_influx_config()

    start_time_iso = start_time.astimezone(timezone.utc).isoformat()
    end_time_iso = end_time.astimezone(timezone.utc).isoformat()

    meas_filter = " or ".join(
        [f'r["_measurement"] == "{m}"' for m in influx_config["measurements"]]
    )

    query = f"""
from(bucket: "{influx_config['bucket']}")
  |> range(start: {start_time_iso}, stop: {end_time_iso})
  |> filter(fn: (r) => {meas_filter})
  |> aggregateWindow(every: {influx_config['window']}, fn: mean, createEmpty: false)
  |> keep(columns: ["_time","_value","_field"])
"""

    try:
        with InfluxDBClient(
                url=influx_config["url"],
                token=influx_config["token"],
                org=influx_config["org"],
        ) as client:
            result = client.query_api().query(query=query)

        rows = [
            {
                "Time": rec.get_time(),
                "Temperature": rec.get_value(),
                "ID": rec.values["_field"],
            }
            for table in result
            for rec in table.records
        ]

        df = pd.DataFrame(rows, columns=["Time", "Temperature", "ID"])

        if df.empty:
            backend_logger.info("Influx returned empty data from weather stations.")

        return df

    except Exception as e:
        backend_logger.error(f"Error reading from InfluxDB: {e}")
        return pd.DataFrame()
