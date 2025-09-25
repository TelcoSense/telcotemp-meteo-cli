from influxdb_client import InfluxDBClient
import pandas as pd
import logging

backend_logger = logging.getLogger("backend_logger")


def get_data(config):
    try:
        influx_config = config.get_influx_config()

        query = f"""
from(bucket: "{influx_config['bucket']}")
  |> range(start: {influx_config['range']})
  |> filter(fn: (r) => r["_measurement"] == "{','.join(influx_config['measurements'])}")
  |> aggregateWindow(every: {influx_config['window']}, fn: mean, createEmpty: false)
  |> keep(columns: ["_time","_value","_field"])
"""

        with InfluxDBClient(
            url=influx_config["url"],
            token=influx_config["token"],
            org=influx_config["org"],
        ) as client:
            result = client.query_api().query(query=query)

        rows = []
        for table in result:
            for rec in table.records:
                rows.append(
                    {
                        "Time": rec.get_time(),
                        "Temperature": rec.get_value(),
                        "ID": rec.values["_field"],
                    }
                )

        df = pd.DataFrame(rows, columns=["Time", "Temperature", "ID"])
        if df.empty:
            backend_logger.info("Influx returned empty data from weather stations.")

        return df

    except Exception as e:
        backend_logger.error(f"Error reading from InfluxDB: {e}")
        return pd.DataFrame()
