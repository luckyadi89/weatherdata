import openmeteo_requests
import requests_cache
import pandas as pd
import os
from retry_requests import retry
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

class WeatherDataHandler:
    def __init__(self):
        self.openmeteo_client = self.setup_openmeteo_client()
        self.snowflake_connection = self.setup_snowflake_connection()

    def setup_openmeteo_client(self):
        cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        return openmeteo_requests.Client(session=retry_session)

    def setup_snowflake_connection(self):
        conn_string = (
            f"snowflake://{os.getenv('SNOWFLAKE_USER')}:{os.getenv('SNOWFLAKE_PASSWORD')}@"
            f"{os.getenv('SNOWFLAKE_ACCOUNT')}/{os.getenv('SNOWFLAKE_DATABASE')}/"
            f"{os.getenv('SNOWFLAKE_SCHEMA')}?role={os.getenv('SNOWFLAKE_ROLE')}"
        )
        return create_engine(conn_string)


    def get_weather_data(self,place, latitude, longitude, start_date, end_date):
        url = "https://archive-api.open-meteo.com/v1/archive"
        global place_name
        global lat
        global long
        lat = latitude
        long = longitude
        place_name = place
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "rain", "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_100m", "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m", "soil_temperature_0_to_7cm", "soil_temperature_7_to_28cm", "soil_temperature_28_to_100cm", "soil_temperature_100_to_255cm", "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm", "soil_moisture_28_to_100cm", "soil_moisture_100_to_255cm"]
        }
        responses = self.openmeteo_client.weather_api(url, params=params)
        return responses[0]

    def process_hourly_data(self, hourly):
        hourly_data = {
            "DATE": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "TEMPERATURE_2M": hourly.Variables(0).ValuesAsNumpy(),
            "RELATIVE_HUMIDITY_2M": hourly.Variables(1).ValuesAsNumpy(),
            "DEW_POINT_2M": hourly.Variables(2).ValuesAsNumpy(),
            "APPARENT_TEMPERATURE": hourly.Variables(3).ValuesAsNumpy(),
            "RAIN": hourly.Variables(4).ValuesAsNumpy(),
            "PRESSURE_MSL": hourly.Variables(5).ValuesAsNumpy(),
            "SURFACE_PRESSURE": hourly.Variables(6).ValuesAsNumpy(),
            "CLOUD_COVER": hourly.Variables(7).ValuesAsNumpy(),
            "CLOUD_COVER_LOW": hourly.Variables(8).ValuesAsNumpy(),
            "CLOUD_COVER_MID": hourly.Variables(9).ValuesAsNumpy(),
            "CLOUD_COVER_HIGH": hourly.Variables(10).ValuesAsNumpy(),
            "VAPOUR_PRESSURE_DEFICIT": hourly.Variables(11).ValuesAsNumpy(),
            "WIND_SPEED_10M": hourly.Variables(12).ValuesAsNumpy(),
            "WIND_SPEED_100M": hourly.Variables(13).ValuesAsNumpy(),
            "WIND_DIRECTION_10M": hourly.Variables(14).ValuesAsNumpy(),
            "WIND_DIRECTION_100M": hourly.Variables(15).ValuesAsNumpy(),
            "WIND_GUSTS_10M": hourly.Variables(16).ValuesAsNumpy(),
            "SOIL_TEMPERATURE_0_TO_7CM": hourly.Variables(17).ValuesAsNumpy(),
            "SOIL_TEMPERATURE_7_TO_28CM": hourly.Variables(18).ValuesAsNumpy(),
            "SOIL_TEMPERATURE_28_TO_100CM": hourly.Variables(19).ValuesAsNumpy(),
            "SOIL_TEMPERATURE_100_TO_255CM": hourly.Variables(20).ValuesAsNumpy(),
            "SOIL_MOISTURE_0_TO_7CM": hourly.Variables(21).ValuesAsNumpy(),
            "SOIL_MOISTURE_7_TO_28CM": hourly.Variables(22).ValuesAsNumpy(),
            "SOIL_MOISTURE_28_TO_100CM": hourly.Variables(23).ValuesAsNumpy(),
            "SOIL_MOISTURE_100_TO_255CM": hourly.Variables(24).ValuesAsNumpy()
        }
        hourly_df = pd.DataFrame(hourly_data)
        hourly_df['RECORD_DATE'] = hourly_df['DATE'].dt.date
        hourly_df['RECORD_HOUR'] = hourly_df['DATE'].dt.hour
        hourly_df['PLACE_NAME'] = place_name
        hourly_df['LATITUDE'] = lat
        hourly_df['LONGITUDE'] = long
        cols = ['PLACE_NAME','LATITUDE','LONGITUDE','RECORD_DATE', 'RECORD_HOUR'] + [col for col in hourly_df.columns if col not in ['DATE', 'RECORD_DATE', 'RECORD_HOUR','PLACE_NAME','LATITUDE','LONGITUDE']]
        hourly_df = hourly_df[cols]
        return hourly_df

    def insert_into_snowflake(self, df, table_name):
        # Set index=False to avoid creating an index in Snowflake
        df.to_sql(table_name, self.snowflake_connection, if_exists='append', method='multi', index=False)
        nrows = len(df)  # Count number of rows inserted
        print(f"Inserted {nrows} rows into {table_name}.")




