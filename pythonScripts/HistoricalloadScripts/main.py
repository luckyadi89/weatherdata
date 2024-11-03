import pandas as pd
import os
import snowflake.connector
from dotenv import load_dotenv
from HistoricalLoad import WeatherDataHandler
load_dotenv()
if __name__=='__main__':
    weather_handler = WeatherDataHandler()
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )

    cur = conn.cursor()
    sql_query = "SELECT * FROM TRIPURATOWNS"
    cur.execute(sql_query)
    result = cur.fetchall()
    df = pd.DataFrame(result)
    print(df)
    cur.close()
    for index, row in df.iterrows():
        place = row[0]
        lat = row[1]
        long = row[2]
        weather_data = weather_handler.get_weather_data(place,lat, long, "2024-10-18", "2024-10-18")
        hourly = weather_handler.process_hourly_data(weather_data.Hourly())
        weather_handler.insert_into_snowflake(hourly.iloc[0:4], 'XTR_HISTORICAL_DATA')



    # conn = snowflake.connector.connect(
    # user=os.getenv('SNOWFLAKE_USER'),
    # password=os.getenv('SNOWFLAKE_PASSWORD'),
    # account=os.getenv('SNOWFLAKE_ACCOUNT'),
    # warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    # database=os.getenv('SNOWFLAKE_DATABASE'),
    # schema=os.getenv('SNOWFLAKE_SCHEMA'),
    # role=os.getenv('SNOWFLAKE_ROLE')
    # )

    # cur = conn.cursor()
    # sql_query = "SELECT * FROM TRIPURATOWNS"
    # cur.execute(sql_query)
    # result = cur.fetch_pandas_all()
    # cur.close()
    # conn.close()
    # print(result)
    # # df = pd.DataFrame(result)
    # # print(df)
    print(hourly)
