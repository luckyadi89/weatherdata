CREATE TABLE {{ this }} IF NOT EXISTS (
        PLACE_NAME VARCHAR(250),
        LATITUDE FLOAT,
        LONGITUDE FLOAT,
        RECORD_DATE DATE,
        RECORD_HOUR INT,
        TEMPERATURE_2M FLOAT,
        RELATIVE_HUMIDITY_2M FLOAT,
        DEW_POINT_2M FLOAT,
        APPARENT_TEMPERATURE FLOAT,
        RAIN FLOAT,
        PRESSURE_MSL FLOAT,
        SURFACE_PRESSURE FLOAT,
        CLOUD_COVER FLOAT,
        CLOUD_COVER_LOW FLOAT,
        CLOUD_COVER_MID FLOAT,
        CLOUD_COVER_HIGH FLOAT,
        VAPOUR_PRESSURE_DEFICIT FLOAT,
        WIND_SPEED_10M FLOAT,
        WIND_SPEED_100M FLOAT,
        WIND_DIRECTION_10M FLOAT,
        WIND_DIRECTION_100M FLOAT,
        WIND_GUSTS_10M FLOAT,
        SOIL_TEMPERATURE_0_TO_7CM FLOAT,
        SOIL_TEMPERATURE_7_TO_28CM FLOAT,
        SOIL_TEMPERATURE_28_TO_100CM FLOAT,
        SOIL_TEMPERATURE_100_TO_255CM FLOAT,
        SOIL_MOISTURE_0_TO_7CM FLOAT,
        SOIL_MOISTURE_7_TO_28CM FLOAT,
        SOIL_MOISTURE_28_TO_100CM FLOAT,
        SOIL_MOISTURE_100_TO_255CM FLOAT
    )