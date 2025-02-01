from pyspark.sql.types import StructType, StructField, StringType, FloatType

class Constants:
    """Class to store reusable constants and configurations."""

    WEATHER_DATA_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("water_content", FloatType(), True),
    StructField("solar_radiation", FloatType(), True),
    StructField("rain", FloatType(), True),
    StructField("temperature", FloatType(), True),
    StructField("rh", FloatType(), True),
    StructField("wind_speed", FloatType(), True),
    StructField("gust_speed", FloatType(), True),
    StructField("wind_direction", FloatType(), True),
    StructField("dew_point", FloatType(), True),
    ])
    
    RAW_DATA_SCHEMA = StructType([
        StructField("Line#", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Time", StringType(), True),
        StructField("Water Content (m3/m3)", FloatType(), True),
        StructField("Solar Radiation (W/m2)", FloatType(), True),
        StructField("Rain (mm)", FloatType(), True),
        StructField("Temperature (Celcius)", FloatType(), True),
        StructField("RH (%)", FloatType(), True),
        StructField("Wind Speed (m/s)", FloatType(), True),
        StructField("Gust Speed (m/s)", FloatType(), True),
        StructField("Wind Direction (Degree)", FloatType(), True),
        StructField("Dew Point (Celcius)", FloatType(), True),
    ])
