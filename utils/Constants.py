from pyspark.sql.types import StructType, StructField, StringType, FloatType

class Constants:
    """Class to store reusable constants and configurations."""

    WEATHER_DATA_SCHEMA = StructType([
        StructField("date", StringType(), True),                  # "date"
        StructField("time", StringType(), True),                  # "time"
        StructField("water_content(m3/m3)", FloatType(), True),  # "water_content(m3/m3)"
        StructField("solar_radiation(w/m2)", FloatType(), True),  # "solar_radiation(w/m2)"
        StructField("rain(mm)", FloatType(), True),               # "rain(mm)"
        StructField("temperature(celcius)", FloatType(), True),   # "temperature(celcius)"
        StructField("rh(%)", FloatType(), True),                  # "rh(%)"
        StructField("wind_speed(m/s)", FloatType(), True),        # "wind_speed(m/s)"
        StructField("gust_speed(m/s)", FloatType(), True),        # "gust_speed(m/s)"
        StructField("wind_direction(degree)", FloatType(), True), # "wind_direction(degree)"
        StructField("dew_point(celcius)", FloatType(), True)      # "dew_point(celcius)"
    ])
