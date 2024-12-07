from pyspark.sql.types import StructType, StructField, StringType, FloatType , DateType , TimestampType

from pyspark.sql import functions as F

class Constants:
    """Class to store reusable constants and configurations."""

    WEATHER_DATA_SCHEMA = StructType([
        StructField("date", DateType(), True),                  
        StructField("time", StringType(), True),                  
        StructField("water_content(m3/m3)", FloatType(), True),  
        StructField("solar_radiation(w/m2)", FloatType(), True),  
        StructField("rain(mm)", FloatType(), True),               
        StructField("temperature(celcius)", FloatType(), True),   
        StructField("rh(%)", FloatType(), True),                  
        StructField("wind_speed(m/s)", FloatType(), True),        
        StructField("gust_speed(m/s)", FloatType(), True),        
        StructField("wind_direction(degree)", FloatType(), True),
        StructField("dew_point(celcius)", FloatType(), True)     
    ])

    @staticmethod
    def find_outliers_IQR(spark_df, column):
        # Calculate Q1 and Q3
        q1, q3 = spark_df.approxQuantile(column, [0.25, 0.75], 0.0)
        IQR = q3 - q1

        # Define outlier bounds
        lower_bound = q1 - 1.5 * IQR
        upper_bound = q3 + 1.5 * IQR

        # Filter outliers
        outliers = spark_df.filter((F.col(column) < lower_bound) | (F.col(column) > upper_bound))
        return outliers




