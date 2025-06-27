from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

raw_weather_schema = StructType([
    StructField("geometry", StructType([
        StructField("coordinates", ArrayType(DoubleType()), True)
    ]), True),
    StructField("properties", StructType([
        StructField("elevation", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True)
        ]), True),
        StructField("station", StringType(), True),
        StructField("stationId", StringType(), True),
        StructField("stationName", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("rawMessage", StringType(), True),
        StructField("textDescription", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("presentWeather", ArrayType(StringType()), True),
        StructField("temperature", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("dewpoint", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("windDirection", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("windSpeed", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("windGust", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("barometricPressure", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("seaLevelPressure", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("visibility", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("maxTemperatureLast24Hours", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True)
        ]), True),
        StructField("minTemperatureLast24Hours", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True)
        ]), True),
        StructField("precipitationLastHour", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("precipitationLast3Hours", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("precipitationLast6Hours", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("relativeHumidity", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("windChill", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("heatIndex", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("qualityControl", StringType(), True)
        ]), True),
        StructField("cloudLayers", ArrayType(StructType([
            StructField("base", StructType([
                StructField("unitCode", StringType(), True),
                StructField("value", DoubleType(), True)
            ]), True),
            StructField("amount", StringType(), True)
        ])), True)
    ]))
])
