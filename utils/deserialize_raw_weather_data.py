from pyspark.sql.functions import from_json, col, to_timestamp
from models.raw_weather_entity import raw_weather_schema

def deserialize_weather_data(raw_data):
    return raw_data.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col('json'), raw_weather_schema).alias('data')) \
        .select(
            col("data.properties.stationId").alias("station_id"),
            col("data.properties.stationName").alias('stationName'),
            to_timestamp(col("data.properties.timestamp")).alias("obs_time"),
            col("data.properties.temperature.value").alias("temperature"),
            col("data.properties.relativeHumidity.value").alias("humidity"),
            col("data.properties.dewpoint.value").alias("dewpoint"),
            col("data.properties.windSpeed.value").alias("wind_speed"),
            col("data.properties.windGust.value").alias("wind_gust"),
            col("data.properties.windDirection.value").alias("wind_direction"),
            col("data.properties.barometricPressure.value").alias("barometric_pressure"),
            col("data.properties.seaLevelPressure.value").alias("sea_level_pressure"),
            col("data.properties.visibility.value").alias("visibility"),
            col("data.properties.heatIndex.value").alias("heat_index"),
            col("data.properties.windChill.value").alias("wind_chill"),
            col("data.properties.textDescription").alias("description"),
            col("data.properties.icon").alias("icon"),
            col("data.properties.elevation.value").alias('elevation'),
            col("data.properties.presentWeather").alias('presentWeather'),
            col("data.geometry.coordinates").alias('coordinates')
    )
