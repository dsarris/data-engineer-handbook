from pyspark.sql import SparkSession

def do_actors_class_transformation(spark, dataframe, year):
    query = f"""
    SELECT 
        actor,
        actorid,
        SUM(votes) AS total_votes,
        CASE
            WHEN SUM(votes*rating)/SUM(votes) > 8 THEN 'star'
            WHEN SUM(votes*rating)/SUM(votes) > 7 AND SUM(votes*rating)/SUM(votes) <= 8 THEN 'good'
            WHEN SUM(votes*rating)/SUM(votes) > 6 AND SUM(votes*rating)/SUM(votes) <= 7 THEN 'average'
            ELSE 'bad'
        END AS quality_class
    FROM actor_films
    WHERE year = {year}
    GROUP BY actor,actorid
    """
    dataframe.createOrReplaceTempView("actor_films")
    return spark.sql(query)

    