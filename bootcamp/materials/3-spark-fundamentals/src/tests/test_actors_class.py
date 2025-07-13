from pyspark.sql import SparkSession

from chispa.dataframe_comparer import *
from pyspark.sql.types import *
from ..jobs.actors_class import do_actors_class_transformation

def test_monthly_site_hits(spark):
    input_data = spark.createDataFrame(
        [
            ["Alain Delon","nm0001128","Le Cercle Rouge",1970,22796,8.0,"tt0065531"],
            ["Alain Delon","nm0001128","Borsalino",1970,3578,7.0,"tt0065486"],
            ["Alain Delon","nm0001128","The Love Mates",1970,236,5.8,"tt0242632"]

        ],
        [
            'actor',
            'actorid',
            'film',
            'year',
            'votes',
            'rating',
            'filmid'
        ],  
    )

    expected_data = spark.createDataFrame(
        [
            ["Alain Delon","nm0001128",26610,"good"],
        ],
        StructType([
            StructField('actor', StringType(), True),
            StructField('actorid', StringType(), True),
            StructField('total_votes', LongType(), True),
            StructField('quality_class', StringType(), False)
        ])

    )
    actual_data = do_actors_class_transformation(spark,input_data,1970)
    assert_df_equality(actual_data, expected_data)