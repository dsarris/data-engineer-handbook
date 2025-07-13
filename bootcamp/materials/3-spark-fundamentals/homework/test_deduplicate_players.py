from pyspark.sql import SparkSession

from chispa.dataframe_comparer import *
from ..jobs.deduplicate_players import do_deduplicate_players_transformation

def test_monthly_site_hits(spark):
    input_data = spark.createDataFrame(
        [
            [22000044,1610612743,"DEN","Denver",1628420,"Monte Morris","","","","23:30",5,7,0.714,1,3,0.333,1,3,0.333,0,4,4,5,0,1,0,2,12,14],
            [22000044,1610612743,"DEN","Denver",1628420,"Monte Morris","","","","23:30",5,7,0.714,1,3,0.333,1,3,0.333,0,4,4,5,0,1,0,2,12,14]
        ],
        [
            'game_id',
            'team_id',
            'team_abbreviation',
            'team_city',
            'player_id',
            'player_name',
            'nickname',
            'start_position',
            'comment',
            'min',
            'fgm',
            'fga',
            'fg_pct',
            'fg3m',
            'fg3a',	
            'fg3_pct',
            'ftm',
            'fta',
            'ft_pct',
            'oreb',
            'dreb',
            'reb',
            'ast',
            'stl',
            'blk',
            'TO',
            'pf',
            'pts',
            'plus_minus'
        ],  
    )

    expected_data = spark.createDataFrame(
        [
            [22000044,1610612743,"DEN","Denver",1628420,"Monte Morris","","","","23:30",5,7,0.714,1,3,0.333,1,3,0.333,0,4,4,5,0,1,0,2,12,14],
        ],
        [
            'game_id',
            'team_id',
            'team_abbreviation',
            'team_city',
            'player_id',
            'player_name',
            'nickname',
            'start_position',
            'comment',
            'min',
            'fgm',
            'fga',
            'fg_pct',
            'fg3m',
            'fg3a',	
            'fg3_pct',
            'ftm',
            'fta',
            'ft_pct',
            'oreb',
            'dreb',
            'reb',
            'ast',
            'stl',
            'blk',
            'TO',
            'pf',
            'pts',
            'plus_minus'
        ],
    )

    actual_data = do_deduplicate_players_transformation(spark,input_data)
    assert_df_equality(actual_data, expected_data)