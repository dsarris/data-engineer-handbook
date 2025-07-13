from pyspark.sql import SparkSession

def do_deduplicate_players_transformation(spark, dataframe):
    query = f"""
    SELECT 
            game_id,
            team_id,
            team_abbreviation,
            team_city,
            player_id,
            player_name,
            nickname,
            start_position,
            comment,
            min,
            fgm,
            fga,
            fg_pct,
            fg3m,
            fg3a,	
            fg3_pct,
            ftm,
            fta,
            ft_pct,
            oreb,
            dreb,
            reb,
            ast,
            stl,
            blk,
            TO,
            pf,
            pts,
            plus_minus
    FROM 
        (
            SELECT 
                *,
                ROW_NUMBER() OVER(PARTITION BY game_id,team_id,player_id ORDER BY game_id) AS dedup_rn
            FROM game_details
    ) t WHERE dedup_rn = 1
    """
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)

    