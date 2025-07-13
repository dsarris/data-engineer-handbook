from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Build session with automatic broadcast joins disabled
spark = SparkSession.builder\
    .appName("Week3homework")\
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")\
    .getOrCreate()

# Read datasets
maps = spark.read.option("header", "true").csv("../data/maps.csv")
match_details = spark.read.option("header", "true").csv("../data/match_details.csv")
matches = spark.read.option("header", "true").csv("../data/matches.csv")
medals_matches_players = spark.read.option("header", "true").csv("../data/medals_matches_players.csv")
medals = spark.read.option("header", "true").csv("../data/medals.csv")

# Prep datasets by creating temp views and bucketised tables
spark.sql("""DROP TABLE IF EXISTS homework.match_details""")
spark.sql("""DROP TABLE IF EXISTS homework.matches""")
spark.sql("""DROP TABLE IF EXISTS homework.medals_matches_players""")
spark.sql("""DROP TABLE IF EXISTS homework.maps""")
spark.sql("""DROP TABLE IF EXISTS homework.medals""")

spark.sql("""
CREATE TABLE IF NOT EXISTS homework.medals_matches_players (
    match_id STRING,
    player_gamertag STRING,
    medal_id STRING,
    count STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
""")
spark.sql("""
CREATE TABLE IF NOT EXISTS homework.match_details (
    match_id STRING,
    player_gamertag STRING,
    previous_spartan_rank STRING,
    spartan_rank STRING,
    previous_total_xp STRING,
    total_xp STRING,
    previous_csr_tier STRING,
    previous_csr_designation STRING,
    previous_csr STRING,
    previous_csr_percent_to_next_tier STRING,
    previous_csr_rank STRING,
    current_csr_tier STRING,
    current_csr_designation STRING,
    current_csr STRING,
    current_csr_percent_to_next_tier STRING,
    current_csr_rank STRING,
    player_rank_on_team STRING,
    player_finished STRING,
    player_average_life STRING,
    player_total_kills STRING,
    player_total_headshots STRING,
    player_total_weapon_damage STRING,
    player_total_shots_landed STRING,
    player_total_melee_kills STRING,
    player_total_melee_damage STRING,
    player_total_assassinations STRING,
    player_total_ground_pound_kills STRING,
    player_total_shoulder_bash_kills STRING,
    player_total_grenade_damage STRING,
    player_total_power_weapon_damage STRING,
    player_total_power_weapon_grabs STRING,
    player_total_deaths STRING,
    player_total_assists STRING,
    player_total_grenade_kills STRING,
    did_win STRING,
    team_id STRING
 )
USING iceberg
PARTITIONED BY (bucket(16, match_id))
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS homework.matches (
    match_id STRING,
    mapid STRING,
    is_team_game STRING,
    playlist_id STRING,
    game_variant_id STRING,
    is_match_over STRING,
    completion_date STRING,
    match_duration STRING,
    game_mode STRING,
    map_variant_id STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS homework.maps (
    mapid STRING,
    name STRING,
    description STRING
)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS homework.medals (
    medal_id STRING,
    sprite_uri STRING,
    sprite_left STRING,
    sprite_top STRING,
    sprite_sheet_width STRING,
    sprite_sheet_height STRING,
    sprite_width STRING,
    sprite_height STRING,
    classification STRING,
    description STRING,
    name STRING,
    difficulty STRING
)
""")

maps.write.mode("append").saveAsTable("homework.maps")
medals.write.mode("append").saveAsTable("homework.medals")
match_details.write.mode("append").bucketBy(16,"match_id").saveAsTable("homework.match_details")
matches.write.mode("append").bucketBy(16,"match_id").saveAsTable("homework.matches")
medals_matches_players.write.mode("append").bucketBy(16,"match_id").saveAsTable("homework.medals_matches_players")

# JoinedDf 
spark.sql("""
SELECT 
    /*+ BROADCAST(m5) */
    /*+ BROADCAST(m4) */
    m1.match_id,
    m5.medal_id,
    count,
    m2.player_gamertag AS player_gamertag_match_detals,
    m3.player_gamertag AS player_gamertag_medals_matches_players,    
    previous_spartan_rank,
    spartan_rank,
    previous_total_xp,
    total_xp,
    previous_csr_tier,
    previous_csr_designation,
    previous_csr,
    previous_csr_percent_to_next_tier,
    previous_csr_rank,
    current_csr_tier,
    current_csr_designation,
    current_csr,
    current_csr_percent_to_next_tier,
    current_csr_rank,
    player_rank_on_team,
    player_finished,
    player_average_life,
    player_total_kills,
    player_total_headshots,
    player_total_weapon_damage,
    player_total_shots_landed,
    player_total_melee_kills,
    player_total_melee_damage,
    player_total_assassinations,
    player_total_ground_pound_kills,
    player_total_shoulder_bash_kills,
    player_total_grenade_damage,
    player_total_power_weapon_damage,
    player_total_power_weapon_grabs,
    player_total_deaths,
    player_total_assists,
    player_total_grenade_kills,
    did_win,
    team_id,
    m1.mapid,
    is_team_game,
    playlist_id,
    game_variant_id,
    is_match_over,
    completion_date,
    match_duration,
    game_mode,
    map_variant_id,
    m4.name AS map_name,
    m4.description AS map_description,
    sprite_uri,
    sprite_left,
    sprite_top,
    sprite_sheet_width,
    sprite_sheet_height,
    sprite_width,
    sprite_height,
    classification,
    m5.description AS medal_description,
    m5.name AS medal_name,
    difficulty
FROM homework.matches m1
JOIN homework.match_details m2
ON m1.match_id = m2.match_id
JOIN homework.medals_matches_players m3
ON m1.match_id = m3.match_id
JOIN homework.maps m4
ON m1.mapid = m4.mapid
JOIN homework.medals m5
ON m3.medal_id = m5.medal_id
""").createOrReplaceTempView("joinedDf")

# - Which player averages the most kills per game?
spark.sql(""" 
SELECT 
    player_gamertag_match_detals,
    SUM(player_total_kills)/COUNT(DISTINCT(match_id)) AS avg_kills_per_game
FROM joinedDf 
GROUP BY player_gamertag_match_detals
ORDER BY 2 DESC
LIMIT 1
""").show()

# - Which playlist gets played the most?
spark.sql("""
SELECT 
    playlist_id,
    COUNT(DISTINCT(match_id)) AS plays
FROM joinedDF
GROUP BY playlist_id
ORDER BY 2 DESC
LIMIT 1
""").show()

# - Which map gets played the most?
spark.sql("""
SELECT 
    mapid,
    map_name,
    COUNT(DISTINCT(match_id)) AS plays
FROM joinedDF
GROUP BY mapid,map_name
ORDER BY 3 DESC
LIMIT 1
""").show()

# - Which map do players get the most Killing Spree medals on?
spark.sql("""
SELECT 
    mapid,
    map_name,
    COUNT(DISTINCT(match_id)) AS plays
FROM joinedDF
WHERE medal_name = 'Killing Spree'
GROUP BY mapid,map_name
ORDER BY 3 DESC
LIMIT 1
""").show()

# - With the aggregated data set
# - Try different `.sortWithinPartitions` to see which has the smallest data size (hint: playlists and maps are both very low cardinality)
spark.sql(""" DROP TABLE IF EXISTS joinedDf.unsorted """)
spark.sql(""" DROP TABLE IF EXISTS joinedDf.sorted """)

spark.sql("""
CREATE TABLE IF NOT EXISTS joinedDf.unsorted (
    match_id STRING,
    medal_id STRING,
    count STRING,
    player_gamertag_match_detals STRING,
    player_gamertag_medals_matches_players STRING,    
    previous_spartan_rank STRING,
    spartan_rank STRING,
    previous_total_xp STRING,
    total_xp STRING,
    previous_csr_tier STRING,
    previous_csr_designation STRING,
    previous_csr STRING,
    previous_csr_percent_to_next_tier STRING,
    previous_csr_rank STRING,
    current_csr_tier STRING,
    current_csr_designation STRING,
    current_csr STRING,
    current_csr_percent_to_next_tier STRING,
    current_csr_rank STRING,
    player_rank_on_team STRING,
    player_finished STRING,
    player_average_life STRING,
    player_total_kills STRING,
    player_total_headshots STRING,
    player_total_weapon_damage STRING,
    player_total_shots_landed STRING,
    player_total_melee_kills STRING,
    player_total_melee_damage STRING,
    player_total_assassinations STRING,
    player_total_ground_pound_kills STRING,
    player_total_shoulder_bash_kills STRING,
    player_total_grenade_damage STRING,
    player_total_power_weapon_damage STRING,
    player_total_power_weapon_grabs STRING,
    player_total_deaths STRING,
    player_total_assists STRING,
    player_total_grenade_kills STRING,
    did_win STRING,
    team_id STRING,
    mapid STRING,
    is_team_game STRING,
    playlist_id STRING,
    game_variant_id STRING,
    is_match_over STRING,
    completion_date STRING,
    match_duration STRING,
    game_mode STRING,
    map_variant_id STRING,
    map_name STRING,
    map_description STRING,
    sprite_uri STRING,
    sprite_left STRING,
    sprite_top STRING,
    sprite_sheet_width STRING,
    sprite_sheet_height STRING,
    sprite_width STRING,
    sprite_height STRING,
    classification STRING,
    medal_description STRING,
    medal_name STRING,
    difficulty STRING
)
USING iceberg
PARTITIONED BY (playlist_id,mapid)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS joinedDf.sorted (
    match_id STRING,
    medal_id STRING,
    count STRING,
    player_gamertag_match_detals STRING,
    player_gamertag_medals_matches_players STRING,    
    previous_spartan_rank STRING,
    spartan_rank STRING,
    previous_total_xp STRING,
    total_xp STRING,
    previous_csr_tier STRING,
    previous_csr_designation STRING,
    previous_csr STRING,
    previous_csr_percent_to_next_tier STRING,
    previous_csr_rank STRING,
    current_csr_tier STRING,
    current_csr_designation STRING,
    current_csr STRING,
    current_csr_percent_to_next_tier STRING,
    current_csr_rank STRING,
    player_rank_on_team STRING,
    player_finished STRING,
    player_average_life STRING,
    player_total_kills STRING,
    player_total_headshots STRING,
    player_total_weapon_damage STRING,
    player_total_shots_landed STRING,
    player_total_melee_kills STRING,
    player_total_melee_damage STRING,
    player_total_assassinations STRING,
    player_total_ground_pound_kills STRING,
    player_total_shoulder_bash_kills STRING,
    player_total_grenade_damage STRING,
    player_total_power_weapon_damage STRING,
    player_total_power_weapon_grabs STRING,
    player_total_deaths STRING,
    player_total_assists STRING,
    player_total_grenade_kills STRING,
    did_win STRING,
    team_id STRING,
    mapid STRING,
    is_team_game STRING,
    playlist_id STRING,
    game_variant_id STRING,
    is_match_over STRING,
    completion_date STRING,
    match_duration STRING,
    game_mode STRING,
    map_variant_id STRING,
    map_name STRING,
    map_description STRING,
    sprite_uri STRING,
    sprite_left STRING,
    sprite_top STRING,
    sprite_sheet_width STRING,
    sprite_sheet_height STRING,
    sprite_width STRING,
    sprite_height STRING,
    classification STRING,
    medal_description STRING,
    medal_name STRING,
    difficulty STRING
)
USING iceberg
PARTITIONED BY (playlist_id,mapid)
""")

join_df.write.mode("overwrite").saveAsTable("joinedDf.unsorted")
join_df.sortWithinPartitions(F.col("playlist_id"), F.col("mapid")).write.mode("overwrite").saveAsTable("joinedDf.sorted")

spark.sql("""
SELECT 
    SUM(file_size_in_bytes) as size, 
    COUNT(1) as num_files, 
    'unsorted' 
FROM joinedDf.unsorted

UNION ALL

SELECT 
    SUM(file_size_in_bytes) as size, 
    COUNT(1) as num_files, 
    'sorted' 
FROM joinedDf.sorted
""").show()