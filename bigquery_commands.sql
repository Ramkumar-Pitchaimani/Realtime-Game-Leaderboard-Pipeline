CREATE TABLE IF NOT EXISTS
  `p101-473210.gaming.leaderboard_5min` (
    game_id      STRING,
    window_start TIMESTAMP,
    user_id      STRING,
    total_score  INT64
  );

--Insert the top 10 players per game over the last 5 minutes
SELECT
  game_id,
  TIMESTAMP_TRUNC(TIMESTAMP(ts), MINUTE) AS window_start,
  user_id,
  SUM(score_delta)                     AS total_score
FROM `p101-473210.gaming.raw_events`
WHERE
  TIMESTAMP(ts) BETWEEN
    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
  AND CURRENT_TIMESTAMP()
GROUP BY game_id, window_start, user_id
ORDER BY total_score DESC
LIMIT 10;