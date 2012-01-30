SET work_mem='64MB';
SHOW work_mem;

-- EXPLAIN ANALYZE
-- SELECT show_id, name, SUM(interactions_count * followers_count) AS impressions
-- FROM (
--       SELECT
--           show_id
--         , interactions_count
--         , CASE followers_count
--           WHEN 0 THEN 130
--           ELSE followers_count END
--       FROM (
--           SELECT show_id, screen_name, COUNT(*)
--           FROM   twitter_interactions JOIN show_bindings USING (interaction_id)
--           WHERE  twitter_interactions.created_at >= '2011-10-17' AND twitter_interactions.created_at < '2011-10-23'
--             AND  show_bindings.created_at >= '2011-10-17' AND show_bindings.created_at < '2011-10-23'
--           GROUP BY 1, 2) AS t1(show_id, screen_name, interactions_count)
--         JOIN twitter_personas USING (screen_name)
--     UNION ALL
--       SELECT
--           show_id
--         , interactions_count
--         , CASE friends_count
--           WHEN 0 THEN 130
--           ELSE friends_count END
--       FROM (
--             SELECT show_id, facebook_user_id, COUNT(*)
--             FROM   facebook_interactions JOIN show_bindings USING (interaction_id)
--             WHERE  facebook_interactions.created_at >= '2011-10-17' AND facebook_interactions.created_at < '2011-10-23'
--               AND  show_bindings.created_at >= '2011-10-17' AND show_bindings.created_at < '2011-10-23'
--             GROUP BY 1, 2) AS t1(show_id, facebook_user_id, interactions_count)
--           JOIN facebook_personas USING (facebook_user_id)) AS t3(show_id, interactions_count, followers_count)
--   JOIN shows USING (show_id)
-- GROUP BY 1, 2
-- ORDER BY 3 DESC
-- ;

CREATE TEMPORARY TABLE personas(profile_id text, followers_count int not null);
INSERT INTO personas
    SELECT screen_name, followers_count
    FROM   twitter_personas
    WHERE  screen_name IN (
      SELECT DISTINCT screen_name
      FROM twitter_interactions JOIN show_bindings USING (interaction_id)
      WHERE  twitter_interactions.created_at >= '2011-10-17' AND twitter_interactions.created_at < '2011-10-23'
        AND  show_bindings.created_at >= '2011-10-17' AND show_bindings.created_at < '2011-10-23')
      AND followers_count > 0
  UNION ALL
    SELECT facebook_user_id, friends_count
    FROM   facebook_personas
    WHERE  facebook_user_id IN (
      SELECT DISTINCT facebook_user_id
      FROM facebook_interactions JOIN show_bindings USING (interaction_id)
      WHERE  facebook_interactions.created_at >= '2011-10-17' AND facebook_interactions.created_at < '2011-10-23'
        AND  show_bindings.created_at >= '2011-10-17' AND show_bindings.created_at < '2011-10-23')
      AND friends_count > 0;
ALTER TABLE personas ADD PRIMARY KEY(profile_id);

-- EXPLAIN ANALYZE
SELECT show_id, name, SUM(interactions_count * followers_count) AS impressions
FROM (
    SELECT
        show_id
      , interactions_count
      , COALESCE(followers_count, 130) AS followers_count
    FROM (
          SELECT show_id, screen_name, COUNT(*)
          FROM   twitter_interactions JOIN show_bindings USING (interaction_id)
          WHERE  twitter_interactions.created_at >= '2011-10-17' AND twitter_interactions.created_at < '2011-10-23'
            AND  show_bindings.created_at >= '2011-10-17' AND show_bindings.created_at < '2011-10-23'
          GROUP BY 1, 2
        UNION ALL
          SELECT show_id, facebook_user_id, COUNT(*)
          FROM   facebook_interactions JOIN show_bindings USING (interaction_id)
          WHERE  facebook_interactions.created_at >= '2011-10-17' AND facebook_interactions.created_at < '2011-10-23'
            AND  show_bindings.created_at >= '2011-10-17' AND show_bindings.created_at < '2011-10-23'
          GROUP BY 1, 2) AS t1(show_id, profile_id, interactions_count)
      LEFT JOIN personas USING (profile_id)) AS t2
  JOIN shows USING (show_id)
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 5
