SET work_mem='64MB';
SHOW work_mem;

-- EXPLAIN ANALYZE
SELECT
    period_start_at
  , rank
  , market_share
  , show_id
  , name
  , interactions_count
  , day_interactions_count
FROM (
    SELECT
        period_start_at
      , rank() OVER (PARTITION BY period_start_at ORDER BY market_share DESC) AS rank
      , market_share
      , show_id
      , interactions_count
      , day_interactions_count
    FROM (
        SELECT
            period_start_at
          , round(100.0 * interactions_count / day_interactions_count, 1) AS market_share
          , show_id
          , interactions_count
          , day_interactions_count
        FROM (
            SELECT
                period_start_at
              , show_id
              , interactions_count
              , SUM(interactions_count) OVER (PARTITION BY period_start_at) AS day_interactions_count
            FROM (
                SELECT date_trunc('day', created_at), show_id, count(*)
                FROM   show_bindings
                WHERE  created_at >= '2011-10-17' AND created_at < '2011-10-23'
                GROUP BY 1, 2) AS t1(period_start_at, show_id, interactions_count)) AS t2) AS t3) AS t4
  JOIN shows USING (show_id)
WHERE rank <= 5
ORDER BY period_start_at, market_share DESC
