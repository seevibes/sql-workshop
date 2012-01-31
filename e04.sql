SET work_mem='64MB';
SHOW work_mem;

CREATE TEMPORARY TABLE events(
    step text primary key
  , start_at timestamp without time zone not null
);

INSERT INTO events VALUES
    ('Startup', '2011-10-01 02:00:00')
  , ('Find Unique Personas', '2011-10-01 02:01:13')
  , ('Calculate Loyalty', '2011-10-01 02:01:41')
  , ('Calculate Social Impressions', '2011-10-01 02:02:38')
  , ('Calculate Feedback', '2011-10-01 02:03:09')
  , ('ANALYZE', '2011-10-01 02:09:27');

SELECT * FROM events ORDER BY start_at;
SELECT
    step
  , start_at AS end_at
  , lag(start_at) OVER () AS start_at
  , start_at - lag(start_at) OVER (ORDER BY start_at) AS duration
FROM events
ORDER BY 2;
