-- week 4 day 1 - Data Engineer Design Patterns at Meta - Growth Accounting
INSERT INTO user_growth_accounting
WITH yesterday AS (
        SELECT * FROM users_growth_accounting
--      WHERE date = DATE ('2023-02-28')
        WHERE date = DATE ('2023-03-01')
),
        today AS (
                SELECT
                        CAST(user_id AS TEXT) as user_id,
                        DATE_TRUNC('day', event_time::tunestanp) as today_date,
                        COUNT(1)
                FROM events
--              WHERE DATE_TRUNC('day', event_time::timestamp) = DATE('2023-02-28')
                WHERE DATE_TRUNC('day', event_time::timestamp) = DATE('2023-03-02')
                AND user_id IS NOT NULL
                GROUP BY user_id, DATE_TRUNC('day', event_time::timestamp)
        )
        
SELECT 
        COALESCE(t.user_id, y.user_id) as user_id,
        COALESCE(y.first_active_date, t.today_date) AS first_active_date,
        COALESCE(t.today_date, y.last_active_date) AS last_active_date
        CASE WHEN y.user_id IS NULL THEN 'New',
             WHEN y.last_active_date = t.today_date - Interval '1 day' THEN 'Retained',
             WHEN y.last_active_date < t.today_date - Interval '1 day' THEN 'Resurrected',
             WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned',
             ELSE 'Stale'
        END as daily_active_state,
        CASE WHEN y.user_id IS NULL THEN 'New',
             WHEN y.last_active_date < t.today_date - Interval '7  day' THEN 'Resurrected',
             WHEN y.last_active_date >= y.date - Interval '7 day' THEN 'Retained',
             WHEN t.today_date IS NULL AND y.last_active_date = y.date - interval '7 day' THEN 'Churned',
             ELSE 'Stale'
        
  --      CASE WHEN 1 = 1 THEN 1 END as daily_active_state,     
  --      CASE WHEN 1 = 1 THEN 1 END as weekly_active_state,
        END as weekly_active_state,
        COALESCE(y.dates_active,
            ARRAY []::DATE[])
              || CASE WHEN
                 t.user_id IS NOT NULL
                 THEN ARRAY [t.today_date]
                 ELSE ARRAY []::DATE[]
                    END AS date_list,
        COALESCE(t.today_date, y.date + Interval '1 day') AS date
     FROM today t
     FULL OUTER JOIN yesterday y
     ON t.user_id = y.user_id
     
     
-- applying analytical patterns lab 2
WITH deduped_events AS (
        SELECT
        user_id, d1.url, d2.url as destination_url, d1.event_time, d2.event_time DATE(event_time) as event_date
        FROM events
        WHERE user_id IS NOT NULL
        AND url IN ('/signup', '/api/v1/users')
        GROUP BY user_id, url, event_time, DATE(event_time)
)
SELECT * FROM deduped_events d1 
        JOIN deduped_events d2 
                ON d1.user_id = d2.user_id 
                AND d1.event_date = d2.event_date
                AND d2.event_time > d1.event_time
--                AND d1.url <> d2.url
--                WHERE d1.url = '/signup' AND d2.url = '/api/v1/users',
                userlevel AS (
                        SELECT
                        user_id,
                        url,
                        COUNT(1) as number_of_hits,
                        MAX(CASE WHEN destination_url = '/api/v1/users' THEN 1 ELSE 0 END) AS converted
                )

SELECT *
        user_id, 
        MAX(CASE WHEN destination_url = '/api/v1/users' THEN 1 ELSE 0 END) AS converted
FROM selfjoined
GROUP BY user_id, url
        )
        SELECT url,SUM(number_of_hits), CAST(SUM(converted) AS REAL) /COUNT(1)
FROM userlevel
GROUP BY url
HAVING SUM(number_of_hits) > 500
