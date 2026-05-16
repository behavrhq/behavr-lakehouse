-- Example BI / dashboard queries against gold tables.

-- Top searches for a site and day
SELECT search_query, search_count, zero_result_searches
FROM behavr.gold.search_metrics
WHERE site_id = 'site_123' AND event_date = DATE '2026-05-16'
ORDER BY search_count DESC
LIMIT 50;

-- Product funnel snapshot
SELECT product_id, product_views, add_to_cart_events, purchase_events, purchase_conversion_rate
FROM behavr.gold.product_metrics
WHERE site_id = 'site_123' AND event_date = DATE '2026-05-16'
ORDER BY product_views DESC
LIMIT 100;

-- Session quality
SELECT site_id, event_date, sessions, avg_duration_seconds, bounce_rate
FROM behavr.gold.session_metrics
WHERE event_date >= DATE '2026-05-01'
ORDER BY event_date DESC, site_id;

-- Silver canonical events (deduped)
SELECT event_id, occurred_at_utc, event_type, search_query, product_id
FROM behavr.silver.events
WHERE site_id = 'site_123' AND event_date = DATE '2026-05-16'
LIMIT 100;
