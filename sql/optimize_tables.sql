-- Delta maintenance (schedule off-peak). Tune retention per governance policy.

OPTIMIZE behavr.bronze.raw_events;
VACUUM  behavr.bronze.raw_events;

OPTIMIZE behavr.silver.events;
VACUUM  behavr.silver.events;

OPTIMIZE behavr.gold.search_metrics;
VACUUM  behavr.gold.search_metrics;

OPTIMIZE behavr.gold.product_metrics;
VACUUM  behavr.gold.product_metrics;

OPTIMIZE behavr.gold.session_metrics;
VACUUM  behavr.gold.session_metrics;

OPTIMIZE behavr.gold.page_metrics;
VACUUM  behavr.gold.page_metrics;

OPTIMIZE behavr.gold.funnel_metrics;
VACUUM  behavr.gold.funnel_metrics;

-- Future ZORDER candidates (silver / gold):
-- ZORDER BY (site_id, event_type, event_date)
