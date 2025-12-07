WITH ads AS (
    SELECT 
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        daily_spent
    FROM ya_ads

    UNION ALL

    SELECT
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        daily_spent
    FROM vk_ads
),

last_paid_click AS (
    SELECT
        l.lead_id,
        l.visitor_id,
        l.created_at,
        s.visit_date AS paid_visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        s.content AS utm_content,
        ROW_NUMBER() OVER (
            PARTITION BY l.lead_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM leads l
    JOIN sessions s
        ON s.visitor_id = l.visitor_id
       AND s.source IN ('yandex', 'vk')
       AND s.visit_date <= l.created_at
),

lpc AS (
    SELECT *
    FROM last_paid_click
    WHERE rn = 1
),

visits AS (
    SELECT
        DATE(visit_date) AS visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign,
        COUNT(*) AS visitors_count
    FROM sessions
    GROUP BY 1,2,3,4
),

leads_aggr AS (
    SELECT
        DATE(paid_visit_date) AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(*) AS leads_count
    FROM lpc
    GROUP BY 1,2,3,4
),

purchases AS (
    SELECT
        DATE(lpc.paid_visit_date) AS visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        COUNT(*) AS purchases_count,
        SUM(l.amount) AS revenue
    FROM lpc
    JOIN leads l USING (lead_id)
    WHERE l.closing_reason = 'Успешно реализовано'
       OR l.status_id = 142
    GROUP BY 1,2,3,4
),

costs AS (
    SELECT
        campaign_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM ads
    GROUP BY 1,2,3,4
)

SELECT
    v.visit_date,
    v.visitors_count,
    v.utm_source,
    v.utm_medium,
    v.utm_campaign,
    COALESCE(c.total_cost, 0) AS total_cost,
    COALESCE(l.leads_count, 0) AS leads_count,
    COALESCE(p.purchases_count, 0) AS purchases_count,
    p.revenue
FROM visits v
LEFT JOIN costs c USING (visit_date, utm_source, utm_medium, utm_campaign)
LEFT JOIN leads_aggr l USING (visit_date, utm_source, utm_medium, utm_campaign)
LEFT JOIN purchases p USING (visit_date, utm_source, utm_medium, utm_campaign)
ORDER BY
    v.visit_date ASC,
    v.visitors_count DESC,
    v.utm_source ASC,
    v.utm_medium ASC,
    v.utm_campaign ASC,
    p.revenue DESC NULLS LAST
LIMIT 15;
