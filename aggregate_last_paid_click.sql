WITH ads AS (
    SELECT
        ya.campaign_date,
        ya.utm_source,
        ya.utm_medium,
        ya.utm_campaign,
        ya.utm_content,
        ya.daily_spent
    FROM ya_ads AS ya

    UNION ALL

    SELECT
        vk.campaign_date,
        vk.utm_source,
        vk.utm_medium,
        vk.utm_campaign,
        vk.utm_content,
        vk.daily_spent
    FROM vk_ads AS vk
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
    FROM leads AS l
    INNER JOIN sessions AS s
        ON
            l.visitor_id = s.visitor_id
),

lpc AS (
    SELECT
        lpc_inner.lead_id,
        lpc_inner.visitor_id,
        lpc_inner.created_at,
        lpc_inner.paid_visit_date,
        lpc_inner.utm_source,
        lpc_inner.utm_medium,
        lpc_inner.utm_campaign,
        lpc_inner.utm_content,
        lpc_inner.rn
    FROM last_paid_click AS lpc_inner
    WHERE lpc_inner.rn = 1
),

visits AS (
    SELECT
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        DATE(s.visit_date) AS visit_date,
        COUNT(*) AS visitors_count
    FROM sessions AS s
    GROUP BY
        s.source,
        s.medium,
        s.campaign,
        DATE(s.visit_date)
),

leads_aggr AS (
    SELECT
        l.utm_source,
        l.utm_medium,
        l.utm_campaign,
        DATE(l.paid_visit_date) AS visit_date,
        COUNT(*) AS leads_count
    FROM lpc AS l
    GROUP BY
        l.utm_source,
        l.utm_medium,
        l.utm_campaign,
        DATE(l.paid_visit_date)
),

purchases AS (
    SELECT
        lp.utm_source,
        lp.utm_medium,
        lp.utm_campaign,
        DATE(lp.paid_visit_date) AS visit_date,
        COUNT(*) AS purchases_count,
        SUM(ld.amount) AS revenue
    FROM lpc AS lp
    INNER JOIN leads AS ld
        ON
            lp.lead_id = ld.lead_id
    WHERE ld.closing_reason = 'Успешно реализовано'
       OR ld.status_id = 142
        GROUP BY
        lp.utm_source,
        lp.utm_medium,
        lp.utm_campaign,
        DATE(lp.paid_visit_date)
),

costs AS (
    SELECT
        a.utm_source,
        a.utm_medium,
        a.utm_campaign,
        a.campaign_date AS visit_date,
        SUM(a.daily_spent) AS total_cost
    FROM ads AS a
    GROUP BY
        a.utm_source,
        a.utm_medium,
        a.utm_campaign,
        a.campaign_date
)

SELECT
    v.visit_date,
    v.utm_source,
    v.utm_medium,
    v.utm_campaign,
    v.visitors_count,
    p.revenue,
    COALESCE(c.total_cost, 0) AS total_cost,
    COALESCE(l.leads_count, 0) AS leads_count,
    COALESCE(p.purchases_count, 0) AS purchases_count
FROM visits AS v
LEFT JOIN costs AS c
    ON
        v.visit_date = c.visit_date
        AND v.utm_source = c.utm_source
        AND v.utm_medium = c.utm_medium
        AND v.utm_campaign = c.utm_campaign
LEFT JOIN leads_aggr AS l
    ON
        v.visit_date = l.visit_date
        AND v.utm_source = l.utm_source
        AND v.utm_medium = l.utm_medium
        AND v.utm_campaign = l.utm_campaign
LEFT JOIN purchases AS p
    ON
        v.visit_date = p.visit_date
        AND v.utm_source = p.utm_source
        AND v.utm_medium = p.utm_medium
        AND v.utm_campaign = p.utm_campaign
ORDER BY
    v.visit_date ASC,
    v.utm_source ASC,
    v.utm_medium ASC,
    v.utm_campaign ASC,
    v.visitors_count DESC,
    p.revenue DESC NULLS LAST
LIMIT 15;
    