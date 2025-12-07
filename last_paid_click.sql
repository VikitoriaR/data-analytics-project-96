WITH paid_sessions AS (
    SELECT
        visitor_id,
        visit_date,
        landing_page,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign,
        content AS utm_content
    FROM sessions
    WHERE medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

last_paid_click AS (
    SELECT
        l.visitor_id,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        ps.utm_source,
        ps.utm_medium,
        ps.utm_campaign,
        ps.visit_date,
        ROW_NUMBER() OVER (
            PARTITION BY l.visitor_id, l.lead_id
            ORDER BY ps.visit_date DESC
        ) AS rn
    FROM leads AS l
),

all_sessions AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign
    FROM sessions AS s
)

SELECT
    a.visitor_id,
    a.visit_date,
    lpc.lead_id,
    lpc.created_at,
    lpc.amount,
    lpc.closing_reason,
    lpc.status_id,
    COALESCE(lpc.utm_source, a.utm_source) AS utm_source,
    COALESCE(lpc.utm_medium, a.utm_medium) AS utm_medium,
    COALESCE(lpc.utm_campaign, a.utm_campaign) AS utm_campaign
FROM all_sessions AS a
LEFT JOIN last_paid_click AS lpc
    ON 
        a.visitor_id = lpc.visitor_id
        AND lpc.rn = 1
ORDER BY
    lpc.amount DESC NULLS LAST,
    a.visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC;
