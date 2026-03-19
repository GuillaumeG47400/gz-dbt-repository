 {{ config(materialized= 'table') }}
SELECT
     date_trunc(date_date, MONTH) AS datemonth,
     SUM(operational_margin - ads_cost) AS ads_margin,
     ROUND(SUM(daily_revenue) / NULLIF(SUM(nb_transactions),0), 2) AS average_basket,
     SUM(operational_margin) AS operational_margin,
     SUM(ads_cost) AS ads_cost,
     SUM(impression) AS ads_impression,
     SUM(click) AS ads_clicks,
     SUM(quantity) AS quantity,
     SUM(daily_revenue) AS revenue,
     SUM(purchase_cost) AS purchase_cost,
     SUM(margin) AS margin,
     SUM(shipping_fee) AS shipping_fee,
     SUM(log_cost) AS log_cost,
     SUM(ship_cost) AS ship_cost,
 FROM {{ ref('int_campaigns_day') }}
 FULL OUTER JOIN {{ ref('finance_days') }}
     USING (date_date)
 GROUP BY datemonth
 ORDER BY datemonth desc