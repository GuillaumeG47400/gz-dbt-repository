WITH combined_data AS  (
    SELECT 
        f.date_date AS date 
        ,f.operational_margin
        ,c.ads_cost
        ,c.impression AS ads_impression
        ,c.click AS ads_clicks

FROM {{ ref('int_campaigns_day') }} AS c 
INNER JOIN {{ ref('finance_days') }} AS f 
    ON c.date_date = f.date_date
)
SELECT 
    date
    ,ads_margin
    ,average_basket,
    operational_margin
    ,ads_cost
    ,ads_impression
    ,ads_clicks
    ,quantity
    ,revenue
    ,purchase_cost
    ,margin
    ,shipping_fee
    ,log_cost
    ,ship_cost


FROM (
    SELECT
        c.*
        ,c.operational_margin - c.ads_cost AS ads_margin
        ,f.quantity
        ,f.daily_revenue AS revenue
        ,f.purchase_cost
        ,f.margin
        ,f.shipping_fee
        ,f.log_cost
        ,f.ship_cost
        ,f.average_basket
    FROM combined_data AS c  
    JOIN {{ ref('finance_days') }} f
        ON date_date = date 
ORDER BY date_date DESC           
)


