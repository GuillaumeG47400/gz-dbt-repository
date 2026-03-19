SELECT
om.orders_id
,om.date_date 
,ROUND((om.margin + sh.shipping_fee - sh.logcost - sh.ship_cost),2) AS operational_margin
,om.quantity
,om.revenue
,om.purchase_cost
,om.margin 
,sh.shipping_fee
,sh.logcost
,sh.ship_cost
FROM {{ ref('int_orders_margin') }} AS om  
    LEFT JOIN {{ ref('stg_gz_data__ship') }} AS sh 
        USING (orders_id)
ORDER BY orders_id DESC