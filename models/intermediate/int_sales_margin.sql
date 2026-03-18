SELECT 
products_id
,date_date 
,orders_id
,revenue
,quantity
,ROUND(s.quantity * p.purchase_price,2) AS purchase_cost 
,ROUND(s.revenue - s.quantity * p.purchase_price) AS margin 
FROM {{ ref('stg_gz_data__sales') }} AS s
    LEFT JOIN {{ ref('stg_gz_data__product') }} AS p 
        USING (products_id)

