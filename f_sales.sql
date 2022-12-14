INSERT INTO mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
SELECT 
    dc.date_id,
    item_id,
    customer_id,
    city_id,
    quantity,
    CASE 
        WHEN status = 'refunded'  THEN payment_amount * (-1)
        ELSE payment_amount
    END AS payment_amount, 
    status
FROM staging.user_order_log uol
LEFT JOIN mart.d_calendar AS dc ON uol.date_time::Date = dc.date_actual
WHERE uol.date_time::Date = '{{ds}}';
