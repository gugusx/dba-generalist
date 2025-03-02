WITH recent_stock AS (
    SELECT product_code, max(transaction_date) as last_update
    FROM product_inventory
    GROUP BY 1
)
SELECT pi.product_code, product_name, qty_balance as stock, qty_balance*cost as value_stock, last_update
FROM product_inventory pi
INNER JOIN recent_stock rs
ON pi.product_code = rs.product_code
AND pi.transaction_date = rs.last_update
INNER JOIN product p
ON pi.product_code = p.product_code;