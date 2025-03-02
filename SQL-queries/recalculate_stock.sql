--- RECALCULATE STOCK BASED ON SPECIFIC PRODUCT
UPDATE product_inventory pi
SET qty_balance = calc_stock, value_balance = calc_stock*cost
FROM (
    SELECT inventory_transaction_id, transaction_date, product_code, SUM(qty_changes) OVER (order by transaction_date) as calc_stock
    FROM product_inventory
    WHERE product_code = 'Product A'
) recalc_stock
WHERE pi.inventory_transaction_id = recalc_stock.inventory_transaction_id
AND pi.product_code = recalc_stock.product_code;


--- RECALCULATE STOCK OF ALL PRODUCT
UPDATE product_inventory pi
SET qty_balance = calc_stock, value_balance = calc_stock*cost
FROM (
    SELECT inventory_transaction_id, transaction_date, product_code, SUM(qty_changes) OVER (partition by product_code order by transaction_date) as calc_stock
    FROM product_inventory
) recalc_stock
WHERE pi.inventory_transaction_id = recalc_stock.inventory_transaction_id
AND pi.product_code = recalc_stock.product_code;
