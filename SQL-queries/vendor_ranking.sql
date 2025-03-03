WITH set_ranking AS (
    SELECT purchase_invoice_no, outlet_id, outlet_name, product_code, product_name, manufacturer, unit_code, unit_conversion, purchase_unit_id, unit_id, purchase_invoice_date, qty, price, discount_per_unit, vendor_name, row_number() over (partition by outlet_id, product_code order by purchase_invoice_date desc) as rn
    FROM 
    (
        SELECT purchase_invoice_no, coalesce(new_outlet_code, old_outlet_code) outlet_id, pi.outlet_name, pid.product_code, i.product_name, manufacturer, pid.unit_code, i.unit_conversion, purchase_unit_id, i.unit_id, purchase_invoice_date, qty, pid.price, discount_per_unit, vendor_name 
        FROM purchase_invoice pi 
        INNER JOIN purchase_invoice_detail pid 
        ON pi.purchase_invoice_id = pid.purchase_invoice_id 
        LEFT JOIN (select transaction_detail_id from purchase_return pr join purchase_return_detail prd ON pr.purchase_return_id = prd.purchase_return_id where status = 2) as pr 
        ON pid.purchase_invoice_detail_id = pr.transaction_detail_id
        LEFT JOIN product i ON pid.product_code = i.product_code 
        LEFT JOIN cv_ke_pt c ON pi.outlet_id = c.old_outlet_code 
        WHERE qty > 0 AND pr.transaction_detail_id is null
        AND year(purchase_invoice_date) >= year(now()-interval '1' year)
    ) as a
),
filter_rank_1 AS (
    SELECT outlet_id, outlet_name, date(purchase_invoice_date) tgl, purchase_invoice_no, product_code, product_name, manufacturer, a.unit_code, u.unit_code unit_code_konversi, qty total_qty, vendor_name, round(price,2) hna, cast(case when price = 0 then 0 else ((discount_per_unit/qty)/price)*100 end varchar) || '%' discount, round(discount_per_unit/qty,2) discount_amount, round(price-case when discount_per_unit = 0 AND qty = 0 then 0 when qty = 0 then 0 else (discount_per_unit/qty) end,2) hpp, case when  a.unit_code = u.unit_code then 1 else unit_conversion end unit_conversion, round(case when a.unit_code <> u.unit_code then (round(price-case when discount_per_unit = 0 AND qty = 0 then 0 when qty = 0 then 0 else (discount_per_unit/qty) end,2))*unit_conversion else round(price-case when discount_per_unit = 0 AND qty = 0 then 0 when qty = 0 then 0 else (discount_per_unit/qty) end,2) end,2) conversion_hpp 
    FROM set_ranking sr
    LEFT JOIN unit u on sr.purchase_unit_id = u.unit_id 
    WHERE rn = 1
)
SELECT b.outlet_id, b.outlet_name, hp.status outlet_tax_status, b.tgl purchase_date, purchase_invoice_no, product_code, product_name, unit_code purchase_unit, unit_code_konversi conversion_unit, manufacturer, total_qty, vendor_name, hna, round(cast((hna-hpp)/hna*100 as double),2) discount, discount_amount, hpp, unit_conversion, conversion_hpp, branch_code, CASE WHEN branch_name = 'Branch City A' THEN 'City A' WHEN branch_name='Branch City B' THEN 'City B' WHEN branch_name ='Branch City C' THEN 'City C' WHEN branch_name= 'Branch City D' THEN 'City D' WHEN branch_name='Branch City E' THEN 'City E' WHEN branch_name='Branch City F' THEN 'City F' END vendor_area, dense_rank() OVER (partition by branch_name, product_code ORDER BY conversion_hpp asc) rank, month(now()), year(now()) 
FROM filter_rank_1 b
INNER JOIN outlet_tax_status hp ON b.outlet_id = hp.company_code 
INNER JOIN (select coalesce(new_outlet_code, old_outlet_code) outlet_code, branch_code, branch_name from mapping_branch_outlet m left join cv_ke_pt c on m.outlet_code = c.old_outlet_code group by 1,2,3) hrk ON b.outlet_id = hrk.outlet_code 
WHERE hna > 0;



