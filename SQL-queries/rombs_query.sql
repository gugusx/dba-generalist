--- USE POSTGRESQL TOOLS
--- OS SERVER USING DEBIAN
--- REPLACE THE TABLES AND COLUMNS WITH YOUR OWN


--- QUERY FOR COLLECTING AND ANALYZING ROMBS DATA
WITH
  otl AS (
    SELECT mo.outlet_code, mo.city_name, mo.outlet_name, mo.region, mo.outlet_status, rom, am, mo.soft_ops_date
    FROM table1 mo
    INNER JOIN table2 m ON mo.outlet_code = m.outlet_code
    WHERE status_operation = 'ACTIVE' AND mo.outlet_code NOT LIKE '4%'
  ),
  oms AS (
    SELECT coalesce(kode_outlet_baru, outlet_code) as outlet_code, 
    monthly_revenue_last_month, daily_revenue_last_month, monthly_revenue_this_month, daily_revenue_this_month, 
    daily_revenue_this_month*(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day)) as prediction_of_monthly_revenue_this_month, revenue_today, 
    round(case when gross_profit_riil_today = 0 and revenue_non_internal_today = 0 then 0 when revenue_non_internal_today = 0 then 0 else gross_profit_riil_today/revenue_non_internal_today end*100,2) as margin, 
    stock_value_today, stock_sku_today, invoice_today, acb_today, 
    round((1.80*monthly_revenue_this_month)/cast(4 as numeric)) as weekly_purchase_target,
    purchase_1, purchase_2, purchase_3, purchase_4
    FROM table3 rr
    LEFT JOIN table17 c ON rr.outlet_code = c.kode_outlet_lama
  ),
  growth_7week AS (
    SELECT ho.outlet_code,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-1 then ((average_omset_daily_as_of_date/daily_revenue_last_month)-1)*100 end),2) as growth_of_revenue_today,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-8 then ((average_omset_daily_as_of_date/daily_revenue_last_month)-1)*100 end),2) as revenue_growth_of_d8,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-7 then ((average_omset_daily_as_of_date/daily_revenue_last_month)-1)*100 end),2) as revenue_growth_of_d7,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-6 then ((average_omset_daily_as_of_date/daily_revenue_last_month)-1)*100 end),2) as revenue_growth_of_d6,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-5 then ((average_omset_daily_as_of_date/daily_revenue_last_month)-1)*100 end),2) as revenue_growth_of_d5,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-4 then ((average_omset_daily_as_of_date/daily_revenue_last_month)-1)*100 end),2) as revenue_growth_of_d4,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-3 then ((average_omset_daily_as_of_date/daily_revenue_last_month)-1)*100 end),2) as revenue_growth_of_d3,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-2 then ((average_omset_daily_as_of_date/daily_revenue_last_month)-1)*100 end),2) as revenue_growth_of_d2
    FROM table4 ho
    INNER JOIN table5 rr ON ho.outlet_code = rr.outlet_code
    GROUP BY 1
  ),
  bep_target AS (
    SELECT coalesce(kode_outlet_baru, outlet_code) as outlet_code, max(bep) as bep_target
    FROM table6 t
    LEFT JOIN table7 c ON t.outlet_code = c.kode_outlet_lama
    GROUP BY 1
  ),
  p1 AS (
    SELECT outlet_code, program1_target, program1_lastmonth, program1_now,
      round((program1_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric),2) as projection_program1,
      round(case when program1_now = 0 and program1_target = 0 then 0 when program1_target = 0 then 0 else program1_now/program1_target end*100,2) as acv_program1
    FROM table8
  ),
  p2 AS (
    SELECT csv_outlet_id as outlet_code, program2_target_trans, trans_program2_lastmonth, trans_program2_now,
      round((trans_program2_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as projection_program2,
      round(case when trans_program2_now = 0 and program2_target_trans = 0 then 0 when program2_target_trans = 0 then 0 else trans_program2_now/program2_target_trans end*100,2) as acv_program2,
      te.repurchase as repurchase_target, repurchase_lastmonth, repurchase_now,
      round((repurchase_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as projection_repurchase,
      round(case when repurchase_now = 0 and te.repurchase = 0 then 0 when te.repurchase = 0 then 0 else repurchase_now/te.repurchase end*100,2) as acv_repurchase
    FROM table9 kp
    LEFT JOIN (select coalesce(kode_outlet_baru, kode) as kode, repurchase from table10 te left join table11 c on te.kode = c.kode_outlet_lama) as te
    ON kp.csv_outlet_id = te.kode
  ),
  p3 AS (
    SELECT outlet_code, new_member as target_program3, program3_lastmonth, program3_now,
    round((program3_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as projection_program3,
    round(case when program3_now = 0 and new_member = 0 then 0 when new_member = 0 then 0 else program3_now/new_member end*100,2) as acv_program3
    FROM table12 nm
    LEFT JOIN (select coalesce(kode_outlet_baru, kode) as kode, new_member from table13 te left join table14 c on te.kode = c.kode_outlet_lama) as tnm
    ON nm.outlet_code = tnm.kode
  ),
  p4 AS (
    SELECT location_id, program4_target, program4_lastmonth, program4_now,
    round((program4_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as projection_program4,
    round(case when program4_now = 0 and program4_target = 0 then 0 when program4_target = 0 then 0 else program4_now/program4_target end*100,2) as acv_program4,
    nominal_program4_lastmonth, nominal_program4_now,
    round((nominal_program4_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as projection_nominal_program4
    FROM table15
  ),
  p5 AS (
    SELECT location_id, program5_target, program5_lastmonth, program5_now,
    round((program5_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as projection_program5,
    round(case when program5_now = 0 and program5_target = 0 then 0 when program5_target = 0 then 0 else program5_now/program5_target end*100,2) as acv_program5,
    program6_target, program6_lastmonth, program6_now,
    round((program6_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as projection_program6,
    round(case when program6_now = 0 and program6_target = 0 then 0 when program6_target = 0 then 0 else program6_now/program6_target end*100,2) as acv_program6
    FROM table16
  )
SELECT o.outlet_code, o.city_name, outlet_name, region, outlet_status, rom, am, monthly_revenue_last_month, daily_revenue_last_month, monthly_revenue_this_month, daily_revenue_this_month, prediction_of_monthly_revenue_this_month, growth_of_revenue_today, revenue_growth_of_d8, revenue_growth_of_d7, revenue_growth_of_d6, revenue_growth_of_d5, revenue_growth_of_d4, revenue_growth_of_d3, revenue_growth_of_d2, revenue_today, margin, stock_value_today, stock_sku_today, bep_target, case when monthly_revenue_last_month < bep_target then 'NO BEP' else 'BEP' end as bep_lastmonth, case when monthly_revenue_this_month < bep_target then 'NO BEP' else 'BEP' end as bep_bulan_ini, invoice_today, acb_today, program1_target, program1_lastmonth, program1_now, projection_program1, acv_program1, program2_target_trans, trans_program2_lastmonth, trans_program2_now, projection_program2, acv_program2, repurchase_target, repurchase_lastmonth, repurchase_now, projection_repurchase, acv_repurchase, target_program3, program3_lastmonth, program3_now, projection_program3, acv_program3, program4_target, program4_lastmonth, program4_now, projection_program4, acv_program4, nominal_program4_lastmonth, nominal_program4_now, projection_nominal_program4, program5_target, program5_lastmonth, program5_now, projection_program5, acv_program5, program6_target, program6_lastmonth, program6_now, projection_program6, acv_program6, weekly_purchase_target, purchase_1, purchase_2, purchase_3, purchase_4
FROM otl o
LEFT JOIN oms ON o.outlet_code = oms.outlet_code
LEFT JOIN growth_7week g ON o.outlet_code = g.outlet_code
LEFT JOIN bep_target tb ON o.outlet_code = tb.outlet_code
LEFT JOIN p1 ON o.outlet_code = p1.outlet_code
LEFT JOIN p2 ON o.outlet_code = p2.outlet_code
LEFT JOIN p3 ON o.outlet_code = p3.outlet_code
LEFT JOIN p4 ON o.outlet_code = p4.location_id
LEFT JOIN p5 ON o.outlet_code = p5.location_id;