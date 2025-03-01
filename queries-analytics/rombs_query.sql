--- USE POSTGRESQL TOOLS
--- OS SERVER USING DEBIAN


--- QUERY FOR COLLECTING AND ANALYZING ROMBS DATA
WITH
  otl AS (
    SELECT mo.outlet_code, mo.city_name, mo.outlet_name, mo.region, mo.status_manajemen, rom, am, mo.soft_ops_date
    FROM table1 mo
    INNER JOIN table2 m ON mo.outlet_code = m.outlet_code
    WHERE status_operation = 'ACTIVE' AND mo.outlet_code NOT LIKE '4%'
  ),
  oms AS (
    SELECT coalesce(kode_outlet_baru, outlet_code) as outlet_code, omset_monthly_bulan_lalu, omset_daily_bulan_lalu, omset_monthly_bulan_ini, omset_daily_bulan_ini, omset_daily_bulan_ini*(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day)) as prediksi_omset_monthly_bulan_ini, omset_today, round(case when gross_profit_riil_today = 0 and revenue_non_internal_today = 0 then 0 when revenue_non_internal_today = 0 then 0 else gross_profit_riil_today/revenue_non_internal_today end*100,2) as margin, stock_value_today, stock_sku_today, invoice_today, acb_today, round((1.80*omset_monthly_bulan_ini)/cast(4 as numeric)) as target_belanja_mingguan, pembelian_1, pembelian_2, pembelian_3, pembelian_4
    FROM table3 rr
    LEFT JOIN table17 c ON rr.outlet_code = c.kode_outlet_lama
  ),
  growth_7week AS (
    SELECT ho.outlet_code,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-1 then ((average_omset_daily_as_of_date/omset_daily_bulan_lalu)-1)*100 end),2) as growth_omset_today,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-8 then ((average_omset_daily_as_of_date/omset_daily_bulan_lalu)-1)*100 end),2) as growth_omset_h_8,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-7 then ((average_omset_daily_as_of_date/omset_daily_bulan_lalu)-1)*100 end),2) as growth_omset_h_7,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-6 then ((average_omset_daily_as_of_date/omset_daily_bulan_lalu)-1)*100 end),2) as growth_omset_h_6,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-5 then ((average_omset_daily_as_of_date/omset_daily_bulan_lalu)-1)*100 end),2) as growth_omset_h_5,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-4 then ((average_omset_daily_as_of_date/omset_daily_bulan_lalu)-1)*100 end),2) as growth_omset_h_4,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-3 then ((average_omset_daily_as_of_date/omset_daily_bulan_lalu)-1)*100 end),2) as growth_omset_h_3,
      round(sum(case when cast(TO_CHAR(TO_DATE(dt::text, 'YYYYMMDD'), 'YYYY-MM-DD') as date) = current_date-2 then ((average_omset_daily_as_of_date/omset_daily_bulan_lalu)-1)*100 end),2) as growth_omset_h_2
    FROM table4 ho
    INNER JOIN table5 rr ON ho.outlet_code = rr.outlet_code
    GROUP BY 1
  ),
  target_bep AS (
    SELECT coalesce(kode_outlet_baru, outlet_code) as outlet_code, max(bep) as target_bep
    FROM table6 t
    LEFT JOIN table7 c ON t.outlet_code = c.kode_outlet_lama
    GROUP BY 1
  ),
  p1 AS (
    SELECT outlet_code, target_program1, program1_lastmonth, program1_now,
      round((program1_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric),2) as proyeksi_program1,
      round(case when program1_now = 0 and target_program1 = 0 then 0 when target_program1 = 0 then 0 else program1_now/target_program1 end*100,2) as acv_program1
    FROM table8
  ),
  p2 AS (
    SELECT csv_outlet_id as outlet_code, target_trans_program2, trans_program2_lastmonth, trans_program2_now,
      round((trans_program2_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_program2,
      round(case when trans_program2_now = 0 and target_trans_program2 = 0 then 0 when target_trans_program2 = 0 then 0 else trans_program2_now/target_trans_program2 end*100,2) as acv_program2,
      te.repurchase as target_repurchase, repurchase_lastmonth, repurchase_now,
      round((repurchase_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_repurchase,
      round(case when repurchase_now = 0 and te.repurchase = 0 then 0 when te.repurchase = 0 then 0 else repurchase_now/te.repurchase end*100,2) as acv_repurchase
    FROM table9 kp
    LEFT JOIN (select coalesce(kode_outlet_baru, kode) as kode, repurchase from table10 te left join table11 c on te.kode = c.kode_outlet_lama) as te
    ON kp.csv_outlet_id = te.kode
  ),
  p3 AS (
    SELECT outlet_code, new_member as target_program3, program3_lastmonth, program3_now, round((program3_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_program3, round(case when program3_now = 0 and new_member = 0 then 0 when new_member = 0 then 0 else program3_now/new_member end*100,2) as acv_program3
    FROM table12 nm
    LEFT JOIN (select coalesce(kode_outlet_baru, kode) as kode, new_member from table13 te left join table14 c on te.kode = c.kode_outlet_lama) as tnm
    ON nm.outlet_code = tnm.kode
  ),
  p4 AS (
    SELECT location_id, target_program4, program4_lastmonth, program4_now, round((program4_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_program4, round(case when program4_now = 0 and target_program4 = 0 then 0 when target_program4 = 0 then 0 else program4_now/target_program4 end*100,2) as acv_program4, nominal_program4_lastmonth, nominal_program4_now, round((nominal_program4_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_nominal_program4
    FROM table15
  ),
  p5 AS (
    SELECT location_id, target_program5, program5_lastmonth, program5_now, round((program5_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_program5, round(case when program5_now = 0 and target_program5 = 0 then 0 when target_program5 = 0 then 0 else program5_now/target_program5 end*100,2) as acv_program5, target_program6, program6_lastmonth, program6_now, round((program6_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_program6, round(case when program6_now = 0 and target_program6 = 0 then 0 when target_program6 = 0 then 0 else program6_now/target_program6 end*100,2) as acv_program6
    FROM table16
  )
SELECT o.outlet_code, o.city_name, outlet_name, region, status_manajemen, rom, am, omset_monthly_bulan_lalu, omset_daily_bulan_lalu, omset_monthly_bulan_ini, omset_daily_bulan_ini, prediksi_omset_monthly_bulan_ini, growth_omset_today, growth_omset_h_8, growth_omset_h_7, growth_omset_h_6, growth_omset_h_5, growth_omset_h_4, growth_omset_h_3, growth_omset_h_2, omset_today, margin, stock_value_today, stock_sku_today, target_bep, case when omset_monthly_bulan_lalu < target_bep then 'NO BEP' else 'BEP' end as bep_bulan_lalu, case when omset_monthly_bulan_ini < target_bep then 'NO BEP' else 'BEP' end as bep_bulan_ini, invoice_today, acb_today, target_program1, program1_lastmonth, program1_now, proyeksi_program1, acv_program1, target_trans_program2, trans_program2_lastmonth, trans_program2_now, proyeksi_program2, acv_program2, target_repurchase, repurchase_lastmonth, repurchase_now, proyeksi_repurchase, acv_repurchase, target_program3, program3_lastmonth, program3_now, proyeksi_program3, acv_program3, target_program4, program4_lastmonth, program4_now, proyeksi_program4, acv_program4, nominal_program4_lastmonth, nominal_program4_now, proyeksi_nominal_program4, target_program5, program5_lastmonth, program5_now, proyeksi_program5, acv_program5, target_program6, program6_lastmonth, program6_now, proyeksi_program6, acv_program6, target_belanja_mingguan, pembelian_1, pembelian_2, pembelian_3, pembelian_4
FROM otl o
LEFT JOIN oms ON o.outlet_code = oms.outlet_code
LEFT JOIN growth_7week g ON o.outlet_code = g.outlet_code
LEFT JOIN target_bep tb ON o.outlet_code = tb.outlet_code
LEFT JOIN p1 ON o.outlet_code = p1.outlet_code
LEFT JOIN p2 ON o.outlet_code = p2.outlet_code
LEFT JOIN p3 ON o.outlet_code = p3.outlet_code
LEFT JOIN p4 ON o.outlet_code = p4.location_id
LEFT JOIN p5 ON o.outlet_code = p5.location_id;