--- USE POSTGRESQL TOOLS
--- OS SERVER USING DEBIAN

--- DATABASE VARIABLE
DB_NAME="customize with your db"
DB_USER="customize with your user"


--- CREATE TEMPORARY TABLE FOR STORING SUMMARY OF ROMBS
CREATE TABLE IF NOT EXIST summary_rombs (
    outlet_code VARCHAR(10),
    city_name VARCHAR(100),
    outlet_name VARCHAR(100),
    region VARCHAR(50),
    status_manajemen VARCHAR(50),
    rom VARCHAR(100),
    am VARCHAR(100),
    omset_monthly_bulan_lalu NUMERIC(20,4),
    omset_daily_bulan_lalu NUMERIC(20,4),
    omset_monthly_bulan_ini NUMERIC(20,4),
    omset_daily_bulan_ini NUMERIC(20,4),
    prediksi_omset_monthly_bulan_ini NUMERIC(20,4),
    growth_omset_today NUMERIC(20,4),
    growth_omset_h_8 NUMERIC(20,4),
    growth_omset_h_7 NUMERIC(20,4),
    growth_omset_h_6 NUMERIC(20,4),
    growth_omset_h_5 NUMERIC(20,4),
    growth_omset_h_4 NUMERIC(20,4),
    growth_omset_h_3 NUMERIC(20,4),
    growth_omset_h_2 NUMERIC(20,4),
    omset_today NUMERIC(20,4),
    margin NUMERIC(20,4),
    stock_value_today NUMERIC(20,4),
    stock_sku_today NUMERIC(20,4),
    target_bep NUMERIC(20,4),
    bep_bulan_lalu VARCHAR(50),
    bep_bulan_ini VARCHAR(50),
    invoice_today NUMERIC(20,4),
    acb_today NUMERIC(20,4),
    target_ls NUMERIC(20,4),
    linkselling_lm NUMERIC(20,4),
    linkselling_now NUMERIC(20,4),
    proyeksi_ls NUMERIC(20,4),
    acv_ls NUMERIC(20,4),
    target_trans_kp NUMERIC(20,4),
    trans_kp_lm NUMERIC(20,4),
    trans_kp_now NUMERIC(20,4),
    proyeksi_kp NUMERIC(20,4),
    acv_kp NUMERIC(20,4),
    target_repurchase NUMERIC(20,4),
    repurchase_lm NUMERIC(20,4),
    repurchase_now NUMERIC(20,4),
    proyeksi_repurchase NUMERIC(20,4),
    acv_repurchase NUMERIC(20,4),
    target_new_member NUMERIC(20,4),
    new_member_last_month NUMERIC(20,4),
    new_member_now NUMERIC(20,4),
    proyeksi_new_member NUMERIC(20,4),
    acv_new_member NUMERIC(20,4),
    target_dso NUMERIC(20,4),
    dso_lm NUMERIC(20,4),
    dso_now NUMERIC(20,4),
    proyeksi_dso NUMERIC(20,4),
    acv_dso NUMERIC(20,4),
    nominal_dso_lm NUMERIC(20,4),
    nominal_dso_now NUMERIC(20,4),
    proyeksi_nominal_dso NUMERIC(20,4),
    target_susu NUMERIC(20,4),
    susu_lm NUMERIC(20,4),
    susu_now NUMERIC(20,4),
    proyeksi_susu NUMERIC(20,4),
    acv_susu NUMERIC(20,4),
    target_alkes NUMERIC(20,4),
    alkes_lm NUMERIC(20,4),
    alkes_now NUMERIC(20,4),
    proyeksi_alkes NUMERIC(20,4),
    acv_alkes NUMERIC(20,4),
    target_belanja_mingguan NUMERIC(20,4),
    pembelian_1 NUMERIC(20,4),
    pembelian_2 NUMERIC(20,4),
    pembelian_3 NUMERIC(20,4),
    pembelian_4 NUMERIC(20,4)
);


--- QUERY FOR COLLECTING AND ANALYZING ROMBS DATA
WITH
  outlet AS (
    SELECT mo.outlet_code, mo.city_name, mo.outlet_name, mo.region, mo.status_manajemen, rom, am, mo.soft_ops_date
    FROM master_outlet mo
    INNER JOIN mapping_outlet_am m ON mo.outlet_code = m.outlet_code
    WHERE status_operation = 'ACTIVE' AND mo.outlet_code NOT LIKE '4%'
  ),
  omset AS (
    SELECT coalesce(kode_outlet_baru, outlet_code) as outlet_code, omset_monthly_bulan_lalu, omset_daily_bulan_lalu, omset_monthly_bulan_ini, omset_daily_bulan_ini, omset_daily_bulan_ini*(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day)) as prediksi_omset_monthly_bulan_ini, omset_today, round(case when gross_profit_riil_today = 0 and revenue_non_internal_today = 0 then 0 when revenue_non_internal_today = 0 then 0 else gross_profit_riil_today/revenue_non_internal_today end*100,2) as margin, stock_value_today, stock_sku_today, invoice_today, acb_today, round((1.80*omset_monthly_bulan_ini)/cast(4 as numeric)) as target_belanja_mingguan, pembelian_1, pembelian_2, pembelian_3, pembelian_4
    FROM rombs_raw rr
    LEFT JOIn cv_to_pt c ON rr.outlet_code = c.kode_outlet_lama
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
    FROM history_omset ho
    INNER JOIN rombs_raw rr ON ho.outlet_code = rr.outlet_code
    GROUP BY 1
  ),
  target_bep AS (
    SELECT coalesce(kode_outlet_baru, outlet_code) as outlet_code, max(bep) as target_bep
    FROM target_bep_rombs t
    LEFT JOIN cv_to_pt c ON t.outlet_code = c.kode_outlet_lama
    GROUP BY 1
  ),
  ls AS (
    SELECT outlet_code, target_ls, linkselling_lm, linkselling_now,
      round((linkselling_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric),2) as proyeksi_ls,
      round(case when linkselling_now = 0 and target_ls = 0 then 0 when target_ls = 0 then 0 else linkselling_now/target_ls end*100,2) as acv_ls
    FROM rombs_linkselling
  ),
  kp AS (
    SELECT csv_outlet_id as outlet_code, target_trans_kp, trans_kp_lm, trans_kp_now,
      round((trans_kp_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_kp,
      round(case when trans_kp_now = 0 and target_trans_kp = 0 then 0 when target_trans_kp = 0 then 0 else trans_kp_now/target_trans_kp end*100,2) as acv_kp,
      te.repurchase as target_repurchase, repurchase_lm, repurchase_now,
      round((repurchase_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_repurchase,
      round(case when repurchase_now = 0 and te.repurchase = 0 then 0 when te.repurchase = 0 then 0 else repurchase_now/te.repurchase end*100,2) as acv_repurchase
    FROM kitapeduli kp
    LEFT JOIN (select coalesce(kode_outlet_baru, kode) as kode, repurchase from target_email te left join cv_to_pt c on te.kode = c.kode_outlet_lama) as te
    ON kp.csv_outlet_id = te.kode
  ),
  nm AS (
    SELECT outlet_code, new_member as target_new_member, new_member_last_month, new_member_now, round((new_member_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_new_member, round(case when new_member_now = 0 and new_member = 0 then 0 when new_member = 0 then 0 else new_member_now/new_member end*100,2) as acv_new_member
    FROM new_member nm
    LEFT JOIN (select coalesce(kode_outlet_baru, kode) as kode, new_member from target_email te left join cv_to_pt c on te.kode = c.kode_outlet_lama) as tnm
    ON nm.outlet_code = tnm.kode
  ),
  dso AS (
    SELECT location_id, target_dso, dso_lm, dso_now, round((dso_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_dso, round(case when dso_now = 0 and target_dso = 0 then 0 when target_dso = 0 then 0 else dso_now/target_dso end*100,2) as acv_dso, nominal_dso_lm, nominal_dso_now, round((nominal_dso_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_nominal_dso
    FROM rombs_dso
  ),
  sa AS (
    SELECT location_id, target_susu, susu_lm, susu_now, round((susu_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_susu, round(case when susu_now = 0 and target_susu = 0 then 0 when target_susu = 0 then 0 else susu_now/target_susu end*100,2) as acv_susu, target_alkes, alkes_lm, alkes_now, round((alkes_now/cast((extract(day from current_date-1)) as numeric))*cast(extract(day from date_trunc('month', now()-interval '1' day)+interval '1' month-interval '1' day) as numeric)) as proyeksi_alkes, round(case when alkes_now = 0 and target_alkes = 0 then 0 when target_alkes = 0 then 0 else alkes_now/target_alkes end*100,2) as acv_alkes
    FROM susu_alkes
  )
INSERT INTO summary_rombs SELECT o.outlet_code, o.city_name, outlet_name, region, status_manajemen, rom, am, omset_monthly_bulan_lalu, omset_daily_bulan_lalu, omset_monthly_bulan_ini, omset_daily_bulan_ini, prediksi_omset_monthly_bulan_ini, growth_omset_today, growth_omset_h_8, growth_omset_h_7, growth_omset_h_6, growth_omset_h_5, growth_omset_h_4, growth_omset_h_3, growth_omset_h_2, omset_today, margin, stock_value_today, stock_sku_today, target_bep, case when omset_monthly_bulan_lalu < target_bep then 'NO BEP' else 'BEP' end as bep_bulan_lalu, case when omset_monthly_bulan_ini < target_bep then 'NO BEP' else 'BEP' end as bep_bulan_ini, invoice_today, acb_today, target_ls, linkselling_lm, linkselling_now, proyeksi_ls, acv_ls, target_trans_kp, trans_kp_lm, trans_kp_now, proyeksi_kp, acv_kp, target_repurchase, repurchase_lm, repurchase_now, proyeksi_repurchase, acv_repurchase, target_new_member, new_member_last_month, new_member_now, proyeksi_new_member, acv_new_member, target_dso, dso_lm, dso_now, proyeksi_dso, acv_dso, nominal_dso_lm, nominal_dso_now, proyeksi_nominal_dso, target_susu, susu_lm, susu_now, proyeksi_susu, acv_susu, target_alkes, alkes_lm, alkes_now, proyeksi_alkes, acv_alkes, target_belanja_mingguan, pembelian_1, pembelian_2, pembelian_3, pembelian_4
FROM outlet o
LEFT JOIN oms ON o.outlet_code = oms.outlet_code
LEFT JOIN growth_7week g ON o.outlet_code = g.outlet_code
LEFT JOIN target_bep tb ON o.outlet_code = tb.outlet_code
LEFT JOIN ls ON o.outlet_code = ls.outlet_code
LEFT JOIN kp ON o.outlet_code = kp.outlet_code
LEFT JOIN nm ON o.outlet_code = nm.outlet_code
LEFT JOIN dso ON o.outlet_code = dso.location_id
LEFT JOIN sa ON o.outlet_code = sa.location_id;