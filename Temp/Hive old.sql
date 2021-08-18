transactional_triex_data = '''
WITH triex AS (
SELECT tran_type, txn_amount, account_number, BUSINESS_CAMO_CLASS1,business_camo_class2,snapshot,
  INT(Concat(Split(eff_date,'-')[0],Split(eff_date,'-')[1])) eff_date_month
FROM business_owner.COVID_TRIEX_TXN_CAT
),
triex_clean_eff_date(
SELECT tran_type, txn_amount, account_number, BUSINESS_CAMO_CLASS1,business_camo_class2,
  if(eff_date_month > (SELECT max(snapshot) FROM triex) or eff_date_month < 200000 or eff_date_month IS NULL, snapshot, eff_date_month) snapshot
FROM triex
)
SELECT t1.*,
  t2.bus_cust_no
FROM triex_clean_eff_date t1 
  LEFT JOIN business_owner_curated.bcaa_acct_depth t2
    ON INT(t1.ACCOUNT_NUMBER) = INT(t2.acct_no) 
      AND t1.snapshot = t2.snapshot
WHERE t2.ACCT_CO_ID = 10 AND t1.snapshot >= 201811
'''

clg_triex_data = '''
SELECT t1.snapshot, t1.bus_cust_no
  ,sum(case when t2.tran_type = 'C' and t2.business_camo_class1 = 'DIVIDEND' then t2.txn_amount else 0 end) as b_c_dividend_l1_sum

FROM business_owner_curated.bcaa_group t1
  LEFT JOIN transactional_triex_data t2
    ON t1.snapshot = t2.snapshot AND t1.bus_cust_no = t2.bus_cust_no
WHERE t1.has_curr_bus_acct_ind_cd = 'Y' AND t1.indvdl_cust_ind_cd = 'N'
GROUP BY t1.snapshot, t1.bus_cust_no
'''