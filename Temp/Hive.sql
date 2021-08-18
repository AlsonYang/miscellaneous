SELECT a.bus_cust_no, a.snapshot, b.snapshot as historical_snapshot, b.total_rev
FROM business_owner.covid_bazooka_train_other_exp_and_tot_rev_{max_bzk_snapshot} a LEFT JOIN
  business_owner.covid_bazooka_train_other_exp_and_tot_rev_{max_bzk_snapshot} b
    ON a.bus_cust_no = b.bus_cust_no 
      AND (b.snapshot between a.snapshot AND select date_format(ADD_MONTHS(TO_DATE(a.snapshot,'yyyyMM'), -11),'yyyyMM'))
WHERE snapshot between '201901' AND '202010'