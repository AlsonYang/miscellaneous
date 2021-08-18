from src.utils.get_max_snapshot import get_bzk_max_snapshot
from src.utils import udf
import config
import src.utils.hdfs_functions as hf
import os


#
MODEL_BASE_TABLE = config.bazooka_data_build.get('MODEL_BASE_TABLE')
DIR_COVID_DATA = config.bazooka_data_build.get('DIR_COVID_DATA')
DIR_COVID_PICKLE_BAU = config.project.get('DIR_COVID_PICKLE')
DIR_COVID_PICKLE_BAZOOKA = config.bazooka.get('DIR_COVID_PICKLE_BAZOOKA')
DIR_COVID_EXTERNAL_BAZOOKA = config.bazooka.get('DIR_COVID_EXTERNAL_BAZOOKA')
DATASETO = config.bazooka_data_build.get('DATASETO')
PREFO = config.bazooka_data_build.get('PREFO')



# hardcoded variables
survial_pickle_name = 'COVID_IOD_FRCST_MONTHS_TO_ZERO_LQDTY_comparison_JK_ReferenceMonth202008_with_JuneData.pkl'
runway_pickle_name = 'df_with_fcst_mths_to_zero_liqiudity_and_EOM_available_funds.pkl'
jk_pickle_name = 'JK_matrix.pkl'
risk_csv_name = 'SBB_LENDING_20200707_Existing.csv'
cluster_csv_name = '20200625_clg_clusters.csv'
product_holding_csv_name = 'dl_temp_bcaa_prod_holding_data.csv'
ofi_csv_name = 'OFI_LEADS_202007.csv'
final_filname = 'cluster_analysis_df.pkl'

#====final columns required====
columns_required = ['flag_risk', 'flag_survivability', 'flag_cluster', 'bus_cust_no', 'state', 'postcode', 
                    'security_indicator', 'segment', 'overall_industry_impact', 'anzsic_lvl4_code', 
                    '1st_Level_ANZSIC_Description', 'runway', 'rebound_segment', 'RISK_COHORT', 'CCR_Cd', 'OFI', 'tenure', 
                    'Product_holding']
# flag_risk, RISK_COHORT: r
# flag_survivability, overall_industry_impact(impacted_industry): s
# flag_cluster, rebound_segment(cluster_label,NEW_LABEL) : c
# 'state', 'postcode', 'security_indicator'(sec_ind), 'segment' , anzsic_lvl4_code(imputed_anzsic), anzsic_lvl1_description: a
# runway: bzk_final_table


#=======main table======
spark = udf.init_pyspark_()
ref_month = get_bzk_max_snapshot(spark)
main_pdf = spark.sql(f'''
SELECT bus_cust_no, state, postcode, sec_ind AS security_indicator, sgmt_curr AS segment, imputed_anzsic AS anzsic_lvl4_code,
  1st_Level_ANZSIC_Description, 2nd_Level_ANZSIC_Description, 3rd_Level_ANZSIC_Description, 4th_Level_ANZSIC_Description, cust_mth_on_book AS tenure
FROM business_owner.covid_clg_biz_num_with_firmography_and_od_utilisation_transactional
WHERE snapshot = {ref_month}
''').toPandas()

#=======risk table======
risk_pdf = hf.hdfs_csv_to_pandas_df(os.path.join(DIR_COVID_EXTERNAL_BAZOOKA, risk_csv_name))
risk_pdf.rename(columns = {'clg_id2':'bus_cust_no', 'RISK_COHORT':'risk_cohort'},inplace=True)
risk_pdf['bus_cust_no'] = risk_pdf['bus_cust_no'].astype(str)
risk_pdf['flag_risk'] = 1


#=======cluster table======
cluster_pdf = hf.hdfs_csv_to_pandas_df(os.path.join(DIR_COVID_EXTERNAL_BAZOOKA, cluster_csv_name))
cluster_pdf.columns = [col.lower() for col in cluster_pdf.columns]
cluster_pdf = cluster_pdf[['bus_cust_no','new_label']]
cluster_pdf.rename(columns={"new_label": "rebound_segment"},inplace = True)
cluster_pdf['flag_cluster'] = 1
cluster_pdf['bus_cust_no'] = cluster_pdf['bus_cust_no'].astype('str')

#=======survivial table from BAU======
survival_pdf = hf.hdfs_pickle_to_pandas_df(os.path.join(DIR_COVID_PICKLE_BAU,survial_pickle_name))
survival_pdf = survival_pdf[['eligible_clg','impacted_industry']]
survival_pdf.rename(columns = {'eligible_clg':'bus_cust_no'},inplace=True)
survival_pdf['overall_industry_impact'] = survival_pdf['impacted_industry'].str.extract(r'(Low|High|Medium)')
survival_pdf.drop(columns = ['impacted_industry'], inplace = True)
survival_pdf['flag_survivability'] = 1

#===runway table from bazooka
runway_pdf = hf.hdfs_pickle_to_pandas_df(os.path.join(DIR_COVID_PICKLE_BAZOOKA, runway_pickle_name))
runway_pdf = runway_pdf[['bus_cust_no','Forecasted Months to Zero Liquidity']]
runway_pdf.rename(columns={'Forecasted Months to Zero Liquidity':'runway'}, inplace=True)
runway_pdf['flag_runway'] = 1


#====product holding sql - not complete, need to convert from David's R code=======
#bcaa_acct_dtl = '''
#SELECT BUS_CUST_NO,
#        ACCT_CURR_IND_CD
#        ACCT_CO_ID
#        PRODUCT_CODE
#        SUB_PRODUCT_CODE
#        HAS_OD_FACILITY_IND_CD
#        MERCHANT_IND
#FROM business_owner_curated.bcaa_acct_depth
#WHERE ACCT_CURR_IND_CD = 'Y' AND snapshot = (SELECT max(snapshot) FROM business_owner_curated.bcaa_acct_depth)
#'''
#
#product_reference_df_raw = '''
#SELECT * 
#FROM business_owner_curated.bcaa_product_reference
#WHERE end_date = (SELECT max(end_date) FROM business_owner_curated.bcaa_product_reference)
#'''
#
#product_reference_df = '''
#SELECT DISTINCT co_id ACCT_SOURCE_SYSTEM, pri_tp_cd PRODUCT_CODE, prd_sub_tp_cd SUB_PRODUCT_CODE, low_lvl_prd_func_nm
#FROM product_reference_df_raw
#'''
#
## Confirm the logic of this table
#clg_od_merchant_df = '''
#SELECT IF(SUM(IF(HAS_OD_FACILITY_IND_CD = 'Y',1,0)))>1,1,0) HAS_OD, IF(SUM(MERCHANT_IND)>1,1,0) HAS_MERCHANT_IND
#FROM bcaa_acct_dtl
#GROUP BY bus_cust_no'''
#
## What is the joining key for bcaa_acct_dtl and product_reference_df --> product_code and sub_product_code
## Need to confirm: Do complete and spread here try to do the same thing - fillna for HAS_OD, HAS_MERCHANT_IND, low_lvl_prd_func_nm and HAS_PROD with False --> spread is doing one hot encoding
## bcaa_acct_dtl doesnt have HAS_OD and HAS_MERCHANT_IND, while clg_od_merchant_df has as you created above. Should I pick it up from clg_od_merchant_df instead? Then in the last table, you joined clg_od_merchant_df again? --> 
## Need to confirm: BUS_CUST_NO %in% current_valid_clg_df$BUS_CUST_NO --> this is for your usecase to filter out clgs, we dont need to apply right? If not, how can I access to that list? --> yeah dont need this
#clg_prod_holding_df = '''
#SELECT t1.*, t2.low_lvl_prd_func_nm, True AS HAS_PROD
#FROM bcaa_acct_dtl t1
#LEFT JOIN product_reference_df t2
#    ON t1.PRODUCT_CODE = t2.PRODUCT_CODE AND t1.SUB_PRODUCT_CODE = t2.SUB_PRODUCT_CODE
#'''
#
#clg_prod_holding_df = '''
#SELECT t1.*, t2.*
#FROM clg_prod_holding_df 
#    LEFT JOIN clg_od_merchant_df
#        ON t1.bus_cust_no = t2.bus_cust_no
#'''

product_holding_pdf = hf.hdfs_csv_to_pandas_df(os.path.join(DIR_COVID_EXTERNAL_BAZOOKA, product_holding_csv_name))
product_holding_pdf.rename(columns = {x: x.lower() for x in product_holding_pdf.columns}, inplace = True)
non_prod_cols = ['bus_cust_no']
product_holding_mapping = {col: f'prod_{col}' for col in product_holding_pdf.columns if col not in non_prod_cols}
product_holding_pdf.rename(columns = product_holding_mapping, inplace = True)
product_holding_pdf['flag_prod'] = 1
product_holding_pdf['bus_cust_no'] = product_holding_pdf['bus_cust_no'].astype(str)

#=======CCR==========
#park it for now, it involves table from teradata
ccr_pdf = spark.table('business_owner.covid_bazooka_ccr').toPandas()
ccr_pdf.rename(columns = {x: x.lower() for x in ccr_pdf.columns}, inplace = True)
ccr_pdf['bus_cust_no'] = ccr_pdf['customer_id'].apply(lambda x:x.lstrip('0'))
ccr_pdf.drop(columns = ['customer_id'],inplace = True)
ccr_pdf['flag_ccr'] = 1

#=======OFI=========
#park it for now, it involves dive into the csv to find useful information
# EST_DEBT_OFI_TL_ALL – estimated total debt of all OFI term loans
# EST_DEBT_OFI_CF_ALL, - CF = car finance?
# EST_DEBT_OFI_CC_ALL, - credit cards
# EST_DEBT_OFI_HL_ALL, - home loans
# EST_DEBT_OFI_PL_ALL, - personal loans
# AVG_SETTLEMENT_OFI_MERC_all, -average size of settlements on merchant facilities?
# L12M_SETTLEMENT_OFI_MERC_ALL -total sum of settlements on merchants over 12m
ofi_pdf = hf.hdfs_csv_to_pandas_df(os.path.join(DIR_COVID_EXTERNAL_BAZOOKA, ofi_csv_name))
ofi_pdf = ofi_pdf[['clg_id','EST_DEBT_OFI_TL_ALL','EST_DEBT_OFI_CF_ALL','EST_DEBT_OFI_CC_ALL','EST_DEBT_OFI_HL_ALL','EST_DEBT_OFI_PL_ALL','AVG_SETTLEMENT_OFI_MERC_all','L12M_SETTLEMENT_OFI_MERC_ALL','TOTAL_OFI']]
ofi_pdf.rename(columns = {x: f'ofi_{x.lower()}' for x in ofi_pdf.columns if x != 'clg_id'}, inplace = True)
ofi_pdf.rename(columns = {'clg_id':'bus_cust_no'}, inplace = True)
ofi_pdf['bus_cust_no'] = ofi_pdf['bus_cust_no'].astype(str)
ofi_pdf['flag_ofi'] = 1

#=====job keeper======
jk_pdf = hf.hdfs_pickle_to_pandas_df(os.path.join(DIR_COVID_PICKLE_BAU,jk_pickle_name))
jk_pdf = jk_pdf[['bus_cust_no']]
jk_pdf['flag_jobkeeper'] = 1
#
#jk_pdf['flag_jobkeeper_v_2_1']=False
#jk_pdf.loc[jk_pdf.eligibility_flag_JK2A>0.0,'flag_jobkeeper_v_2_1']=True
#jk_pdf['flag_jobkeeper_v_2_2']=False
#jk_pdf.loc[jk_pdf.eligibility_flag_JK2B>0.0,'flag_jobkeeper_v_2_2']=True

#====relief package===
package_pdf = spark.sql('''
SELECT DISTINCT eligible_clg
FROM business_owner.sv_covid_relief_master
WHERE assistant_category = "Relief Package"
AND derived_bus_cust_acct_rlshp_cd_src = "CLG"
''').toPandas()
package_pdf["flag_package"] = 1
package_pdf.rename(columns = {'eligible_clg':'bus_cust_no'},inplace=True)


#=====TBL from EPS====

tbl_pdf = spark.sql('''
select t2.bus_cust_no, SUM(t1.lending_fum_eop_mtd) lending_fum_eop_mtd, SUM(t1.deposit_fum_eop_mtd) deposit_fum_eop_mtd
from business_owner_curated.eps_datamart t1
LEFT JOIN business_owner_curated.bcaa_acct_depth t2
  ON t1.acct_num = t2.ACCT_NO AND t1.snapshot = t2.snapshot
WHERE t2.acct_curr_ind_cd == "Y"
GROUP BY t2.bus_cust_no
''').toPandas()

#======final_df======
final_pdf = main_pdf.merge(risk_pdf, on='bus_cust_no', how='left')\
                  .merge(cluster_pdf, on='bus_cust_no', how='left')\
                  .merge(survival_pdf, on='bus_cust_no', how='left')\
                  .merge(runway_pdf, on='bus_cust_no', how='left')\
                  .merge(product_holding_pdf, on='bus_cust_no', how='left')\
                  .merge(ccr_pdf, on='bus_cust_no', how='left')\
                  .merge(ofi_pdf, on='bus_cust_no', how='left')\
                  .merge(package_pdf, on='bus_cust_no',how='left')\
                  .merge(jk_pdf, on='bus_cust_no', how='left')\
                  .merge(tbl_pdf, on='bus_cust_no', how='left')
            
final_pdf.fillna({x:0 for x in final_pdf.columns if x.startswith('flag_')},inplace = True)

final_pdf.isnull().sum()


#======save final_pdf on hdfs======
hf.pandas_df_to_hdfs_pickle(final_pdf, final_filname, dir_hdfs=DIR_COVID_PICKLE_BAZOOKA)

#====read final_pdf from hdfs======
final_pdf = hf.hdfs_pickle_to_pandas_df(os.path.join(DIR_COVID_PICKLE_BAZOOKA, final_filname))


print(f'these required columns are missing:\n --> {(set([x.lower() for x in columns_required]) - set([x.lower() for x in final_pdf.columns]))}')
