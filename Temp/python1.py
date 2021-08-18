import os
import math
from datetime import datetime
import pandas as pd
import numpy as np


from src.modelling.helpers.class_helper import get_binary_from_prob
import src.utils.months_functions as mf


#=====common biz rules====
class StateVar:
    pass
class CommonBizRules:
    #=====parameters====
    CURR_SNAPSHOT = '202010' #TODO: pickup from config
    FIRST_FORECAST_MONTH = mf.add_months(CURR_SNAPSHOT,1)

    N_MONTH_FORWARD = 24
    SNAPSHOTS_TO_FORECAST = mf.get_months_with_duration(from_month=FIRST_FORECAST_MONTH,n=N_MONTH_FORWARD)
    ALL_SNAPSHOTS = [CURR_SNAPSHOT] + SNAPSHOTS_TO_FORECAST

    # assumptions
    INCOME_MINIMUM_THRESHOLD = 1000
    TAX_RATE = 0 # for HH_application_income
    # MONTHS_OF_NEG_FUM_TO_DELINQUENCY = 5

    ARREARS_MONTHS_TO_DELINQUENT = 3
    DELINQUENT_MONTHS_TO_EXIT = 10
    ARREARS_DAYS_THRESHOLD = 90


    JOBSEEKER_1P = 1549
    JOBSEEKER_GT_1P = 2863

    JOBSEEKER_END_SNAPSHOT = '202103'
    
    NEWSTART_1P = 559*26/12 #NEW6
    NEWSTART_GT_1P = 504.7*26/12*2 #NEW6

    DISCR_EXP_MULTIPLIER = 0
    DISCR_EXP_DEPOSIT_BAL_THRESHOLD = 20000
    NON_BASIC_EXP_MULTIPLIER = 0
    NON_BASIC_EXP_DEPOSIT_BAL_THRESHOLD = 20000

    CF_BUFFER_TO_ADJUST_IMPUTED_EXPENSE = 1
    CF_BUFFER_TO_ADJUST_IMPUTED_INCOME = 1
    MINIMUM_EXP_PP_TO_ADJUST_IMPUTED_EXPENSE = 1000

    #RENTAL_INCO_MULTIPLIER = 1
    #DIVIDENDS_INCO_MULTIPLIER = 1

    ATTRITION_PROB = 0.00371
    CPI_ASSUMPTION = 0.02
    
    #===columns=====
    ACCT_FIELDS = [
        'SNAPSHOT_DATE',
        'ACCOUNT_NUMBER',
        'CURR_BAL_AMT',
        'R_IDR',
        'A_REDRW',
        'A_OFFSET',
        'repayment_amount',
        'F_INT_ONLY',
        'A_ORIG_AMT_EMA_LIMIT',
        'HL_Open_Date', 
        'TOTAL_REVENUE',
        'A_LVR',
        'DVLR_MATCH',
        'ACCT_LINK_SEC_VAL',
        'ACCT_SECVAL_BAL_EXPSV'
    ]


    HH_FIELDS = [
        'retail_complete_ind',
        'TOT_SALARY_AMT_LATEST',
        'TOT_SALARY_AMT_PRECOVID',
        'RAW_TOT_SALARY_AMT_LATEST',
        'RAW_TOT_SALARY_AMT_PRECOVID',
        'R_C_SALARY_LATEST',
        'R_C_SALARY_PRECOVID',
        'TOT_PENSION_AMT_LATEST',
        'TOT_RENT_AMT_LATEST',
        'TOT_DIVIDEND_AMT_LATEST',
        'TOT_CHILD_AMT_LATEST',
        'TOT_OTHER_AMT_LATEST',
        'tot_abs_basic_exp',
        'tot_discr_basic_exp',
        'tot_non_basic_exp',
        'tot_fixed_exp',
        'HH_FINANCIAL_REPAYMENTS',
        'HH_DEPOSIT_FUM_BAL',
        'HH_LATEST_APP_DATE',
        'HH_LATEST_APP_NET_TOTAL_INCOME',
        'HH_LATEST_APP_ANZ_CASH',
        'HH_LATEST_APP_OFI_CASH',
        'HH_RELIEF_IND',
        'HH_RELIEF_DUE_DATE',
        'HH_TOTAL_LOAN_BAL',
        'HH_TOTAL_REDRAW',
        'HH_TOTAL_OFFSET',
        'HH_ARREARS_DAYS',
        'HH_ARREARS_AMOUNT'
    ]

    GENERAL_FIELDS = ['tot_individual', 'High_Level_Occu', 'State_Grouped']
    
    #===============static helpers================
    @staticmethod
    def compute_orig_house_price(A_ORIG_AMT_EMA_LIMIT, A_LVR): 
        return A_ORIG_AMT_EMA_LIMIT / A_LVR * 100

    def flag_current_relief(HH_RELIEF_IND, HH_RELIEF_DUE_DATE_modified):
        if HH_RELIEF_IND == 1:
            if CommonBizRules.CURR_SNAPSHOT <= HH_RELIEF_DUE_DATE_modified:
                return 1
            else:
                return 0
        else:
            return 0
    flag_current_relief = np.vectorize(flag_current_relief)
            
    ## imputed expense
    def flag_impute_expense(retail_complete_ind):
        return 0 if retail_complete_ind in ('CC','CE') else 1
    flag_impute_expense = np.vectorize(flag_impute_expense)

    @staticmethod
    def compute_tot_abs_basic_exp_imputed(tot_abs_basic_exp_pp, tot_individual):
        return tot_abs_basic_exp_pp * np.sqrt(tot_individual) #NEW

    @staticmethod
    def compute_tot_discr_basic_exp_imputed(tot_discr_basic_exp_pp, tot_individual):
        return tot_discr_basic_exp_pp * np.sqrt(tot_individual) #NEW

    @staticmethod
    def compute_tot_non_basic_exp_imputed(tot_non_basic_exp_pp, tot_individual):
        # return tot_non_basic_exp_pp * np.sqrt(tot_individual) #NEW
        return 0 #NEW

    @staticmethod
    def compute_tot_fixed_exp_imputed(tot_fixed_exp_pp, tot_individual):
        return tot_fixed_exp_pp * np.sqrt(tot_individual) #NEW
    
    @staticmethod
    def compute_tot_all_exp_imputed(tot_abs_basic_exp_imputed, tot_discr_basic_exp_imputed, tot_non_basic_exp_imputed, tot_fixed_exp_imputed): #NEW4
        return (tot_abs_basic_exp_imputed + tot_discr_basic_exp_imputed + tot_non_basic_exp_imputed + tot_fixed_exp_imputed)

    # adjusted imputed expense
    def flag_adjust_imputed_expense(flag_impute_expense, tot_all_exp_imputed, HH_FINANCIAL_REPAYMENTS, used_income_field): #NEW4
        if (flag_impute_expense == 1) and (used_income_field - tot_all_exp_imputed - HH_FINANCIAL_REPAYMENTS < 0):
            return 1
        else:
            return 0
    flag_adjust_imputed_expense = np.vectorize(flag_adjust_imputed_expense)

    def compute_tot_abs_basic_exp_imputed_adjusted(flag_adjust_imputed_expense,tot_abs_basic_exp_imputed, tot_all_exp_imputed, tot_individual, HH_FINANCIAL_REPAYMENTS, used_income_field): #NEW4
        if flag_adjust_imputed_expense == 1:
            expense_prop = tot_abs_basic_exp_imputed/tot_all_exp_imputed
            return max(CommonBizRules.MINIMUM_EXP_PP_TO_ADJUST_IMPUTED_EXPENSE * np.sqrt(tot_individual), 
                        used_income_field - HH_FINANCIAL_REPAYMENTS - CommonBizRules.CF_BUFFER_TO_ADJUST_IMPUTED_EXPENSE
                        ) * expense_prop
        else:
            return tot_abs_basic_exp_imputed
    compute_tot_abs_basic_exp_imputed_adjusted = np.vectorize(compute_tot_abs_basic_exp_imputed_adjusted)

    def compute_tot_discr_basic_exp_imputed_adjusted(flag_adjust_imputed_expense,tot_discr_basic_exp_imputed, tot_all_exp_imputed, tot_individual, HH_FINANCIAL_REPAYMENTS, used_income_field): #NEW4
        if flag_adjust_imputed_expense == 1:
            expense_prop = tot_discr_basic_exp_imputed/tot_all_exp_imputed
            return max(CommonBizRules.MINIMUM_EXP_PP_TO_ADJUST_IMPUTED_EXPENSE * np.sqrt(tot_individual), used_income_field - HH_FINANCIAL_REPAYMENTS - CommonBizRules.CF_BUFFER_TO_ADJUST_IMPUTED_EXPENSE) * expense_prop
        else:
            return tot_discr_basic_exp_imputed
    compute_tot_discr_basic_exp_imputed_adjusted = np.vectorize(compute_tot_discr_basic_exp_imputed_adjusted)

    def compute_tot_non_basic_exp_imputed_adjusted(flag_adjust_imputed_expense, tot_non_basic_exp_imputed, tot_all_exp_imputed, tot_individual, HH_FINANCIAL_REPAYMENTS, used_income_field): #NEW4
        if flag_adjust_imputed_expense == 1:
            expense_prop = tot_non_basic_exp_imputed/tot_all_exp_imputed
            return max(CommonBizRules.MINIMUM_EXP_PP_TO_ADJUST_IMPUTED_EXPENSE * np.sqrt(tot_individual), used_income_field - HH_FINANCIAL_REPAYMENTS - CommonBizRules.CF_BUFFER_TO_ADJUST_IMPUTED_EXPENSE) * expense_prop
        else:
            return tot_non_basic_exp_imputed
    compute_tot_non_basic_exp_imputed_adjusted = np.vectorize(compute_tot_non_basic_exp_imputed_adjusted)

    def compute_tot_fixed_exp_imputed_adjusted(flag_adjust_imputed_expense, tot_fixed_exp_imputed, tot_all_exp_imputed, tot_individual, HH_FINANCIAL_REPAYMENTS, used_income_field): #NEW4
        if flag_adjust_imputed_expense == 1:
            expense_prop = tot_fixed_exp_imputed/tot_all_exp_imputed
            return max(CommonBizRules.MINIMUM_EXP_PP_TO_ADJUST_IMPUTED_EXPENSE * np.sqrt(tot_individual), used_income_field - HH_FINANCIAL_REPAYMENTS - CommonBizRules.CF_BUFFER_TO_ADJUST_IMPUTED_EXPENSE) * expense_prop
        else:
            return tot_fixed_exp_imputed
    compute_tot_fixed_exp_imputed_adjusted = np.vectorize(compute_tot_fixed_exp_imputed_adjusted)

    #===========dynamic helpers - time variant========
    def flag_future_relief(HH_RELIEF_IND, HH_RELIEF_DUE_DATE_modified, snapshot):
        if HH_RELIEF_IND == 1:
            if snapshot <= HH_RELIEF_DUE_DATE_modified:
                return 1
            else:
                return 0
        else:
            return 0
    flag_future_relief = np.vectorize(flag_future_relief)

    def compute_gov_payment_inco(flag_employed_or_survived_snapshot, flag_employed_or_survived_last_snapshot, tot_individual, snapshot):
        # if snapshot == CommonBizRules.CURR_SNAPSHOT:
        #     return 0
        if (flag_employed_or_survived_snapshot + flag_employed_or_survived_last_snapshot) == 0:
            if snapshot <= CommonBizRules.JOBSEEKER_END_SNAPSHOT:
                if tot_individual == 1:
                    return CommonBizRules.JOBSEEKER_1P
                else:
                    return CommonBizRules.JOBSEEKER_GT_1P
            else:
                if tot_individual == 1:
                    return CommonBizRules.NEWSTART_1P
                else:
                    return CommonBizRules.NEWSTART_GT_1P
        else:
            return 0
    compute_gov_payment_inco = np.vectorize(compute_gov_payment_inco)


    # expense
    def compute_abs_basic_exp(tot_abs_basic_exp_imputed_adjusted, flag_impute_expense, tot_abs_basic_exp):
        return tot_abs_basic_exp_imputed_adjusted if flag_impute_expense else tot_abs_basic_exp #NEW4
    compute_abs_basic_exp = np.vectorize(compute_abs_basic_exp)
            
    def compute_discr_basic_exp(flag_impute_expense, tot_discr_basic_exp_imputed_adjusted, tot_discr_basic_exp, flag_employed_or_survived_snapshot, deposit_bal_last_snapshot):
        if flag_impute_expense == 1:
            expense_col = tot_discr_basic_exp_imputed_adjusted  #NEW4
        else:
            expense_col = tot_discr_basic_exp
            
        if flag_employed_or_survived_snapshot == 0 and (deposit_bal_last_snapshot < CommonBizRules.DISCR_EXP_DEPOSIT_BAL_THRESHOLD):
            return expense_col * CommonBizRules.DISCR_EXP_MULTIPLIER
        else:
            return expense_col
    compute_discr_basic_exp = np.vectorize(compute_discr_basic_exp)

    def compute_discr_basic_exp(flag_impute_expense, tot_discr_basic_exp_imputed_adjusted, tot_discr_basic_exp, flag_employed_or_survived_snapshot, deposit_bal_last_snapshot):
        if flag_impute_expense == 1:
            expense_col = tot_discr_basic_exp_imputed_adjusted  #NEW4
        else:
            expense_col = tot_discr_basic_exp
            
        if flag_employed_or_survived_snapshot == 0 and (deposit_bal_last_snapshot < CommonBizRules.DISCR_EXP_DEPOSIT_BAL_THRESHOLD):
            return expense_col * CommonBizRules.DISCR_EXP_MULTIPLIER
        else:
            return expense_col
    compute_discr_basic_exp = np.vectorize(compute_discr_basic_exp)
            
    def compute_non_basic_exp(flag_impute_expense, tot_non_basic_exp_imputed_adjusted, tot_non_basic_exp, flag_employed_or_survived_snapshot, deposit_bal_last_snapshot):
        if flag_impute_expense == 1:
            expense_col = tot_non_basic_exp_imputed_adjusted #NEW4
        else:
            expense_col = tot_non_basic_exp
            
        if (flag_employed_or_survived_snapshot == 0) and (deposit_bal_last_snapshot < CommonBizRules.NON_BASIC_EXP_DEPOSIT_BAL_THRESHOLD):
            return expense_col * CommonBizRules.NON_BASIC_EXP_MULTIPLIER
        else:
            return expense_col
    compute_non_basic_exp = np.vectorize(compute_non_basic_exp)

    def compute_fixed_exp(tot_fixed_exp_imputed_adjusted, flag_impute_expense, tot_fixed_exp):
        return tot_fixed_exp_imputed_adjusted if flag_impute_expense == 1 else tot_fixed_exp #NEW4
    compute_fixed_exp = np.vectorize(compute_fixed_exp)
    
        
    def compute_financial_repayments(flag_on_relief_snapshot, HH_FINANCIAL_REPAYMENTS): # HH level repayment
        return 0 if flag_on_relief_snapshot == 1 else HH_FINANCIAL_REPAYMENTS
    compute_financial_repayments = np.vectorize(compute_financial_repayments)

    # # deposits and arrears
    # def compute_deposit_bal_or_arrears_amt(HH_DEPOSIT_FUM_BAL, HH_CURR_OFI_Cash, HH_TOTAL_REDRAW, HH_ARREARS_AMOUNT, total_income_snapshot, total_expense_snapshot, deposit_bal_last_snapshot, arrears_amt_last_snapshot, financial_repayments_snapshot, for_field):
    #     for_deposit = 1 if for_field == 'deposit_bal' else 0
    #     if snapshot == CommonBizRules.CURR_SNAPSHOT:
    #         if for_deposit == 1:
    #             return HH_DEPOSIT_FUM_BAL + HH_CURR_OFI_Cash + HH_TOTAL_REDRAW
    #         else:
    #             return HH_ARREARS_AMOUNT
    #     else:
    #         profit_before_repayment = total_income_snapshot - total_expense_snapshot
    #         net_balance = deposit_bal_last_snapshot + profit_before_repayment
    #         all_repayment = arrears_amt_last_snapshot + financial_repayments_snapshot

    #         if net_balance < 0: 
    #             return net_balance if for_deposit else all_repayment
    #         else: 
    #             if net_balance < all_repayment:
    #                 return 0 if for_deposit else all_repayment - net_balance
    #             else:
    #                 return net_balance - all_repayment if for_deposit else 0
    # compute_deposit_bal_or_arrears_amt = np.vectorize(compute_deposit_bal_or_arrears_amt)

    def compute_current_deposit_bal_or_arrears_amt(HH_DEPOSIT_FUM_BAL, HH_CURR_OFI_Cash, HH_TOTAL_REDRAW, HH_ARREARS_AMOUNT, for_field):
        for_deposit = 1 if for_field == 'deposit_bal' else 0
        if for_deposit == 1:
            return HH_DEPOSIT_FUM_BAL + HH_CURR_OFI_Cash + HH_TOTAL_REDRAW
        else:
            return HH_ARREARS_AMOUNT
    compute_current_deposit_bal_or_arrears_amt = np.vectorize(compute_current_deposit_bal_or_arrears_amt)

    def compute_future_deposit_bal_or_arrears_amt(total_income_snapshot, total_expense_snapshot, deposit_bal_last_snapshot, arrears_amt_last_snapshot, financial_repayments_snapshot, for_field):
        for_deposit = 1 if for_field == 'deposit_bal' else 0
        profit_before_repayment = total_income_snapshot - total_expense_snapshot
        net_balance = deposit_bal_last_snapshot + profit_before_repayment
        all_repayment = arrears_amt_last_snapshot + financial_repayments_snapshot

        if net_balance < 0: 
            return net_balance if for_deposit else all_repayment
        else: 
            if net_balance < all_repayment:
                return 0 if for_deposit else all_repayment - net_balance
            else:
                return net_balance - all_repayment if for_deposit else 0
    compute_future_deposit_bal_or_arrears_amt = np.vectorize(compute_future_deposit_bal_or_arrears_amt)


    @staticmethod
    def compute_arrears_months(arrears_amt_snapshot, HH_FINANCIAL_REPAYMENTS):
        return arrears_amt_snapshot / HH_FINANCIAL_REPAYMENTS

    def compute_delinquent_duration(arrears_months_snapshot, delinquent_duration_last_snapshot, snapshot): #TODO: might need to revisit Daryl's suggestion on changing the threshold
        if snapshot == CommonBizRules.CURR_SNAPSHOT:
            return max(arrears_months_snapshot - CommonBizRules.ARREARS_MONTHS_TO_DELINQUENT, 0)
        else:
            if arrears_months_snapshot >= CommonBizRules.ARREARS_MONTHS_TO_DELINQUENT:
                return delinquent_duration_last_snapshot + 1
            else:
                return 0
    compute_delinquent_duration = np.vectorize(compute_delinquent_duration)

    def flag_delinquent(delinquent_duration_snapshot, flag_loan_exited_snapshot): # for reporting
        if delinquent_duration_snapshot > 0 and flag_loan_exited_snapshot == 0:
            return 1
        else:
            return 0
    flag_delinquent = np.vectorize(flag_delinquent)

    def flag_delinquent_exited(flag_delinquent_exited_last_snapshot, delinquent_duration_snapshot, snapshot):
        if snapshot == CommonBizRules.CURR_SNAPSHOT:
            return 0
        elif flag_delinquent_exited_last_snapshot == 1:
            return 1
        else:
            return int(delinquent_duration_snapshot >= CommonBizRules.DELINQUENT_MONTHS_TO_EXIT)
    flag_delinquent_exited = np.vectorize(flag_delinquent_exited)

    def flag_attrition_exited(flag_attrition_exited_last_snapshot, snapshot):
        if snapshot == CommonBizRules.CURR_SNAPSHOT:
            return 0
        elif flag_attrition_exited_last_snapshot == 1:
            return 1  
        else:
            return get_binary_from_prob(CommonBizRules.ATTRITION_PROB)
    flag_attrition_exited = np.vectorize(flag_attrition_exited)

    def flag_loan_exited(flag_attrition_exited_snapshot, flag_delinquent_exited_snapshot, loan_principle_snapshot, snapshot):
        if snapshot == CommonBizRules.CURR_SNAPSHOT: # TODO - verify: we assume loan is not exited in current month even when arrears months are huge
            return 0
        else:
            return int(flag_attrition_exited_snapshot == 1 or flag_delinquent_exited_snapshot == 1 or loan_principle_snapshot == 0)
    flag_loan_exited = np.vectorize(flag_loan_exited)

    ## lvr
    def cumpute_house_price(DVLR_MATCH, orig_house_price, hpi_snapshot, hpi_curr_snapshot, hpi_at_purchase, ACCT_SECVAL_BAL_EXPSV, ACCT_LINK_SEC_VAL):
        if DVLR_MATCH == 0: 
            return orig_house_price * hpi_snapshot / hpi_at_purchase
        elif ACCT_SECVAL_BAL_EXPSV != 0:
            return ACCT_SECVAL_BAL_EXPSV * hpi_snapshot / hpi_curr_snapshot
        else:
            return ACCT_LINK_SEC_VAL * hpi_snapshot / hpi_curr_snapshot
    cumpute_house_price = np.vectorize(cumpute_house_price)

    @staticmethod
    def compute_lvr(loan_principle_snapshot, house_price_snapshot): # for reporting
        return loan_principle_snapshot/ house_price_snapshot 

    def flag_employed_or_bus_status(flag_loan_exited_snapshot, flag_employed_or_survived_snapshot): # for reporting
        if flag_loan_exited_snapshot == 0:
            return flag_employed_or_survived_snapshot
        else: 
            return 0
    flag_employed_or_bus_status = np.vectorize(flag_employed_or_bus_status)

    ## loan
    def compute_loan_principle(CURR_BAL_AMT, F_INT_ONLY, loan_principle_last_snapshot, loan_financial_repayment_snapshot, interest_payment_snapshot, snapshot):
        if snapshot == CommonBizRules.CURR_SNAPSHOT:
            return -CURR_BAL_AMT
        else:
            if F_INT_ONLY =='N':
                return loan_principle_last_snapshot - loan_financial_repayment_snapshot + interest_payment_snapshot
            else:
                return loan_principle_last_snapshot
    compute_loan_principle = np.vectorize(compute_loan_principle)

    def compute_loan_principle_outstanding(flag_loan_exited_snapshot, loan_principle_snapshot):
        return 0 if flag_loan_exited_snapshot == 1 else loan_principle_snapshot
    compute_loan_principle_outstanding = np.vectorize(compute_loan_principle_outstanding)
        
    @staticmethod
    def compute_interest_amount(loan_principle_last_snapshot, A_OFFSET, R_IDR):
        return (loan_principle_last_snapshot - A_OFFSET) * R_IDR / 12 /100
    
    def compute_loan_financial_repayment_amount(flag_on_relief_snapshot, loan_principle_last_snapshot, repayment_amount, interest_payment_snapshot): # Acc/loan level repayment
        if flag_on_relief_snapshot == 1:
            return 0
        elif loan_principle_last_snapshot == 0:
            return 0
        else:
            return min(repayment_amount, loan_principle_last_snapshot + interest_payment_snapshot)
    compute_loan_financial_repayment_amount = np.vectorize(compute_loan_financial_repayment_amount)

    def anz_revenue_from_loan(TOTAL_REVENUE, flag_loan_exited_snapshot):
        return TOTAL_REVENUE if flag_loan_exited_snapshot == 0 else 0
    anz_revenue_from_loan = np.vectorize(anz_revenue_from_loan)

    def compute_current_HH_OFI_cash(HH_ARREARS_DAYS, SNAPSHOT_DATE, HH_LATEST_APP_DATE, HH_LATEST_APP_ANZ_CASH, HH_LATEST_APP_OFI_CASH, HH_DEPOSIT_FUM_BAL):
        if HH_ARREARS_DAYS >= 90:
            return 0
        cpi_adjust = (1 + CommonBizRules.CPI_ASSUMPTION) ** max((SNAPSHOT_DATE-HH_LATEST_APP_DATE).days/365.25, 0)
        app_anz_now = HH_LATEST_APP_ANZ_CASH * cpi_adjust
        app_ofi_now = HH_LATEST_APP_OFI_CASH * cpi_adjust 
        return min(max(app_anz_now + app_ofi_now - HH_DEPOSIT_FUM_BAL, 0), app_ofi_now)
    compute_current_HH_OFI_cash = np.vectorize(compute_current_HH_OFI_cash)

    #=======execution functions==========
    def preprocessing(self):
        # set non_basic_exp to 0
        self.pdf['tot_non_basic_exp'] = 0
        # temp fix for 0 LVR
        self.pdf['A_LVR'] = self.pdf['A_LVR'].mask(self.pdf['A_LVR'] < 1, 80) #TODO --> this is ad hoc fix for data entry error, needs to be replaced using dynamic LVR
        # temp fix for 0 HH_FINANCIAL_REPAYMENTS
        self.pdf['HH_FINANCIAL_REPAYMENTS'] = self.pdf['HH_FINANCIAL_REPAYMENTS'].apply(lambda x:100 if x < 100 else x) # TODO --> this is ad hoc fix to solve zero division error for compute arrears_months
        # triex complete indicator
        self.pdf['retail_complete_ind'] = self.pdf['retail_complete_ind'].fillna('IC')
        # relief expiry date
        self.pdf['HH_RELIEF_DUE_DATE_modified'] = pd.to_datetime(self.pdf['HH_RELIEF_DUE_DATE'], format='%Y-%m-%d') 
        self.pdf['HH_RELIEF_DUE_DATE_modified'] = self.pdf['HH_RELIEF_DUE_DATE_modified'].fillna(datetime(2021,3,31))
        self.pdf['HH_RELIEF_DUE_DATE_modified'] = pd.to_datetime(self.pdf['HH_RELIEF_DUE_DATE_modified'], format='%Y-%m-%d').apply(lambda x:str(x.year)+f'{str(x.month):0>2}')
        # application date
        self.pdf['HH_LATEST_APP_DATE'] = pd.to_datetime(self.pdf['HH_LATEST_APP_DATE'], format='%Y-%m-%d').dt.date
        self.pdf['SNAPSHOT_DATE'] = pd.to_datetime(self.pdf['SNAPSHOT_DATE'], format='%Y-%m-%d').dt.date        
        self.pdf['HH_LATEST_APP_DATE'] = self.pdf['HH_LATEST_APP_DATE'].fillna(self.pdf['SNAPSHOT_DATE'])

    def wide_to_long(self):
        self.pdf = self.pdf.sort_values(by='ACCOUNT_NUMBER')
        # transform to long format
        self.pdf_long = pd.wide_to_long(self.pdf.reset_index(), 
                    stubnames = set([col[:-7] for col in self.pdf.columns if col[-6:] in CommonBizRules.ALL_SNAPSHOTS]),
                    i='index',
                    j='snapshot',
                    sep='_').reset_index()

    def combine_mc(self):
        self.pdf_long = self.pdf_long[['ACCOUNT_NUMBER','snapshot','flag_loan_exited','loan_principle_final','anz_revenue_from_loan','lvr', 
                                'flag_employed_or_survived_final','flag_delinquent_final']]
        self.pdf_long = self.pdf_long.groupby(['ACCOUNT_NUMBER','snapshot']).mean().reset_index().sort_values(by=['ACCOUNT_NUMBER','snapshot'])
        self.pdf_long['perc_loan_survived'] = 1 - self.pdf_long['flag_loan_exited']
        def compute_dlvr_category(lvr):
            if lvr <= 0.6:
                return '0-60%'
            elif lvr <= 0.8:
                return '61%-80%'
            elif lvr <= 0.9:
                return '81%-90%'
            else:
                return '>90%'
        compute_dlvr_category = np.vectorize(compute_dlvr_category)

        self.pdf_long['dlvr_category'] = compute_dlvr_category(self.pdf_long['lvr'])
        self.pdf_long.drop(columns = ['lvr', 'flag_loan_exited'], inplace=True)


#======PAYG, complex class====
class MonteCarloPAYGv4(CommonBizRules):      
    def __init__(self, pdf_raw, pmdf_macro, pmdf_income, pmdf_expense, pmdf_hpi, 
                num_of_simulations=1000, verbose=0):
        assert len(pdf_raw.cohort.unique()) == 1, f'pdf_raw contains multiple or cohort or no data --> {pdf_raw.cohort.unique()}'
        self.num_of_simulations = num_of_simulations
        
        self.pdf = pdf_raw[self.ACCT_FIELDS + self.HH_FIELDS + self.GENERAL_FIELDS].\
                    merge(pmdf_income, how='left', on=['State_Grouped','High_Level_Occu']).\
                    merge(pmdf_expense, how='left', on=['State_Grouped','High_Level_Occu']).\
                    merge(pmdf_macro, how ='left', 
                        left_on = ['State_Grouped'], right_on = ['State']).drop(columns=['State'])
            
        self.pdf['HL_Open_Month'] = pd.to_datetime(self.pdf['HL_Open_Date']).values.astype('datetime64[M]')
        self.pdf = self.pdf.merge(pmdf_hpi, how='left',
                        left_on=['HL_Open_Month','State_Grouped'],
                        right_on=['Date','State']).\
                drop(columns=['HL_Open_Month', 'State','Date']).\
                rename(columns={'HP':'hpi_at_purchase'})
      
        pmdf_hpi = pmdf_hpi.copy()
        pmdf_hpi['Date'] = pmdf_hpi['Date'].apply(lambda x:str(x.year)+f'{str(x.month):0>2}')
        pmdf_hpi.rename(columns = {'HP':'hpi'}, inplace=True)
        
        pmdf_hpi = pmdf_hpi.loc[(pmdf_hpi.Date >= self.CURR_SNAPSHOT) & (pmdf_hpi.Date <= self.ALL_SNAPSHOTS[-1])]
        pmdf_hpi_pivot = pmdf_hpi.pivot(index=['State'], columns='Date', values=['hpi'])
        pmdf_hpi_pivot.columns = ['_'.join(col).strip() for col in pmdf_hpi_pivot.columns.values]
        pmdf_hpi_pivot.reset_index(inplace = True)
                
                
        self.pdf = self.pdf.merge(pmdf_hpi_pivot, how='left',
                        left_on=['State_Grouped'],
                        right_on=['State']).\
                        drop(columns=['State'])
                
        self.pdf = pd.concat([self.pdf] * self.num_of_simulations, axis=0, ignore_index=True)
        self.verbose = verbose
        if self.verbose > 1:
            print(f'number of records = {len(pdf_raw)}')
    
        
    def __str__(self):
        return 'payg and complex class for biz rules'
        
 
    #====fn to create current status fields=====
    ## employment and relief flags
    def flag_current_employement(HH_ARREARS_DAYS, retail_complete_ind, R_C_SALARY_LATEST, R_C_SALARY_PRECOVID):
        if HH_ARREARS_DAYS >= CommonBizRules.ARREARS_DAYS_THRESHOLD: # TODO: new rules --> need to revisit, eg. people could be employed, but just taking time to payoff debts (either dep_bal or arrears)
            return 0
        elif retail_complete_ind in ('CC','CI'):
            return 1 if R_C_SALARY_LATEST > CommonBizRules.INCOME_MINIMUM_THRESHOLD else 0
        else:
            if R_C_SALARY_PRECOVID > CommonBizRules.INCOME_MINIMUM_THRESHOLD and R_C_SALARY_LATEST < CommonBizRules.INCOME_MINIMUM_THRESHOLD:
                return 0
            else:
                return 1
    flag_current_employement = np.vectorize(flag_current_employement)

    ## income
    def flag_impute_salary_income(retail_complete_ind, HH_LATEST_APP_NET_TOTAL_INCOME, R_C_SALARY_LATEST, R_C_SALARY_PRECOVID):
        if retail_complete_ind in ('CC','CI') or \
            HH_LATEST_APP_NET_TOTAL_INCOME > CommonBizRules.INCOME_MINIMUM_THRESHOLD or \
            R_C_SALARY_LATEST > CommonBizRules.INCOME_MINIMUM_THRESHOLD or \
            R_C_SALARY_PRECOVID > CommonBizRules.INCOME_MINIMUM_THRESHOLD:
            return 0
        else:
            return 1
    flag_impute_salary_income = np.vectorize(flag_impute_salary_income)

    def compute_derived_employed_salary_inco(HH_LATEST_APP_NET_TOTAL_INCOME, SNAPSHOT_DATE, HH_LATEST_APP_DATE, TOT_SALARY_AMT_LATEST, RAW_TOT_SALARY_AMT_LATEST,
             TOT_PENSION_AMT_LATEST, TOT_RENT_AMT_LATEST, TOT_DIVIDEND_AMT_LATEST, TOT_CHILD_AMT_LATEST, TOT_OTHER_AMT_LATEST,
             retail_complete_ind, tot_individual, R_C_SALARY_LATEST):
        hh_app_cpi_adjusted_income = HH_LATEST_APP_NET_TOTAL_INCOME * (1 - CommonBizRules.TAX_RATE) * \
                    ((1 + CommonBizRules.CPI_ASSUMPTION) ** max((SNAPSHOT_DATE-HH_LATEST_APP_DATE).days/365.25, 0))
        triex_salary = TOT_SALARY_AMT_LATEST if TOT_SALARY_AMT_LATEST > 0 else RAW_TOT_SALARY_AMT_LATEST
        triex_income = triex_salary + TOT_PENSION_AMT_LATEST + TOT_RENT_AMT_LATEST + TOT_DIVIDEND_AMT_LATEST \
                                                    + TOT_CHILD_AMT_LATEST + TOT_OTHER_AMT_LATEST

        if (retail_complete_ind in ('CC','CI')) and (HH_LATEST_APP_NET_TOTAL_INCOME > CommonBizRules.INCOME_MINIMUM_THRESHOLD) and \
            tot_individual > 1:
            return max(hh_app_cpi_adjusted_income, triex_income)

        elif retail_complete_ind in ('CC','CI'):
            return triex_income
        else:
            if HH_LATEST_APP_NET_TOTAL_INCOME > CommonBizRules.INCOME_MINIMUM_THRESHOLD:
                return hh_app_cpi_adjusted_income
        
            else:
                if R_C_SALARY_LATEST > CommonBizRules.INCOME_MINIMUM_THRESHOLD:
                    return RAW_TOT_SALARY_AMT_LATEST + TOT_PENSION_AMT_LATEST + TOT_RENT_AMT_LATEST + TOT_DIVIDEND_AMT_LATEST \
                                                    + TOT_CHILD_AMT_LATEST + TOT_OTHER_AMT_LATEST
    #            else will use imputed salary, so no else value
    compute_derived_employed_salary_inco = np.vectorize(compute_derived_employed_salary_inco)

    def compute_derived_unemployed_salary_inco(HH_LATEST_APP_NET_TOTAL_INCOME, SNAPSHOT_DATE, HH_LATEST_APP_DATE, TOT_SALARY_AMT_PRECOVID, RAW_TOT_SALARY_AMT_PRECOVID,
                 TOT_PENSION_AMT_LATEST, TOT_RENT_AMT_LATEST, TOT_DIVIDEND_AMT_LATEST, TOT_CHILD_AMT_LATEST, TOT_OTHER_AMT_LATEST,
                 retail_complete_ind, tot_individual, R_C_SALARY_PRECOVID): #NEW 3
        hh_app_cpi_adjusted_income = HH_LATEST_APP_NET_TOTAL_INCOME * (1 - CommonBizRules.TAX_RATE) * \
                    ((1 + CommonBizRules.CPI_ASSUMPTION) ** max((SNAPSHOT_DATE-HH_LATEST_APP_DATE).days/365.25, 0))
        triex_salary = TOT_SALARY_AMT_PRECOVID if TOT_SALARY_AMT_PRECOVID > 0 else RAW_TOT_SALARY_AMT_PRECOVID
        triex_income = triex_salary + TOT_PENSION_AMT_LATEST + TOT_RENT_AMT_LATEST + TOT_DIVIDEND_AMT_LATEST \
                                                    + TOT_CHILD_AMT_LATEST + TOT_OTHER_AMT_LATEST
                                                    
        if (retail_complete_ind in ('CC','CI')) and (HH_LATEST_APP_NET_TOTAL_INCOME > CommonBizRules.INCOME_MINIMUM_THRESHOLD) and \
            tot_individual > 1:
            return max(hh_app_cpi_adjusted_income, triex_income)

        elif retail_complete_ind in ('CC','CI'):
            return triex_income

        else:
            if HH_LATEST_APP_NET_TOTAL_INCOME > CommonBizRules.INCOME_MINIMUM_THRESHOLD:
                return hh_app_cpi_adjusted_income
            else:
                if R_C_SALARY_PRECOVID > CommonBizRules.INCOME_MINIMUM_THRESHOLD:
                    return RAW_TOT_SALARY_AMT_PRECOVID + TOT_PENSION_AMT_LATEST + TOT_RENT_AMT_LATEST + TOT_DIVIDEND_AMT_LATEST \
                                                    + TOT_CHILD_AMT_LATEST + TOT_OTHER_AMT_LATEST
    #            else will use imputed salary, so no else value
    compute_derived_unemployed_salary_inco = np.vectorize(compute_derived_unemployed_salary_inco)

    @staticmethod
    def compute_imputed_salary(ave_salary, tot_individual, TOT_PENSION_AMT_LATEST, TOT_RENT_AMT_LATEST, TOT_DIVIDEND_AMT_LATEST,TOT_CHILD_AMT_LATEST, TOT_OTHER_AMT_LATEST):
        return ave_salary * tot_individual + TOT_PENSION_AMT_LATEST + TOT_RENT_AMT_LATEST + TOT_DIVIDEND_AMT_LATEST \
                                                    + TOT_CHILD_AMT_LATEST + TOT_OTHER_AMT_LATEST

    def flag_adjust_imputed_salary(flag_impute_expense, flag_impute_salary, imputed_salary, tot_abs_basic_exp, tot_discr_basic_exp, tot_non_basic_exp, tot_fixed_exp, HH_FINANCIAL_REPAYMENTS): #new5
        if (flag_impute_expense == 0) and (flag_impute_salary == 1) and \
                (imputed_salary - 
                    (tot_abs_basic_exp + tot_discr_basic_exp + tot_non_basic_exp + tot_fixed_exp) - 
                    HH_FINANCIAL_REPAYMENTS < 0):
            return 1
        else:
            return 0
    flag_adjust_imputed_salary = np.vectorize(flag_adjust_imputed_salary)

    def compute_imputed_salary_adjusted(flag_adjust_imputed_salary, imputed_salary, tot_abs_basic_exp, tot_discr_basic_exp, tot_non_basic_exp, tot_fixed_exp, HH_FINANCIAL_REPAYMENTS): #NEW5
        if flag_adjust_imputed_salary == 0:
            return imputed_salary
        else:
            return (tot_abs_basic_exp + tot_discr_basic_exp + tot_non_basic_exp + tot_fixed_exp) + HH_FINANCIAL_REPAYMENTS + CommonBizRules.CF_BUFFER_TO_ADJUST_IMPUTED_INCOME
    compute_imputed_salary_adjusted = np.vectorize(compute_imputed_salary_adjusted)

    def compute_used_salary_inco(flag_impute_salary, imputed_salary_adjusted, derived_employed_salary, flag_employed_or_survived_curr_snapshot, derived_unemployed_salary):
        if flag_impute_salary == 1:
            return imputed_salary_adjusted #NEW5
        else:
            return derived_employed_salary if flag_employed_or_survived_curr_snapshot else derived_unemployed_salary
    compute_used_salary_inco = np.vectorize(compute_used_salary_inco)

    #==== execution function for compute current static field========
    def prepare_current_state_columns(self):
        curr_snapshot = CommonBizRules.CURR_SNAPSHOT
        ## employment and relief current flags
        self.pdf[f'flag_employed_or_survived_{CommonBizRules.CURR_SNAPSHOT}'] = self.flag_current_employement(self.pdf['HH_ARREARS_DAYS'], self.pdf['retail_complete_ind'], self.pdf['R_C_SALARY_LATEST'], self.pdf['R_C_SALARY_PRECOVID']) 
        self.pdf[f'flag_on_relief_{CommonBizRules.CURR_SNAPSHOT}'] = self.flag_current_relief(self.pdf['HH_RELIEF_IND'], self.pdf['HH_RELIEF_DUE_DATE_modified'])

        ## ref income
        self.pdf['flag_impute_salary'] = self.flag_impute_salary_income(self.pdf['retail_complete_ind'], self.pdf['HH_LATEST_APP_NET_TOTAL_INCOME'], self.pdf['R_C_SALARY_LATEST'], self.pdf['R_C_SALARY_PRECOVID'])
        self.pdf['flag_impute_expense'] = self.flag_impute_expense(self.pdf['retail_complete_ind']) # moved here because it is used by 'flag_adjust_imputed_salary'
        self.pdf['derived_employed_salary'] = self.compute_derived_employed_salary_inco(self.pdf['HH_LATEST_APP_NET_TOTAL_INCOME'], self.pdf['SNAPSHOT_DATE'], self.pdf['HH_LATEST_APP_DATE'], self.pdf['TOT_SALARY_AMT_LATEST'], self.pdf['RAW_TOT_SALARY_AMT_LATEST'], self.pdf['TOT_PENSION_AMT_LATEST'], self.pdf['TOT_RENT_AMT_LATEST'], self.pdf['TOT_DIVIDEND_AMT_LATEST'], self.pdf['TOT_CHILD_AMT_LATEST'], self.pdf['TOT_OTHER_AMT_LATEST'], self.pdf['retail_complete_ind'], self.pdf['tot_individual'], self.pdf['R_C_SALARY_LATEST'])
        self.pdf['derived_unemployed_salary'] = self.compute_derived_unemployed_salary_inco(self.pdf['HH_LATEST_APP_NET_TOTAL_INCOME'], self.pdf['SNAPSHOT_DATE'], self.pdf['HH_LATEST_APP_DATE'], self.pdf['TOT_SALARY_AMT_PRECOVID'], self.pdf['RAW_TOT_SALARY_AMT_PRECOVID'], self.pdf['TOT_PENSION_AMT_LATEST'], self.pdf['TOT_RENT_AMT_LATEST'], self.pdf['TOT_DIVIDEND_AMT_LATEST'], self.pdf['TOT_CHILD_AMT_LATEST'], self.pdf['TOT_OTHER_AMT_LATEST'], self.pdf['retail_complete_ind'], self.pdf['tot_individual'], self.pdf['R_C_SALARY_PRECOVID'])
        self.pdf['imputed_salary'] = self.compute_imputed_salary(self.pdf['ave_salary'], self.pdf['tot_individual'], self.pdf['TOT_PENSION_AMT_LATEST'], self.pdf['TOT_RENT_AMT_LATEST'], self.pdf['TOT_DIVIDEND_AMT_LATEST'], self.pdf['TOT_CHILD_AMT_LATEST'], self.pdf['TOT_OTHER_AMT_LATEST'])
        self.pdf['flag_adjust_imputed_salary'] = self.flag_adjust_imputed_salary(self.pdf['flag_impute_expense'], self.pdf['flag_impute_salary'], self.pdf['imputed_salary'], self.pdf['tot_abs_basic_exp'], self.pdf['tot_discr_basic_exp'], self.pdf['tot_non_basic_exp'], self.pdf['tot_fixed_exp'], self.pdf['HH_FINANCIAL_REPAYMENTS']) #NEW5
        self.pdf['imputed_salary_adjusted'] = self.compute_imputed_salary_adjusted(self.pdf['flag_adjust_imputed_salary'], self.pdf['imputed_salary'], self.pdf['tot_abs_basic_exp'], self.pdf['tot_discr_basic_exp'], self.pdf['tot_non_basic_exp'], self.pdf['tot_fixed_exp'], self.pdf['HH_FINANCIAL_REPAYMENTS']) #NEW5
        self.pdf['used_salary'] = self.compute_used_salary_inco(self.pdf['flag_impute_salary'], self.pdf['imputed_salary_adjusted'], self.pdf['derived_employed_salary'], self.pdf[f'flag_employed_or_survived_{curr_snapshot}'], self.pdf['derived_unemployed_salary'])

        ## ref imputed expense
        self.pdf['tot_abs_basic_exp_imputed'] = self.compute_tot_abs_basic_exp_imputed(self.pdf['tot_abs_basic_exp_pp'], self.pdf['tot_individual'])
        self.pdf['tot_discr_basic_exp_imputed'] = self.compute_tot_discr_basic_exp_imputed(self.pdf['tot_discr_basic_exp_pp'], self.pdf['tot_individual'])
        self.pdf['tot_non_basic_exp_imputed'] = self.compute_tot_non_basic_exp_imputed(self.pdf['tot_non_basic_exp_pp'], self.pdf['tot_individual'])
        self.pdf['tot_fixed_exp_imputed'] = self.compute_tot_fixed_exp_imputed(self.pdf['tot_fixed_exp_pp'], self.pdf['tot_individual'])
        self.pdf['tot_all_exp_imputed'] = self.compute_tot_all_exp_imputed(self.pdf['tot_abs_basic_exp_imputed'], self.pdf['tot_discr_basic_exp_imputed'], self.pdf['tot_non_basic_exp_imputed'], self.pdf['tot_fixed_exp_imputed']) #NEW4
        

        # ref imputed expense adjusted
        self.pdf['flag_adjust_imputed_expense'] = self.flag_adjust_imputed_expense(self.pdf['flag_impute_expense'], self.pdf['tot_all_exp_imputed'], self.pdf['HH_FINANCIAL_REPAYMENTS'], used_income_field=self.pdf['used_salary']) #NEW4
        self.pdf['tot_abs_basic_exp_imputed_adjusted'] = self.compute_tot_abs_basic_exp_imputed_adjusted(self.pdf['flag_adjust_imputed_expense'], self.pdf['tot_abs_basic_exp_imputed'], self.pdf['tot_all_exp_imputed'], self.pdf['tot_individual'], self.pdf['HH_FINANCIAL_REPAYMENTS'], used_income_field=self.pdf['used_salary']) #NEW4
        self.pdf['tot_discr_basic_exp_imputed_adjusted'] = self.compute_tot_discr_basic_exp_imputed_adjusted(self.pdf['flag_adjust_imputed_expense'], self.pdf['tot_discr_basic_exp_imputed'], self.pdf['tot_all_exp_imputed'], self.pdf['tot_individual'], self.pdf['HH_FINANCIAL_REPAYMENTS'], used_income_field=self.pdf['used_salary']) #NEW4
        self.pdf['tot_non_basic_exp_imputed_adjusted'] = self.compute_tot_non_basic_exp_imputed_adjusted(self.pdf['flag_adjust_imputed_expense'], self.pdf['tot_non_basic_exp_imputed'], self.pdf['tot_all_exp_imputed'], self.pdf['tot_individual'], self.pdf['HH_FINANCIAL_REPAYMENTS'], used_income_field=self.pdf['used_salary']) #NEW4
        self.pdf['tot_fixed_exp_imputed_adjusted'] = self.compute_tot_fixed_exp_imputed_adjusted(self.pdf['flag_adjust_imputed_expense'], self.pdf['tot_fixed_exp_imputed'], self.pdf['tot_all_exp_imputed'], self.pdf['tot_individual'], self.pdf['HH_FINANCIAL_REPAYMENTS'], used_income_field=self.pdf['used_salary']) #NEW4
        
        ## for deposit fum impute
        self.pdf['HH_CURR_OFI_Cash'] = self.compute_current_HH_OFI_cash(self.pdf['HH_ARREARS_DAYS'], self.pdf['SNAPSHOT_DATE'], self.pdf['HH_LATEST_APP_DATE'], self.pdf['HH_LATEST_APP_ANZ_CASH'], self.pdf['HH_LATEST_APP_OFI_CASH'], self.pdf['HH_DEPOSIT_FUM_BAL'])
        self.pdf['orig_house_price'] = self.compute_orig_house_price(self.pdf['A_ORIG_AMT_EMA_LIMIT'], self.pdf['A_LVR'])
       


    #====fn to create predicted fields=====
    ## indicators and probs per snapshot====
    def flag_future_employement(flag_employed_or_survived_last_snapshot, PKeepJob_snapshot, PGetJob_snapshot):
        if flag_employed_or_survived_last_snapshot == 1:
            return get_binary_from_prob(PKeepJob_snapshot) 
        else:
            return get_binary_from_prob(PGetJob_snapshot)
    flag_future_employement = np.vectorize(flag_future_employement)

    ## income
    def compute_salary_inco(flag_employed_or_survived_snapshot, used_salary, TOT_PENSION_AMT_LATEST, TOT_RENT_AMT_LATEST, TOT_DIVIDEND_AMT_LATEST, TOT_CHILD_AMT_LATEST, TOT_OTHER_AMT_LATEST):
        if flag_employed_or_survived_snapshot:
            return used_salary
        else:
            return min(used_salary, \
                        TOT_PENSION_AMT_LATEST + TOT_RENT_AMT_LATEST + TOT_DIVIDEND_AMT_LATEST + TOT_CHILD_AMT_LATEST + TOT_OTHER_AMT_LATEST)
    compute_salary_inco = np.vectorize(compute_salary_inco)

      

    def create_future_state_columns(self):
        curr_snapshot = CommonBizRules.CURR_SNAPSHOT
        for snapshot in CommonBizRules.ALL_SNAPSHOTS:
            last_snapshot = last_snapshot = mf.add_months(snapshot, -1)
            if self.verbose > 0:
                print(f'pid_{os.getpid()}: {snapshot}')

            if snapshot != CommonBizRules.CURR_SNAPSHOT:
                # flags
                self.pdf[f'flag_employed_or_survived_{snapshot}'] = self.flag_future_employement(self.pdf[f'flag_employed_or_survived_{last_snapshot}'], self.pdf[f'PKeepJob_{snapshot}'], self.pdf[f'PGetJob_{snapshot}'])
                self.pdf[f'flag_on_relief_{snapshot}'] = self.flag_future_relief(self.pdf['HH_RELIEF_IND'], self.pdf['HH_RELIEF_DUE_DATE_modified'], snapshot)
            
                # interest and principal repayments
                self.pdf[f'interest_payment_{snapshot}'] = self.compute_interest_amount(self.pdf[f'loan_principle_{last_snapshot}'], self.pdf['A_OFFSET'], self.pdf['R_IDR'])
                self.pdf[f'loan_financial_repayment_{snapshot}'] = self.compute_loan_financial_repayment_amount(self.pdf[f'flag_on_relief_{snapshot}'], self.pdf[f'loan_principle_{last_snapshot}'], self.pdf['repayment_amount'], self.pdf[f'interest_payment_{snapshot}'])
                
                # income
                self.pdf[f'inco_salary_{snapshot}'] = self.compute_salary_inco(self.pdf[f'flag_employed_or_survived_{snapshot}'], self.pdf['used_salary'], self.pdf['TOT_PENSION_AMT_LATEST'], self.pdf['TOT_RENT_AMT_LATEST'], self.pdf['TOT_DIVIDEND_AMT_LATEST'], self.pdf['TOT_CHILD_AMT_LATEST'], self.pdf['TOT_OTHER_AMT_LATEST'])
                self.pdf[f'inco_gov_payment_{snapshot}'] = self.compute_gov_payment_inco(self.pdf[f'flag_employed_or_survived_{snapshot}'], self.pdf[f'flag_employed_or_survived_{last_snapshot}'], self.pdf['tot_individual'], snapshot)
                self.pdf[f'total_income_{snapshot}'] = self.pdf[[col for col in self.pdf.columns if col.startswith('inco_') and col.endswith(f'{snapshot}')]].sum(axis = 1)

                # expense
                self.pdf[f'exp_abs_basic_{snapshot}'] = self.compute_abs_basic_exp(self.pdf['tot_abs_basic_exp_imputed_adjusted'], self.pdf['flag_impute_expense'], self.pdf['tot_abs_basic_exp'])
                self.pdf[f'exp_discr_basic_{snapshot}'] = self.compute_discr_basic_exp(self.pdf['flag_impute_expense'], self.pdf['tot_discr_basic_exp_imputed_adjusted'], self.pdf['tot_discr_basic_exp'], self.pdf[f'flag_employed_or_survived_{snapshot}'], self.pdf[f'deposit_bal_{last_snapshot}'])
                self.pdf[f'exp_non_basic_{snapshot}'] = self.compute_non_basic_exp(self.pdf['flag_impute_expense'], self.pdf['tot_non_basic_exp_imputed_adjusted'], self.pdf['tot_non_basic_exp'], self.pdf[f'flag_employed_or_survived_{snapshot}'], self.pdf[f'deposit_bal_{last_snapshot}'])
                self.pdf[f'exp_fixed_{snapshot}'] = self.compute_fixed_exp(self.pdf['tot_fixed_exp_imputed_adjusted'], self.pdf['flag_impute_expense'], self.pdf['tot_fixed_exp'])
                self.pdf[f'total_expense_{snapshot}'] = self.pdf[[col for col in self.pdf.columns if col.startswith('exp_') and col.endswith(f'{snapshot}')]].sum(axis = 1)
                # exclude financial repayment from expense as it is not the first priority expense, it can be allocated to arrears amount
                self.pdf[f'financial_repayments_{snapshot}'] = self.compute_financial_repayments(self.pdf[f'flag_on_relief_{snapshot}'], self.pdf['HH_FINANCIAL_REPAYMENTS'])

                
            # others
            if snapshot == CommonBizRules.CURR_SNAPSHOT:
                self.pdf[f'deposit_bal_{snapshot}'] = self.compute_current_deposit_bal_or_arrears_amt(self.pdf['HH_DEPOSIT_FUM_BAL'], self.pdf['HH_CURR_OFI_Cash'], self.pdf['HH_TOTAL_REDRAW'], self.pdf['HH_ARREARS_AMOUNT'], for_field = 'deposit_bal')
                self.pdf[f'arrears_amt_{snapshot}'] = self.compute_current_deposit_bal_or_arrears_amt(self.pdf['HH_DEPOSIT_FUM_BAL'], self.pdf['HH_CURR_OFI_Cash'], self.pdf['HH_TOTAL_REDRAW'], self.pdf['HH_ARREARS_AMOUNT'], for_field = 'arrears_amt')
            else:
                self.pdf[f'deposit_bal_{snapshot}'] = self.compute_future_deposit_bal_or_arrears_amt(self.pdf[f'total_income_{snapshot}'], self.pdf[f'total_expense_{snapshot}'], self.pdf[f'deposit_bal_{last_snapshot}'], self.pdf[f'arrears_amt_{last_snapshot}'], self.pdf[f'financial_repayments_{snapshot}'], for_field='deposit_bal')
                self.pdf[f'arrears_amt_{snapshot}'] = self.compute_future_deposit_bal_or_arrears_amt(self.pdf[f'total_income_{snapshot}'], self.pdf[f'total_expense_{snapshot}'], self.pdf[f'deposit_bal_{last_snapshot}'], self.pdf[f'arrears_amt_{last_snapshot}'], self.pdf[f'financial_repayments_{snapshot}'], for_field='arrears_amt')
            self.pdf[f'arrears_months_{snapshot}'] = self.compute_arrears_months(self.pdf[f'arrears_amt_{snapshot}'], self.pdf['HH_FINANCIAL_REPAYMENTS'])
            if snapshot == CommonBizRules.CURR_SNAPSHOT:
                self.pdf[f'delinquent_duration_{snapshot}'] = self.compute_delinquent_duration(self.pdf[f'arrears_months_{snapshot}'], None, snapshot)
                self.pdf[f'loan_principle_{snapshot}'] = self.compute_loan_principle(self.pdf['CURR_BAL_AMT'], self.pdf['F_INT_ONLY'], None, None, None, snapshot)
                self.pdf[f'flag_delinquent_exited_{snapshot}'] = self.flag_delinquent_exited(None, None, snapshot)
                self.pdf[f'flag_attrition_exited_{snapshot}'] = self.flag_attrition_exited(None, snapshot)
            else:
                self.pdf[f'delinquent_duration_{snapshot}'] = self.compute_delinquent_duration(self.pdf[f'arrears_months_{snapshot}'], self.pdf[f'delinquent_duration_{last_snapshot}'], snapshot)
                self.pdf[f'loan_principle_{snapshot}'] = self.compute_loan_principle(self.pdf['CURR_BAL_AMT'], self.pdf['F_INT_ONLY'], self.pdf[f'loan_principle_{last_snapshot}'], self.pdf[f'loan_financial_repayment_{snapshot}'], self.pdf[f'interest_payment_{snapshot}'], snapshot)
                self.pdf[f'flag_delinquent_exited_{snapshot}'] = self.flag_delinquent_exited(self.pdf[f'flag_delinquent_exited_{last_snapshot}'], self.pdf[f'delinquent_duration_{snapshot}'], snapshot)
                self.pdf[f'flag_attrition_exited_{snapshot}'] = self.flag_attrition_exited(self.pdf[f'flag_attrition_exited_{last_snapshot}'], snapshot)
            
            self.pdf[f'flag_loan_exited_{snapshot}'] = self.flag_loan_exited(self.pdf[f'flag_attrition_exited_{snapshot}'], self.pdf[f'flag_delinquent_exited_{snapshot}'], self.pdf[f'loan_principle_{snapshot}'], snapshot)
            self.pdf[f'flag_delinquent_final_{snapshot}'] = self.flag_delinquent(self.pdf[f'delinquent_duration_{snapshot}'], self.pdf[f'flag_loan_exited_{snapshot}'])
            
            self.pdf[f'flag_employed_or_survived_final_{snapshot}'] = self.flag_employed_or_bus_status(self.pdf[f'flag_loan_exited_{snapshot}'], self.pdf[f'flag_employed_or_survived_{snapshot}'])
            if snapshot == CommonBizRules.CURR_SNAPSHOT:
                self.pdf[f'loan_principle_final_{snapshot}'] = self.compute_loan_principle_outstanding(self.pdf[f'flag_loan_exited_{snapshot}'], None)
            else:
                self.pdf[f'loan_principle_final_{snapshot}'] = self.compute_loan_principle_outstanding(self.pdf[f'flag_loan_exited_{snapshot}'], self.pdf[f'loan_principle_{snapshot}'])
            self.pdf[f'anz_revenue_from_loan_{snapshot}'] = self.anz_revenue_from_loan(self.pdf['TOTAL_REVENUE'], self.pdf[f'flag_loan_exited_{snapshot}'])

            # lvr
            self.pdf[f'house_price_{snapshot}'] = self.cumpute_house_price(self.pdf['DVLR_MATCH'], self.pdf['orig_house_price'], self.pdf[f'hpi_{snapshot}'], self.pdf[f'hpi_{curr_snapshot}'], self.pdf['hpi_at_purchase'], self.pdf['ACCT_SECVAL_BAL_EXPSV'], self.pdf['ACCT_LINK_SEC_VAL'])
            self.pdf[f'lvr_{snapshot}'] = self.compute_lvr(self.pdf[f'loan_principle_{snapshot}'], self.pdf[f'house_price_{snapshot}'])

    def one_click(self, combine_mc=True):
        self.preprocessing()
        self.prepare_current_state_columns()
        self.create_future_state_columns()
        self.wide_to_long()
        if combine_mc:
            self.combine_mc()
            
           

#======BOHO, OFI-BOHO class====
class MonteCarloBOHOv4(CommonBizRules):
    BOHO_FIELDS = ['1st_level_anzsic_description', 'GEOGRAPHY']
    def __init__(self, pdf_raw, pmdf_macro, pmdf_income, pmdf_expense, pmdf_hpi, 
                num_of_simulations=1000, verbose=0):
        assert len(pdf_raw.cohort.unique()) == 1, f'pdf_raw contains multiple or cohort or no data --> {pdf_raw.cohort.unique()}'
        cohort = pdf_raw.cohort.unique()[0]
        self.num_of_simulations = num_of_simulations
        if cohort == '3.OFI_BOHO':
            income_key = 'High_Level_Occu'
        elif cohort == '2.BOHO':
            income_key = '1st_level_anzsic_description'
        else:
            raise Exception('This class is for BOHO or ofi boho only')
            
        self.pdf = pdf_raw[self.ACCT_FIELDS + self.HH_FIELDS + self.GENERAL_FIELDS + self.BOHO_FIELDS].\
                    merge(pmdf_income, how='left', on=income_key).\
                    merge(pmdf_expense, how='left', on=['State_Grouped','High_Level_Occu']).\
                    merge(pmdf_macro, how ='left', 
                        left_on = ['GEOGRAPHY', '1st_level_anzsic_description'], right_on = ['State', '1st_level_anzsic_description']).\
                    drop(columns=['State'])

        # get hpi future
        pmdf_hpi = pmdf_hpi.copy()      
        self.pdf['HL_Open_Month'] = pd.to_datetime(self.pdf['HL_Open_Date']).values.astype('datetime64[M]')
        self.pdf = self.pdf.merge(pmdf_hpi, how='left',
                        left_on=['HL_Open_Month','State_Grouped'],
                        right_on=['Date','State']).\
                drop(columns=['HL_Open_Month', 'State','Date']).\
                rename(columns={'HP':'hpi_at_purchase'})
      
        #get hpi at purchase
        pmdf_hpi['Date'] = pmdf_hpi['Date'].apply(lambda x:str(x.year)+f'{str(x.month):0>2}')
        pmdf_hpi.rename(columns = {'HP':'hpi'}, inplace=True)
        
        pmdf_hpi = pmdf_hpi.loc[(pmdf_hpi.Date >= self.CURR_SNAPSHOT) & (pmdf_hpi.Date <= self.ALL_SNAPSHOTS[-1])]
        pmdf_hpi_pivot = pmdf_hpi.pivot(index=['State'], columns='Date', values=['hpi'])
        pmdf_hpi_pivot.columns = ['_'.join(col).strip() for col in pmdf_hpi_pivot.columns.values]
        pmdf_hpi_pivot.reset_index(inplace = True)
                
        self.pdf = self.pdf.merge(pmdf_hpi_pivot, how='left', 
                        left_on=['State_Grouped'],
                        right_on=['State']).\
                        drop(columns=['State'])
        
              
        self.pdf = pd.concat([self.pdf] * self.num_of_simulations, axis=0, ignore_index=True)
        self.verbose = verbose
        if self.verbose > 1:
            print(f'number of records = {len(pdf_raw)}')
        

    def __str__(self):
        return 'BOHO and ofi_BOHO class for biz rules'
        

    #====fn to create current status fields=====
    ## employment and relief flags
    @staticmethod
    def flag_current_bus_status(): 
        return 1

    ## income
    def flag_impute_business_income(HH_LATEST_APP_NET_TOTAL_INCOME): 
        if HH_LATEST_APP_NET_TOTAL_INCOME > CommonBizRules.INCOME_MINIMUM_THRESHOLD:
            return 0
        else:
            return 1
    flag_impute_business_income = np.vectorize(flag_impute_business_income)

    def compute_derived_business_inco(HH_LATEST_APP_NET_TOTAL_INCOME, SNAPSHOT_DATE, HH_LATEST_APP_DATE):
        if HH_LATEST_APP_NET_TOTAL_INCOME > CommonBizRules.INCOME_MINIMUM_THRESHOLD:
                return HH_LATEST_APP_NET_TOTAL_INCOME * (1 - CommonBizRules.TAX_RATE) * \
                    ((1 + CommonBizRules.CPI_ASSUMPTION) ** max((SNAPSHOT_DATE-HH_LATEST_APP_DATE).days/365.25, 0))
    #            else will use imputed business, so no else value
    compute_derived_business_inco = np.vectorize(compute_derived_business_inco)

    @staticmethod
    def compute_imputed_business(bus_inc_pp, tot_individual):
        return bus_inc_pp * tot_individual * (1 - CommonBizRules.TAX_RATE)
    
    def flag_adjust_imputed_business(flag_impute_expense, flag_impute_business, imputed_business, tot_abs_basic_exp, tot_discr_basic_exp, tot_non_basic_exp, tot_fixed_exp, HH_FINANCIAL_REPAYMENTS): #new5
        if (flag_impute_expense == 0) and (flag_impute_business == 1) and \
                (imputed_business - 
                    (tot_abs_basic_exp + tot_discr_basic_exp + tot_non_basic_exp + tot_fixed_exp) - 
                    HH_FINANCIAL_REPAYMENTS < 0):
            return 1
        else:
            return 0
    flag_adjust_imputed_business = np.vectorize(flag_adjust_imputed_business)

    def compute_imputed_business_adjusted(flag_adjust_imputed_business, imputed_business, tot_abs_basic_exp, tot_discr_basic_exp, tot_non_basic_exp, tot_fixed_exp, HH_FINANCIAL_REPAYMENTS): #NEW5
        if flag_adjust_imputed_business == 0:
            return imputed_business
        else:
            return (tot_abs_basic_exp + tot_discr_basic_exp + tot_non_basic_exp + tot_fixed_exp) + HH_FINANCIAL_REPAYMENTS + CommonBizRules.CF_BUFFER_TO_ADJUST_IMPUTED_INCOME
    compute_imputed_business_adjusted = np.vectorize(compute_imputed_business_adjusted)

    def compute_used_business_inco(flag_impute_business,imputed_business_adjusted, derived_business):
        if flag_impute_business==1:
            return imputed_business_adjusted
        else:
            return derived_business
    compute_used_business_inco = np.vectorize(compute_used_business_inco)

    def prepare_current_state_columns(self):
        ## employment and relief current flags
        self.pdf[f'flag_employed_or_survived_{CommonBizRules.CURR_SNAPSHOT}'] = self.flag_current_bus_status() 
        self.pdf[f'flag_on_relief_{CommonBizRules.CURR_SNAPSHOT}'] = self.flag_current_relief(self.pdf['HH_RELIEF_IND'], self.pdf['HH_RELIEF_DUE_DATE_modified'])

        ## ref income
        self.pdf['flag_impute_business'] = self.flag_impute_business_income(self.pdf['HH_LATEST_APP_NET_TOTAL_INCOME'])
        self.pdf['flag_impute_expense'] = self.flag_impute_expense(self.pdf['retail_complete_ind']) # moved here because it is used by 'flag_adjust_imputed_salary'
        self.pdf['derived_business'] = self.compute_derived_business_inco(self.pdf['HH_LATEST_APP_NET_TOTAL_INCOME'], self.pdf['SNAPSHOT_DATE'], self.pdf['HH_LATEST_APP_DATE'])
        self.pdf['imputed_business'] = self.compute_imputed_business(self.pdf['bus_inc_pp'], self.pdf['tot_individual'])
        self.pdf['flag_adjust_imputed_business'] = self.flag_adjust_imputed_business(self.pdf['flag_impute_expense'], self.pdf['flag_impute_business'], self.pdf['imputed_business'], self.pdf['tot_abs_basic_exp'], self.pdf['tot_discr_basic_exp'], self.pdf['tot_non_basic_exp'], self.pdf['tot_fixed_exp'], self.pdf['HH_FINANCIAL_REPAYMENTS']) #NEW5
        self.pdf['imputed_business_adjusted'] = self.compute_imputed_business_adjusted(self.pdf['flag_adjust_imputed_business'], self.pdf['imputed_business'], self.pdf['tot_abs_basic_exp'], self.pdf['tot_discr_basic_exp'], self.pdf['tot_non_basic_exp'], self.pdf['tot_fixed_exp'], self.pdf['HH_FINANCIAL_REPAYMENTS']) #NEW5
        self.pdf['used_business'] = self.compute_used_business_inco(self.pdf['flag_impute_business'], self.pdf['imputed_business_adjusted'], self.pdf['derived_business']) #re-ref

        ## ref imputed expense
        self.pdf['tot_abs_basic_exp_imputed'] = self.compute_tot_abs_basic_exp_imputed(self.pdf['tot_abs_basic_exp_pp'], self.pdf['tot_individual'])
        self.pdf['tot_discr_basic_exp_imputed'] = self.compute_tot_discr_basic_exp_imputed(self.pdf['tot_discr_basic_exp_pp'], self.pdf['tot_individual'])
        self.pdf['tot_non_basic_exp_imputed'] = self.compute_tot_non_basic_exp_imputed(self.pdf['tot_non_basic_exp_pp'], self.pdf['tot_individual'])
        self.pdf['tot_fixed_exp_imputed'] = self.compute_tot_fixed_exp_imputed(self.pdf['tot_fixed_exp_pp'], self.pdf['tot_individual'])
        self.pdf['tot_all_exp_imputed'] = self.compute_tot_all_exp_imputed(self.pdf['tot_abs_basic_exp_imputed'], self.pdf['tot_discr_basic_exp_imputed'], self.pdf['tot_non_basic_exp_imputed'], self.pdf['tot_fixed_exp_imputed']) #NEW4

        # ref imputed expense adjusted
        self.pdf['flag_adjust_imputed_expense'] = self.flag_adjust_imputed_expense(self.pdf['flag_impute_expense'], self.pdf['tot_all_exp_imputed'], self.pdf['HH_FINANCIAL_REPAYMENTS'], used_income_field=self.pdf['used_business']) #NEW4
        self.pdf['tot_abs_basic_exp_imputed_adjusted'] = self.compute_tot_abs_basic_exp_imputed_adjusted(self.pdf['flag_adjust_imputed_expense'], self.pdf['tot_abs_basic_exp_imputed'], self.pdf['tot_all_exp_imputed'], self.pdf['tot_individual'], self.pdf['HH_FINANCIAL_REPAYMENTS'], used_income_field=self.pdf['used_business']) #NEW4
        self.pdf['tot_discr_basic_exp_imputed_adjusted'] = self.compute_tot_discr_basic_exp_imputed_adjusted(self.pdf['flag_adjust_imputed_expense'], self.pdf['tot_discr_basic_exp_imputed'], self.pdf['tot_all_exp_imputed'], self.pdf['tot_individual'], self.pdf['HH_FINANCIAL_REPAYMENTS'], used_income_field=self.pdf['used_business']) #NEW4
        self.pdf['tot_non_basic_exp_imputed_adjusted'] = self.compute_tot_non_basic_exp_imputed_adjusted(self.pdf['flag_adjust_imputed_expense'], self.pdf['tot_non_basic_exp_imputed'], self.pdf['tot_all_exp_imputed'], self.pdf['tot_individual'], self.pdf['HH_FINANCIAL_REPAYMENTS'], used_income_field=self.pdf['used_business']) #NEW4
        self.pdf['tot_fixed_exp_imputed_adjusted'] = self.compute_tot_fixed_exp_imputed_adjusted(self.pdf['flag_adjust_imputed_expense'], self.pdf['tot_fixed_exp_imputed'], self.pdf['tot_all_exp_imputed'], self.pdf['tot_individual'], self.pdf['HH_FINANCIAL_REPAYMENTS'], used_income_field=self.pdf['used_business']) #NEW4
        
        ## for deposit fum impute
        self.pdf['HH_CURR_OFI_Cash'] = self.compute_current_HH_OFI_cash(self.pdf['HH_ARREARS_DAYS'], self.pdf['SNAPSHOT_DATE'], self.pdf['HH_LATEST_APP_DATE'], self.pdf['HH_LATEST_APP_ANZ_CASH'], self.pdf['HH_LATEST_APP_OFI_CASH'], self.pdf['HH_DEPOSIT_FUM_BAL'])
        self.pdf['orig_house_price'] = self.compute_orig_house_price(self.pdf['A_ORIG_AMT_EMA_LIMIT'], self.pdf['A_LVR'])

    #====fn to create predicted fields=====
    ## indicators and probs per snapshot====
    def flag_future_bus_status(flag_employed_or_survived_last_snapshot, PBusSurvive_snapshot):
        if flag_employed_or_survived_last_snapshot == 1:
            return get_binary_from_prob(PBusSurvive_snapshot) 
        else:
            return 0
    flag_future_bus_status = np.vectorize(flag_future_bus_status)

    def compute_business_inco(flag_employed_or_survived_snapshot, used_business, TOT_PENSION_AMT_LATEST, TOT_RENT_AMT_LATEST, TOT_DIVIDEND_AMT_LATEST, TOT_CHILD_AMT_LATEST, TOT_OTHER_AMT_LATEST):
        if flag_employed_or_survived_snapshot:
            return used_business
        else:
            return min(used_business, \
                        TOT_PENSION_AMT_LATEST + TOT_RENT_AMT_LATEST + TOT_DIVIDEND_AMT_LATEST + TOT_CHILD_AMT_LATEST + TOT_OTHER_AMT_LATEST)
    compute_business_inco = np.vectorize(compute_business_inco)

    def create_future_state_columns(self):
        curr_snapshot = CommonBizRules.CURR_SNAPSHOT
        for snapshot in CommonBizRules.ALL_SNAPSHOTS:
            last_snapshot = last_snapshot = mf.add_months(snapshot, -1)
            if self.verbose > 0:
                print(f'pid_{os.getpid()}: {snapshot}')

            if snapshot != CommonBizRules.CURR_SNAPSHOT:
                # flags
                self.pdf[f'flag_employed_or_survived_{snapshot}'] = self.flag_future_bus_status(self.pdf[f'flag_employed_or_survived_{last_snapshot}'], self.pdf[f'PBusSurvive_{snapshot}'])
                self.pdf[f'flag_on_relief_{snapshot}'] = self.flag_future_relief(self.pdf['HH_RELIEF_IND'], self.pdf['HH_RELIEF_DUE_DATE_modified'], snapshot)
            
                # interest
                self.pdf[f'interest_payment_{snapshot}'] = self.compute_interest_amount(self.pdf[f'loan_principle_{last_snapshot}'], self.pdf['A_OFFSET'], self.pdf['R_IDR'])
                self.pdf[f'loan_financial_repayment_{snapshot}'] = self.compute_loan_financial_repayment_amount(self.pdf[f'flag_on_relief_{snapshot}'], self.pdf[f'loan_principle_{last_snapshot}'], self.pdf['repayment_amount'], self.pdf[f'interest_payment_{snapshot}'])

                # income
                self.pdf[f'inco_business_{snapshot}'] = self.compute_business_inco(self.pdf[f'flag_employed_or_survived_{snapshot}'], self.pdf['used_business'], self.pdf['TOT_PENSION_AMT_LATEST'], self.pdf['TOT_RENT_AMT_LATEST'], self.pdf['TOT_DIVIDEND_AMT_LATEST'], self.pdf['TOT_CHILD_AMT_LATEST'], self.pdf['TOT_OTHER_AMT_LATEST'])
                self.pdf[f'inco_gov_payment_{snapshot}'] = self.compute_gov_payment_inco(self.pdf[f'flag_employed_or_survived_{snapshot}'], self.pdf[f'flag_employed_or_survived_{last_snapshot}'], self.pdf['tot_individual'], snapshot)
                self.pdf[f'total_income_{snapshot}'] = self.pdf[[col for col in self.pdf.columns if col.startswith('inco_') and col.endswith(f'{snapshot}')]].sum(axis = 1)

                # expense
                self.pdf[f'exp_abs_basic_{snapshot}'] = self.compute_abs_basic_exp(self.pdf['tot_abs_basic_exp_imputed_adjusted'], self.pdf['flag_impute_expense'], self.pdf['tot_abs_basic_exp'])
                self.pdf[f'exp_discr_basic_{snapshot}'] = self.compute_discr_basic_exp(self.pdf['flag_impute_expense'], self.pdf['tot_discr_basic_exp_imputed_adjusted'], self.pdf['tot_discr_basic_exp'], self.pdf[f'flag_employed_or_survived_{snapshot}'], self.pdf[f'deposit_bal_{last_snapshot}'])
                self.pdf[f'exp_non_basic_{snapshot}'] = self.compute_non_basic_exp(self.pdf['flag_impute_expense'], self.pdf['tot_non_basic_exp_imputed_adjusted'], self.pdf['tot_non_basic_exp'], self.pdf[f'flag_employed_or_survived_{snapshot}'], self.pdf[f'deposit_bal_{last_snapshot}'])
                self.pdf[f'exp_fixed_{snapshot}'] = self.compute_fixed_exp(self.pdf['tot_fixed_exp_imputed_adjusted'], self.pdf['flag_impute_expense'], self.pdf['tot_fixed_exp'])
                self.pdf[f'total_expense_{snapshot}'] = self.pdf[[col for col in self.pdf.columns if col.startswith('exp_') and col.endswith(f'{snapshot}')]].sum(axis = 1)
                # exclude financial repayment from expense as it is not the first priority expense, it can be allocated to arrears amount
                self.pdf[f'financial_repayments_{snapshot}'] = self.compute_financial_repayments(self.pdf[f'flag_on_relief_{snapshot}'], self.pdf['HH_FINANCIAL_REPAYMENTS'])

            # others
            if snapshot == CommonBizRules.CURR_SNAPSHOT:
                self.pdf[f'deposit_bal_{snapshot}'] = self.compute_current_deposit_bal_or_arrears_amt(self.pdf['HH_DEPOSIT_FUM_BAL'], self.pdf['HH_CURR_OFI_Cash'], self.pdf['HH_TOTAL_REDRAW'], self.pdf['HH_ARREARS_AMOUNT'], for_field = 'deposit_bal')
                self.pdf[f'arrears_amt_{snapshot}'] = self.compute_current_deposit_bal_or_arrears_amt(self.pdf['HH_DEPOSIT_FUM_BAL'], self.pdf['HH_CURR_OFI_Cash'], self.pdf['HH_TOTAL_REDRAW'], self.pdf['HH_ARREARS_AMOUNT'], for_field = 'arrears_amt')
            else:
                self.pdf[f'deposit_bal_{snapshot}'] = self.compute_future_deposit_bal_or_arrears_amt(self.pdf[f'total_income_{snapshot}'], self.pdf[f'total_expense_{snapshot}'], self.pdf[f'deposit_bal_{last_snapshot}'], self.pdf[f'arrears_amt_{last_snapshot}'], self.pdf[f'financial_repayments_{snapshot}'], for_field = 'deposit_bal')
                self.pdf[f'arrears_amt_{snapshot}'] = self.compute_future_deposit_bal_or_arrears_amt(self.pdf[f'total_income_{snapshot}'], self.pdf[f'total_expense_{snapshot}'], self.pdf[f'deposit_bal_{last_snapshot}'], self.pdf[f'arrears_amt_{last_snapshot}'], self.pdf[f'financial_repayments_{snapshot}'], for_field = 'arrears_amt')
            self.pdf[f'arrears_months_{snapshot}'] = self.compute_arrears_months(self.pdf[f'arrears_amt_{snapshot}'], self.pdf['HH_FINANCIAL_REPAYMENTS'])
            if snapshot == CommonBizRules.CURR_SNAPSHOT:
                self.pdf[f'delinquent_duration_{snapshot}'] = self.compute_delinquent_duration(self.pdf[f'arrears_months_{snapshot}'], None, snapshot)
                self.pdf[f'loan_principle_{snapshot}'] = self.compute_loan_principle(self.pdf['CURR_BAL_AMT'], self.pdf['F_INT_ONLY'], None, None, None, snapshot)
                self.pdf[f'flag_delinquent_exited_{snapshot}'] = self.flag_delinquent_exited(None, None, snapshot)
                self.pdf[f'flag_attrition_exited_{snapshot}'] = self.flag_attrition_exited(None, snapshot)
            else:
                self.pdf[f'delinquent_duration_{snapshot}'] = self.compute_delinquent_duration(self.pdf[f'arrears_months_{snapshot}'], self.pdf[f'delinquent_duration_{last_snapshot}'], snapshot)
                self.pdf[f'loan_principle_{snapshot}'] = self.compute_loan_principle(self.pdf['CURR_BAL_AMT'], self.pdf['F_INT_ONLY'], self.pdf[f'loan_principle_{last_snapshot}'], self.pdf[f'loan_financial_repayment_{snapshot}'], self.pdf[f'interest_payment_{snapshot}'], snapshot)
                self.pdf[f'flag_delinquent_exited_{snapshot}'] = self.flag_delinquent_exited(self.pdf[f'flag_delinquent_exited_{last_snapshot}'], self.pdf[f'delinquent_duration_{snapshot}'], snapshot)
                self.pdf[f'flag_attrition_exited_{snapshot}'] = self.flag_attrition_exited(self.pdf[f'flag_attrition_exited_{last_snapshot}'], snapshot)
            
            self.pdf[f'flag_loan_exited_{snapshot}'] = self.flag_loan_exited(self.pdf[f'flag_attrition_exited_{snapshot}'], self.pdf[f'flag_delinquent_exited_{snapshot}'], self.pdf[f'loan_principle_{snapshot}'], snapshot)
            self.pdf[f'flag_delinquent_final_{snapshot}'] = self.flag_delinquent(self.pdf[f'delinquent_duration_{snapshot}'], self.pdf[f'flag_loan_exited_{snapshot}'])
            
            self.pdf[f'flag_employed_or_survived_final_{snapshot}'] = self.flag_employed_or_bus_status(self.pdf[f'flag_loan_exited_{snapshot}'], self.pdf[f'flag_employed_or_survived_{snapshot}'])
            if snapshot == CommonBizRules.CURR_SNAPSHOT:
                self.pdf[f'loan_principle_final_{snapshot}'] = self.compute_loan_principle_outstanding(self.pdf[f'flag_loan_exited_{snapshot}'], None)
            else:
                self.pdf[f'loan_principle_final_{snapshot}'] = self.compute_loan_principle_outstanding(self.pdf[f'flag_loan_exited_{snapshot}'], self.pdf[f'loan_principle_{snapshot}'])
            self.pdf[f'anz_revenue_from_loan_{snapshot}'] = self.anz_revenue_from_loan(self.pdf['TOTAL_REVENUE'], self.pdf[f'flag_loan_exited_{snapshot}'])

            # lvr
            self.pdf[f'house_price_{snapshot}'] = self.cumpute_house_price(self.pdf['DVLR_MATCH'], self.pdf['orig_house_price'], self.pdf[f'hpi_{snapshot}'], self.pdf[f'hpi_{curr_snapshot}'], self.pdf['hpi_at_purchase'], self.pdf['ACCT_SECVAL_BAL_EXPSV'], self.pdf['ACCT_LINK_SEC_VAL'])
            self.pdf[f'lvr_{snapshot}'] = self.compute_lvr(self.pdf[f'loan_principle_{snapshot}'], self.pdf[f'house_price_{snapshot}'])

    def one_click(self, combine_mc=True):
        self.preprocessing()
        self.prepare_current_state_columns()
        self.create_future_state_columns()
        self.wide_to_long()
        if combine_mc:
            self.combine_mc()
 