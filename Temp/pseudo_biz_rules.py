# priority: profit gap > arrears_amount > financial_repayment
# with buffer
if deposit_balance < buffer:
    buffer_gap = buffer - deposit_balance_lag1
    if profit_before_repayment <= buffer_gap:
        deposit_balance = deposit_balance_lag1 + profit_before_repayment 
        arrears_amount = all_repayment
    elif profit_before_repayment - buffer_gap <= all_repayment:
        deposit_balance = buffer_gap
        arrears_amount = all_repayment - (profit_before_repayment - buffer_gap)
    else: # profit_before_repayment - buffer_gap > all_repayment
        arrears_amount = 0
        deposit_balance = deposit_balance_lag1 + (profit_before_repayment - buffer_gap - arrears_amount_lag1 - financial_repayment)
else: #TODO: this section is not correct as it doesnt consider the deposit balance to decrease
    if profit_before_repayment - financial_repayment < 0:
        if deposit_balance > buffer:
            arrears_amount = all_repayment - (deposit_balance_lag1 + profit_before_repayment - buffer)
    
    
    if profit_before_repayment <= all_repayment
        deposit_balance = deposit_balance_lag1
        arrears_amount = all_repayment - profit_after_repayment
    else:
        arrears_amount = 0
        deposit_balance = deposit_balance_lag1 + (profit_before_repayment - arrears_amount_lag1 - financial_repayment)

# without buffer
# dimensions: deposit_balance_lag1, profit_before_repayment, arrears_amount_lag1 and the relationship between them. 
net_balance = deposit_balance_lag1 + profit_before_repayment
all_repayment = arrears_amount_lag1 + financial_repayment
if net_balance < 0: # 1. +ve profit < -ve balance 2. -ve profit > +ve balance 3. -ve profit, -ve balace
    deposit_balance = net_balance
    arrears_amount = all_repayment
else: # 1. +ve profit > -ve balance 2. -ve profit < +ve balance 3. +ve profit, +ve balance_lag1, +ve arrears_amount_lag1
    if net_balance < all_repayment:
        deposit_balance = 0
        arrears_amount = all_repayment - net_balance
    else:
        deposit_balance = net_balance - all_repayment
        arrears_amount = 0

