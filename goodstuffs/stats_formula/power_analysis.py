import math
from scipy.stats import norm

def calculate_sample_size(p1, p2, alpha=0.05, beta=0.2, k=1):
    """ This function calculates the required sample size for two groups based on the formula provided.
        - Two independent groups
        - Binary outcome (success/failure)
        ref: https://clincalc.com/stats/samplesize.aspx
    
    Parameters:
        p1 (float): proportion of the first group (between 0 and 1)
        p2 (float): proportion of the second group (between 0 and 1)
        alpha (float): probability of type I error (usually 0.05)
        beta (float): probability of type II error (usually 0.2)
        k (int): ratio of sample size for group 2 to group 1

    Returns:
        n1 (int): sample size for group 1
        n2 (int): sample size for group 2
    
    """
    # calculate the critical Z values for alpha and beta
    z_alpha = norm.ppf(1 - alpha/2)
    z_beta = norm.ppf(1 - beta/2)
    
    # calculate q1, q2, p_bar, q_bar
    q1 = 1 - p1
    q2 = 1 - p2
    p_bar = (p1 + k*p2) / (1 + k)
    q_bar = 1 - p_bar
    
    # calculate the absolute difference between two proportions
    delta = abs(p2 - p1)
    
    # calculate the sample size for group 1
    numerator = (z_alpha * math.sqrt(p_bar * q_bar * (1 + 1/k)) + z_beta * math.sqrt(p1 * q1 + p2 * q2 / k)) ** 2
    N1 = numerator / delta ** 2
    
    # calculate the sample size for group 2
    N2 = k * N1
    
    # round up to the nearest integer
    N1 = math.ceil(N1)
    N2 = math.ceil(N2)
    
    return N1, N2

# usage
P1 = 0.054
P2 = 0.03
ALPHA = 0.05
BETA = 0.2 # 1 - power
K = 2

N1, N2 = calculate_sample_size(P1, P2, ALPHA, BETA, K)
print("N1 =", N1)
print("N2 =", N2)
