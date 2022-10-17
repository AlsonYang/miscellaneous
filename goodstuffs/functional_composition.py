# to mimic the behaviour of torch.nn.Squential for vanilla python code
# https://www.youtube.com/watch?v=ka70COItN40&list=RDCMUCVhQ2NnY5Rskt6UjCUkJ_DA&index=2 22:27
import pandas as pd

df = pd.DataFrame(data=[[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["A", "B", "C"])
df1 = df.copy()
df2 = df.copy()
df3 = df.copy()

# ===solution 1===
df1["AB"] = df1["A"] * df1["B"]
df1["ABC"] = df1["AB"] * df1["C"]

print(df1)
# ====solution 2====
def create_column_AB(df):
    df = df.copy()
    df["AB"] = df["A"] * df["B"]
    return df


def create_column_ABC(df):
    df = df.copy()
    df["ABC"] = df["AB"] * df["C"]
    return df


df2 = create_column_AB(df2)
df2 = create_column_ABC(df2)

print(df2)
# ====solution 3 - function composition====
from functools import reduce

from typing import Callable

ComposableFunctions = Callable[[pd.DataFrame], pd.DataFrame] # abstract class interface

def function_pipeline(*functions: ComposableFunctions) -> ComposableFunctions:
    return reduce(lambda i, j: lambda df: j(i(df)), functions)


pd_pipeline = function_pipeline(create_column_AB, create_column_ABC)
df3 = pd_pipeline(df3)

print(df3)
