import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("f1")
parser.add_argument("f2")
args = parser.parse_args()
f1 = args.f1
f2 = args.f2

from utils import select_reader
reader = select_reader(f1)

df1 = reader(f1)
df2 = reader(f2)

df1["key"] = 0
df2["key"] = 0

df_cj = pd.merge(left=df1, right=df2, how="outer", on="key")
df_cj.drop(columns=["key"], inplace=True)

df_cj.to_csv("crossjoin.csv", index=False)
