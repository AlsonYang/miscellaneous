import pandas as pd
from IPython.display import display

path = "~/Desktop/"
pdf = pd.read_excel(path + "risk_data_fields.xlsx")
print((pdf.Description[67:]))

# print(len(list(pdf.DataElem)))
