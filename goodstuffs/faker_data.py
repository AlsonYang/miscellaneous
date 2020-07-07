'''
usage: df = create_df(nrows)
'''
from faker import Faker
import pandas as pd
import random
fake = Faker()
def create_rows(n = 10):
    output = [{"name":fake.name(),
                "address":fake.address(),
                "email":fake.email(),
                "bs":fake.bs(),
                "city":fake.city(),
                "state":fake.state(),
                "date_time":fake.date_time(),
                "paragraph":fake.paragraph(),
                "Conrad":fake.catch_phrase(),
                "randomdata":random.randint(1000,2000)} for x in range(n)]
    return output

def create_df(nrows = 10):
    data = create_rows(nrows)
    return pd.DataFrame.from_dict(data)