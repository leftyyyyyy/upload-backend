import os
import modin.pandas as pd

os.environ["MODIN_ENGINE"] = "ray"

def calculate(my_file):

    print(my_file)
    df = pd.read_csv(my_file)

    df['year'] = pd.DatetimeIndex(df['date']).year

    return df['year'].value_counts().to_dict()
