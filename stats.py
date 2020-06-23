import modin.pandas as pd

def calculate(my_file):

    """
    Read the csv file in a distrubuted fashion to handle bigger files then return the years frequency
    """
    
    df = pd.read_csv(my_file)

    df['year'] = pd.DatetimeIndex(df['date']).year

    return df['year'].value_counts().to_dict()
