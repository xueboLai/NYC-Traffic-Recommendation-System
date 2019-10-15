import pandas as pd
FILE = "data/subway_cost.csv"


def get_travel_time(start_id,end_id):
    df = pd.read_csv(FILE)
    return df[end_id].tolist()[int(start_id)]




print(get_travel_time("1","2"))

