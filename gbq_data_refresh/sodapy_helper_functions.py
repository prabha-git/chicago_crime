from sodapy import Socrata
from datetime import datetime,timedelta
import pandas as pd

def get_chicago_crime_data(updated_last_n_days = 8):
    client = Socrata("data.cityofchicago.org", app_token="Ttz4HIh52J3g53HKTYKMNxu4M")

    # Get all the updates in the last week.
    updated_on_filter = "updated_on >= '"+datetime.strftime(datetime.today().date()-timedelta(days = updated_last_n_days),'%Y-%m-%d')+"T00:00:00.000'"

    crimes = client.get_all("ijzp-q8t2",where = updated_on_filter)
    return pd.DataFrame.from_records(crimes)