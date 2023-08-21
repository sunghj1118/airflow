import pandas as pd
import datetime as dt

class Extract:
    OWID_CSV_PATH = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv'
    COL_LIST = ['location','date','new_cases_per_million','new_tests_per_thousand']
    
    def __init__(self, date : dt.datetime) -> None:
        self.date = self.process_time(date)

    def process_time(self,time: dt.datetime):
        """Round the supplied time to date"""
        return time.replace(second=0, microsecond=0, minute=0, hour=0)

    def execute_extraction(self) -> pd.DataFrame:
        df = pd.read_csv(self.OWID_CSV_PATH, usecols=self.COL_LIST, parse_dates=['date'])
        df = df[df.date == self.date]
        return df