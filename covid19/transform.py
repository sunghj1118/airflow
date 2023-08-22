import pandas as pd
from numpy import abs as np_abs


class Transform:

    def __init__(self, data: pd.DataFrame) -> None:
        self.data = data

    def transform_data(self) -> pd.DataFrame:
        """
        Process the daily COVID-19 for loading into BigQuery.
        For each Country, calculate the following metrics:
            1. cases_per_million - Standard Score compared to other countries.
            2. cases_per_million - "IQR Score" compared to other countries (using the formula score = (val - q3) / IQR).
        As well, daily tests per million are provided to realise if the data is biased.
        The metrics are good for realizing each country's cases per population, in relation to most other countries.
        """
        daily_data = self.data.copy()
        daily_data.dropna(axis='rows', subset=[
                          'new_cases_per_million'], inplace=True)
        daily_data['new_tests_per_million'] = daily_data['new_tests_per_thousand'] / 1000
        del daily_data['new_tests_per_thousand']
        # std score
        cases_mean = daily_data['new_cases_per_million'].mean()
        cases_stdev = daily_data['new_cases_per_million'].std()
        daily_data['cases_standard_score'] = np_abs(
            daily_data['new_cases_per_million'] - cases_mean) / cases_stdev
        # re-order cols
        daily_data = daily_data[['location', 'date', 'new_cases_per_million',
                                 'cases_standard_score', 'new_tests_per_million']]
        return daily_data
