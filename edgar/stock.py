'''
This module ties it all together; it will be the main module that's used 
'''
import pandas as pd
from edgar.edgar import get_financial_filing_info, get_latest_quarter_dir, find_latest_filing_info_going_back_from, SYMBOLS_DATA_PATH
from edgar.filing import Filing
from datetime import datetime

class Stock:
    def __init__(self, symbol):
        self.symbol = symbol
        self.cik = self._find_cik()

    def _find_cik(self):
        df = pd.read_csv(SYMBOLS_DATA_PATH, converters={'cik': str})
        try:
            cik = df.loc[df['symbol'] == self.symbol]['cik'].iloc[0]
            print(f'cik for {self.symbol} is {cik}')
            return cik
        except IndexError:
            raise IndexError('could not find cik, must add to symbols.csv') from None

    def get_filing(self, period='annual', year=0, quarter=0):
        """
        Return the first matching Filing searching in this order:
        1) target year: Q4 -> Q1
        2) next year:   Q1 -> Q4  (common case: report year N filed in year N+1)
        3) prior year:  Q4 -> Q1  (last resort)
        """
        target_year = datetime.now().year if year == 0 else year

        search_plan = []
        # same year, reverse quarters
        search_plan += [(target_year, q) for q in (4, 3, 2, 1)]
        # next year, forward quarters
        search_plan += [(target_year + 1, q) for q in (1, 2, 3, 4)]
        # prior year, reverse quarters (last resort)
        search_plan += [(target_year - 1, q) for q in (4, 3, 2, 1)]

        for y, q in search_plan:
            filing_info_list = get_financial_filing_info(period=period, cik=self.cik, year=y, quarter=q)
            if filing_info_list:
                url = filing_info_list[0].url
                return Filing(company=self.symbol, url=url)

        # nothing found anywhere
        raise NoFilingInfoException(
            f'No filing info found for {self.symbol} (period={period}, target_year={target_year})'
        )

class NoFilingInfoException(Exception):
    pass
