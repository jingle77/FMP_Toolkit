from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
from tqdm import tqdm
import time

class SP500data:
    def __init__(self, api_key):
        self.api_key = api_key

        ## Symbols and Profiles
        self.symbol_list = []
        self.company_profile_df = None

        ## Price
        self.price_data = {}

        ## Financials
        self.income_statement_data_annual = {}
        self.balance_sheet_data_annual = {}
        self.cash_flow_data_annual = {}
        self.key_metrics_data_annual = {}
        self.income_statement_data_quarter = {}
        self.balance_sheet_data_quarter = {}
        self.cash_flow_data_quarter = {}
        self.key_metrics_data_quarter = {}

    def get_symbols(self):
        url = f'https://financialmodelingprep.com/api/v3/sp500_constituent?apikey={self.api_key}'
        stocks = requests.get(url).json()
        self.symbol_list = [stock['symbol'] for stock in stocks]

    def fetch_profile(self, symbol):
            url = f'https://financialmodelingprep.com/api/v3/profile/{symbol}?apikey={self.api_key}'
            response = requests.get(url).json()
            return pd.DataFrame(response)

    def fetch_profile_data(self):
            profile_dfs = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_symbol = {executor.submit(self.fetch_profile, symbol): symbol for symbol in self.symbol_list}
                for future in tqdm(as_completed(future_to_symbol), total=len(self.symbol_list)):
                    try:
                        single_df = future.result()
                        profile_dfs.append(single_df)
                    except Exception as exc:
                        print(f'Exception for {future_to_symbol[future]}: {exc}')
            self.company_profile_df = pd.concat(profile_dfs, ignore_index=True)

    def fetch_price(self, symbol):
        url = f'https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={self.api_key}'
        response = requests.get(url).json()
        prices_df = pd.DataFrame(response['historical'])
        prices_df = prices_df.sort_values(by='date').reset_index(drop=True)
        return prices_df

    def fetch_price_data(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(self.fetch_price, symbol): symbol for symbol in self.symbol_list}
            for future in tqdm(as_completed(future_to_symbol), total=len(self.symbol_list)):
                try:
                    symbol = future_to_symbol[future]
                    prices_df = future.result()
                    self.price_data[symbol] = prices_df
                except Exception as exc:
                    print(f'Exception for {symbol}: {exc}')

    def fetch_income_statement_annual(self, symbol):
        url = f'https://financialmodelingprep.com/api/v3/income-statement/{symbol}?period=annual&apikey={self.api_key}'
        response = requests.get(url).json()
        return pd.DataFrame(response)

    def fetch_income_statement_quarter(self, symbol):
        url = f'https://financialmodelingprep.com/api/v3/income-statement/{symbol}?period=quarter&apikey={self.api_key}'
        response = requests.get(url).json()
        return pd.DataFrame(response)

    def fetch_income_statement_data_annual_concurrent(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(self.fetch_income_statement_annual, symbol): symbol for symbol in self.symbol_list}
            for future in tqdm(as_completed(future_to_symbol), total=len(self.symbol_list)):
                try:
                    symbol = future_to_symbol[future]
                    income_df = future.result()
                    self.income_statement_data_annual[symbol] = income_df
                except Exception as exc:
                    print(f'Exception for {symbol}: {exc}')

    def fetch_income_statement_data_quarter_concurrent(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(self.fetch_income_statement_quarter, symbol): symbol for symbol in self.symbol_list}
            for future in tqdm(as_completed(future_to_symbol), total=len(self.symbol_list)):
                try:
                    symbol = future_to_symbol[future]
                    income_df = future.result()
                    self.income_statement_data_quarter[symbol] = income_df
                except Exception as exc:
                    print(f'Exception for {symbol}: {exc}')

    def fetch_balance_sheet_annual(self, symbol):
        url = f'https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?period=annual&apikey={self.api_key}'
        response = requests.get(url).json()
        return pd.DataFrame(response)

    def fetch_balance_sheet_quarter(self, symbol):
        url = f'https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?period=quarter&apikey={self.api_key}'
        response = requests.get(url).json()
        return pd.DataFrame(response)

    def fetch_balance_sheet_data_annual_concurrent(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(self.fetch_balance_sheet_annual, symbol): symbol for symbol in self.symbol_list}
            for future in tqdm(as_completed(future_to_symbol), total=len(self.symbol_list)):
                try:
                    symbol = future_to_symbol[future]
                    balance_df = future.result()
                    self.balance_sheet_data_annual[symbol] = balance_df
                except Exception as exc:
                    print(f'Exception for {symbol}: {exc}')

    def fetch_balance_sheet_data_quarter_concurrent(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(self.fetch_balance_sheet_quarter, symbol): symbol for symbol in self.symbol_list}
            for future in tqdm(as_completed(future_to_symbol), total=len(self.symbol_list)):
                try:
                    symbol = future_to_symbol[future]
                    balance_df = future.result()
                    self.balance_sheet_data_quarter[symbol] = balance_df
                except Exception as exc:
                    print(f'Exception for {symbol}: {exc}')

    def fetch_cash_flow_annual(self, symbol):
        url = f'https://financialmodelingprep.com/api/v3/cash-flow-statement/{symbol}?period=annual&apikey={self.api_key}'
        response = requests.get(url).json()
        return pd.DataFrame(response)

    def fetch_cash_flow_quarter(self, symbol):
        url = f'https://financialmodelingprep.com/api/v3/cash-flow-statement/{symbol}?period=quarter&apikey={self.api_key}'
        response = requests.get(url).json()
        return pd.DataFrame(response)

    def fetch_cash_flow_data_annual_concurrent(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(self.fetch_cash_flow_annual, symbol): symbol for symbol in self.symbol_list}
            for future in tqdm(as_completed(future_to_symbol), total=len(self.symbol_list)):
                try:
                    symbol = future_to_symbol[future]
                    cash_flow_df = future.result()
                    self.cash_flow_data_annual[symbol] = cash_flow_df
                except Exception as exc:
                    print(f'Exception for {symbol}: {exc}')

    def fetch_cash_flow_data_quarter_concurrent(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(self.fetch_cash_flow_quarter, symbol): symbol for symbol in self.symbol_list}
            for future in tqdm(as_completed(future_to_symbol), total=len(self.symbol_list)):
                try:
                    symbol = future_to_symbol[future]
                    cash_flow_df = future.result()
                    self.cash_flow_data_quarter[symbol] = cash_flow_df
                except Exception as exc:
                    print(f'Exception for {symbol}: {exc}')

    def fetch_key_metrics_annual(self, symbol):
        url = f'https://financialmodelingprep.com/api/v3/key-metrics/{symbol}?period=annual&apikey={self.api_key}'
        response = requests.get(url).json()
        return pd.DataFrame(response)

    def fetch_key_metrics_quarter(self, symbol):
        url = f'https://financialmodelingprep.com/api/v3/key-metrics/{symbol}?period=quarter&apikey={self.api_key}'
        response = requests.get(url).json()
        return pd.DataFrame(response)

    def fetch_key_metrics_data_annual_concurrent(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(self.fetch_key_metrics_annual, symbol): symbol for symbol in self.symbol_list}
            for future in tqdm(as_completed(future_to_symbol), total=len(self.symbol_list)):
                try:
                    symbol = future_to_symbol[future]
                    key_metrics_df = future.result()
                    self.key_metrics_data_annual[symbol] = key_metrics_df
                except Exception as exc:
                    print(f'Exception for {symbol}: {exc}')

    def fetch_key_metrics_data_quarter_concurrent(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(self.fetch_key_metrics_quarter, symbol): symbol for symbol in self.symbol_list}
            for future in tqdm(as_completed(future_to_symbol), total=len(self.symbol_list)):
                try:
                    symbol = future_to_symbol[future]
                    key_metrics_df = future.result()
                    self.key_metrics_data_quarter[symbol] = key_metrics_df
                except Exception as exc:
                    print(f'Exception for {symbol}: {exc}')
