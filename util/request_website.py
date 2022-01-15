import requests
import random
from configs import job_configs as jcfg
from configs import prox_configs as pcfg
from configs import finviz_configs as fcfg
from util.finviz_cnv_str_to_num import convert_str_to_num
import json
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np


class WebParseError(Exception):
    pass


class GetWebsite:

    def __init__(self, url, proxy=True):
        self.url = url
        self.proxy = proxy

    def _get_session(self):
        session = requests.session()
        if self.proxy:
            session.proxies = {'http': 'socks5://{}:{}'.format(pcfg.PROXY_URL, pcfg.PROXY_PROT),
                               'https': 'socks5://{}:{}'.format(pcfg.PROXY_URL, pcfg.PROXY_PROT)}
        session.headers = {
                            'user-agent': random.choice(jcfg.UA_LIST),
                            'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8,zh-CN;q=0.7,zh;q=0.6,zh-TW;q=0.5',
                            'Cache-Control': 'no-cache',
                            'Connection': 'keep-alive',
                            'Sec-Fetch-Dest': 'iframe',
                            'Sec-Fetch-Mode': 'navigate',
                            'Sec-Fetch-Site': 'cross-site',
                            'origin': 'https://google.com'
                        }
        return session

    def _get_response(self):
        session = self._get_session()
        try:
            response = session.get(self.url, allow_redirects=False)
            return response
        except requests.exceptions.ConnectTimeout:
            response = session.get(self.url, allow_redirects=True)
            return response
        except requests.exceptions.HTTPError as e:
            raise WebParseError(f'unable to parse url = {self.url} due to {e}')
        except requests.exceptions.RequestException as e:
            raise WebParseError(f'unable to parse url = {self.url} due to {e}')
        except Exception as e:
            raise WebParseError(f'unable to parse url = {self.url} due to {e}')

    def response(self):
        return self._get_response()


class YahooWebParser(GetWebsite):
    def _parse_html_for_json(self):
        if self.response() is None:
            raise WebParseError(f'Response is empty for {self.url}')
        elif self.response().status_code == 200:
            soup = BeautifulSoup(self.response().text, 'html.parser')
            pattern = re.compile(r'\s--\sData\s--\s')
            try:
                script_data = soup.find('script', text=pattern).contents[0]
                start = script_data.find("context") - 2
                if start >= 0:
                    json_data = json.loads(script_data[start:-12])
                else:
                    json_data = None
                return json_data
            except AttributeError as e:
                raise WebParseError(f'Attribution error {self.response().status_code} for {self.url}')
        else:
            raise WebParseError(f'Response status code is {self.response().status_code} for {self.url}')

    def parse(self):
        for trail in range(5):
            js = self._parse_html_for_json()
            if js is not None:
                return self._parse_html_for_json()


class YahooAPIParser(YahooWebParser):
    def _parse_for_json(self):
        if self.response() is None:
            raise WebParseError(f'Response is empty for {self.url}')
        elif self.response().status_code == 200:
            return json.loads(self.response().text)
        else:
            raise WebParseError(f'Response status code is {self.response().status_code} for {self.url}')

    def parse(self):
        for trail in range(5):
            js = self._parse_for_json()
            if js is not None:
                return self._parse_for_json()

        return None


class FinvizParserPerPage(GetWebsite):
    def parse_for_df(self):
        df = pd.read_html(self.response().text)[7]

        if df.shape[1] == 70:
            df.columns = df.iloc[0]
            df = df.iloc[1:]
            df.replace(to_replace='-', value=np.NaN, inplace=True)
            df.rename(columns=fcfg.COL_RENAMES, inplace=True)
            for column in df.columns:
                df[column] = df[column].apply(convert_str_to_num)
            df['ipo_date'] = pd.to_datetime(df['ipo_date'], format='%m/%d/%Y')
            return df
        else:
            return pd.DataFrame()

    @property
    def no_of_population(self):
        df = pd.read_html(self.response().text)[6]

        if df.shape[0] == 1:
            return int(df[0][0].split(' ')[1])
        else:
            return None

    def parse(self):
        return self.parse_for_df()
