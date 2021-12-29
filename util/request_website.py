import requests
import random
from configs import job_configs as jcfg
from configs import prox_configs as pcfg
import json
from bs4 import BeautifulSoup
import re


class GetWebsite:

    def __init__(self, url, proxy=True):
        self.url = url
        self.proxy = proxy

    def _get_header(self):
        user_agent = random.choice(jcfg.UA_LIST)
        headers = {
            'user-agent': user_agent,
            'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8,zh-CN;q=0.7,zh;q=0.6,zh-TW;q=0.5',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'iframe',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'cross-site',
            'origin': 'https://google.com'
        }
        return headers

    def _get_session(self):
        session = requests.session()
        if self.proxy:
            session.proxies = {'http': 'socks5://{}:{}'.format(pcfg.PROXY_URL, pcfg.PROXY_PROT),
                               'https': 'socks5://{}:{}'.format(pcfg.PROXY_URL, pcfg.PROXY_PROT)}
        session.headers = self._get_header()

        return session

    def _get_response(self):
        session = self._get_session()
        try:
            response = session.get(self.url, allow_redirects=False)
        except requests.exceptions.ConnectTimeout:
            # try once more
            response = session.get(self.url, allow_redirects=False)
        except requests.exceptions.HTTPError:
            return None
        except requests.exceptions.RequestException:
            return None

        return response

    def _get_json_data_from_html(self):
        response = self._get_response()
        if response is not None:
            try:
                data = json.loads(response.text)
                return data
            except Exception as e:
                return None
        else:
            return None

    def _parse_html_for_json(self):
        response = self._get_response()
        soup = BeautifulSoup(response.text, 'html.parser')
        pattern = re.compile(r'\s--\sData\s--\s')
        try:
            script_data = soup.find('script', text=pattern).contents[0]
            start = script_data.find("context") - 2
            if start >= 0:
                json_data = json.loads(script_data[start:-12])
            else:
                json_data = None
            return json_data
        except AttributeError as err:
            # self.logger.debug("Unable to find JSON for stock = {}".format(stock))
            # print(err)
            # raise AttributeError
            return None

    def get_json(self):
        return self._parse_html_for_json()
