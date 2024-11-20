from scrapy.cmdline import execute
from datetime import datetime
from typing import Iterable
from scrapy import Request
import urllib.parse
import random
import scrapy
import json
import time
import evpn
import os


class PaginationUrlsSpider(scrapy.Spider):
    name = "pagination_urls"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print('Connecting to VPN (BRAZIL)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (BRAZIL)
        self.api.connect(country_id='163')  # BRAZIL country code for vpn
        time.sleep(5)  # keep some time delay before starting scraping because connecting
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

        self.delivery_date = datetime.now().strftime('%Y%m%d')
        self.pagination_urls = list()  # List of data to make DataFrame then Excel
        self.page_number = 1  # Initialize page counter
        # Path to store the Pagination Urls can be customized by the user
        self.pagination_urls_path = r"../Pagination_Urls"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.pagination_urls_path, exist_ok=True)  # Create Folder if not exists
        self.pagination_urls_filename = fr"{self.pagination_urls_path}/pagination_urls_{self.delivery_date}.json"  # Filename with Scrape Date

        self.cookies = {
            'lgpd-cookie': '{"essenciais":true,"desempenho":false}',
            '_hjSessionUser_3454957': 'eyJpZCI6Ijk3YzQ5YWUwLTdlOGQtNTExYi1iNjI1LTU0YWQyOGRiYTI5ZiIsImNyZWF0ZWQiOjE3Mjk0OTk3ODQwNTUsImV4aXN0aW5nIjp0cnVlfQ==',
            'aws-waf-token': 'a9602ccf-0f55-4cc6-a174-ed0873e4c243:EAoAYOl8a58qBAAA:DSLMn4/MgaUjuQ8UYtlki2pTc8yyW861H2EkpEfzGBAFwA/IHGXbqPi+VaBxNxJEn1nsLthqZoiGSD0oTq4VZRlUJv2eGfKjPJL8zY0w8oTAX/JND57baAaQIaHqQfrslqbk2Vj0R8AxUkvacjDBuQm/Bcm4zNOJ6cm6b2dE5K6fy+OIU+HTf+lXMPrEwWFUdOSfVUrh5wHqJ9bh5n8rW5LMsjeO5rASsn0lVsBcAdXnzjOmnCabbazlQ96D5ZojJj9w+FhkaN4WwLT5svRV8cJAfA==',
            'SESSION': 'MzAzMjAzNjEtYmE5ZS00N2U0LWE2NjAtMzZhZGMwZjE4NDAz',
            '_hjSession_3454957': 'eyJpZCI6ImJlNjc5YzllLWY0YWQtNDMzMy05MWMzLTJlZTg4Y2FmYjUzNSIsImMiOjE3Mjk4MzcyNTY2MTIsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MX0=',
        }

        self.headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=1, i',
            'referer': 'https://portaldatransparencia.gov.br/sancoes/consulta?cadastro=1&ordenarPor=nomeSancionado&direcao=asc',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
        }

        self.params = {
            'paginacaoSimples': 'false',
            'tamanhoPagina': '15',
            'offset': '0',
            'direcaoOrdenacao': 'asc',
            'colunaOrdenacao': 'nomeSancionado',
            'cadastro': '1',
            'colunasSelecionadas': 'linkDetalhamento,cadastro,cpfCnpj,nomeSancionado,ufSancionado,orgao,categoriaSancao,dataPublicacao,valorMulta,quantidade',
            '_': '1729840466319',
        }

        self.browsers = ["chrome110", "edge99", "safari15_5"]
        self.url = 'https://portaldatransparencia.gov.br/sancoes/consulta/resultado?'

    def start_requests(self) -> Iterable[Request]:
        url = self.url + urllib.parse.urlencode(self.params)
        yield scrapy.Request(url=url, cookies=self.cookies, headers=self.headers, method='GET', meta={'impersonate': random.choice(self.browsers)},
                             callback=self.parse, dont_filter=True, cb_kwargs={'params': self.params})

    def parse(self, response, **kwargs):
        json_dict = json.loads(response.text)
        cases_list: list = json_dict.get('data', [])
        if cases_list:
            self.pagination_urls.append(response.url)
            offset = 0
            records_filterd = json_dict['recordsFiltered']
            div = records_filterd % 10
            step = div * 10 - div
            last_offset = records_filterd - (div * 10) + step
            print(last_offset)
            while offset != last_offset:
                print(f"Currently on page: {self.page_number}")  # Print the current page number
                self.page_number += 1  # Increment page counter
                params = kwargs['params'].copy()  # Copy the params from the current request
                # Increase the offset for the next page (assuming the current offset is already in params)
                offset += 10  # Increment the offset by 10
                params['offset'] = str(offset)  # Update the offset in params

                next_page_url = self.url + urllib.parse.urlencode(params)  # Generate the next page URL
                print('next_page_url:', next_page_url)
                self.pagination_urls.append(next_page_url)

    def close(self, reason):
        print("Converting List of Next Page Urls into JSON...")
        with open(self.pagination_urls_filename, 'w', encoding='utf-8') as file:
            file.write(json.dumps(self.pagination_urls))
        print("List of Next Page Urls converted into JSON!")
        if self.api.is_connected:
            self.api.disconnect()  # Disconnecting VPN if it's still connected


if __name__ == '__main__':
    execute(f'scrapy crawl {PaginationUrlsSpider.name}'.split())
