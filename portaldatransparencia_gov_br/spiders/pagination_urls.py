from scrapy.cmdline import execute
from unidecode import unidecode
from datetime import datetime
from typing import Iterable
import browserforge.headers
from scrapy import Request
import pandas as pd
import urllib.parse
import lxml.html
import random
import string
import scrapy
import json
import time
import evpn
import os
import re


def df_cleaner(data_frame: pd.DataFrame) -> pd.DataFrame:
    data_frame = data_frame.astype(str)  # Convert all data to string
    data_frame.drop_duplicates(inplace=True)  # Remove duplicate data from DataFrame

    # Apply the function to all columns for Cleaning
    for column in data_frame.columns:
        data_frame[column] = data_frame[column].apply(remove_link_text)  # Remove Link Click Text from text
        data_frame[column] = data_frame[column].apply(set_date_format)  # Set the Date format
        data_frame[column] = data_frame[column].apply(set_na)  # Setting "N/A" where data is "No Information" or some empty string characters on site
        data_frame[column] = data_frame[column].apply(unidecode)  # Remove diacritics characters
        data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column
        if 'nome' in column:  # "nome" => "Name" columns
            data_frame[column] = data_frame[column].str.replace('–', '')  # Remove specific punctuation 'dash' from name string
            data_frame[column] = data_frame[column].str.translate(str.maketrans('', '', string.punctuation))  # Removing Punctuation from name text
        data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column

    data_frame.replace(to_replace='nan', value=pd.NA, inplace=True)  # After cleaning, replace 'nan' strings back with actual NaN values
    data_frame.fillna(value='N/A', inplace=True)  # Replace NaN values with "N/A"
    return data_frame


# def remove_link_text(text: str) -> str:  # Remove Link Click Text from text
#     text = text.replace('Clique aqui para saber mais sobre essa empresa', '')  # Click here to learn more about this company
#     text = text.replace('Clique aqui para saber mais sobre a pessoa', '')  # Click here to learn more about the person
#     return text.strip()

def remove_link_text(text: str) -> str:  # Remove Link Click Text from text
    # Define a regex pattern to match both link texts
    # pattern = r'(Clique aqui para saber mais sobre essa empresa|Clique aqui para saber mais sobre a pessoa)'
    # pattern = r'(Clique aqui para saber mais sobre [(a pessoa)(essa empresa)]*\b)'
    pattern = r'Clique aqui para saber mais sobre (a pessoa|essa empresa)\b'
    text = re.sub(pattern=pattern, repl='', string=text).strip()  # Use re.sub to replace the matched patterns with an empty string and strip whitespace
    return text


# def set_na(text: str) -> str:
#     text = remove_extra_spaces(text=text)
#     if text.title() == 'Sem Informação' or text == '**' or text == '.':
#         text = text.replace('Sem Informação', 'N/A')  # Setting "N/A" where data is "No Information" on site
#         text = text.replace('**', 'N/A')  # Setting "N/A" where data is "**" on site
#         return text
#     return text


def set_na(text: str) -> str:
    # Remove extra spaces (assuming remove_extra_spaces is a custom function)
    text = remove_extra_spaces(text=text)
    # pattern = r'^(Sem Informação|\*{1,}|\.{1,}|\(Não Informado\))$'  # Define a regex pattern to match all the conditions in a single expression
    pattern = r'^(Sem Informação|\(Não Informado\)|[^\w\s]+)$'  # Define a regex pattern to match all the conditions in a single expression
    text = re.sub(pattern=pattern, repl='N/A', string=text.title())  # Replace matches with "N/A" using re.sub
    return text


def set_date_format(text: str) -> str:
    date_pattern = r'(\d{2}/\d{2}/\d{4})'  # Regular expression to extract the date
    match = re.search(date_pattern, text)  # Search for the pattern anywhere in the string
    # If a match is found, try to format the date
    if match:
        date_str = match.group(1)  # Extract the date part from the match
        try:
            # Try converting the extracted date to a datetime object
            date_object = datetime.strptime(date_str, "%d/%m/%Y")
            return date_object.strftime("%Y/%m/%d")  # Format the date to 'YYYY/MM/DD'
        except ValueError:
            # If the date is invalid, return the original text
            return text
    else:
        return text  # Return the original text if no date is found


# Function to remove Extra Spaces from Text
def remove_extra_spaces(text: str) -> str:
    return re.sub(pattern=r'\s+', repl=' ', string=text).strip()  # Regular expression to replace multiple spaces and newlines with a single space


def header_cleaner(header_text: str) -> str:
    header_text = header_text.strip()
    header = unidecode('_'.join(header_text.lower().split()))
    return header


def get_sanctioned_name(case_dict: dict) -> str:
    name = case_dict.get('nomeSancionado', 'N/A')
    return name if name not in ['', ' ', None] else 'N/A'


def get_cnpjcpf_sanctioned(case_dict: dict) -> str:
    cnpjcpf_sanctioned = case_dict.get('cpfCnpj', 'N/A')
    return cnpjcpf_sanctioned if cnpjcpf_sanctioned not in ['', ' ', None] else 'N/A'


def get_sanctioned_state(case_dict: dict) -> str:
    sanctioned_state = case_dict.get('ufSancionado', 'N/A')
    return sanctioned_state if sanctioned_state not in ['', ' ', None] else 'N/A'


def get_registration(case_dict: dict) -> str:
    registration = case_dict.get('cadastro', 'N/A')
    return registration if registration not in ['', ' ', None] else 'N/A'


def get_details_link(case_dict: dict) -> str:
    details_link_slug = case_dict.get('linkDetalhamento', 'N/A')
    details_link = 'https://portaldatransparencia.gov.br' + details_link_slug
    return details_link if details_link_slug not in ['', ' ', None, 'N/A'] else 'N/A'


def get_organization(case_dict: dict) -> str:
    organization = case_dict.get('orgao', 'N/A')
    return organization if organization not in ['', ' ', None] else 'N/A'


def get_sanction_categry(case_dict: dict) -> str:
    sanction_categry = case_dict.get('categoriaSancao', 'N/A')
    return sanction_categry if sanction_categry not in ['', ' ', None] else 'N/A'


def get_sanction_publication_date(case_dict: dict) -> str:
    publication_date = case_dict.get('dataPublicacao', 'N/A')  # Original date string
    if publication_date != 'Sem informação':
        date_object = datetime.strptime(publication_date, "%d/%m/%Y")  # Convert the date string to a datetime object
        publication_date = date_object.strftime("%Y/%m/%d")  # Format the date to 'YYYY/MM/DD'
    return publication_date if publication_date not in ['', ' ', None, 'Sem informação', '**'] else 'N/A'


def get_fine_amount(case_dict: dict) -> str:
    fine_amount = case_dict.get('valorMulta', 'N/A')
    return fine_amount if fine_amount not in ['', ' ', None, 'Não se aplica'] else 'N/A'


def get_quantity(case_dict: dict) -> str:
    quantity = f"{case_dict.get('quantidade', 'N/A')}"  # Using Format string as quantity value is integer in dictionary
    return quantity if quantity not in ['', ' ', None] else 'N/A'


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
        self.final_data_list = list()  # List of data to make DataFrame then Excel
        self.page_number = 1  # Initialize page counter

        # Path to store the Excel file can be customized by the user
        self.excel_path = r"../Excel_Files"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.excel_path, exist_ok=True)  # Create Folder if not exists
        self.filename = fr"{self.excel_path}/{self.name}_{self.delivery_date}.xlsx"  # Filename with Scrape Date

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

        self.cookies_details = {
            'lgpd-cookie': '{"essenciais":true,"desempenho":false}',
            '_hjSessionUser_3454957': 'eyJpZCI6IjkzYjA1ZjhjLTRmMGMtNTY4Ny04NTUyLWYwZDQ0NDU2OWYwZCIsImNyZWF0ZWQiOjE3MzAwOTIxMDU2MzIsImV4aXN0aW5nIjp0cnVlfQ==',
            'SESSION': 'M2RmMjhiMDctZmI2MC00M2VhLTg5YTctNDg1YWQzMGFhMWNj',
            '_hjSession_3454957': 'eyJpZCI6IjE2ZjgyMDJhLThkNDEtNDhiZi1hYWNhLWU0ZjY4ZmNkOTdmMiIsImMiOjE3MzAxMDQ3OTcwNzMsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MX0=',
        }

        self.headers_details = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,id;q=0.8',
            'priority': 'u=0, i',
            'referer': 'https://portaldatransparencia.gov.br/sancoes/consulta?paginacaoSimples=true&tamanhoPagina=&offset=&direcaoOrdenacao=asc',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        }

        # Headers might change at some interval, try using HeaderGenerator to generate headers
        # self.headers = browserforge.headers.HeaderGenerator().generate()
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
            # Pagination Request
            print(f"Currently on page: {self.page_number}")  # Print the current page number
            print('Performing Pagination...')
            # Increment page counter
            self.page_number += 1
            params = kwargs['params'].copy()  # Copy the params from the current request
            print(params)
            current_offset = int(params['offset'])  # Get the current offset
            # Increase the offset for the next page (assuming the current offset is already in params)
            next_offset = current_offset + 10  # Increment the offset by 10
            params['offset'] = str(next_offset)  # Update the offset in params

            # Check if there are more results by evaluating if the cases list is non-empty
            next_page_url = self.url + urllib.parse.urlencode(params)  # Generate the next page URL
            print(next_page_url)
            print(f"Requesting next page with offset: {next_offset}")
            yield scrapy.Request(url=next_page_url, cookies=self.cookies, headers=self.headers, method='GET',
                                 meta={'impersonate': random.choice(self.browsers)}, callback=self.parse,
                                 dont_filter=True, cb_kwargs={'params': params})
        else:
            print(f'Data and Next Page not Found! on page number {self.page_number}')

    def close(self, reason):
        print('closing spider...')
        print("Converting List of Dictionaries into DataFrame, then into Excel file...")
        # try:
        #     print("Creating Native sheet...")
        #     data_df = pd.DataFrame(self.final_data_list)
        #     data_df = df_cleaner(data_frame=data_df)  # Apply the function to all columns for Cleaning
        #     with pd.ExcelWriter(path=self.filename, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
        #         data_df.to_excel(excel_writer=writer, index=False)
        #     print("Native Excel file Successfully created.")
        # except Exception as e:
        #     print('Error while Generating Native Excel file:', e)
        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()


if __name__ == '__main__':
    start = time.time()
    execute(f'scrapy crawl {PaginationUrlsSpider.name}'.split())
    end = time.time()
    print(f'Scraping done in {end - start} seconds.')
