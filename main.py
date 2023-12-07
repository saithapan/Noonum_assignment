import concurrent.futures
import pandas as pd
import csv
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from requests.exceptions import HTTPError
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from gensim.test.utils import common_texts
from gensim.models import Word2Vec
from gensim.models import KeyedVectors
from nltk.tokenize import sent_tokenize, word_tokenize



all_html_content = []
max_attempts = 3
try_again_interval = 60
failed_urls_file = 'bad_urls.csv'
processed_urls_file = 'completed_urls.csv'
embedding_file = 'embedding_for_title.csv'

def logging_err_for_bad_urls(url, exp):
    with open(failed_urls_file, 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow([url, datetime.now(), exp])

def logging_processed_url(url):
    with open(processed_urls_file, 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow([url, datetime.now()])

def logging_embedding(title, embed):
    with open(embedding_file, 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow([title, datetime.now(), embed])

def fetch_content_from_url(each_url):
    retry_count = 0

    while retry_count < max_attempts:
        try:
            resp = requests.get(each_url)
            resp.raise_for_status()
            # If we get proper text then will return it. Else will try for few more attempts
            return resp.text

        except HTTPError as e:
            if e.response.status_code == 502:
                print(f"502 error due to site is too busy. Retrying after {max_attempts} seconds")
                time.sleep(max_attempts)
                retry_count += 1
                continue

            else:
                logging_err_for_bad_urls(url=each_url, exp=e.response.status_code)
                return None

        except Exception as e:
            logging_err_for_bad_urls(url=each_url, exp=str(e))

            return None

    logging_err_for_bad_urls(url=each_url, exp="Max Attempts reached but no data found")

    return None

def extract_domain(url):
    domain_name = urlparse(url)
    return domain_name.netloc,

def extract_endpoint(url):
    domain_name = urlparse(url)
    return domain_name.path

def extract_title(html_content):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        title_tag = soup.title

        if title_tag:
            title_text = title_tag.get_text()
            return title_text
        else:
            return None
    except:
        return None

def process_page(input_html, domain, url_path):
    '''
    Extracting the embedding for the title from html content
    :param input_html:
    :param domain:
    :param url_path:
    :return:
    '''
    # print(input_html)
    print(domain)
    print(url_path)
    print("__________________")

    title = extract_title(input_html)
    data = []
    for i in sent_tokenize(input_html):
        temp = []

        # tokenize the sentence into words
        for j in word_tokenize(i):
            temp.append(j.lower())

        data.append(temp)

    if title:
        print(title)
        data[0].append(title)
        model = Word2Vec(sentences=data, vector_size=100, window=5, min_count=1, workers=4)
        vector = model.wv['&']
        print(vector)
        logging_embedding(title=title, embed=vector)


def load_and_process_url(urls):

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(fetch_content_from_url, url): url for url in urls}

        for fut in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[fut]
            try:
                page_content = fut.result()
                if page_content:
                    process_page(input_html=page_content, domain=extract_domain(url), url_path=extract_endpoint(url))
                    logging_processed_url(url=url)
            except Exception as e:
                logging_err_for_bad_urls(url=url, exp=str(e))



if __name__ == "__main__":
    with open('web_urls.txt', 'r') as f:
        list_of_urls = [l.strip() for l in f]

    load_and_process_url(urls=list_of_urls)

