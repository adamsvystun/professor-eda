import pandas as pd
import urllib.request
import urllib
import re
import argparse
import pickle
import sys
import time
import os
import json
import requests
import traceback
from bs4 import BeautifulSoup
from urllib.parse import quote
from tqdm import tqdm
from functools import wraps
from joblib import Memory
from lxml.html import fromstring
from itertools import cycle


location = '.cache/google_scholar'
memory = Memory(location, verbose=1)


def memory_cache(*args):
    recalculate_none = False
    def _memory_cache(f):
        cache_file = f.__name__ + '.json'
        print(cache_file)

        cache = {}
        if os.path.exists(cache_file):
            with open(cache_file) as fd:
                cache = json.load(fd)

        @wraps(f)
        def wrapper(arg):
            if arg in cache:
                print('[>] Cache hit')
                res = cache[arg]
                if res or (not res and not recalculate_none):
                    return res
                print('[>] Recalclating')
            res = f(arg)

            cache[arg] = res
            print('[>] Saving cache')
            with open(cache_file, 'w+') as fd:
                json.dump(cache, fd)
            return res

        return wrapper
    
    if len(args) == 1 and callable(args[0]):
        return _memory_cache(args[0])
    else:
        recalculate_none = args[0]
        return _memory_cache


def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    for i in parser.xpath('//tbody/tr'):
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            # Grabbing IP and corresponding PORT
            proxy = ":".join([i.xpath('.//td[1]/text()')[0],
                              i.xpath('.//td[2]/text()')[0]])
            proxies.add(proxy)
    return proxies


proxies = get_proxies()

def get_soup_from_url(url, check_captcha=None):
    global proxies
    print('Making request')
    soup = None
    new_proxies = proxies.copy()
    print(len(proxies)) 
    for proxy in proxies:
        if soup:
            break
        try:
            print(url)
            response = requests.get(url, proxies={"http": proxy, "https": proxy}, timeout=5)
            data = response.text
            soup = BeautifulSoup(data, 'html.parser')
            if check_captcha(soup):
                new_proxies.remove(proxy)
                print('[>] Found captcha')
                soup = None
        except Exception as e:
            print(e)
            new_proxies.remove(proxy)
            print("[>] Skipping. Connnection error")
    if not soup:
        print('All proxies failed')
        exit()
    proxies = new_proxies
    return soup


class CaptchaPresent(Exception):
    pass

captcha_regex = re.compile(r"www.google.com/recaptcha/api.js")

@memory_cache(True)
def find_author_link(author):
    def check_captcha(soup):
        are_results_on_page = soup.find('svg', class_='gs_or_svg') is not None
        return not are_results_on_page
    soup = get_soup_from_url("http://scholar.google.com/scholar?q=" + quote(author), check_captcha)
    # is_captcha_on_page = (soup.find("input", id="recaptcha-token") is not None) or (
    #     soup.find("form", id="gs_captcha_f") is not None
    # ) or (
    #     soup.find("div", id="recaptcha") is not None
    # ) or (
    #     soup.find("script", { "url": captcha_regex }) is not None
    # ) or (
    #     soup.find("a", { "href": 'https://support.google.com/websearch/answer/86640' }) is not None
    # )
    
    h4 = soup.find('h4', class_='gs_rt2')
    if not h4:
        return None
    return "http://scholar.google.com" + h4.a.get('href')


@memory_cache
def scrape_link(link):
    def check_captcha(soup):
        are_results_on_page = soup.find('div', class_='gsc_prf_il') is not None
        return not are_results_on_page
    soup = get_soup_from_url(link, check_captcha)

    affiliation = soup.find('div', class_='gsc_prf_il').get_text()
    raw_stats = soup.find('table', id='gsc_rsb_st').find_all('td', class_='gsc_rsb_std')

    citations = raw_stats[0].get_text()
    hindex = raw_stats[2].get_text()
    i10index = raw_stats[4].get_text()

    return {
        'affiliation': affiliation,
        'citations': citations,
        'hindex': hindex,
        'i10index': i10index
    }


def scrape_google_scholar(options):
    professors = pd.read_csv(options.input)

    # Create a dataframe where to save results
    gs_data = pd.DataFrame(
        columns=['name', 'affiliation', 'citations', 'hindex', 'i10index'])
    number_of_authors_not_found = 0

    # Go through each professor
    for professor_name in tqdm(professors['Name']):
        try:
            link = find_author_link(str(professor_name))
            if not link:
                continue
            data = scrape_link(link)
            data['name'] = professor_name
            gs_data = gs_data.append(data, ignore_index=True)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(e)
            break
        # Sleep for 1 second so google doesn't mark as a bot
        # time.sleep(1)

    gs_data.to_csv(options.output, index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input', '-i', type=str, default='data/ratemyprofessors.csv',
        help='Path where input csv with professor names are located'
    )
    parser.add_argument(
        '--output', '-o', type=str, default='data/googlescholar.csv',
        help='Path where to save output csv'
    )

    args = parser.parse_args()
    scrape_google_scholar(args)