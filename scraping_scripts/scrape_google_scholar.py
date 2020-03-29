from bs4 import BeautifulSoup
import pandas as pd
import urllib.request
from urllib.parse import quote
import argparse
from tqdm import tqdm
import sys
import time
from joblib import Memory

location = '.cache/google_scholar'
memory = Memory(location, verbose=1)


def get_soup_from_url(url):
    print('Making request')
    request = urllib.request.Request(url, None, {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:72.0) Gecko/20100101 Firefox/72.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Cookie': 'NID=201=tsT4MeVfdMOSnO3fzigATm_-2FyQYs9rh01h84HJNUMCpj140XHKbqCX2MrRxO88PFCSYcZh7qhzwvrDh3Ua3qBG3ULu9TN1xwCdIiWanv82YQiGhPBe3umCfRz4-AZ_GMSk-XsyT2w_rWBQ7TtAOQdf_dQ9JMTnUE1qng_Jv3jphvJNnmZb-VRWGJf2oN81v80lgvJ0s1WonF8M2omkpGNQYpjnp_a91QG83SUVvr7YZar-rK0qHhuiOSUX_LJLAecweYLc_jT7bPavE1hx20Ouc6fD44uXMai-iBSUgzia7kvo9SYI5nsUOe2p91iPA_cwlirFQvswWtR51wA8scauH2Xn90gii96f0uhFOJl86IZa4nlOe_6_q8N2shxjDgSCijGTWVHWkU7YJOBEzvQiqxL2TzrriNkbhuj-SSalpS04Onz0; SID=vAebElD7Qx5O65-RFK-OT5xoVMapS6woW1CyDVjA0jU2ML6HHSidGz4wSG57-eDB5yBCPg.; HSID=AdjqUTd7HClZERhnD; SSID=AIzMbx_RcT_NmvVBx; APISID=He3Z8Ic3xZhNMBBp/A9Sa-R2wH5QtLacDS; SAPISID=nnQDDU6r0YRmj-V_/AaGKEL9LOxDxLLP2v; SIDCC=AJi4QfHuCVL2ZoxjQuyAccLjA7MvF9JL_9pQxWgWtauJ6689SqnRW-2-2PKiFl1tsDlHvQIstSQ; 1P_JAR=2020-3-29-0; SEARCH_SAMESITE=CgQIrY8B; CONSENT=YES+PL.pl+; __Secure-3PSID=vAebElD7Qx5O65-RFK-OT5xoVMapS6woW1CyDVjA0jU2ML6HAaC81OAN6-aGJNhwClkA5g.; __Secure-3PAPISID=nnQDDU6r0YRmj-V_/AaGKEL9LOxDxLLP2v; __Secure-HSID=AdjqUTd7HClZERhnD; __Secure-SSID=AIzMbx_RcT_NmvVBx; __Secure-APISID=He3Z8Ic3xZhNMBBp/A9Sa-R2wH5QtLacDS; GSP=LM=1580681697:S=hUSDj9EBQ2L5P2lp; ANID=AHWqTUktSi_LkUrdd866zaLcplGrkH3vuxPAJi9DH6UXahmRchR7SGGqjj48KCiX; OGPC=19016664-5:',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    })
    response = urllib.request.urlopen(request)
    data = response.read()
    soup = BeautifulSoup(data, 'html.parser')
    return soup


class AuthorLinkNotFoundException(Exception):
    pass


@memory.cache
def find_author_link(author):
    soup = get_soup_from_url("https://scholar.google.com/scholar?q=" + quote(author))
    h4 = soup.find('h4', class_='gs_rt2')
    if not h4:
        raise AuthorLinkNotFoundException()
    return "https://scholar.google.com" + h4.a.get('href')


@memory.cache
def scrape_link(link):
    soup = get_soup_from_url(link)

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
    gs_data = pd.DataFrame(columns=['name', 'affiliation', 'citations', 'hindex', 'i10index'])
    number_of_authors_not_found = 0
    
    # Go through each professor
    for professor_name in tqdm(professors['Name']): 
        try:
            link = find_author_link(str(professor_name))
            data = scrape_link(link)
            data['name'] = professor_name
            gs_data = gs_data.append(data, ignore_index=True)
        except KeyboardInterrupt:
            sys.exit()
        except AuthorLinkNotFoundException:
            number_of_authors_not_found += 1
            pass
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
