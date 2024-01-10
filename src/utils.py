"""Utility functions for scraping huggingface model repositories"""

import csv
import json
import pickle
import re
import time
from ast import literal_eval
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

URL_BASE = 'https://huggingface.co'
URL_MODELS = 'https://huggingface.co/models'
PATH_COOKIES = Path('cookie')

if PATH_COOKIES.is_file():
    COOKIES = requests.cookies.RequestsCookieJar()
    with open('cookie', 'rb') as f:
        cookies = pickle.load(f)
        for cookie in cookies:
            COOKIES.update({cookie['name']: cookie['value']})
else:
    COOKIES = None


def get_repo_links(url: str, fpath: Path = None) -> List[str]:
    """ function to scrape model repositories from overview page
    :param url: url of model overview
    :param fpath: file path to store links
    :return: list of model links
    """

    all_links = []
    next_url = url
    gathered_pages = 0

    while next_url:
        links, next_url = scrape_index_page(next_url)
        all_links += links
        gathered_pages += 1
        if fpath:
            fpath.touch(exist_ok=True)
            fpath.open('a', encoding='utf-8').writelines([str(nm) + '\n' for nm in links])
        print(f"Sucessfully scraped p.{gathered_pages} (equals {len(all_links)} links)")

    return links


def scrape_index_page(url: str) -> Tuple[List[str], str]:
    """ helper function to scrape links to model repositories from overview page
    :param url: url of index page
    :return: list of model links and url to next page
    """

    html = requests.get(url, cookies=COOKIES, timeout=10).content
    html = BeautifulSoup(html, features='html.parser')
    model_links_html = html.select('article.overview-card-wrapper > a')

    # construct full urls to model repositories
    links = [URL_BASE + link['href'] for link in model_links_html]

    # get sub-header infos
    sub_header_text = [l.find('div').text for l in model_links_html]
    has_downloads = [bool(l.select('div > span:has(time[datetime]) ~ svg > path:only-child[fill]'))
                     for l in model_links_html]
    has_likes = [bool(l.select('div > span:has(time[datetime]) ~ svg > path:only-child:not(path[fill])'))
                 for l in model_links_html]

    # zip links and sub-header infos
    links = list(zip(links, sub_header_text, has_downloads, has_likes))

    # get next page url
    try:
        next_url = next(URL_MODELS + l['href'] for l in html.find_all('a')
                        if l.text.strip()=='Next' and l['href'])
    except StopIteration:
        next_url = None
        print(f'Couldnt find next page on {url}')

    return links, next_url


def segment_links(fpath: Path) -> List[Tuple[str, int, int]]:
    """ function to segment the sub header string of model repository links
    :param fpath: file path to stored links
    :return: list of tuples containing repository url, download count and like count
    """

    downloads_pat = re.compile(r'\t(\d+\.)?\d+[kM]?\n\t\t\t•')
    downloads_pat_without_likes = re.compile(r'\t(\d+\.)?\d+[kM]?\n\t\t\t')
    likes_pat = re.compile(r'\t(\d+\.)?\d+[kM]?\n\t\t\t(?!•)')

    # read list of links
    links = fpath.open('r', encoding='utf-8').readlines()
    links = [literal_eval(l.strip()) for l in links]

    # iterate over links and parse sub-headers
    results = []
    for _, link_tuple in enumerate(links):
        sub_header = link_tuple[1]
        has_downloads, has_likes = link_tuple[2:]
        # parse download counts
        if has_downloads:
            if not has_likes:
                download_count = downloads_pat_without_likes.search(sub_header).group(0)[:-1].strip()
            else:
                download_count = downloads_pat.search(sub_header).group(0)[:-1].strip()
            if 'k' in download_count:
                download_count = int(float(download_count[:-1])*1_000)
            elif 'M' in download_count:
                download_count = int(float(download_count[:-1])*1_000_000)
            else:
                download_count = int(download_count)
        else:
            download_count = 0
        # parse like counts
        if has_likes:
            likes_count = likes_pat.search(sub_header).group().strip()
            if 'k' in likes_count:
                likes_count = int(float(likes_count[:-1])*1_000)
            elif 'M' in likes_count:
                likes_count = int(float(likes_count[:-1])*1_000)
            else:
                likes_count = int(likes_count)
        else:
            likes_count = 0

        results.append((link_tuple[0], download_count, likes_count))

    return results


def allocate_to_workers(links: List, n: int) -> List[List[str]]:
    """ helper function to allocate links to workers 
    :param links: list of links to allocate
    :param n: number of workers
    :return: list of lists of links
    """
    d, r = divmod(len(links), n)
    for i in range(n):
        si = (d+1)*(i if i < r else r) + d*(0 if i < r else i - r)
        yield links[si:si+(d+1 if i < r else d)]


def main_parallel(store_dir: Path, meta_path: Path, links: List[str], like_thld: int=0):
    """ main function to scrape model repositories in parallel
    :param store_dir: path to save directory for README files
    :param meta_path: path to meta data file
    :param links: list of repository links
    :param like_thld: likes threshold
    :return: None
    """

    for link, download_count, like_count in tqdm(links):
        if like_count < like_thld:
            continue
        model_dict = get_model(link, store_dir)
        if not model_dict:
            continue
        model_dict['downloads'] = download_count
        model_dict['likes'] = like_count

        meta_file_exists = meta_path.is_file()
        with meta_path.open('a', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=list(model_dict.keys()))
            if not meta_file_exists:
                writer.writeheader()
            writer.writerow(model_dict)


def get_model(url: str, store_dir: Path) -> Dict:
    """ function to retrieve model repository information
    :param url: url of model repository
    :param store_dir: directory to store README files
    :return: dictionary containing model information
    """

    base_dictionary = {
        'repo_url': url,
        'user': '',
        'model_name': '',
        'tags': [],
        'commit_history': []
    }
    result_dictionary = base_dictionary.copy()
    model_page = requests.get(url, cookies=COOKIES, timeout=10)

    # return empty dict if request fails
    if model_page.status_code != 200:
        print(f"\nModel page: Received code {model_page.status_code} for {url=}\n-> Repository not found!")
        base_dictionary['model_name'] = model_page.status_code
        return base_dictionary

    model_soup = BeautifulSoup(model_page.content, features='html.parser')

    # get username
    try:
        result_dictionary['user'] = model_soup.select('header > div > h1 > div:nth-of-type(1) > a')[0].text.strip()
    except IndexError:
        print(f'Could not find user at {url}. Thats a problem because it means there is also no model name.')

    # get model name
    try:
        result_dictionary['model_name'] = model_soup.select('header > div > h1 > div:nth-of-type(2) > a')[0].text.strip()
    except IndexError:
        result_dictionary['model_name'] = result_dictionary['user']
        title_string = model_soup.select('header > div > h1')[0].text.strip()
        result_dictionary['user'] = title_string

    # assemble model specific README dir
    readme_dir = Path(
        store_dir,
        f"{result_dictionary['model_name']}" if '\n\n\n' in result_dictionary['user'] else f"{result_dictionary['user']}__{result_dictionary['model_name']}"
    )

    # get model tags
    tags = model_soup.select('a.tag')
    result_dictionary['tags'] = [t.text.strip() for t in tags]

    # get no of commit pages
    model_tree_page = requests.get(url + '/tree/main?not-for-all-audiences=true', cookies=COOKIES, timeout=10)
    if model_tree_page.status_code != 200:
        print(f"\nModel tree page: Received code {model_tree_page.status_code} for {url=}")
        base_dictionary['commit_history'] = [model_tree_page.status_code]
        return base_dictionary
    model_tree_page = BeautifulSoup(model_tree_page.content, features='html.parser')
    try:
        no_of_commits = model_tree_page.select('header > div > a > span')[1].text
        no_of_commits = int(re.match(r'\d+', no_of_commits).group())
        commit_pages = divmod(no_of_commits, 50)[0] # max 50 entries per page
    except IndexError:
        commit_pages = 0

    # get commit history
    commits = []
    for p in range(0, commit_pages + 1):
        commit_history_page = requests.get(url + f'/commits/main?p={p}', cookies=COOKIES, timeout=10)
        if commit_history_page.status_code != 200:
            print(f"\nCommit history: Received code {commit_history_page.status_code} for {url=}\n-> Lack access permission!")
            base_dictionary['commit_history'] = [commit_history_page.status_code]
            return base_dictionary
        commit_soup = BeautifulSoup(commit_history_page.content, features='html.parser')
        commits.extend(commit_soup.select('div[data-target="Commit"]'))
        #commits = commit_soup.select('div[data-target="Commit"]')
    commit_data = [json.loads(c['data-props']) for c in commits]
    commit_ids = [c['commit']['commit']['id'] for c in commit_data]
    commit_dates = [c['commit']['date'] for c in commit_data]
    commit_urls = [url + f"/tree/{id}" for id in commit_ids]

    # retrieve data per commit
    commit_infos = [get_commit_infos(commit_url, id, readme_dir) for commit_url, id in zip(commit_urls, commit_ids)]
    # check if there was a not 200 code for any commit
    if any(isinstance(commit_info, int) for commit_info in commit_infos):
        print("\nReceived commit-history rate-limit!")
        base_dictionary['commit_history'] = [next(isinstance(commit_info, int) for commit_info in commit_infos)]
        return base_dictionary

    # add dates to commit dicts
    for idx, commit_dict in enumerate(commit_infos):
        commit_dict['commit_date'] = commit_dates[idx]

    result_dictionary['commit_history'] = commit_infos

    time.sleep(2.5)

    return result_dictionary



def get_commit_infos(url: str, commit_id: str, store_dir: Path) -> Dict[str, str]:
    """ function to retrieve commit data
    :param url: url of commit
    :param commit_id: id of commit
    :param store_dir: path to directory to store README files
    :return: dictionary containing commit data
    """

    results = {'commit_id': commit_id, 'commit_url': url}

    commit_page = requests.get(url, cookies=COOKIES, timeout=10)
    if commit_page.status_code != 200:
        return commit_page.status_code
    commit_soup = BeautifulSoup(commit_page.content, features='html.parser')

    # get list of files
    files = commit_soup.select('div[data-target="ViewerIndexTreeList"] > ul > li > div > a')
    file_names = [f.text.strip() for f in files]
    results['files'] = file_names

    # find and download README.md
    file_links = commit_soup.select('div[data-target="ViewerIndexTreeList"] > ul > li > a[download]')
    readme_url = [fl['href'] for fl in file_links if 'readme.md' in fl['href'].lower()]

    if readme_url:
        store_dir.mkdir(exist_ok=True)
        readme = requests.get(URL_BASE + readme_url[0], cookies=COOKIES, timeout=10).content
        time.sleep(2.5)
        fpath = Path(store_dir, f"{commit_id}_README.md")
        fpath.open('wb').write(readme)
        results['readme_path'] = fpath
    else:
        results['readme_path'] = ''

    return results


def load_meta(path: Path) -> pd.DataFrame:
    """ function to load meta data from CSV file 
    :param path: path to CSV file
    :return: pandas DataFrame
    """
    meta = pd.read_csv(path)

    # format 'commit_history' column and pivot longer
    meta['commit_history'] = meta['commit_history'].map(lambda x: literal_eval(x.replace('WindowsPath(', '').replace(')', '')))
    meta = meta.explode('commit_history', ignore_index=True)
    meta = pd.concat([meta.drop(columns=['commit_history']), pd.json_normalize(meta['commit_history'])], axis=1)

    # format other columns
    meta['date'] = pd.to_datetime(meta['date'])
    return meta
