"""Main script for scraping HuggingFace model repositories"""

from pathlib import Path
from threading import Thread

import pandas as pd

from utils import (allocate_to_workers, get_repo_links,
                   main_parallel, segment_links, load_meta)

PATH_LINKS = Path('output', 'links.txt')
PATH_README = Path('output', 'readmes')
PATH_META = Path('output', 'meta.csv')

URL_BASE = 'https://huggingface.co'
URL_MODELS = 'https://huggingface.co/models'
WORKERS = 2
MIN_LIKES = 3


if __name__ == '__main__':

    # scrape links to model repositories
    if not PATH_LINKS.is_file():
        get_repo_links(URL_MODELS, PATH_LINKS)

    # read and segment links
    links = segment_links(PATH_LINKS)

    # filter for links that have not yet been scraped
    try:
        # remove unsuccesful tries
        for file in PATH_META.parent.iterdir():
            if file.stem.startswith(PATH_META.stem) and file.suffix.endswith(PATH_META.suffix):
                meta = pd.read_csv(file)
                meta = meta[meta['user'].notna()]
                meta = meta[~meta.commit_history.str.contains(r'\[4\d{2}\]', regex=True)]
                meta.to_csv(file, index=False)
        # filter for remaining links
        meta_links = []
        for file in PATH_META.parent.iterdir():
            if file.stem.startswith(PATH_META.stem) and file.suffix.endswith(PATH_META.suffix):
                meta_links += list(pd.read_csv(file)['repo_url'])
        links = [l for l in links if l[0] not in meta_links]
    except FileNotFoundError:
        pass

    # scrape commit histories in parallel
    PATH_README.mkdir(exist_ok=True)
    links_chunks = allocate_to_workers(links, WORKERS)
    meta_files = [Path(PATH_META.parent, f'{PATH_META.stem}{i}.csv') for i in range(WORKERS)]
    for links, mpath in zip(links_chunks, meta_files):
        Thread(target=main_parallel, args=(PATH_README, mpath, links, MIN_LIKES,)).start()

    # combine meta files
    meta = pd.concat([load_meta(mpath) for mpath in meta_files], axis=0).reset_index(drop=True)
    meta.to_parquet(Path('output', 'meta.parquet'), index=False)
