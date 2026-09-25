'''Command-line front end for the kurokami library.

Preserves the documented `python kurokami.py` flows: interactive prompts,
-t/-s parse modes, utf-8-sig CSV output, and sys.exit(1) on no-results.
The dict form of main() is the server-side API used by the (future) bot,
with args i, n, o, t, s, c (ph/pl accepted as optional price bounds).
'''

import argparse
import os
import pickle
import re
import sys
from datetime import datetime
from typing import Union

import pandas as pd

from .browser import request_page
from .core import (
    NO_RESULTS_MSG,
    NO_VALID_ITEMS_MSG,
    SNAPSHOT_PATH,
    build_search_url,
    detect_item_div_class,
    load_blacklist,
    make_valid_counter,
    new_rows,
    parse_items,
    save_soup_snapshot,
)
from .exceptions import NoResultsError, NoValidItemsError

FILE_REG = r'^[A-Za-z0-9_\-]+\.csv$'


def default_output(item):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    clean_item_name = item.replace(" ", "_")
    return os.path.join("output", f"{timestamp}-{clean_item_name}.csv")


def _safe_print(text):
    """Print text without crashing on consoles that cannot encode it."""
    try:
        print(text)
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or "utf-8"
        sys.stdout.write(text.encode(encoding, errors="replace").decode(encoding) + "\n")


def build_parser():
    ps = argparse.ArgumentParser()
    ps.add_argument('-i', '--item', type=str, help='Name of the item to scrape')
    ps.add_argument('-n', '--number', type=int, help='Number of items to scrape')
    ps.add_argument('-o', '--output', type=str,
        help='CSV file to write out to, defaults to timestamped')
    ps.add_argument('-t', '--test', action='store_true',
        help=r'''For debugging of parsers which could break often due to the changing structure,
        using a snapshot of a bs4 object while overriding these flags with the respective values: -i shirakami fubuki -n 10''')
    ps.add_argument('-s', '--serialize', action='store_true',
        help=r'''For debugging of parsers which could break often due to the changing structure,
        the BS4 object is serialised for fast access, must not have -t''')
    ps.add_argument('-c', '--compare', type=str,
        help='Name of a .csv file output from this program to compare with')
    ps.add_argument('-ph', '--price-high', type=int,
        help='Upper price limit')
    ps.add_argument('-pl', '--price-low', type=int,
        help='Lower price limit')
    return ps


async def main(options: Union[dict, None] = None):
    os.makedirs("output", exist_ok=True)
    blacklist = load_blacklist()
    """options keys: i (item), n (number/count), o (output), t (test), s (serialize), c (compare)"""
    if options is None:
        server_side = False
        args = build_parser().parse_args()

        if args.test:
            test = True
            item = 'test'
            item_limit = 10
            if args.item or args.number:
                print('Entered test mode, overriding some user provided arguments')
        else:
            test = False
            if args.item:
                item = args.item
            else:
                item = input('-i Item name: ')
            if args.number:
                item_limit = args.number
            else:
                while True:
                    inp = input('-n Number of items to scrape: ')
                    if inp.isdigit():
                        item_limit = int(inp)
                        break
                    print("Invalid integer")

        if args.output:
            output_file = args.output
        else:
            output_file = default_output(item)
        serialize = args.serialize
        compare_file = args.compare
        if compare_file:
            if not re.match(FILE_REG, args.compare):
                print(f"Invalid CSV file name {compare_file}. Please provide a name consisting of letters, numbers, underscores, and dashes, ending with .csv")
                sys.exit(1)
            elif not os.path.exists(compare_file):
                print(f"{compare_file} does not exist")
                sys.exit(1)
        price_high = args.price_high
        price_low = args.price_low

    else:  # Praying that this does not result in a SSRF, used in bot.py with no user inputs yet. Validate user inputs
        server_side = True
        item = options.get("i")
        output_file = options.get("o")
        item_limit = options.get("n", 25)
        if options.get("t"):
            test = True
            item = 'shirakami fubuki'
            item_limit = 10
        else:
            test = False
        serialize = options.get("s")
        compare_file = options.get("c")
        price_high = options.get("ph")
        price_low = options.get("pl")

    if not server_side:
        print("Author: Andrew Higgins")
        print("https://github.com/speckly")

    try:
        if not server_side:
            print(f'Retrieving search results for {item_limit} items on {item}...')
        if not test:
            url = build_search_url(item, price_low, price_high)
            if not server_side:
                print("Creating webdriver")
            search_results_soup = await request_page(url, item_limit=item_limit,
                                                     count_valid=make_valid_counter(blacklist))
            if not server_side:
                print(f'Target reached or button exhausted.')
            if serialize:
                save_soup_snapshot(search_results_soup)
                print(f"Serialized: -i {item}")
        else:
            with open(SNAPSHOT_PATH, "rb") as f:
                search_results_soup = pickle.load(f)
        # Strip down
        item_div_class = detect_item_div_class(search_results_soup)
        if not server_side:
            print(f'Detected item_divs class: {item_div_class}')
        item_divs = search_results_soup.find_all('div', class_=item_div_class)  # ads
        if not server_side:
            print(f'Found {len(item_divs)} potential listings. Parsing...')
    except NoResultsError:
        print(NO_RESULTS_MSG)
        sys.exit(1)
    except (AttributeError, FileNotFoundError):  # malformed soup or serialized file missing
        print(NO_RESULTS_MSG)
        sys.exit(1)

    try:
        items_list = parse_items(item_divs, blacklist, item_limit)
    except NoValidItemsError:
        print(NO_VALID_ITEMS_MSG)
        sys.exit(1)

    df = pd.DataFrame(items_list)
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    if not server_side:
        print(f'Results saved to {output_file}')

    if compare_file:
        if not server_side:
            print("Comparing resuls with given csv")
        prev_df = pd.read_csv(compare_file, encoding='utf-8-sig')
        rows = new_rows(prev_df, df)
        if not server_side and rows:
            _safe_print(f"The difference between the previous and this query is {rows}")
            print(f"There are {len(rows)} new listings")
        return rows
    return df.values.tolist()