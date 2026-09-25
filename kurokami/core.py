'''
Author: Andrew Higgins
https://github.com/speckly

Two parse modes only differs in item divs 2nd a
Structure of Carousell HTML FORMAT 1 (parse_mode 1):
body > find main > 1st div > 1st div > divs of items
    in divs of items > parents of each item
        parent > 1st div > 1st a is seller, 2nd a is item page
            in 1st a: 2nd div > p is seller name, > div > p is time posted
            in 2nd a: 2nd div > p is item name but with ... if too long, directly under 2nd a first p is price, 2nd p is condition
        parent > 2nd div > button > span is number of likes
total 24 or 25 results loaded once.

Structure of Carousell HTML FORMAT 2 (parse_mode 2, found in legacy):
body > find main > 1st div > 1st div > divs of items
    in divs of items > parents of each item
        parent > 1st div > 1st a is seller, 2nd a is item page
            in 1st a: 2nd div > p is seller name, > div > p is time posted
            in 2nd a: 1st p is FULl NAME, 2nd p is price, 3rd p is description, 4th p is condition
        parent > 2nd div > button > span is number of likes
total 24 or 25 results loaded once.

body > find main > div > button to view more
view more button loads on top of existing, so can prob spam view more then gather all items at once
MAY NOT BE FIRST DIV! Temp workaround is to get class name of the correct item divs

My way (modified 1 here):
.asm-browse-listings > div > div > div of item > div with testid > div of item stripped
'''

import os
import pickle
import re
import urllib

import pandas as pd

from .browser import request_page
from .exceptions import NoResultsError, NoValidItemsError

HOME = 'https://carousell.com.hk'
LISTINGS_SELECTOR = '.asm-browse-listings'
ITEM_DIV_SELECTOR = LISTINGS_SELECTOR + ' > div > div > div > div > div'
SNAPSHOT_PATH = "utils/soup.pkl"
DEFAULT_BLACKLIST_PATH = "utils/blacklist.txt"
# Expected failures on ads or malformed items while parsing.
PARSE_EXCEPTIONS = (IndexError, ValueError, AttributeError)

NO_RESULTS_MSG = 'The search has returned no result or serialized file missing.'
NO_VALID_ITEMS_MSG = 'Parsing failed to find any valid items.'


def load_blacklist(filepath=DEFAULT_BLACKLIST_PATH):
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        # Read lines, strip whitespace, and ignore empty lines
        return [line.strip().lower() for line in f if line.strip()]


def is_blacklisted(item_name, blacklist):
    item_name_lower = item_name.lower()
    for word in blacklist:
        if word in item_name_lower:
            return True
    return False


def parse_info(item_div, home=HOME):
    """Author: Andrew Higgins
    https://github.com/speckly

    Parses the item_div and returns the list of items
    """
    a = item_div.find_all('a', recursive=False)
    if len(a) < 2:
        raise ValueError("Div does not contain expected seller and item links.")

    seller_divs = a[0].find_all('div', recursive=False)[1]
    item_p = a[1].find_all('p', recursive=False)
    img = item_div.find('img')
    item_url = home+a[1]['href']
    return {'uid': re.search(r"\/p\/[^\/]+-(\d+)", item_url).group(1),
            'seller_name': seller_divs.p.get_text(),
            'price': re.findall(r"FREE|\$\d{0,3},?\d+\.?\d{,2}", a[1].get_text()),
            'time_posted': seller_divs.div.p.get_text(),  # Attempt to get absolute datetime?
            'condition': item_p[1].get_text() if len(item_p) > 1 else "N/A",
            'item_name': item_p[0].get_text(strip=True),
            'item_url': item_url,
            'item_img': img['src'] if img else None,
            'seller_url': home+a[0]['href'],
            }  # 0 is discounted price, 1 is original price, if applicable


def build_search_url(item, price_low=None, price_high=None, home=HOME):
    subdirs = f'/search/{urllib.parse.quote(item)}'

    params = {
        'addRecent': 'false',
        'canChangeKeyword': 'false',
        'includeSuggestions': 'false',
        'sort_by': '3'
    }
    if price_low:
        params['price_start'] = price_low
    if price_high:
        params['price_end'] = price_high

    return f'{home}{subdirs}?{urllib.parse.urlencode(params)}'


def load_soup_snapshot(path=SNAPSHOT_PATH):
    with open(path, "rb") as f:
        return pickle.load(f)


def save_soup_snapshot(soup, path=SNAPSHOT_PATH):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(soup, f)


def detect_item_div_class(soup, selector=ITEM_DIV_SELECTOR):
    """Extracts the class name of the listing item divs from a parsed page."""
    if soup is None:
        raise NoResultsError(NO_RESULTS_MSG)
    listings = soup.find(class_="asm-browse-listings")
    if listings is None:
        raise NoResultsError(NO_RESULTS_MSG)
    node = listings.select_one(selector)
    if node is None or "class" not in node.attrs:
        raise NoResultsError(NO_RESULTS_MSG)
    return node['class']


def find_item_divs(soup):
    return soup.find_all('div', class_=detect_item_div_class(soup))


def parse_items(item_divs, blacklist, item_limit, home=HOME):
    """Parses listing divs into dicts, applying the blacklist, keeping the
    first row per uid, and stopping at item_limit valid items. Raises
    NoValidItemsError if none survive."""
    items = []
    seen_uids = set()
    for item_div in item_divs:
        try:
            item_data = parse_info(item_div, home)
        except PARSE_EXCEPTIONS:
            continue  # Skip advertisements or malformed items
        if (is_blacklisted(item_data['item_name'], blacklist)
                or is_blacklisted(item_data['seller_name'], blacklist)):
            continue
        if item_data['uid'] in seen_uids:
            continue
        seen_uids.add(item_data['uid'])
        items.append(item_data)
        if len(items) >= item_limit:
            break
    if not items:
        raise NoValidItemsError(NO_VALID_ITEMS_MSG)
    return items


def make_valid_counter(blacklist):
    """Builds a count_valid(soup) closure for request_page.

    Returns how many distinct uids on a parsed page survive parsing and the
    blacklist, so the browser loop keeps loading until item_limit useful
    results are in sight instead of stopping at a raw div count."""
    seen = set()

    def _count_valid(soup):
        try:
            item_divs = find_item_divs(soup)
        except NoResultsError:
            return len(seen)
        for item_div in item_divs:
            try:
                item_data = parse_info(item_div)
            except PARSE_EXCEPTIONS:
                continue  # Skip advertisements or malformed items
            if (is_blacklisted(item_data['item_name'], blacklist)
                    or is_blacklisted(item_data['seller_name'], blacklist)):
                continue
            seen.add(item_data['uid'])
        return len(seen)

    return _count_valid


async def scrape(item, count=25, *, price_low=None, price_high=None,
                 test=False, serialize=False, blacklist=None, home=HOME):
    """High-level search that returns a DataFrame of parsed listings.

    Uses utils/soup.pkl when test=True, otherwise drives a fresh browser
    session (and serializes the page when serialize=True). Raises
    NoResultsError / NoValidItemsError instead of writing output.
    """
    if blacklist is None:
        blacklist = load_blacklist()
    if test:
        soup = load_soup_snapshot()
    else:
        soup = await request_page(build_search_url(item, price_low, price_high, home), count,
                                  count_valid=make_valid_counter(blacklist))
        if serialize:
            save_soup_snapshot(soup)
    return pd.DataFrame(parse_items(find_item_divs(soup), blacklist, count, home))


COMPARE_COLUMNS = ["seller_name", "price", "time_posted", "condition",
                   "item_name", "item_url", "item_img", "seller_url"]


def new_rows(prev_df, new_df):
    """Right-only rows of a uid outer join as list-of-lists (new listings).

    Matches the CLI's -c behaviour: rows present in new_df but not prev_df.
    """
    prev_df = prev_df.copy()
    new_df = new_df.copy()
    prev_df['uid'] = prev_df['uid'].astype(str)
    new_df['uid'] = new_df['uid'].astype(str)
    right_only = pd.merge(prev_df, new_df, on='uid', how="outer", indicator='ind').query('ind == "right_only"')
    right_only.drop(columns=["ind"] + [col + "_x" for col in COMPARE_COLUMNS])
    return right_only.values.tolist()  # consider using dict?