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

from typing import Union
import pickle
import traceback
import argparse
import os
import asyncio
import re
import urllib
import sys
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from datetime import datetime

async def request_page(url, item_limit):
    """ Returns BeautifulSoup4 Objects (soup) based on item count """

    opts = Options()
    opts.add_argument("--log-level=3")
    # opts.add_argument("--headless") # Requires human verification
    opts.add_experimental_option('prefs', {'intl.accept_languages': 'en,en_US'})
    driver = webdriver.Chrome(options=opts)
    driver.minimize_window()

    driver.get(url)
    timeout = 10

    while True:
        current_items = driver.find_elements(By.CSS_SELECTOR, ".asm-browse-listings > div > div > div")
        
        if len(current_items) >= int(item_limit * 1.1):
            break
            
        try:
            next_page_btn = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, '//button[contains(text(), "Show more results")]')))  # wait max timeout sec for loading
            driver.execute_script("arguments[0].click();", next_page_btn)  # click the load more button through ads
            
            await asyncio.sleep(1.5) 
        except TimeoutException:
            print("Button not found, reached end of page or load more button not found.")
            break

    pg = driver.page_source
    driver.quit()
    return BeautifulSoup(pg, "html.parser")


def parse_info(item_div, home,):
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

async def main(options: Union[dict, None] = None):
    os.makedirs("output", exist_ok=True)
    """options keys: i (item), n (number/count), o (output), t (test), s (serialize), c (compare)"""
    if options is None:
        server_side = False
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
        args = ps.parse_args()

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
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            clean_item_name = item.replace(" ", "_")
            output_file = os.path.join("output", f"{timestamp}-{clean_item_name}.csv")
        serialize = args.serialize
        compare_file = args.compare
        if compare_file:
            if not re.match(file_reg, args.compare):
                print(f"Invalid CSV file name {compare_file}. Please provide a name consisting of letters, numbers, underscores, and dashes, ending with .csv")
                sys.exit(1)
            elif not os.path.exists(compare_file):
                print(f"{compare_file} does not exist")
                sys.exit(1)
        price_high = args.price_high
        price_low = args.price_low

    else: # Praying that this does not result in a SSRF, used in bot.py with no user inputs yet. Validate user inputs
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

    if not server_side:
        print("Author: Andrew Higgins")
        print("https://github.com/speckly")

    home = 'https://carousell.com.hk'
    subdirs = f'/search/{urllib.parse.quote(item)}'
    
    params = {
        'addRecent': 'false',
        'canChangeKeyword': 'false',
        'includeSuggestions': 'false',
        'sort_by': '3'
    }
    if price_low: params['price_start'] = price_low
    if price_high: params['price_end'] = price_high
    
    parameters = f"?{urllib.parse.urlencode(params)}"
    try:
        if not server_side:
            print(f'Retrieving search results for {item_limit} items on {item}...')
        if not test:
            if not server_side:
                print("Creating webdriver")
            search_results_soup = await request_page(home+subdirs+parameters, item_limit=item_limit)
            if not server_side:
                print(f'Target reached or button exhausted.')
            if serialize:
                os.makedirs("./utils", exist_ok=True)
                with open("./utils/soup.pkl", "wb") as f:
                    pickle.dump(search_results_soup, f)
                print(f"Serialized: -i {item}")
        else:
            with open("./utils/soup.pkl", "rb") as f:
                search_results_soup = pickle.load(f)
        # Strip down
        browse_listings_divs = search_results_soup.find(class_="asm-browse-listings")
        item_divs_class = browse_listings_divs.select_one('.asm-browse-listings > div > div > div > div > div')['class']
        if not server_side:
            print(f'Detected item_divs class: {item_divs_class}')
        item_divs = search_results_soup.find_all('div', class_=item_divs_class)  # ads
        if not server_side:
            print(f'Found {len(item_divs)} potential listings. Parsing...')
    except (AttributeError, FileNotFoundError):  # no item_divs at all
        print('The search has returned no result or serialized file missing.')
        sys.exit(1)

    items_list = []
    for item_div in item_divs:
        try:
            items_list.append(parse_info(item_div, home))
        except (IndexError, ValueError, AttributeError):
            continue # Skip advertisements or malformed items
        
        if len(items_list) >= item_limit:
            break
 
    if not items_list:
        print('Parsing failed to find any valid items.')
        sys.exit(1)
 
    df = pd.DataFrame(items_list)
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    if not server_side:
        print(f'Results saved to {output_file}')

    if compare_file:
        if not server_side:
            print("Comparing resuls with given csv")
        prev_df = pd.read_csv(compare_file, encoding='utf-8-sig')
        # df_standardized = df.iloc[:len([prev_df])] # cases where there might be extra old results appended to new df, remove these
        # new_rows = df_standardized[~df_standardized['uid'].isin(prev_df['uid'])]
        # right exclusive join, old.column != new.column so we drop all cols named x as its from left
        cols = ["seller_name","price","time_posted","condition","item_name","item_url","item_img","seller_url"]
        df['uid'] = df['uid'].astype(str)
        prev_df['uid'] = prev_df['uid'].astype(str)
        new_rows = pd.merge(prev_df, df, on='uid', how="outer", indicator='ind').query('ind == "right_only"')
        new_rows.drop(columns=["ind"]+[col + "_x" for col in cols])
        return new_rows.values.tolist() # consider using dict?
    return df.values.tolist()

if __name__ == "__main__":
    compare_results = asyncio.run(main())
    if compare_results:
        print(f"The difference between the previous and this query is {compare_results}")
        print(f"There are {len(compare_results)} new listings")
