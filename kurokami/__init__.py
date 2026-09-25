'''Kurokami: an importable scraper/monitor API for the Carousell marketplace.

Public call surface:
  scrape(item, count=25, *, price_low=None, price_high=None, test=False,
         serialize=False, blacklist=None, home=HOME)
      -> pandas.DataFrame of parsed listings (raises, never writes output).
  request_page(url, item_limit)          -> BeautifulSoup page (selenium).
  parse_info(item_div, home=HOME)        -> parsed item dict.
  parse_items(item_divs, blacklist, item_limit, home=HOME) -> list of dicts.
  find_item_divs(soup)                   -> listing divs from a parsed page.
  new_rows(prev_df, new_df)              -> right_only (new) rows as lists.
  main(options=None)                     -> CLI/server entry, writes CSV,
      returns list-of-lists; args i, n, o, t, s, c (ph/pl optional).

Exceptions: KurokamiError, NoResultsError, NoValidItemsError.
'''

from .browser import request_page
from .cli import main
from .core import (
    build_search_url,
    detect_item_div_class,
    find_item_divs,
    is_blacklisted,
    load_blacklist,
    load_soup_snapshot,
    new_rows,
    parse_info,
    parse_items,
    save_soup_snapshot,
    scrape,
)
from .exceptions import KurokamiError, NoResultsError, NoValidItemsError

__all__ = [
    "KurokamiError",
    "NoResultsError",
    "NoValidItemsError",
    "build_search_url",
    "detect_item_div_class",
    "find_item_divs",
    "is_blacklisted",
    "load_blacklist",
    "load_soup_snapshot",
    "main",
    "new_rows",
    "parse_info",
    "parse_items",
    "request_page",
    "save_soup_snapshot",
    "scrape",
]