'''Selenium side of kurokami: turns a search URL into a parsed BeautifulSoup
page, clicking "Show more results" until the target item count is reached
(minimum ~1.1x to account for ads/duplicates).'''

import asyncio

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException


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