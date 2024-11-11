import streamlit as st
import asyncio
import logging
import random
from playwright.async_api import async_playwright
import os
import csv

prefinal_file_path = os.path.join(
    os.getcwd(), "data", "csv_xlsx", "Facebook_Marketplace_Inventory.csv"
)

locations = ["Los Angeles, California", "Newport Beach, California"]


# Define a function to write data to CSV
async def write_to_csv(data):
    if not os.path.exists(prefinal_file_path):
        with open(prefinal_file_path, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "title",
                    "price",
                    "listed_time",
                    "location",
                    "current_url",
                    "car_description_text",
                    "additional_details",
                ]
            )

    with open(prefinal_file_path, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(data)


# Setting up logging
log_file_path = os.path.join(os.getcwd(), "prefinal.log")
logging.basicConfig(
    filename=log_file_path,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# User-Agent Pool
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:68.0) Gecko/20100101 Firefox/68.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
]


# Function to wait for a specific element
async def wait_for_element(page, selector, timeout=5000):
    try:
        await page.wait_for_selector(selector, timeout=timeout)
        elements = await page.query_selector_all(selector)
        if elements:
            logging.info(
                f"Found {len(elements)} elements matching selector: {selector}"
            )
            return elements[0]
        else:
            logging.error(f"No elements found with selector {selector}")
            return None
    except Exception as e:
        logging.error(f"Error finding element with selector {selector}: {e}")
        return None


# Function to click on an item with retry logic
async def click_with_retry(element, retries=3):
    for attempt in range(retries):
        try:
            await element.click()
            return True
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} to click failed: {e}")
            await asyncio.sleep(1)
    return False


# Function to extract data from the listing page
async def extract_listing_data(page, data, location):
    title_elem = await wait_for_element(page, "h1 span")
    title = await title_elem.inner_text() if title_elem else "N/A"

    price_elem = await wait_for_element(page, "div.x1xmf6yo span")
    price = await price_elem.inner_text() if price_elem else "N/A"

    listed_time_elem = await wait_for_element(page, "div.x1yztbdb span")
    listed_time = await listed_time_elem.inner_text() if listed_time_elem else "N/A"

    location_elem = await wait_for_element(page, 'a[href^="/marketplace/"] span')
    location_text = await location_elem.inner_text() if location_elem else "N/A"

    data.append(
        {
            "location": location,
            "title": title,
            "price": price,
            "listed_time": listed_time,
            "location_text": location_text,
        }
    )

    price(data)


# Function to scroll down the page to load more items
async def scroll_to_load_more(page, scroll_pause_time=2, max_scrolls=3):
    previous_height = await page.evaluate("document.body.scrollHeight")

    for _ in range(max_scrolls):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
        await asyncio.sleep(5)
        await asyncio.sleep(scroll_pause_time)

        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == previous_height:
            logging.info("No more items loaded.")
            break
        previous_height = new_height

    await page.evaluate("window.scrollTo(0, 0);")
    await asyncio.sleep(2)


# Main scraping function
async def scrape_facebook_marketplace(locations, stop_event):
    data = []
    async with async_playwright() as p:
        logging.info("Launching browser...")
        user_agent = random.choice(USER_AGENTS)
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(user_agent=user_agent)
        page = await context.new_page()

        await facebook_login(page, FB_EMAIL, FB_PASSWORD)
        await asyncio.sleep(5)

        for location in locations:
            if stop_event.is_set():
                logging.info("Stopping the scraping process.")
                break

            logging.info(f"Processing location: {location}")
            await page.goto(f"https://www.facebook.com/marketplace/{location}")
            await asyncio.sleep(6)
            await search_cars_in_search_bar(page)
            await asyncio.sleep(3)
            await scroll_to_load_more(page)
            await process_listings(page, location, data)

        await browser.close()


# Function to search for cars in the search bar
async def search_cars_in_search_bar(page):
    try:
        logging.info("Searching for cars in the search bar.")
        search_bar = await page.query_selector(
            'input[placeholder="Search Marketplace"][type="search"]'
        )
        if search_bar:
            logging.info("Found the search bar, entering 'Cars'.")
            await search_bar.fill("Cars")
            await asyncio.sleep(2)
            await search_bar.press("Enter")
            await page.wait_for_load_state("networkidle")
            logging.info("Search for cars submitted.")
        else:
            logging.warning("Search bar not found.")
    except Exception as e:
        logging.error(f"Error while searching for cars: {e}")


async def process_listings(page, location, data):
    item_divs = await page.query_selector_all(
        "div.x9f619.x78zum5.x1r8uery.xdt5ytf.x1iyjqo2.xs83m0k.x1e558r4.x150jy0e.x1iorvi4.xjkvuk6.xnpuxes.x291uyu.x1uepa24"
    )

    logging.info(f"Total listings found: {len(item_divs)}")

    for idx, item_div in enumerate(item_divs):
        try:
            logging.info(f"Processing listing {idx + 1} for {location}")
            await asyncio.sleep(1)

            await item_div.click()
            await asyncio.sleep(5)
            await page.wait_for_load_state("networkidle")

            await page.wait_for_selector(
                "div.x78zum5.xdt5ytf.x1iyjqo2.x1n2onr6", timeout=5000
            )

            current_url = page.url

            title_div = await page.query_selector(
                "div.xyamay9.x1pi30zi.x18d9i69.x1swvt13 h1 span"
            )
            title = await title_div.inner_text() if title_div else "Title not found"

            price_div = await page.query_selector("div.x1xmf6yo > div > span")
            price = await price_div.inner_text() if price_div else "Price not found"

            time_div = await page.query_selector(
                'div.x9f619 div.x1yztbdb div span[aria-hidden="true"]'
            )
            listed_time = await time_div.inner_text() if time_div else "Time not found"

            location_div = await page.query_selector("div.x9f619 div.x1yztbdb a span")
            location = (
                await location_div.inner_text()
                if location_div
                else "Location not found"
            )

            details_divs = await page.query_selector_all(
                "div.xod5an3 div.x78zum5.xdj266r.x1emribx.xat24cr.x1i64zmx.x1y1aw1k.x1sxyh0.xwib8y2.xurb0ha"
            )

            additional_details = ""
            for detail_div in details_divs:
                detail_text_span = await detail_div.query_selector("span")
                detail_text = (
                    await detail_text_span.inner_text() if detail_text_span else None
                )
                if detail_text:
                    additional_details = additional_details + detail_text

            car_description = await page.query_selector(
                "div.xod5an3 div.xexx8yu.x1pi30zi.x18d9i69.x1swvt13"
            )

            see_more_button = await car_description.query_selector('div[role="button"]')
            if see_more_button:
                await see_more_button.click()

            description_text = await car_description.query_selector("span")
            car_description_text = (
                await description_text.inner_text() if description_text else None
            )

            row_data = [
                title,
                price,
                listed_time,
                location,
                current_url,
                car_description_text,
                additional_details,
            ]

            logging.info(f"Title: {title}")
            logging.info(f"Price: {price}")
            logging.info(f"Listed Time: {listed_time}")
            logging.info(f"Location: {location}")
            logging.info(f"Seller Description: {car_description_text}")
            for detail in additional_details:
                logging.info(f" - {detail}")

            await write_to_csv(row_data)

            await page.go_back()
            await page.wait_for_load_state("networkidle")

        except Exception as e:
            logging.error(f"Error processing listing {idx + 1} for {location}: {e}")
            await asyncio.sleep(2)


# Constants for Facebook login
FB_EMAIL = os.getenv("FB_EMAIL")
FB_PASSWORD = os.getenv("FB_PASSWORD")


# Function to log in to Facebook
async def facebook_login(page, email, password):
    await page.goto("https://www.facebook.com")
    await page.fill("input[name='email']", email)
    await page.fill("input[name='pass']", password)
    await page.click("button[name='login']")
    await page.wait_for_load_state("networkidle")
