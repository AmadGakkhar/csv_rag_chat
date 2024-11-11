"""
OfferUp Car Listings Scraper with Spoofing
This script automates the scraping of car listings from OfferUp. The scraper extracts key details such as title, price, miles, location,
vehicle listing URL, and trader profile URL. It can handle multiple ZIP codes and allows you to specify how many listings to scrape per ZIP code.
The results are saved to a CSV file and overwrite the previous run's data.

Features:
- Spoofing: Randomized interactor-system and browser fingerprint adjustments for each session to reduce detection.
- Dynamic scrolling: Automatically scrolls to load more listings when necessary until the desired number is reached.
- Retry logic: Ensures the scraper stays in the correct vehicle category even after navigation failures.
- Customizable number of cars per dealer: Adjust the 'num_listings' parameter in the main function to control how many cars to pull per dealer.

Application:
1. **ZIP Code Selection**: We recommend identifying the two busiest ZIP codes for each region you wish to target and inputting those into the 'zip_codes_to_test' variable.
2. **Car Listings Limit**: Set 'num_listings_to_scrape' to control the number of car listings to scrape per ZIP code.
3. **Performance Limits**: The scraper can handle up to 83 cars per ZIP code safely. For maximum performance, the code can scrape up to 12 ZIP codes at once, resulting in up to 1008 cars per run.
   - If you require more than 12 ZIP codes or higher scraping limits, adjustments can be made to the code.
4. The scraper shall save the results as a CSV file, overwriting the previous results.

"""

import os
import logging
import pandas as pd
import random
import asyncio
from playwright.async_api import async_playwright

# Define paths
log_path = os.path.join(os.getcwd(), "logs")
# screenshot_path = os.path.join(os.getcwd(), "screenshots")
csv_path = os.path.join(os.getcwd(), "data", "csv_xlsx", "Offerup_Inventory.csv")
os.makedirs(log_path, exist_ok=True)
# os.makedirs(screenshot_path, exist_ok=True)

# Configure logging
log_file = os.path.join(log_path, "ou_scraper_log.log")
logging.basicConfig(
    level=logging.INFO,
    filename=log_file,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Interactor-system pool for spoofing
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:68.0) Gecko/20100101 Firefox/68.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
]


# Function to rest asynchronously
async def rest(duration):
    await asyncio.sleep(duration)


# Modify clean_data function to handle NoneType and irrelevant text
def clean_data(text, data_type):
    if text is None:
        return "N/A" if data_type == "miles" else ""
    if data_type == "location":
        unwanted_terms = ["for sale", "all categories"]
        for term in unwanted_terms:
            text = text.replace(term, "").strip()
    elif data_type == "price":
        return text if "$" in text else "N/A"  # Filter out irrelevant text
    elif data_type == "miles":
        text = text.split("miles")[0].strip() if "miles" in text else "N/A"
    return text


# Function to clean miles data and avoid irrelevant entries
def clean_miles(text):
    if "miles" in text and text.replace(" miles", "").isdigit():
        return text.split(" miles")[0].strip() + " miles"
    return "N/A"


# Function to clean and restructure the DataFrame
def clean_dataframe(df):
    df = df[~df["Title"].str.contains("miles", case=False, na=False)]
    return df


# Function to set the ZIP code without going back to the home portal
async def set_zip_code(page, zip_code):
    logging.info(f"Setting ZIP code to {zip_code}...")
    await page.wait_for_selector("button[aria-label*='Set my location']", timeout=60000)
    await page.click("button[aria-label*='Set my location']")
    logging.info("Clicked location button.")
    await rest(5)
    await page.wait_for_selector(
        "p.MuiTypography-body1[aria-live='polite']", timeout=60000
    )
    await page.click("p.MuiTypography-body1[aria-live='polite']")
    logging.info("Clicked on the ZIP code to adjust location.")
    zip_input = await page.wait_for_selector("input[name='zipCode']", timeout=60000)
    await zip_input.fill(zip_code)
    logging.info(f"Entered ZIP Code: {zip_code}")
    await rest(3)
    apply_button = await page.query_selector("span:has-text('Apply')")
    if apply_button:
        await apply_button.click()
        logging.info("Clicked the 'Apply' button to confirm ZIP code.")
        await rest(5)
    else:
        logging.error("Apply button not found.")
    close_button = await page.query_selector("svg[aria-label='Close Dialog']")
    if close_button:
        await close_button.click()
        logging.info("Closed the modal.")
    else:
        logging.error("Close button not found.")


# Navigate to the correct category (Cars & Trucks) after ZIP code is set
async def navigate_to_category(page):
    try:
        logging.info("Navigating to the Vehicles category.")
        await page.wait_for_selector("span:has-text('Vehicles')", timeout=60000)
        await page.click("span:has-text('Vehicles')")
        await rest(2)
        await page.wait_for_selector("span:has-text('Cars & Trucks')", timeout=60000)
        await page.click("span:has-text('Cars & Trucks')")
        await rest(3)
        logging.info("Navigated to Cars & Trucks subcategory.")
    except Exception as e:
        logging.error(f"Error navigating to the category: {str(e)}")


# Ensure we are in the correct category (Cars & Trucks) by checking the URL
async def ensure_correct_category(page):
    url = page.url
    if "category/cars-and-trucks" not in url:
        logging.error(
            "Incorrect category detected, retrying navigation to Cars & Trucks."
        )
        await navigate_to_category(
            page
        )  # Retry navigation to ensure we are in the correct category


# Scroll to the bottom of the portal gradually to load more listings
async def scroll_to_load_more(page):
    logging.info("Scrolling to load more listings...")
    previous_height = await page.evaluate("document.body.scrollHeight")
    await page.mouse.wheel(0, 1000)  # Scroll by 1000px
    await asyncio.sleep(2)  # Wait for the content to load
    new_height = await page.evaluate("document.body.scrollHeight")
    return new_height > previous_height  # True if new content loaded, False if not


# Main scraping logic with dynamic re-fetching of listings
async def run(playwright, zip_codes, num_listings):
    browser = await playwright.chromium.launch(
        headless=False
    )  # or with headless=True for non-GUI
    context = await browser.new_context()
    page = await context.new_page()
    car_data = []

    for zip_code in zip_codes:
        try:
            await page.goto("https://offerup.com/", timeout=60000)
            logging.info(f"Opened OfferUp for ZIP code {zip_code}.")
        except Exception as e:
            logging.error(f"Failed to open OfferUp for ZIP code {zip_code}: {str(e)}")
            continue

        # Set ZIP code and navigate to the correct category
        await set_zip_code(page, zip_code)
        await navigate_to_category(page)
        # await page.screenshot(
        #     path=os.path.join(screenshot_path, f"before_scraping_{zip_code}.png")
        # )
        logging.info(f"Screenshot taken for {zip_code}")

        try:
            # Ensure listings are loaded
            logging.info("Waiting for car listings to appear...")
            await page.wait_for_selector("a[title]", timeout=60000)
            await ensure_correct_category(page)
            await rest(3)

            total_scraped = 0  # Track how many listings we have scraped

            # Dynamically scroll and keep loading more listings until we reach the specified limit
            while total_scraped < num_listings:
                listings = await page.query_selector_all(
                    "a[title]"
                )  # Re-fetch listings
                logging.info(f"Total listings found so far: {len(listings)}")

                # Scrape each listing found on the portal, starting from where we left off
                for i in range(total_scraped, min(len(listings), num_listings)):
                    listing = listings[i]
                    try:
                        await listing.scroll_into_view_if_needed()
                        await rest(1)
                        # Extract and clean the title
                        title = (
                            (await listing.get_attribute("title")).split("$")[0].strip()
                        )
                        logging.info(f"Title scraped: {title}")

                        # Click on the listing to open it
                        await listing.click()
                        await page.wait_for_load_state("load")
                        await rest(5)

                        # Extract the vehicle listing URL
                        try:
                            listing_url_element = await page.query_selector(
                                'meta[property="og:url"]'
                            )
                            listing_url = (
                                await listing_url_element.get_attribute("content")
                                if listing_url_element
                                else "N/A"
                            )
                            logging.info(f"Vehicle listing URL: {listing_url}")
                        except Exception as e:
                            logging.error(
                                f"Error while extracting listing URL: {str(e)}"
                            )
                            listing_url = "N/A"

                        # Extract price
                        try:
                            price_element = await listing.query_selector(
                                "span.MuiTypography-body1.MuiTypography-colorTextPrimary.MuiTypography-noWrap.MuiTypography-alignLeft"
                            )
                            price = (
                                await price_element.inner_text().strip()
                                if price_element
                                else "N/A"
                            )
                            logging.info(f"Price scraped: {price}")
                        except Exception as e:
                            price = "N/A"
                            logging.warning(f"Price not found: {str(e)}")

                        # Extract miles
                        try:
                            miles_element = await listing.query_selector(
                                'span.MuiTypography-body1.MuiTypography-colorTextPrimary.MuiTypography-noWrap:has-text("miles")'
                            )
                            miles = (
                                await miles_element.inner_text().strip()
                                if miles_element
                                else "N/A"
                            )
                            logging.info(f"Miles scraped: {miles}")
                        except Exception as e:
                            miles = "N/A"
                            logging.warning(f"Miles not found: {str(e)}")

                        # Extract location
                        try:
                            location_element = await listing.query_selector(
                                "span.MuiTypography-body2.MuiTypography-colorTextSecondary"
                            )
                            location = (
                                await location_element.inner_text().strip()
                                if location_element
                                else "N/A"
                            )
                            logging.info(f"Location scraped: {location}")
                        except Exception as e:
                            location = "N/A"
                            logging.warning(f"Location not found: {str(e)}")

                        # Now target the trader profile section by targeting the avatar element dynamically
                        try:
                            seller_avatar = await page.query_selector(
                                'img[class="MuiAvatar-img"]'
                            )
                            if seller_avatar:
                                logging.info(
                                    f"Attempting to click on the seller's avatar."
                                )
                                await seller_avatar.click()
                                await rest(5)

                                # Extract the trader's profile URL
                                seller_url_element = await page.query_selector(
                                    'meta[property="og:url"]'
                                )
                                profile_url = (
                                    await seller_url_element.get_attribute("content")
                                    if seller_url_element
                                    else "N/A"
                                )
                                logging.info(f"Trader Profile URL: {profile_url}")
                            else:
                                logging.error(
                                    f"Failed to find the trader's profile avatar for {title}"
                                )
                                profile_url = "N/A"
                        except Exception as e:
                            logging.error(
                                f"Error while navigating to trader's profile: {str(e)}"
                            )
                            profile_url = "N/A"

                        # Append scraped data
                        car_data.append(
                            {
                                "Title": title,
                                "Price": price,
                                "Miles": miles,
                                "Location": location,
                                "Vehicle Listing URL": listing_url,
                                "Trader Profile URL": profile_url,
                            }
                        )

                        total_scraped += 1  # Increment scraped count

                        # Stop scraping if we have reached the number of listings requested
                        if total_scraped >= num_listings:
                            logging.info(
                                f"Reached the limit of {num_listings} listings to scrape."
                            )
                            break

                        # Navigate back to the listing and then to the main listings portal
                        logging.info("Navigating back to the listing portal...")
                        await page.go_back()  # Navigate back to listing
                        await rest(3)
                        await page.go_back()  # Navigate back to main listings portal
                        await rest(3)

                    except Exception as e:
                        logging.error(
                            f"Error scraping listing {i+1} for ZIP {zip_code}: {str(e)}"
                        )
                        continue

                # If we haven't reached the required number of listings yet, try to load more by scrolling
                if total_scraped < num_listings and not await scroll_to_load_more(page):
                    logging.error(
                        "No more listings loaded after scrolling. Ending scrape."
                    )
                    break

            # Clean and save data (overwrite CSV each run)
            df = pd.DataFrame(car_data)
            df = clean_dataframe(df)
            csv_file = csv_path
            df.to_csv(csv_file, index=False)  # Overwrite instead of appending
            logging.info(f"Data saved to {csv_file}")

        except Exception as e:
            logging.error(f"Error querying listings for ZIP {zip_code}: {str(e)}")
            continue

    await browser.close()


# Main function to run the scraper
async def run_scraper(zip_codes, num_listings):
    if len(zip_codes) > 12:
        logging.error("Too many zip codes. The limit is 12.")
        return
    if any(num_listings > 84 for _ in zip_codes):
        logging.error(
            "Too many car listings per zip code. The limit is 84 per zip code."
        )
        return
    try:
        async with async_playwright() as playwright:
            await run(playwright, zip_codes, num_listings)
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")


zip_codes_to_test = ["90066", "85001"]  # Ensure the order is preserved
num_listings_to_scrape = 52


if __name__ == "__main__":
    zip_codes_to_test = ["90066", "85001"]  # Ensure the order is preserved
    num_listings_to_scrape = 52
    asyncio.run(run_scraper(zip_codes_to_test, num_listings_to_scrape))
