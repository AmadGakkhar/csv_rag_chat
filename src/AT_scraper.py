import asyncio
import csv
import logging
import os
import pandas as pd
import psutil
import shutil
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    ElementClickInterceptedException,
)
import time

# Define interactor systems for rotation
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
]
# Set up logging
# log_file_path = '/Users/kingcarlos/Imperium/Lead_Generator/Car_Dealerships_LG/Logs/proxy_test_logs.log'
log_file_path = os.path.join(os.getcwd(), "experiments.log")
output_file = os.path.join(os.getcwd(), "proxy_test_dealers.csv")
# driver_path = shutil.which("chromedriver")
driver_path = "/Users/kingcarlos/Downloads/chromedriver_mac64 2/chromedriver"

extension_path = os.path.join(os.getcwd(), "pphgdbgldlmicfdkhondlafkiomnelnk.crx")


csv_file = os.path.join(os.getcwd(), "proxy_test_dealers.csv")
# output_csv_file = '/Users/kingcarlos/Imperium/Lead_Generator/Car_Dealerships_LG/proxy_test_inventory.csv'
output_csv_file = os.path.join(
    os.getcwd(), "data", "csv_xlsx", "Autotraders_Inventory.csv"
)
# driver_path = '/Users/kingcarlos/Imperium/Lead_Generator/Car_Dealerships_LG/chromedriver-mac-x64/chromedriver'
# log_file_path = '/Users/kingcarlos/Imperium/Lead_Generator/Car_Dealerships_LG/Logs/proxy_test_logs_codeblock2.log'
log_file_path_2 = os.path.join(os.getcwd(), "experiments2.log")


logging.basicConfig(
    filename=log_file_path, level=logging.INFO, format="%(asctime)s - %(message)s"
)


async def rest(duration):
    logging.info(f"Resting for {duration:.2f} seconds")
    await asyncio.sleep(duration)


def get_scroll_height(driver):
    return driver.execute_script("return document.body.scrollHeight")


async def change_zip_code(driver, zip_code):
    try:
        logging.info(
            f"The zip code which I am processing in change_zip_code: {zip_code}"
        )
        # Open the location modal and enter the new ZIP code
        map_marker_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable(
                (
                    By.CSS_SELECTOR,
                    'span.text-bold.text-size-300.text-link span[role="button"]',
                )
            )
        )
        driver.execute_script("arguments[0].click();", map_marker_button)
        await rest(2)

        zip_input = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.ID, "filterLocationZipInput"))
        )
        driver.execute_script("arguments[0].value = '';", zip_input)
        await rest(2)
        driver.execute_script(f"arguments[0].value = '{zip_code}';", zip_input)
        # zip_input.clear()
        await rest(1)
        # zip_input.send_keys(zip_code)
        logging.info(f"Entered ZIP code: {zip_code}")
        await rest(2)

        submit_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn.btn-primary"))
        )
        driver.execute_script("arguments[0].click();", submit_button)
        await rest(5)
        # logging.info(f'The zip code which I am processing in change_zip_code: {zip_code}')
        # # Open the location modal and enter the new ZIP code
        # map_marker_button = WebDriverWait(driver, 20).until(
        #     EC.element_to_be_clickable((By.CSS_SELECTOR, 'span.text-bold.text-size-300.text-link span[role="button"]'))
        # )
        # driver.execute_script("arguments[0].click();", map_marker_button)
        # # await rest(2)
        # time.sleep(2)

        # zip_input = WebDriverWait(driver, 20).until(
        #     EC.element_to_be_clickable((By.ID, 'filterLocationZipInput'))
        # )

        # logging.info(f'Before the input field is being cleared')
        # zip_input.clear()
        # logging.info(f'After the input field is being cleared')
        # # await rest(1)
        # time.sleep(1)
        # zip_input.send_keys(zip_code)
        # logging.info(f"Entered ZIP code: {zip_code}")
        # # await rest(2)
        # time.sleep(2)

        # submit_button = WebDriverWait(driver, 20).until(
        #     EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.btn.btn-primary'))
        # )
        # driver.execute_script("arguments[0].click();", submit_button)
        # # await rest(5)
        # time.sleep(5)
    except Exception as e:
        logging.error(f"Failed to change ZIP code to {zip_code}: {e}")


async def scrape_dealer_data(
    zip_codes, output_file=output_file, driver_path=driver_path
):
    with open(output_file, "w", newline="", encoding="utf-8") as csv_file:
        fieldnames = [
            "Dealership Name",
            "Address",
            "Phone Number",
            "Website URL",
            "Dealer Details URL",
            "Miles",
        ]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

    logging.info(f"The zip code I am getting: {zip_codes}")

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
    # Load the VPN extension
    # chrome_options.add_extension(extension_path)
    service = Service(driver_path)

    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        existing_dealers = set()
        for zip_code in zip_codes:
            logging.info(f"The zip code which I am processing: {zip_code}")
            logging.info(f"Processing ZIP code: {zip_code}")
            driver.get("https://www.autotrader.com/car-dealers")
            await rest(2)
            await change_zip_code(driver, zip_code)
            logging.info(f"Completed processing for ZIP code: {zip_code}")

            waypoint_divs = WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, 'div[data-cmp="delayedImpressionWaypoint"]')
                )
            )
            dealer_count = 0

            with open(output_file, "a", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                for div in waypoint_divs:
                    if dealer_count >= 3:
                        break
                    dealership_name = div.find_element(
                        By.CSS_SELECTOR, "h2.css-1jcwmgy"
                    ).text
                    address = div.find_element(
                        By.CSS_SELECTOR, 'div[data-testid="address"] span'
                    ).text
                    phone_number = div.find_element(
                        By.CSS_SELECTOR, 'span[data-testid="phoneNumber"]'
                    ).text
                    miles = div.find_element(
                        By.CSS_SELECTOR, 'span[data-testid="mileageContent"]'
                    ).text
                    try:
                        website_url = div.find_element(
                            By.CSS_SELECTOR, 'div[data-cy="secondaryBtn"] a'
                        ).get_attribute("href")
                    except NoSuchElementException:
                        website_url = "URL Not Found"
                        logging.info(
                            f"Website URL not found for dealer at '{zip_code}'"
                        )

                    dealer_details_url = div.find_element(
                        By.CSS_SELECTOR, 'a[href*="car-dealers"]'
                    ).get_attribute("href")

                    if (dealership_name, address) not in existing_dealers:
                        writer.writerow(
                            {
                                "Dealership Name": dealership_name,
                                "Address": address,
                                "Phone Number": phone_number,
                                "Website URL": website_url,
                                "Dealer Details URL": dealer_details_url,
                                "Miles": miles,
                            }
                        )
                        existing_dealers.add((dealership_name, address))
                        dealer_count += 1
    finally:
        driver.quit()
        df = pd.read_csv(output_file)
        df.drop_duplicates(
            subset=["Dealership Name", "Address"], keep="first", inplace=True
        )
        df.to_csv(output_file, index=False)
        logging.info("Duplicates removed from CSV and file saved.")

        # Confirming the output CSV path
        if os.path.isfile(output_file):
            logging.info(
                f"File '{output_file}' exists and has been successfully overwritten."
            )
        else:
            logging.warning(
                f"File '{output_file}' could not be found; please confirm file path."
            )

        def kill_chrome_processes():
            for proc in psutil.process_iter():
                try:
                    if proc.name() in ["chrome", "chromedriver"]:
                        proc.kill()
                        logging.info(
                            f"Terminated process {proc.name()} with PID {proc.pid}"
                        )
                except (
                    psutil.NoSuchProcess,
                    psutil.AccessDenied,
                    psutil.ZombieProcess,
                ):
                    logging.warning(f"Failed to terminate process with PID {proc.pid}")

        kill_chrome_processes()
        logging.info("All Chrome processes terminated after CSV operations.")


## Second Cell Functions


def get_scroll_height(driver):
    return driver.execute_script("return document.body.scrollHeight")


def split_vehicle_title(title):
    parts = title.split()
    if len(parts) >= 3:
        condition = parts[0]
        model_year = parts[1]
        item = " ".join(parts[2:])
        return condition, model_year, item
    else:
        logging.warning(f"Unexpected title format: '{title}'")
        return "Unknown", "Unknown", title


def extract_mileage(mileage_text):
    mileage_parts = mileage_text.split(" ")
    mileage = "".join(filter(str.isdigit, mileage_parts[0]))
    return mileage


def extract_vin(driver, max_scroll_attempts=10):
    vin = None
    last_height = get_scroll_height(driver)
    scroll_attempts = 0

    while scroll_attempts < max_scroll_attempts:
        try:
            vin_element = driver.find_element(
                By.XPATH,
                "//div[contains(@class, 'text-gray-dark')]//span[contains(text(), 'VIN')]",
            )
            vin_text = vin_element.text
            vin = vin_text.split(":")[1].strip()
            logging.info(f"VIN found: {vin}")
            return vin
        except NoSuchElementException:
            scroll_attempts += 1
            driver.execute_script("window.scrollBy(0, 500);")
            time.sleep(2)
            new_height = get_scroll_height(driver)
            if new_height == last_height:
                logging.warning("Reached the bottom of the page; VIN not found.")
                break
            last_height = new_height
    return vin


def extract_price_and_msrp(price_text):
    msrp = "N/A"
    price = price_text.strip()
    if "MSRP" in price_text:
        msrp_lines = price_text.splitlines()
        for i, line in enumerate(msrp_lines):
            if "MSRP" in line and i > 0:
                price = msrp_lines[i - 1].strip()
                msrp = line.replace("MSRP", "").strip()
                break
    return price, msrp


async def get_vehicle_links(dealer_url, driver_path, max_vehicles=3):
    vehicle_links = []
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
    # Load the VPN extension
    # chrome_options.add_extension(extension_path)
    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        driver.get(dealer_url)
        await rest(3)

        while len(vehicle_links) < max_vehicles:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, 'div[data-cmp="delayedImpressionWaypoint"]')
                )
            )

            waypoint_divs = driver.find_elements(
                By.CSS_SELECTOR, 'div[data-cmp="delayedImpressionWaypoint"]'
            )
            new_links = [
                div.find_element(By.TAG_NAME, "a").get_attribute("href")
                for div in waypoint_divs
            ]

            vehicle_links.extend(new_links[: max_vehicles - len(vehicle_links)])

            # Break loop if vehicle links exceed or reach the target
            if len(vehicle_links) >= 5:
                break

            try:
                next_button = driver.find_element(
                    By.XPATH, '//button[contains(@aria-label, "Next Page")]'
                )
                next_button.click()
                await rest(3)
            except (NoSuchElementException, ElementClickInterceptedException):
                break

    except Exception as e:
        logging.error(f"Error fetching vehicle links: {e}")

    finally:
        driver.quit()

    return vehicle_links[:max_vehicles]


async def extract_vehicle_data(
    vehicle_links, dealer_data, driver_path, output_csv_file
):
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    try:
        with open(output_csv_file, "a", newline="", encoding="utf-8") as file:
            fieldnames = [
                "Dealership Name",
                "Address",
                "Phone Number",
                "Website URL",
                "Vehicle Links",
                "Dealer Details URL",
                "Item",
                "Model Year",
                "Condition",
                "Miles",
                "Engine Description",
                "MPG",
                "Transmission",
                "Drive Type",
                "Exterior Color",
                "Interior Color",
                "Price",
                "MSRP",
                "VIN",
            ]
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            for link in vehicle_links:
                logging.info(f"These are the links: {link}")
                driver.get(link)
                await rest(3)

                # Initialize vehicle_data with default keys
                vehicle_data = {key: "" for key in fieldnames}
                vehicle_data.update(
                    {
                        "Dealership Name": dealer_data["Dealership Name"],
                        "Address": dealer_data["Address"],
                        "Phone Number": dealer_data["Phone Number"],
                        "Website URL": dealer_data["Website URL"],
                        "Vehicle Links": link,
                        "Dealer Details URL": dealer_data["Dealer Details URL"],
                    }
                )

                logging.info(f"This is the vehicle data list: {vehicle_data}")

                # logging.info(f'Here is the content of the page: {driver.page_source}')
                # break

                list_items = WebDriverWait(driver, 10).until(
                    EC.visibility_of_all_elements_located(
                        (By.XPATH, '//ul[@data-cmp="listColumns"]/li')
                    )
                )

                # list_items = driver.find_elements(By.XPATH, '//ul[@data-cmp="listColumns"]/li')
                # vehicle_data = {}
                for item in list_items:

                    if "Exterior" in item.text:
                        vehicle_data["Exterior Color"] = item.text
                    elif "Interior" in item.text:
                        vehicle_data["Interior Color"] = item.text

                    logging.info(f"Here is the Item which i am getting: {item.text}")

                # Extracting the Drive Type
                # try:
                #     drive_type_element = driver.find_element(By.XPATH, 'your_xpath_for_drive_type')
                #     vehicle_data["Drive Type"] = drive_type_element.text

                # except NoSuchElementException:
                #     logging.info(f"Drive Type not found for '{link}' - Skipped.")

                # Extracting the Title and related data
                try:
                    title_element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, 'h1[data-cmp="heading"]')
                        )
                    )
                    title_text = title_element.text.strip()
                    condition, model_year, item = split_vehicle_title(title_text)
                    vehicle_data["Item"] = item
                    vehicle_data["Model Year"] = model_year
                    vehicle_data["Condition"] = condition
                except NoSuchElementException:
                    logging.warning(
                        f"Title element not found for '{link}'; using default values."
                    )
                    vehicle_data["Item"] = "Unknown Item"
                    vehicle_data["Model Year"] = "Unknown Year"
                    vehicle_data["Condition"] = "Unknown Condition"
                except Exception as e:
                    logging.warning(f"Failed to extract title data for '{link}': {e}")

                # Extracting the VIN
                try:
                    vin = extract_vin(driver, max_scroll_attempts=10)
                    vehicle_data["VIN"] = vin
                    if vin:
                        logging.info(f"VIN found: {vin}")
                    else:
                        logging.warning(f"VIN not found for '{link}'.")
                except Exception as e:
                    logging.warning(f"Failed to extract VIN for '{link}': {e}")

                # Extracting other vehicle details
                data_items = driver.find_elements(
                    By.CSS_SELECTOR, 'ul[data-cmp="listColumns"] li'
                )
                for item in data_items:

                    try:
                        title = item.find_element(
                            By.CSS_SELECTOR, "div[aria-label]"
                        ).get_attribute("aria-label")
                        value = item.find_element(
                            By.CLASS_NAME, "display-flex"
                        ).text.strip()

                        if "MILEAGE" in title:
                            vehicle_data["Miles"] = extract_mileage(value)
                        elif "ENGINE_DESCRIPTION" in title:
                            vehicle_data["Engine Description"] = value
                        elif "MPG" in title:
                            vehicle_data["MPG"] = value
                        elif "TRANSMISSION" in title:
                            vehicle_data["Transmission"] = value
                        elif "DRIVE TYPE" in title:
                            vehicle_data["Drive Type"] = value
                        # elif "Exterior" in title:
                        #     vehicle_data["Exterior Color"] = value
                        # elif "Interior" in title:
                        #     vehicle_data["Interior Color"] = value
                        # elif "Seats" in v_value:
                        #     vehicle_data["Interior Color"] = v_value
                    except NoSuchElementException:
                        logging.info(
                            f"Missing data item for '{link}': {title} - Skipped."
                        )

                # Price extraction
                try:
                    price_element = driver.find_element(
                        By.CSS_SELECTOR,
                        'div[data-cmp="pricing"] span[data-cmp="firstPrice"]',
                    )
                    vehicle_data["Price"], vehicle_data["MSRP"] = (
                        extract_price_and_msrp(price_element.text)
                    )
                except NoSuchElementException:
                    logging.info(f"Price data not found for {link}")
                    vehicle_data["Price"] = "N/A"
                    vehicle_data["MSRP"] = "N/A"

                # Write data to CSV
                writer.writerow(vehicle_data)
                logging.info(f"Vehicle data saved for {link}")

    except Exception as e:
        logging.error(f"Error during data extraction for vehicles: {e}")
    finally:
        driver.quit()


# Terminate lingering Chrome processes
def kill_chrome_processes():
    for proc in psutil.process_iter():
        try:
            if proc.name() in ["chrome", "chromedriver"]:
                proc.kill()
                logging.info(f"Terminated {proc.name()} with PID {proc.pid}")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            logging.warning("Failed to terminate process")


async def open_dealer_urls(csv_file, output_csv_file, driver_path):
    # Open the CSV for writing headers
    with open(output_csv_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "Dealership Name",
                "Address",
                "Phone Number",
                "Website URL",
                "Vehicle Links",
                "Dealer Details URL",
                "Item",
                "Model Year",
                "Condition",
                "Miles",
                "Engine Description",
                "MPG",
                "Transmission",
                "Drive Type",
                "Exterior Color",
                "Interior Color",
                "Price",
                "MSRP",
                "VIN",
            ],
        )
        writer.writeheader()

    # Read dealer data and process each
    with open(csv_file, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            dealer_data = {
                "Dealership Name": row["Dealership Name"],
                "Address": row["Address"],
                "Phone Number": row["Phone Number"],
                "Website URL": row["Website URL"],
                "Dealer Details URL": row["Dealer Details URL"],
            }

            # Log processing information
            logging.info(
                f"Processing dealer: {dealer_data['Dealership Name']} - URL: {dealer_data['Dealer Details URL']}"
            )

            vehicle_links = await get_vehicle_links(
                row["Dealer Details URL"], driver_path, max_vehicles=3
            )
            await extract_vehicle_data(
                vehicle_links, dealer_data, driver_path, output_csv_file
            )


# Example application


zip_codes = [90061, 90066]


async def main(zip):
    await scrape_dealer_data(zip)
    await open_dealer_urls(csv_file, output_csv_file, driver_path)
    # Load the data into a DataFrame for deduplication
    df = pd.read_csv(output_csv_file)

    # Identify rows that are duplicates of the header row by comparing against the first row
    header_row = df.iloc[0]
    df = df[~(df == header_row).all(axis=1)]

    # Save the cleaned data back to CSV
    df.to_csv(output_csv_file, index=False)
    logging.info("Removed duplicate header rows and saved the cleaned CSV.")

    # Confirm existence of the output CSV file path
    if os.path.exists(output_csv_file):
        logging.info(f"Output CSV path '{output_csv_file}' confirmed.")
    else:
        logging.error(
            f"Output CSV path '{output_csv_file}' not found; please confirm the path."
        )


# asyncio.run(main(zip))
