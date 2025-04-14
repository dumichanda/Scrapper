import requests
from bs4 import BeautifulSoup
import logging
import time
import os
from supabase import create_client
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from io import BytesIO
from PIL import Image
import uuid
import re
import threading
import json
import concurrent.futures
from queue import Queue
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log")
    ]
)

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME", "screenshots")
TABLE_NAME = os.getenv("TABLE_NAME", "listings")

# Performance configuration
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "150"))
SCREENSHOT_WAIT_TIME = int(os.getenv("SCREENSHOT_WAIT_TIME", "3"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_URL = os.getenv("BASE_URL", "https://www.adsafrica.co.za/category/65")
TEMP_DIR = os.getenv("TEMP_DIR", "/tmp")

# Ensure temp directory exists
os.makedirs(TEMP_DIR, exist_ok=True)

# Initialize Supabase client
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logging.info("Supabase client initialized")
except Exception as e:
    logging.error(f"Failed to initialize Supabase client: {e}")
    raise

local_storage = threading.local()
screenshot_queue = Queue()
screenshot_urls = {}
screenshot_lock = threading.Lock()

def get_webdriver():
    if not hasattr(local_storage, 'driver'):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-dev-tools")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--single-process")

        try:
            local_storage.driver = webdriver.Chrome(options=chrome_options)
            logging.info(f"WebDriver initialized")
        except Exception as e:
            logging.error(f"Failed to initialize WebDriver: {e}")
            local_storage.driver = None

    return local_storage.driver

def process_screenshot_queue():
    if screenshot_queue.empty():
        return

    batch = []
    file_paths = []

    logging.info(f"Processing {screenshot_queue.qsize()} screenshots")

    while not screenshot_queue.empty() and len(batch) < BATCH_SIZE:
        try:
            item = screenshot_queue.get_nowait()
            if item:
                batch.append(item)
        except Exception:
            break

    for filename, temp_path, url in batch:
        try:
            with open(temp_path, "rb") as file:
                file_content = file.read()
                supabase.storage.from_(BUCKET_NAME).upload(
                    path=filename,
                    file=file_content,
                    file_options={"contentType": "image/png"}
                )
                screenshot_url = supabase.storage.from_(BUCKET_NAME).get_public_url(filename)

                with screenshot_lock:
                    screenshot_urls[url] = {"url": screenshot_url, "resolved": True, "filename": filename}

                file_paths.append(temp_path)

        except Exception as e:
            logging.error(f"Error uploading screenshot {filename} for {url}: {e}")

    for path in file_paths:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            logging.error(f"Error deleting temp file {path}: {e}")

    logging.info(f"Uploaded {len(batch)} screenshots to Supabase")

def take_screenshot(url, name):
    driver = get_webdriver()
    if not driver:
        return None

    try:
        driver.get(url)
        time.sleep(SCREENSHOT_WAIT_TIME)
        handle_age_verification(driver)
        time.sleep(1)

        safe_name = ''.join(c if c.isalnum() else '_' for c in name)[:20] if name != 'N/A' else 'unnamed'
        filename = f"img_{uuid.uuid4().hex[:8]}.png"

        screenshot = driver.get_screenshot_as_png()
        img = Image.open(BytesIO(screenshot))

        temp_file_path = os.path.join(TEMP_DIR, filename)
        img.save(temp_file_path, format="PNG", quality=95)

        screenshot_queue.put((filename, temp_file_path, url))

        with screenshot_lock:
            screenshot_urls[url] = {"filename": filename, "resolved": False}

        return {"pending": True, "filename": filename}

    except Exception as e:
        logging.error(f"Error taking screenshot for {url}: {e}")
        return None

def handle_age_verification(driver):
    try:
        selectors = [
            ".age-verification button.btn-success",
            ".btn-success",
            "button:contains('Enter')",
            "button:contains('18')"
        ]

        for selector in selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    elements[0].click()
                    return True
            except Exception:
                pass

        driver.execute_script("""
            const buttons = document.querySelectorAll('button, a.btn, .btn-success, .age-verification button');
            for (const btn of buttons) {
                const text = btn.innerText.toLowerCase();
                if (text.includes('enter') || text.includes('18') || btn.classList.contains('btn-success')) {
                    btn.click();
                    return true;
                }
            }
            return false;
        """)
    except Exception:
        pass

    return False

def scrape_listing_page(listing_url):
    try:
        response = requests.get(listing_url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        detail_links = soup.find_all('a', class_='list_item_title')
        return [link['href'] for link in detail_links if link.get('href')], soup
    except Exception as e:
        logging.error(f"Failed to scrape listing page {listing_url}: {e}")
        return [], None

def get_next_page_url(soup):
    try:
        paginator = soup.find('div', id='paginator')
        if paginator:
            next_page = paginator.find('a', string='next >')
            if next_page and next_page.get('href'):
                next_url = next_page['href']
                if not next_url.startswith('http'):
                    next_url = 'https://www.adsafrica.co.za' + next_url
                return next_url
    except Exception as e:
        logging.error(f"Error finding next page URL: {e}")
    return None

def scrape_detail_page(url):
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        title = soup.find('h1', class_='item_title').get_text(strip=True) if soup.find('h1', class_='item_title') else 'N/A'
        date = soup.find('span', id='item_date').get_text(strip=True) if soup.find('span', id='item_date') else 'N/A'
        city = soup.find('div', id='city_name').find('span', class_='params_field_value').get_text(strip=True) if soup.find('div', id='city_name') else 'N/A'
        main_image = soup.find('img', id='mainPic')['src'] if soup.find('img', id='mainPic') else 'N/A'
        thumb_images = '; '.join([img['src'] for img in soup.find('div', id='thumbs').find_all('img')]) if soup.find('div', id='thumbs') else 'N/A'
        contact_info = soup.find_all('span', id='contact_field_value')
        name = contact_info[0].get_text(strip=True) if len(contact_info) > 0 else 'N/A'
        phone = contact_info[1].get_text(strip=True) if len(contact_info) > 1 else 'N/A'
        description = soup.find('div', id='item_text_value').get_text(strip=True) if soup.find('div', id='item_text_value') else 'N/A'

        screenshot_url = take_screenshot(url, name)

        return {
            'url': url,
            'title': title,
            'date': date,
            'city': city,
            'main_image': main_image,
            'thumbnail_images': thumb_images,
            'name': name,
            'phone': phone,
            'description': description,
            'screenshot_url': screenshot_url if screenshot_url else 'N/A'
        }
    except Exception as e:
        logging.error(f"Error scraping detail page {url}: {e}")
        return None

def insert_to_supabase(data_batch):
    if not data_batch:
        logging.info("No data to insert to Supabase.")
        return

    try:
        for item in data_batch:
            for key, value in item.items():
                if value is None:
                    item[key] = 'N/A'
                elif isinstance(value, dict):
                    item[key] = json.dumps(value)

        supabase.table(TABLE_NAME).insert(data_batch, returning='minimal').execute()
        logging.info(f"Inserted {len(data_batch)} records into Supabase.")
    except Exception as e:
        logging.error(f"Error inserting to Supabase: {e}")

def count_supabase_rows():
    try:
        response = supabase.table(TABLE_NAME).select("id", count="exact").execute()
        total_rows = response.count
        logging.info(f"Total rows in Supabase table '{TABLE_NAME}': {total_rows}")
        return total_rows
    except Exception as e:
        logging.error(f"Error counting rows in Supabase: {e}")
        return None

def write_summary(total_rows):
    try:
        with open("summary.txt", "w") as summary_file:
            summary_file.write(f"Scraping Summary\n")
            summary_file.write(f"Total rows in Supabase table '{TABLE_NAME}': {total_rows}\n")
        logging.info("Summary file written successfully")
    except Exception as e:
        logging.error(f"Error writing summary file: {e}")

def cleanup_resources():
    if hasattr(local_storage, 'driver') and local_storage.driver:
        try:
            local_storage.driver.quit()
            logging.info("WebDriver closed")
        except Exception:
            pass

def main():
    start_time = time.time()
    logging.info("=== AdsAfrica Scraper Started ===")
    batch_data = []
    page_url = BASE_URL
    page_count = 0

    while page_url and page_count < MAX_PAGES:
        page_count += 1
        logging.info(f"Scraping page {page_count}: {page_url}")
        urls, soup = scrape_listing_page(page_url)
        if not urls or not soup:
            break

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(scrape_detail_page, url) for url in urls]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    batch_data.append(result)

        process_screenshot_queue()

        if batch_data:
            insert_to_supabase(batch_data)
            batch_data.clear()

        page_url = get_next_page_url(soup)

    total_rows = count_supabase_rows()
    if total_rows is not None:
        print(f"✅ Total rows in Supabase table '{TABLE_NAME}': {total_rows}")
        write_summary(total_rows)

    elapsed_time = time.time() - start_time
    logging.info(f"✅ Scraping completed in {elapsed_time:.2f} seconds")
    cleanup_resources()

if __name__ == "__main__":
    main()
