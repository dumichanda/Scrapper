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
import base64
from io import BytesIO
from PIL import Image
import uuid
import re
import threading
import json
import concurrent.futures
from queue import Queue
from dotenv import load_dotenv

# === Load environment variables ===
load_dotenv()

# === Configure logging ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log")
    ]
)

# === Supabase configuration ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME", "screenshots")
TABLE_NAME = os.getenv("TABLE_NAME", "listings")

# === Performance configuration ===
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "150"))
SCREENSHOT_WAIT_TIME = int(os.getenv("SCREENSHOT_WAIT_TIME", "3"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_URL = os.getenv("BASE_URL", "https://www.adsafrica.co.za/category/65")
TEMP_DIR = os.getenv("TEMP_DIR", "/tmp")

# === Prepare temp directory ===
os.makedirs(TEMP_DIR, exist_ok=True)

# === Initialize Supabase client ===
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logging.info("Supabase client initialized")
except Exception as e:
    logging.error(f"Failed to initialize Supabase client: {e}")
    raise

# === Thread-local storage for WebDriver instances ===
local_storage = threading.local()

# === Screenshot queue ===
screenshot_queue = Queue()
screenshot_urls = {}
screenshot_lock = threading.Lock()

# === Get WebDriver instance ===
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

# === Process screenshot queue ===
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

# === Take screenshot ===
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

# === Handle age verification popup ===
def handle_age_verification(driver):
    try:
        wait = WebDriverWait(driver, 3)
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

# === Extract date from page ===
def extract_date(url, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            date_span = soup.find('span', id='item_date')
            return date_span.get_text(strip=True) if date_span else None
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            logging.error(f"Failed to extract date from {url}: {e}")
            return None

# === Scrape a detail page ===
def scrape_detail_page(url, take_screenshots=True):
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

        screenshot_url = take_screenshot(url, name) if take_screenshots else 'N/A'

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
        logging.error(f"Error scraping {url}: {e}")
        return None

# === Main function ===
def main():
    start_time = time.time()
    logging.info("=== AdsAfrica Scraper Started ===")

    if not ensure_table_exists():
        logging.error("Table does not exist. Exiting.")
        return

    last_url = load_checkpoint()
    start_found = not bool(last_url)
    batch_data = []
    page_url = BASE_URL
    current_date = None
    page_count = 0
    date_changed = False

    logging.info(f"Scraper configuration: MAX_PAGES={MAX_PAGES}, MAX_WORKERS={MAX_WORKERS}, BATCH_SIZE={BATCH_SIZE}")

    while page_url and page_count < MAX_PAGES and not date_changed:
        page_count += 1
        logging.info(f"Processing page {page_count}: {page_url}")

        detail_links, soup = scrape_listing_page(page_url)

        if not detail_links or not soup:
            logging.info("No detail links found, stopping.")
            break

        urls_to_process = []

        for link in detail_links:
            detail_url = link['href']
            if not start_found:
                if detail_url == last_url:
                    start_found = True
                    logging.info(f"Found checkpoint URL: {last_url}. Continuing scrape.")
                continue
            urls_to_process.append(detail_url)

        new_items, date_changed = process_detail_pages(urls_to_process, current_date)
        batch_data.extend(new_items)

        if current_date is None and new_items:
            current_date = new_items[0].get('date')

        if len(batch_data) >= BATCH_SIZE:
            process_screenshot_queue()
            resolve_screenshot_urls(batch_data)
            insert_to_supabase(batch_data)
            batch_data = []

        next_page_url = get_next_page_url(soup, page_url)
        if not next_page_url:
            logging.info("No next page found. Exiting loop.")
            break
        page_url = next_page_url

    process_screenshot_queue()
    if batch_data:
        resolve_screenshot_urls(batch_data)
        insert_to_supabase(batch_data)

    logging.info("Scraping completed")
    elapsed_time = time.time() - start_time
    logging.info(f"Total elapsed time: {elapsed_time:.2f} seconds")

    cleanup_resources()

# === Entry point ===
if __name__ == "__main__":
    main()
