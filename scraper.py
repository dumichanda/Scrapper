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
    handlers=[logging.StreamHandler(), logging.FileHandler("scraper.log")]
)

# Supabase config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME", "screenshots")
TABLE_NAME = os.getenv("TABLE_NAME", "listings")

# Scraper performance config
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "150"))
SCREENSHOT_WAIT_TIME = int(os.getenv("SCREENSHOT_WAIT_TIME", "3"))
BASE_URL = os.getenv("BASE_URL", "https://www.adsafrica.co.za/category/65")
TEMP_DIR = os.getenv("TEMP_DIR", "/tmp")

# Ensure temp directory exists
os.makedirs(TEMP_DIR, exist_ok=True)

# Initialize Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Screenshot queue
screenshot_queue = Queue()
screenshot_urls = {}
screenshot_lock = threading.Lock()
pending_records = {}  # NEW: Keep track of records with pending screenshots

def get_webdriver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    try:
        return webdriver.Chrome(options=chrome_options)
    except Exception as e:
        logging.error(f"WebDriver creation failed: {e}")
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
            except:
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
    except:
        pass
    return False

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
        temp_path = os.path.join(TEMP_DIR, filename)
        img.save(temp_path, format="PNG", quality=95)
        screenshot_queue.put((filename, temp_path, url))
        with screenshot_lock:
            screenshot_urls[url] = {"filename": filename, "resolved": False}
        return {"pending": True, "filename": filename}
    except Exception as e:
        logging.error(f"Error taking screenshot for {url}: {e}")
        return None
    finally:
        try:
            driver.quit()
        except:
            pass

def update_records_with_screenshots():
    """Update database records with resolved screenshot URLs"""
    to_update = []
    with screenshot_lock:
        for url, info in screenshot_urls.items():
            if info.get("resolved") and url in pending_records:
                record_id = pending_records[url]
                to_update.append({"id": record_id, "screenshot_url": info["url"]})
                del pending_records[url]
    
    if to_update:
        try:
            # Update records in batches
            for i in range(0, len(to_update), BATCH_SIZE):
                batch = to_update[i:i+BATCH_SIZE]
                supabase.table(TABLE_NAME).upsert(batch).execute()
                logging.info(f"Updated {len(batch)} records with screenshot URLs")
        except Exception as e:
            logging.error(f"Failed to update screenshots in database: {e}")

def process_screenshot_queue():
    if screenshot_queue.empty():
        return
    batch, file_paths = [], []
    while not screenshot_queue.empty() and len(batch) < BATCH_SIZE:
        try:
            item = screenshot_queue.get_nowait()
            if item:
                batch.append(item)
        except:
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
                url_public = supabase.storage.from_(BUCKET_NAME).get_public_url(filename)
                with screenshot_lock:
                    screenshot_urls[url] = {"url": url_public, "resolved": True}
                file_paths.append(temp_path)
        except Exception as e:
            logging.error(f"Upload failed for {filename}: {e}")
    for path in file_paths:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            logging.error(f"Cleanup failed: {e}")
    
    # Update any resolved screenshots in the database
    update_records_with_screenshots()

def scrape_listing_page(listing_url):
    try:
        response = requests.get(listing_url, timeout=15)
        soup = BeautifulSoup(response.content, 'html.parser')
        detail_links = soup.find_all('a', class_='list_item_title')
        return [link['href'] for link in detail_links if link.get('href')], soup
    except Exception as e:
        logging.error(f"Listing page scrape error: {e}")
        return [], None

def get_next_page_url(soup):
    try:
        paginator = soup.find('div', id='paginator')
        next_page = paginator.find('a', string='next >') if paginator else None
        if next_page:
            url = next_page['href']
            return 'https://www.adsafrica.co.za' + url if not url.startswith('http') else url
    except Exception as e:
        logging.error(f"Next page error: {e}")
    return None

def scrape_detail_page(url):
    try:
        response = requests.get(url, timeout=15)
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
        screenshot = take_screenshot(url, name)
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
            'screenshot_url': screenshot if screenshot else 'N/A'
        }
    except Exception as e:
        logging.error(f"Detail scrape failed for {url}: {e}")
        return None

def insert_to_supabase(data_batch):
    try:
        # Format data for insertion
        for item in data_batch:
            for k, v in item.items():
                if v is None:
                    item[k] = 'N/A'
                elif isinstance(v, dict):
                    item[k] = json.dumps(v)
            
            # If this record has a pending screenshot, track it for later updating
            if isinstance(item.get('screenshot_url'), dict) and item['screenshot_url'].get('pending'):
                pending_records[item['url']] = None  # Will store ID after insertion
        
        # Insert records
        result = supabase.table(TABLE_NAME).insert(data_batch).execute()
        
        # Save record IDs for pending screenshots
        for i, record in enumerate(result.data):
            if i < len(data_batch) and data_batch[i]['url'] in pending_records:
                pending_records[data_batch[i]['url']] = record['id']
        
        logging.info(f"Inserted {len(data_batch)} records.")
    except Exception as e:
        logging.error(f"Insertion error: {e}")

def count_supabase_rows():
    try:
        res = supabase.table(TABLE_NAME).select("id", count="exact").execute()
        count = res.count
        logging.info(f"Supabase row count: {count}")
        return count
    except Exception as e:
        logging.error(f"Count query failed: {e}")
        return None

def write_summary(row_count):
    try:
        with open("summary.txt", "w") as f:
            f.write("Scraping Summary\n")
            f.write(f"Total rows in Supabase table '{TABLE_NAME}': {row_count}\n")
    except Exception as e:
        logging.error(f"Failed writing summary: {e}")

def main():
    start_time = time.time()
    logging.info("🚀 Scraper started")
    page_url = BASE_URL
    page_count = 0
    batch_data = []

    while page_url and page_count < MAX_PAGES:
        page_count += 1
        logging.info(f"Processing page {page_count}: {page_url}")
        urls, soup = scrape_listing_page(page_url)
        if not urls or not soup:
            break

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(scrape_detail_page, url) for url in urls]
            for future in concurrent.futures.as_completed(futures):
                data = future.result()
                if data:
                    batch_data.append(data)

        # Process screenshot queue and update database
        process_screenshot_queue()
        
        # Insert data into database
        if batch_data:
            insert_to_supabase(batch_data)
            batch_data.clear()
        
        # Process screenshot queue again to catch any remaining screenshots
        process_screenshot_queue()
        
        # Get next page URL
        page_url = get_next_page_url(soup)
    
    # Final processing of any remaining screenshots
    process_screenshot_queue()
    
    # Check for any pending screenshots and update them
    if pending_records:
        logging.info(f"Final update for {len(pending_records)} pending screenshots")
        update_records_with_screenshots()

    total_rows = count_supabase_rows()
    if total_rows:
        write_summary(total_rows)
        print(f"✅ Total rows in Supabase: {total_rows}")

    logging.info(f"✅ Done in {time.time() - start_time:.2f} sec")

if __name__ == "__main__":
    main()
