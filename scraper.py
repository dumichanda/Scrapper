import requests
from bs4 import BeautifulSoup
import logging
import time
import os
from supabase import create_client
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from io import BytesIO
from PIL import Image
import uuid
import threading
import json
import concurrent.futures
from queue import Queue
from dotenv import load_dotenv

load_dotenv()

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler("scraper.log")]
)

# Configs
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME", "screenshots")
TABLE_NAME = os.getenv("TABLE_NAME", "listings")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "150"))
SCREENSHOT_WAIT_TIME = int(os.getenv("SCREENSHOT_WAIT_TIME", "3"))
BASE_URL = os.getenv("BASE_URL", "https://www.adsafrica.co.za/category/65")
TEMP_DIR = os.getenv("TEMP_DIR", "/tmp")
os.makedirs(TEMP_DIR, exist_ok=True)

# Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
screenshot_queue = Queue()
screenshot_urls = {}
screenshot_lock = threading.Lock()
pending_records = {}

def get_webdriver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        return driver
    except Exception as e:
        logging.error(f"WebDriver init failed: {e}")
        return None

def handle_age_verification(driver):
    try:
        driver.execute_script('''
            const btns = document.querySelectorAll('button, a.btn');
            for (let btn of btns) {
                const t = btn.innerText.toLowerCase();
                if (t.includes('enter') || t.includes('18')) {
                    btn.click(); return true;
                }
            }
            return false;
        ''')
    except: pass

def take_screenshot(url, name):
    driver = get_webdriver()
    if not driver: return None
    try:
        driver.get(url)
        time.sleep(SCREENSHOT_WAIT_TIME)
        handle_age_verification(driver)
        filename = f"img_{uuid.uuid4().hex[:8]}.png"
        img = Image.open(BytesIO(driver.get_screenshot_as_png()))
        path = os.path.join(TEMP_DIR, filename)
        img.save(path, format="PNG", quality=95)
        screenshot_queue.put((filename, path, url))
        with screenshot_lock:
            screenshot_urls[url] = {"filename": filename, "resolved": False}
        return {"pending": True, "filename": filename}
    except Exception as e:
        logging.error(f"Screenshot error {url}: {e}")
        return None
    finally:
        driver.quit()

def process_screenshot_queue():
    if screenshot_queue.empty(): return
    batch, file_paths = [], []
    while not screenshot_queue.empty() and len(batch) < BATCH_SIZE:
        batch.append(screenshot_queue.get())
    for filename, path, url in batch:
        try:
            with open(path, "rb") as f:
                content = f.read()
                supabase.storage.from_(BUCKET_NAME).upload(
                    path=filename, 
                    file=content, 
                    file_options={"contentType": "image/png"}
                )
            public_url = supabase.storage.from_(BUCKET_NAME).get_public_url(filename)
            with screenshot_lock:
                screenshot_urls[url] = {"url": public_url, "resolved": True}
            file_paths.append(path)
        except Exception as e:
            logging.error(f"Upload fail {filename}: {e}")
    for p in file_paths:
        try: os.remove(p)
        except: pass
    update_records_with_screenshots()

def update_records_with_screenshots():
    updates = []
    with screenshot_lock:
        for url, info in screenshot_urls.items():
            if info.get("resolved") and url in pending_records:
                updates.append({"id": pending_records[url], "screenshot_url": info["url"]})
                del pending_records[url]
    if updates:
        supabase.table(TABLE_NAME).upsert(updates).execute()

def scrape_listing_page(url):
    try:
        soup = BeautifulSoup(requests.get(url, timeout=10).content, 'html.parser')
        links = [a['href'] for a in soup.select("a.list_item_title") if a.get('href')]
        return links, soup
    except Exception as e:
        logging.error(f"Listing scrape error: {e}")
        return [], None

def get_next_page_url(soup):
    try:
        pag = soup.find('div', id='paginator')
        current = pag.find('b').text.strip()
        next_page = int(current) + 1
        for a in pag.find_all('a'):
            if a.text.strip() == str(next_page):
                href = a['href']
                return 'https://www.adsafrica.co.za' + href if not href.startswith('http') else href
    except: pass
    return None

def scrape_detail_page(url):
    try:
        soup = BeautifulSoup(requests.get(url, timeout=10).content, 'html.parser')
        get = lambda sel: soup.select_one(sel).get_text(strip=True) if soup.select_one(sel) else 'N/A'
        title = get('h1.item_title')
        date = get('span#item_date')
        city = get('div#city_name span.params_field_value')
        main = soup.select_one('img#mainPic')['src'] if soup.select_one('img#mainPic') else 'N/A'
        thumbs = '; '.join([img['src'] for img in soup.select('div#thumbs img')]) or 'N/A'
        contact = soup.select('span#contact_field_value')
        name = contact[0].text.strip() if len(contact) > 0 else 'N/A'
        phone = contact[1].text.strip() if len(contact) > 1 else 'N/A'
        desc = get('div#item_text_value')
        
        # Check if phone doesn't contain any digits
        has_digits = any(char.isdigit() for char in phone)
        
        # Only take screenshot if phone doesn't contain digits
        shot = take_screenshot(url, name) if not has_digits else 'N/A'
        
        return {
            'url': url, 'title': title, 'date': date, 'city': city,
            'main_image': main, 'thumbnail_images': thumbs,
            'name': name, 'phone': phone, 'description': desc,
            'screenshot_url': shot if shot else 'N/A'
        }
    except Exception as e:
        logging.error(f"Detail error {url}: {e}")
        return None

def insert_to_supabase(batch):
    try:
        clean = []
        for i in batch:
            row = {}
            for k, v in i.items():
                if v is None: row[k] = 'N/A'
                elif k == 'screenshot_url' and isinstance(v, dict) and v.get('pending'):
                    pending_records[i['url']] = None
                    row[k] = v['filename']
                else:
                    row[k] = json.dumps(v) if isinstance(v, dict) else v
            clean.append(row)
        res = supabase.table(TABLE_NAME).insert(clean).execute()
        if hasattr(res, 'data'):
            for i, r in enumerate(res.data):
                u = batch[i]['url']
                if u in pending_records:
                    pending_records[u] = r['id']
        logging.info(f"Inserted {len(clean)} records")
    except Exception as e:
        logging.error(f"Insert fail: {e}")

def main():
    page_url = BASE_URL
    page_count, batch = 0, []
    while page_url and page_count < MAX_PAGES:
        page_count += 1
        urls, soup = scrape_listing_page(page_url)
        with concurrent.futures.ThreadPoolExecutor(MAX_WORKERS) as ex:
            for f in concurrent.futures.as_completed([ex.submit(scrape_detail_page, u) for u in urls]):
                data = f.result()
                if data: batch.append(data)
        process_screenshot_queue()
        if batch:
            insert_to_supabase(batch)
            batch.clear()
        process_screenshot_queue()
        page_url = get_next_page_url(soup)
        time.sleep(2)
    process_screenshot_queue()
    update_records_with_screenshots()
    count = supabase.table(TABLE_NAME).select("id", count="exact").execute().count
    logging.info(f"✅ Final row count: {count}")

if __name__ == "__main__":
    main()
