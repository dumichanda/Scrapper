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
        for url, info in list(screenshot_urls.items()):
            if info.get("resolved") and url in pending_records:
                record_id = pending_records[url]
                # Only proceed if we have a valid record ID
                if record_id:
                    to_update.append({
                        "id": record_id, 
                        "screenshot_url": info["url"]
                    })
                    logging.info(f"Preparing to update record {record_id} with URL {info['url']}")
                    # Remove from pending after adding to update list
                    del pending_records[url]
                    # Keep the URL info in screenshot_urls for debugging
                else:
                    logging.warning(f"Missing record ID for URL {url}")
    
    if to_update:
        logging.info(f"Updating {len(to_update)} records with screenshot URLs")
        try:
            # Update records in batches
            for i in range(0, len(to_update), BATCH_SIZE):
                batch = to_update[i:i+BATCH_SIZE]
                response = supabase.table(TABLE_NAME).upsert(batch).execute()
                
                if hasattr(response, 'data'):
                    logging.info(f"Updated {len(response.data)} records successfully")
                    # Log the first record for debugging
                    if response.data and len(response.data) > 0:
                        logging.info(f"Sample updated record: {response.data[0]}")
                else:
                    logging.warning("Update response has no data attribute")
        except Exception as e:
            logging.error(f"Failed to update screenshots in database: {e}")
            logging.error(f"Error details: {str(e)}")
            
    # Check if we need to fetch and update any records that were already inserted
    if not to_update and len(pending_records) > 0:
        logging.info(f"Found {len(pending_records)} pending records to check")
        try:
            # Get the filenames of pending screenshots
            filenames = []
            for url, record_id in list(pending_records.items()):
                if record_id and url in screenshot_urls:
                    filename = screenshot_urls[url].get("filename")
                    if filename:
                        filenames.append((record_id, filename))
            
            # Update records with matching filenames
            for record_id, filename in filenames:
                try:
                    url_public = supabase.storage.from_(BUCKET_NAME).get_public_url(filename)
                    supabase.table(TABLE_NAME).update({"screenshot_url": url_public}).eq("id", record_id).execute()
                    logging.info(f"Updated record {record_id} with URL for {filename}")
                except Exception as e:
                    logging.error(f"Failed to update record {record_id}: {e}")
        except Exception as e:
            logging.error(f"Error checking pending records: {e}")

def process_screenshot_queue():
    if screenshot_queue.empty():
        logging.info("Screenshot queue is empty")
        return
    
    batch, file_paths = [], []
    while not screenshot_queue.empty() and len(batch) < BATCH_SIZE:
        try:
            item = screenshot_queue.get_nowait()
            if item:
                batch.append(item)
        except:
            break
    
    logging.info(f"Processing {len(batch)} screenshots")
    
    for filename, temp_path, url in batch:
        try:
            # Check if file exists
            if not os.path.exists(temp_path):
                logging.error(f"Screenshot file not found: {temp_path}")
                continue
                
            # Get file size for logging
            file_size = os.path.getsize(temp_path)
            logging.info(f"Uploading screenshot {filename} ({file_size} bytes)")
            
            with open(temp_path, "rb") as file:
                file_content = file.read()
                # Try to upload with upsert to avoid duplicates
                supabase.storage.from_(BUCKET_NAME).upload(
                    path=filename,
                    file=file_content,
                    file_options={"contentType": "image/png"},
                    upsert=True
                )
                
                # Get the public URL
                url_public = supabase.storage.from_(BUCKET_NAME).get_public_url(filename)
                logging.info(f"Screenshot uploaded: {url_public}")
                
                with screenshot_lock:
                    screenshot_urls[url] = {"url": url_public, "resolved": True}
                file_paths.append(temp_path)
        except Exception as e:
            logging.error(f"Upload failed for {filename}: {e}")
            logging.error(f"Error details: {str(e)}")
    
    # Clean up temporary files
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
        if not paginator:
            logging.warning("Paginator not found on page")
            return None
            
        # Looking at the pagination HTML structure, find links with numeric page values
        page_links = paginator.find_all('a')
        current_page = paginator.find('b').text.strip() if paginator.find('b') else "1"
        logging.info(f"Current page: {current_page}")
        
        # Find the next page number
        next_page_num = int(current_page) + 1
        
        # Look for a link to the next page
        next_page = None
        for link in page_links:
            if link.text.strip() == str(next_page_num):
                next_page = link
                break
        
        # If next page link found
        if next_page:
            url = next_page['href']
            logging.info(f"Found next page URL: {url}")
            return 'https://www.adsafrica.co.za' + url if not url.startswith('http') else url
        else:
            logging.warning(f"No link found for page {next_page_num}")
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
        processed_data = []
        
        # Format data for insertion and track which URLs have pending screenshots
        for item in data_batch:
            processed_item = {}
            
            for k, v in item.items():
                if v is None:
                    processed_item[k] = 'N/A'
                elif k == 'screenshot_url' and isinstance(v, dict) and v.get('pending'):
                    # Store the original URL to update later
                    if item['url'] not in pending_records:
                        pending_records[item['url']] = None
                    # Store filename as string to avoid JSON serialization issues
                    processed_item[k] = v['filename']
                elif isinstance(v, dict):
                    processed_item[k] = json.dumps(v)
                else:
                    processed_item[k] = v
            
            processed_data.append(processed_item)
        
        # Insert records
        result = supabase.table(TABLE_NAME).insert(processed_data).execute()
        
        # Save record IDs for pending screenshots
        if result and hasattr(result, 'data'):
            for i, record in enumerate(result.data):
                if i < len(data_batch) and data_batch[i]['url'] in pending_records:
                    pending_records[data_batch[i]['url']] = record['id']
                    logging.info(f"Tracking pending screenshot for ID {record['id']}")
        
        logging.info(f"Inserted {len(processed_data)} records.")
        return True
    except Exception as e:
        logging.error(f"Insertion error: {e}")
        logging.error(f"Error details: {str(e)}")
        return False

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
    
    # Just for debugging - print all environment variables
    logging.info(f"SUPABASE_URL: {'*' * 5 + SUPABASE_URL[-4:] if SUPABASE_URL else 'Not set'}")
    logging.info(f"BUCKET_NAME: {BUCKET_NAME}")
    logging.info(f"TABLE_NAME: {TABLE_NAME}")
    logging.info(f"BASE_URL: {BASE_URL}")
    
    # Initialize the page URL
    page_url = BASE_URL
    page_count = 0
    batch_data = []
    
    # Create a test connection to verify Supabase is working
    try:
        count = count_supabase_rows()
        logging.info(f"Supabase connection test: {count} rows currently in table")
    except Exception as e:
        logging.error(f"Supabase connection test failed: {e}")
        return

    # Main scraping loop
    while page_url and page_count < MAX_PAGES:
        page_count += 1
        logging.info(f"Processing page {page_count}: {page_url}")
        
        # Scrape listing page
        urls, soup = scrape_listing_page(page_url)
        if not urls or not soup:
            logging.error("Failed to get listing page or no URLs found")
            break

        logging.info(f"Found {len(urls)} listings on page {page_count}")
        
        # Scrape detail pages
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(scrape_detail_page, url) for url in urls]
            for future in concurrent.futures.as_completed(futures):
                data = future.result()
                if data:
                    batch_data.append(data)

        # Print sample data for debugging
        if batch_data and len(batch_data) > 0:
            sample = batch_data[0].copy()
            if 'description' in sample:
                sample['description'] = sample['description'][:50] + '...' if len(sample['description']) > 50 else sample['description']
            logging.info(f"Sample data: {sample}")

        # Process screenshot queue
        logging.info("Processing screenshots...")
        process_screenshot_queue()
        
        # Insert data into database
        if batch_data:
            logging.info(f"Inserting {len(batch_data)} records into Supabase")
            success = insert_to_supabase(batch_data)
            if success:
                batch_data.clear()
            else:
                logging.error("Failed to insert batch, retrying individually")
                # Try inserting one by one
                for item in batch_data:
                    insert_to_supabase([item])
                batch_data.clear()
        
        # Process screenshot queue again
        process_screenshot_queue()
        
        # Get next page URL - with extra debugging
        logging.info("Looking for next page link...")
        if soup:
            paginator = soup.find('div', id='paginator')
            if paginator:
                logging.info(f"Paginator HTML: {paginator}")
                links = paginator.find_all('a')
                logging.info(f"Found {len(links)} links in paginator")
                for link in links:
                    logging.info(f"Link: {link.text.strip()} -> {link.get('href')}")
        
        next_url = get_next_page_url(soup)
        
        if next_url:
            logging.info(f"Moving to next page: {next_url}")
            page_url = next_url
        else:
            logging.warning("No next page found, ending scrape")
            break
            
        # Add a small delay between pages to be kind to the server
        time.sleep(2)
    
    # Final processing of any remaining screenshots
    logging.info("Final screenshot processing...")
    process_screenshot_queue()
    
    # Check for any pending screenshots and update them
    if pending_records:
        logging.info(f"Final update for {len(pending_records)} pending screenshots")
        update_records_with_screenshots()

    # Count rows and write summary
    total_rows = count_supabase_rows()
    if total_rows:
        write_summary(total_rows)
        print(f"✅ Total rows in Supabase: {total_rows}")

    # Done!
    logging.info(f"✅ Scraper completed in {time.time() - start_time:.2f} sec")

if __name__ == "__main__":
    main()
