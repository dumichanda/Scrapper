import requests
from bs4 import BeautifulSoup
import logging
import time
import os
from supabase import create_client
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import base64
from io import BytesIO
from PIL import Image
import uuid
import re
from datetime import datetime
import concurrent.futures
from queue import Queue
import threading
import json
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

# Supabase configuration from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://itxqremrurulfvkjdlkn.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Iml0eHFyZW1ydXJ1bGZ2a2pkbGtuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQyMzI2NTQsImV4cCI6MjA1OTgwODY1NH0.rRZopLXd2CVdq_pexOHbmyk_BIzf1AiJa_o9WGeD9cQ")
BUCKET_NAME = os.getenv("BUCKET_NAME", "screenshots")
TABLE_NAME = os.getenv("TABLE_NAME", "listings")

# Performance configuration from environment variables
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

# Thread-local storage for WebDriver instances
local_storage = threading.local()

# Screenshot queue for batch processing
screenshot_queue = Queue()
screenshot_urls = {}
screenshot_lock = threading.Lock()

# Initialize Chrome WebDriver for screenshots
def get_webdriver():
    """Get a WebDriver instance for the current thread."""
    if not hasattr(local_storage, 'driver'):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Performance optimizations (without affecting image quality)
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-dev-tools")
        chrome_options.add_argument("--disable-software-rasterizer")
        
        # Additional options for Render environment
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--single-process")
        
        try:
            # Simplified WebDriver initialization
            local_storage.driver = webdriver.Chrome(options=chrome_options)
            logging.info(f"Thread {threading.current_thread().name}: WebDriver initialized")
        except Exception as e:
            logging.error(f"Thread {threading.current_thread().name}: Could not initialize Chrome: {e}")
            local_storage.driver = None
            
    return local_storage.driver

# Function to process screenshot queue in batches
def process_screenshot_queue():
    if screenshot_queue.empty():
        return
    
    batch = []
    file_paths = []
    
    logging.info(f"Processing screenshot queue with {screenshot_queue.qsize()} items")
    
    # Process up to BATCH_SIZE screenshots
    while not screenshot_queue.empty() and len(batch) < BATCH_SIZE:
        try:
            item = screenshot_queue.get_nowait()
            if item:
                batch.append(item)
        except:
            break
    
    if not batch:
        return
    
    # Upload batch to Supabase
    for filename, temp_path, url in batch:
        try:
            with open(temp_path, "rb") as file:
                file_content = file.read()
                
                # Upload the file
                result = supabase.storage.from_(BUCKET_NAME).upload(
                    path=filename,
                    file=file_content,
                    file_options={"contentType": "image/png"}
                )
                
                # Get the public URL
                screenshot_url = supabase.storage.from_(BUCKET_NAME).get_public_url(filename)
                
                # Update the URL mapping with the actual URL
                with screenshot_lock:
                    if url in screenshot_urls:
                        screenshot_urls[url] = {"url": screenshot_url, "resolved": True, "filename": filename}
                        logging.debug(f"Updated screenshot URL for {url}: {screenshot_url}")
                
                file_paths.append(temp_path)
                
        except Exception as e:
            logging.error(f"Error uploading screenshot {filename} for {url}: {e}")
    
    # Clean up temp files after successful uploads
    for path in file_paths:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            logging.error(f"Error removing temp file {path}: {e}")
    
    logging.info(f"Batch uploaded {len(batch)} screenshots to Supabase")

# Function to take a screenshot of a page
def take_screenshot(url, name):
    """Take a screenshot and queue it for batch uploading."""
    driver = get_webdriver()
    if not driver:
        return None
    
    try:
        # Navigate to the URL
        driver.get(url)
        
        # Allow time for the page to fully load (maintain original 3s for quality)
        time.sleep(SCREENSHOT_WAIT_TIME)
        
        # Check for and handle age verification popup
        handle_age_verification(driver)
        
        # Additional wait after handling age verification
        time.sleep(1)
        
        # Generate a unique filename
        if name == 'N/A':
            safe_name = 'unnamed'
        else:
            safe_name = ''.join(c if c.isalnum() else '_' for c in name)
            safe_name = safe_name[:20]
        
        filename = f"img_{uuid.uuid4().hex[:8]}.png"
        
        # Take screenshot and save it to a temporary file
        screenshot = driver.get_screenshot_as_png()
        img = Image.open(BytesIO(screenshot))
        
        temp_file_path = os.path.join(TEMP_DIR, filename)
        img.save(temp_file_path, format="PNG", quality=95)  # High quality PNG
        
        # Add to queue for batch processing
        # Store the filename with the URL for reliable lookup later
        screenshot_queue.put((filename, temp_file_path, url))
        
        # Return a placeholder with the filename for reliable resolution
        with screenshot_lock:
            screenshot_urls[url] = {"filename": filename, "resolved": False}
        
        return {"pending": True, "filename": filename}
        
    except Exception as e:
        logging.error(f"Error taking screenshot of {url}: {e}")
        return None

# Handle age verification popup with reliable approach
def handle_age_verification(driver):
    """Handle age verification popups."""
    try:
        # Try to find and click verification elements
        try:
            wait = WebDriverWait(driver, 3)
            
            # Try clicking common verification buttons
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
        except:
            pass
        
        # JavaScript approach as fallback
        driver.execute_script("""
            // Click any button that looks like verification
            var buttons = document.querySelectorAll('button, a.btn, .btn-success, .age-verification button');
            for(var i=0; i<buttons.length; i++) {
                if(buttons[i].innerText.toLowerCase().includes('enter') || 
                   buttons[i].innerText.includes('18') ||
                   buttons[i].classList.contains('btn-success')) {
                    buttons[i].click();
                    return true;
                }
            }
            return false;
        """)
        
    except Exception as e:
        pass
    
    return False

# Function to extract date from a page with retry logic
def extract_date(url, max_retries=MAX_RETRIES):
    """Extract date from a page with retries for resilience."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            date_span = soup.find('span', id='item_date')
            if date_span:
                return date_span.get_text(strip=True)
            return None
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait before retry
                continue
            logging.error(f"Failed to extract date from {url} after {max_retries} attempts: {e}")
            return None

# Function to scrape a single detail page
def scrape_detail_page(url, take_screenshots=True):
    """Scrape a single detail page with optimized approach."""
    try:
        response = requests.get(url, timeout=15)  # Increased timeout for reliability
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract all required information
        title = soup.find('h1', class_='item_title').get_text(strip=True) if soup.find('h1', class_='item_title') else 'N/A'
        date = soup.find('span', id='item_date').get_text(strip=True) if soup.find('span', id='item_date') else 'N/A'
        city = soup.find('div', id='city_name').find('span', class_='params_field_value').get_text(strip=True) if soup.find('div', id='city_name') else 'N/A'
        main_image = soup.find('img', id='mainPic')['src'] if soup.find('img', id='mainPic') else 'N/A'
        thumb_images = '; '.join([img['src'] for img in soup.find('div', id='thumbs').find_all('img')]) if soup.find('div', id='thumbs') else 'N/A'
        contact_info = soup.find_all('span', id='contact_field_value')
        name = contact_info[0].get_text(strip=True) if len(contact_info) > 0 else 'N/A'
        phone = contact_info[1].get_text(strip=True) if len(contact_info) > 1 else 'N/A'
        description = soup.find('div', id='item_text_value').get_text(strip=True) if soup.find('div', id='item_text_value') else 'N/A'
        
        # Take screenshot only if requested
        screenshot_url = take_screenshot(url, name) if take_screenshots else 'N/A'

        # Return the scraped data
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

# Process a batch of detail pages in parallel
def process_detail_pages(urls, current_date):
    """Process multiple detail pages in parallel."""
    if not urls:
        return [], False
    
    date_changed = False
    scraped_items = []
    dates_to_check = []
    
    # First check dates to see if we need to stop
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        date_futures = {executor.submit(extract_date, url): url for url in urls}
        
        for future in concurrent.futures.as_completed(date_futures):
            url = date_futures[future]
            try:
                item_date = future.result()
                
                # Initialize current_date if it's the first item
                if current_date is None and item_date:
                    current_date = item_date
                    logging.info(f"First item date: {current_date}")
                
                # Check if date has changed
                if current_date and item_date and item_date != current_date:
                    logging.info(f"Date changed from {current_date} to {item_date}. Will stop after current batch.")
                    date_changed = True
                    break
                
                # Add to list of URLs to scrape fully
                dates_to_check.append((url, item_date == current_date))
                
            except Exception as e:
                logging.error(f"Error checking date for {url}: {e}")
                # Keep the URL to scrape anyway as fallback
                dates_to_check.append((url, True))
    
    # Only process URLs with the current date
    urls_to_scrape = [url for url, should_scrape in dates_to_check if should_scrape]
    
    # Now scrape the detail pages in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        page_futures = {executor.submit(scrape_detail_page, url): url for url in urls_to_scrape}
        
        for future in concurrent.futures.as_completed(page_futures):
            url = page_futures[future]
            try:
                scraped_data = future.result()
                if scraped_data:
                    scraped_items.append(scraped_data)
                    logging.info(f"Successfully scraped {url}")
            except Exception as e:
                logging.error(f"Error processing future for {url}: {e}")
    
    # Process any pending screenshots
    process_screenshot_queue()
    
    # Update screenshot URLs with actual URLs from batch processing
    for item in scraped_items:
        screenshot_data = item.get('screenshot_url')
        if isinstance(screenshot_data, dict) and screenshot_data.get('pending'):
            # Get the URL for this item
            item_url = item['url']
            filename = screenshot_data.get('filename')
            
            with screenshot_lock:
                resolved_data = screenshot_urls.get(item_url)
                if resolved_data and resolved_data.get('resolved'):
                    # We have a resolved URL
                    item['screenshot_url'] = resolved_data.get('url')
                    logging.debug(f"Resolved screenshot URL for {item_url}: {item['screenshot_url']}")
                else:
                    # Still pending, use placeholder for now
                    item['screenshot_url'] = f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET_NAME}/{filename}"
                    logging.debug(f"Using constructed URL for {item_url}: {item['screenshot_url']}")
    
    return scraped_items, date_changed

# Function to scrape listing page
def scrape_listing_page(listing_url):
    """Scrape a listing page to get links to detail pages."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(listing_url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            detail_links = soup.find_all('a', class_='list_item_title')
            return detail_links, soup
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(1)  # Wait before retry
                continue
            logging.error(f"Failed to scrape listing page {listing_url} after {MAX_RETRIES} attempts: {e}")
            return None, None

# Function to get the next page URL
def get_next_page_url(soup, current_url):
    """Find the URL for the next page of listings with improved reliability."""
    paginator = soup.find('div', id='paginator')
    if paginator:
        # Try multiple approaches to find the next page link
        next_page = None
        
        # Approach 1: Look for "next >" text
        next_page = paginator.find('a', string='next >')
        
        # Approach 2: Look for any link with "next" in the text
        if not next_page:
            for link in paginator.find_all('a'):
                if link.text and 'next' in link.text.lower():
                    next_page = link
                    break
        
        # Approach 3: Look for the current page number and find link to next number
        if not next_page:
            current_page_elem = paginator.find('b')
            if current_page_elem and current_page_elem.text.isdigit():
                current_page = int(current_page_elem.text)
                next_page_num = current_page + 1
                next_page = paginator.find('a', string=str(next_page_num))
        
        if next_page and 'href' in next_page.attrs:
            next_url = next_page['href']
            if not next_url.startswith('http'):
                next_url = 'https://www.adsafrica.co.za' + next_url
            return next_url
    
    # Fallback: Construct URL based on pattern
    try:
        match = re.search(r'/page/(\d+)/', current_url)
        if match:
            current_page = int(match.group(1))
            next_page_num = current_page + 1
            next_url = re.sub(r'/page/\d+/', f'/page/{next_page_num}/', current_url)
            return next_url
        elif '/category/65' in current_url and '/page/' not in current_url:
            # First page without page number in URL
            return 'https://www.adsafrica.co.za/category/65/page/2/'
    except Exception as e:
        logging.error(f"Error constructing next page URL: {e}")
    
    return None

# Create database table if it doesn't exist
def ensure_table_exists():
    """Check if the required table exists and create it if needed."""
    try:
        # Try to query the table
        response = supabase.table(TABLE_NAME).select("*").limit(1).execute()
        logging.info(f"Table {TABLE_NAME} already exists")
        return True
    except Exception as e:
        logging.error(f"Error checking table: {e}")
        logging.error(f"Table {TABLE_NAME} might not exist.")
        logging.error("Please create the table in Supabase before running this script.")
        logging.error("""
        SQL to create table:
        CREATE TABLE IF NOT EXISTS listings (
            id SERIAL PRIMARY KEY,
            url TEXT,
            title TEXT,
            date TEXT,
            city TEXT,
            main_image TEXT,
            thumbnail_images TEXT,
            name TEXT,
            phone TEXT,
            description TEXT,
            screenshot_url TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """)
        return False

# Function to load checkpoint from Supabase
def load_checkpoint():
    """Load the most recent URL processed from Supabase."""
    try:
        response = supabase.table(TABLE_NAME).select("url").order("created_at", desc=True).limit(1).execute()
        data = response.data
        last_url = data[0]['url'] if data and len(data) > 0 else None
        if last_url:
            logging.info(f"Found checkpoint with URL: {last_url}")
        else:
            logging.info("No checkpoint found. Starting from scratch.")
        return last_url
    except Exception as e:
        logging.error(f"Error loading checkpoint: {e}")
        return None

# Insert data into Supabase with batching
def insert_to_supabase(data_batch):
    """Insert a batch of data into Supabase."""
    if not data_batch:
        return None
    
    try:
        # Ensure all values are strings
        for item in data_batch:
            for key, value in item.items():
                if value is None:
                    item[key] = 'N/A'
                elif isinstance(value, dict):
                    # Convert dict to string if any remain
                    item[key] = json.dumps(value)
        
        # Insert the batch
        result = supabase.table(TABLE_NAME).insert(
            data_batch, 
            returning='minimal'
        ).execute()
        
        logging.info(f"Inserted {len(data_batch)} records into Supabase")
        return result
    except Exception as e:
        logging.error(f"Error inserting to Supabase: {e}")
        logging.error("Check Supabase permissions and table structure")
        return None

# Cleanup function for thread-local resources
def cleanup_resources():
    """Clean up WebDriver instances."""
    if hasattr(local_storage, 'driver') and local_storage.driver:
        try:
            local_storage.driver.quit()
            logging.info(f"Thread {threading.current_thread().name}: WebDriver closed")
        except:
            pass

# Main function
def main():
    start_time = time.time()
    
    # Log script start with configuration
    logging.info(f"=== AdsAfrica Scraper Started ===")
    logging.info(f"Configuration: MAX_PAGES={MAX_PAGES}, MAX_WORKERS={MAX_WORKERS}, BATCH_SIZE={BATCH_SIZE}")
    
    # Ensure we have the table
    if not ensure_table_exists():
        logging.error("Required table does not exist. Exiting.")
        return
    
    try:
        # Load checkpoint
        last_url = load_checkpoint()
        start_found = not bool(last_url)  # If no last URL, start from scratch
        batch_data = []
        page_url = BASE_URL  # Start with the base URL
        
        # Variable to store the current date
        current_date = None
        
        # Loop through listing pages
        page_count = 0
        date_changed = False
        
        logging.info(f"Starting scraper with parallel processing (Max Workers: {MAX_WORKERS})")
        
        while page_url and page_count < MAX_PAGES and not date_changed:
            page_count += 1
            logging.info(f"Processing listing page {page_count}: {page_url}")
            
            detail_links, soup = scrape_listing_page(page_url)

            if not detail_links or not soup:
                logging.info("No detail links or soup found. Ending scraping.")
                break

            # Extract URLs to process
            urls_to_process = []
            for link in detail_links:
                detail_url = link['href']
                
                # Skip urls until we find the last processed one
                if not start_found:
                    if detail_url == last_url:
                        start_found = True
                        logging.info(f"Found last processed URL: {last_url}. Starting from next item.")
                    continue
                
                urls_to_process.append(detail_url)
            
            # Process the batch of detail pages in parallel
            new_items, date_changed = process_detail_pages(urls_to_process, current_date)
            batch_data.extend(new_items)
            
            # Update current_date from first item if needed
            if current_date is None and new_items:
                first_item = new_items[0]
                current_date = first_item.get('date')
                if current_date:
                    logging.info(f"First item date: {current_date}")
            
            # Insert batch data if we have enough
            if len(batch_data) >= BATCH_SIZE:
                # Process any pending screenshots before insertion
                process_screenshot_queue()
                
                # Resolve any pending screenshot URLs
                for item in batch_data:
                    screenshot_data = item.get('screenshot_url')
                    if isinstance(screenshot_data, dict) and screenshot_data.get('pending'):
                        item_url = item['url']
                        filename = screenshot_data.get('filename')
                        
                        with screenshot_lock:
                            resolved_data = screenshot_urls.get(item_url)
                            if resolved_data and resolved_data.get('resolved'):
                                # We have a resolved URL
                                item['screenshot_url'] = resolved_data.get('url')
                            else:
                                # Still pending, construct URL directly
                                item['screenshot_url'] = f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET_NAME}/{filename}"
                
                # Convert any remaining dict values to strings
                for item in batch_data:
                    if isinstance(item['screenshot_url'], dict):
                        item['screenshot_url'] = str(item['screenshot_url'])
                
                insert_to_supabase(batch_data)
                batch_data = []  # Clear after successful insert
            
            # Stop if date changed
            if date_changed:
                logging.info("Date change detected. Stopping pagination.")
                break
            
            # Get the next page URL
            next_page_url = get_next_page_url(soup, page_url)
            
            # If no next page found, try to construct it
            if not next_page_url:
                match = re.search(r'/page/(\d+)/', page_url)
                if match:
                    current_page = int(match.group(1))
                    next_page = current_page + 1
                    next_page_url = re.sub(r'/page/\d+/', f'/page/{next_page}/', page_url)
                elif page_url == BASE_URL:
                    next_page_url = f"{BASE_URL}/page/2/"
                
                # Verify the constructed URL is valid
                if next_page_url:
                    try:
                        test_response = requests.get(next_page_url, timeout=10)
                        if test_response.status_code != 200:
                            next_page_url = None
                    except:
                        next_page_url = None
            
            if next_page_url:
                page_url = next_page_url
            else:
                logging.info("No next page found. Ending scraping.")
                break

        # Process any remaining screenshots
        process_screenshot_queue()
        
        # Insert any remaining data
        if batch_data:
            # Update screenshot URLs with actual URLs
            for item in batch_data:
                screenshot_data = item.get('screenshot_url')
                if isinstance(screenshot_data, dict) and screenshot_data.get('pending'):
                    item_url = item['url']
                    filename = screenshot_data.get('filename')
                    
                    with screenshot_lock:
                        resolved_data = screenshot_urls.get(item_url)
                        if resolved_data and resolved_data.get('resolved'):
                            # We have a resolved URL
                            item['screenshot_url'] = resolved_data.get('url')
                        else:
                            # Still pending, construct URL directly
                            item['screenshot_url'] = f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET_NAME}/{filename}"
                
                # Ensure all screenshot URLs are strings, not objects
                if not isinstance(item['screenshot_url'], str):
                    item['screenshot_url'] = str(item['screenshot_url'])
            
            insert_to_supabase(batch_data)

        # Report status
        if date_changed:
            logging.info('Scraping stopped due to date change')
        else:
            logging.info('All data saved to Supabase')
        
        # Report performance
        elapsed_time = time.time() - start_time
        logging.info(f"Scraping completed in {elapsed_time:.2f} seconds")
        
    except Exception as e:
        logging.error(f"Unexpected error in main process: {e}", exc_info=True)
    
    finally:
        # Clean up WebDriver instances
        for thread in threading.enumerate():
            if thread != threading.current_thread():
                try:
                    thread._target = cleanup_resources
                except:
                    pass
        
        cleanup_resources()
        logging.info("=== AdsAfrica Scraper Finished ===")

if __name__ == "__main__":
    main()
