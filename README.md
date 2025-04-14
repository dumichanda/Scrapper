# Scrapper
# Ads Web Scraper

This project scrapes listings from AdsAfrica.co.za and stores the data in a Supabase database. It's optimized for deployment on Render.com.

## Features

- Parallel processing of listings for high performance
- Takes high-quality screenshots of each listing
- Stops scraping when date changes to focus only on recent listings
- Automatically resumes from where it left off
- Batch processing for efficient database operations
- Robust error handling and retries

## Prerequisites

- Supabase account with:
  - Database table for listings
  - Storage bucket for screenshots
- Render.com account

## Setup

### Supabase Setup

1. Create a new Supabase project
2. Create a storage bucket named "screenshots"
3. Create a table with the following SQL:

```sql
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
```

4. Update the storage bucket permissions to allow public access

### Render Deployment

1. Push this repository to GitHub
2. Connect to Render
3. Create a new Blueprint (using the render.yaml configuration)
4. Add your Supabase credentials in the environment variables
5. Deploy!

## Configuration

Configure the scraper using the following environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| SUPABASE_URL | Supabase project URL | Required |
| SUPABASE_KEY | Supabase API key | Required |
| BUCKET_NAME | Supabase storage bucket name | screenshots |
| TABLE_NAME | Supabase table name | listings |
| BASE_URL | Starting URL for scraping | https://www.adsafrica.co.za/category/65 |
| BATCH_SIZE | Number of items per batch | 10 |
| MAX_PAGES | Maximum pages to scrape | 150 |
| MAX_WORKERS | Number of parallel workers | 5 |
| SCREENSHOT_WAIT_TIME | Wait time for screenshots (seconds) | 3 |
| MAX_RETRIES | Max retries for failed requests | 3 |

## Local Development

1. Clone the repository
2. Create a virtual environment: `python -m venv venv`
3. Activate the environment: 
   - Windows: `venv\Scripts\activate`
   - Linux/Mac: `source venv/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`
5. Copy `.env.example` to `.env` and fill in your Supabase credentials
6. Run the scraper: `python scraper.py`

## Notes

- The scraper is scheduled to run once daily at 16:00 UTC
- It automatically stops when listings from a different date are encountered
- It processes up to 150 pages, but will stop earlier if date changes
- Uses 5 parallel workers for performance by default
- Selenium requires Chrome to be installed on the server, which Render provides
