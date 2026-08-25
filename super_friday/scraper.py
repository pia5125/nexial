"""
SUPER FRIDAY Scraper - Optimized with Concurrent Subcategory Scraping
Scrapes https://www.sheeel.com/ar/sale11.html and all subcategories
Uses asyncio + semaphore for parallel subcategory processing
Saves data to S3 with date partitioning and downloads images  
Each subcategory is saved as a separate sheet in the Excel file
"""

from playwright.async_api import async_playwright
import asyncio
import json
import re
import os
import requests
from datetime import datetime
from pathlib import Path
import time
import pandas as pd
import boto3
from urllib.parse import urlparse
import hashlib

# Helper function to clean illegal characters for Excel
def clean_for_excel(value):
    """Remove illegal characters that Excel/openpyxl cannot handle
    
    Handles strings, lists, and nested structures recursively
    """
    if value is None:
        return None
    
    if isinstance(value, str):
        # Remove control characters except tab, newline, carriage return
        # Excel doesn't allow characters in range 0x00-0x1F except 0x09, 0x0A, 0x0D
        cleaned = ''.join(char for char in value if ord(char) >= 32 or char in '\t\n\r')
        return cleaned
    elif isinstance(value, list):
        # Recursively clean each item in the list
        return [clean_for_excel(item) for item in value]
    elif isinstance(value, dict):
        # Recursively clean each value in the dict
        return {k: clean_for_excel(v) for k, v in value.items()}
    
    return value

class SuperFridayScraper:
    def __init__(self, s3_bucket=None, aws_access_key=None, aws_secret_key=None, max_concurrent_subcategories=3):
        self.base_url = "https://www.sheeel.com/ar/sale11.html"
        self.category = "super_friday"
        self.subcategories ={}  # Will store {subcategory_name: [products]}
        self.all_products = []  # All products combined
        self.s3_bucket = s3_bucket
        self.max_concurrent = max_concurrent_subcategories  # Control concurrency
        
        # Setup S3 if credentials provided
        if s3_bucket and aws_access_key and aws_secret_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
        else:
            self.s3_client = None
        
        # Date partitioning
        now = datetime.now()
        self.year = now.strftime("%Y")
        self.month = now.strftime("%m")
        self.day = now.strftime("%d")
        
        # Local folders
        self.local_data_dir = Path("data")
        self.local_images_dir = self.local_data_dir / "images"
        self.local_data_dir.mkdir(exist_ok=True)
        self.local_images_dir.mkdir(exist_ok=True)
    
    async def get_subcategories(self, page):
        """Extract all subcategory links from the main category page"""
        
        print("\n" + "="*70)
        print("🔍 EXTRACTING SUBCATEGORIES")
        print("="*70)
        
        try:
            # Wait for subcategory links to load
            await page.wait_for_selector('.subcategory-link', timeout=10000)
            
            # Get all subcategory links
            subcategory_elements = await page.query_selector_all('.subcategory-link')
            
            subcategories = []
            seen_slugs = set()  # Track unique subcategories
            
            for elem in subcategory_elements:
                url = await elem.get_attribute('href')
                name = await elem.inner_text()
                name = name.strip()
                
                # Filter: Only include URLs that belong to this category
                if url and name and '/ar/sale11/' in url:
                    # Extract clean subcategory slug from URL
                    slug = url.split('/')[-1].replace('.html', '')
                    
                    # Skip empty slugs and duplicates
                    if slug and slug not in seen_slugs:
                        seen_slugs.add(slug)
                        subcategories.append({
                            'name': name,
                            'url': url,
                            'slug': slug
                        })
            
            print(f"✓ Found {len(subcategories)} subcategories:")
            for i, subcat in enumerate(subcategories, 1):
                print(f"  {i}. {subcat['name']} → {subcat['slug']}")
            
            return subcategories
            
        except Exception as e:
            print(f"❌ Error extracting subcategories: {e}")
            return []
    
    async def has_next_page(self, page):
        """Check if there's a next page by looking for the Next button"""
        
        try:
            next_button = await page.query_selector('.pages-item-next a.next')
            return next_button is not None
        except Exception as e:
            print(f"  ⚠ Error checking next page: {e}")
            return False
    
    async def scrape_page(self, page, page_num, subcategory_name):
        """Scrape a single page by visiting each product link and extracting full details"""
        print(f"\n{'='*70}")
        print(f"📄 SCRAPING PAGE {page_num} - {subcategory_name}")
        print("="*70)
        try:
            # Wait for products
            print(f"  ⏳ Waiting for product links...")
            await page.wait_for_selector('[id^="product-item-info_"] > a', timeout=10000)
            # Scroll to load all products
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
            # Get all product links
            product_links = await page.eval_on_selector_all('[id^="product-item-info_"] > a', 'elements => elements.map(e => e.href)')
            print(f"  ✓ Found {len(product_links)} product links on page {page_num}\n")
            
            # Extract data from each product page
            page_products = []
            for i, product_url in enumerate(product_links, 1):
                print(f"  [{i}/{len(product_links)}] 🔗 {product_url.split('/')[-1][:50]}...")
                product_data = await self.scrape_product_detail(page.context, product_url, i)
                if product_data:
                    product_data['page_number'] = page_num
                    product_data['subcategory'] = subcategory_name
                    page_products.append(product_data)
                    print(f"       ✓ Extracted: {product_data.get('name', 'N/A')[:40]}")
                else:
                    print(f"       ⚠ Failed to extract product data")
                if i % 5 == 0:
                    print(f"\n  📊 Progress: {i}/{len(product_links)} products ({(i/len(product_links)*100):.1f}%)\n")
            
            print(f"\n✓ Successfully extracted {len(page_products)} products from page {page_num}")
            return page_products
        except Exception as e:
            print(f"❌ Error scraping page {page_num}: {e}")
            return []

    async def scrape_product_detail(self, context, product_url, index):
        """Visit product detail page and extract all available fields"""
        try:
            detail_page = await context.new_page()
            response = await detail_page.goto(product_url, wait_until='networkidle', timeout=30000)
            if response and response.status == 404:
                print(f"       ⚠ Skipping (404 Not Found): {product_url}")
                await detail_page.close()
                return None
            await detail_page.wait_for_selector('#maincontent .product-info-main', timeout=10000)
            
            # Extract fields from product-info-main
            info = await detail_page.query_selector('#maincontent .product-info-main')
            product_data = {}
            
            # Product ID from form
            product_id_input = await detail_page.query_selector('input[name="product"]')
            if product_id_input:
                product_data['product_id'] = int(await product_id_input.get_attribute('value'))
            else:
                product_data['product_id'] = None
            
            # Title
            title_el = await info.query_selector('.page-title .base')
            product_data['name'] = (await title_el.inner_text()).strip() if title_el else None
            
            # SKU
            sku_el = await detail_page.query_selector('.product-info.sku')
            if sku_el:
                sku_text = await sku_el.inner_text()
                product_data['sku'] = sku_text.split(':')[0].strip()
            else:
                product_data['sku'] = None
            
            # Availability
            avail_el = await detail_page.query_selector('.availability-info')
            product_data['availability'] = (await avail_el.inner_text()).strip() if avail_el else None
            
            # Times bought
            bought_el = await detail_page.query_selector('.x-bought-count')
            product_data['times_bought'] = (await bought_el.inner_text()).strip() if bought_el else None
            
            # Old price
            old_price_el = await detail_page.query_selector('.old-price .price')
            product_data['old_price'] = (await old_price_el.inner_text()).strip() if old_price_el else None
            
            # Special price (handles both .special-price and .normal-price display cases)
            special_price_el = await detail_page.query_selector('.special-price .price, .normal-price .price')
            product_data['special_price'] = (await special_price_el.inner_text()).strip() if special_price_el else None

            # Normal price (only populated when product displays with .normal-price element)
            normal_price_el = await detail_page.query_selector('.normal-price .price')
            product_data['normal_price'] = (await normal_price_el.inner_text()).strip() if normal_price_el else None
            
            # Description
            desc_el = await detail_page.query_selector('.product.attribute.overview .value')
            product_data['description'] = (await desc_el.inner_text()).strip() if desc_el else None
            
            # Brand name (optional - may not exist for all products)
            brand_el = await detail_page.query_selector('a.amshopby-brand-title-link')
            product_data['brand'] = (await brand_el.inner_text()).strip() if brand_el else None
            
            # All images from product gallery
            image_elements = await detail_page.query_selector_all('.product-gallery-image')
            image_urls = []
            for img_el in image_elements:
                img_url = await img_el.get_attribute('data-src') or await img_el.get_attribute('src')
                if img_url:
                    image_urls.append(img_url)
            
            product_data['image_urls'] = image_urls  # Store as array
            
            # Deal timer
            timer_el = await detail_page.query_selector('#deal-timer .time')
            product_data['deal_time_left'] = (await timer_el.inner_text()).strip() if timer_el else None
            
            # Discount badge
            discount_el = await detail_page.query_selector('.discount-percent-item')
            product_data['discount_badge'] = (await discount_el.inner_text()).strip() if discount_el else None
            
            # Extract features by section with labels
            more_info_container = await detail_page.query_selector('#more-info')
            if more_info_container:
                # Get all attribute sections
                attribute_labels = await more_info_container.query_selector_all('.attribute-info.label')
                
                for label_el in attribute_labels:
                    section_name = (await label_el.inner_text()).strip()
                    
                    # Get the next sibling <ul> element
                    ul_element = await label_el.evaluate_handle('node => node.nextElementSibling')
                    
                    # Extract list items
                    section_features = []
                    try:
                        li_elements = await ul_element.as_element().query_selector_all('li')
                        for li in li_elements:
                            section_features.append((await li.inner_text()).strip())
                    except:
                        pass
                    
                    # Store features based on section name
                    if 'المميزات' in section_name or 'المواصفات' in section_name:
                        product_data['features_specs'] = section_features
                        # Flatten features_specs
                        for i, feature in enumerate(section_features):
                            product_data[f'feature_spec_{i}'] = feature
                    elif 'محتوى' in section_name or 'العلبة' in section_name:
                        # Box contents - single value
                        product_data['box_contents'] = section_features[0] if section_features else None
                    elif 'الكفالة' in section_name or 'ضمان' in section_name:
                        # Warranty - single value
                        product_data['warranty'] = section_features[0] if section_features else None
                    else:
                        # For any other sections, store with sanitized key
                        key = section_name.replace(' ', '_').replace(':', '')
                        product_data[f'other_{key}'] = section_features
            
            # Product URL
            product_data['url'] = product_url
            
            # Scraped at
            product_data['scraped_at'] = datetime.now().isoformat()
            
            await detail_page.close()
            return product_data
        except Exception as e:
            print(f"       ❌ Error: {str(e)[:50]}")
            try:
                await detail_page.close()
            except:
                pass
            return None
    
    async def scrape_subcategory(self, browser_context, subcategory, semaphore):
        """Scrape all pages of a subcategory (with semaphore for concurrency control)"""
        
        async with semaphore:  # Limit concurrent subcategories
            print("\n" + "="*70)
            print(f"📱 SCRAPING SUBCATEGORY: {subcategory['name']}")
            print("="*70)
            print(f"URL: {subcategory['url']}\n")
            
            page = await browser_context.new_page()
            subcategory_products = []
            
            try:
                # Load first page
                print("📡 Loading first page...")
                response = await page.goto(subcategory['url'], wait_until='networkidle', timeout=30000)
                if response and response.status == 404:
                    print(f"⚠ Subcategory '{subcategory['name']}' returned 404 - URL may have changed. Skipping.")
                    await page.close()
                    return subcategory['slug'], subcategory['name'], []
                print(f"✓ Page loaded: {await page.title()}\n")
                
                page_num = 1
                
                # Keep scraping while there are more pages
                while True:
                    # Scrape current page
                    page_products = await self.scrape_page(page, page_num, subcategory['name'])
                    subcategory_products.extend(page_products)
                    
                    # Check if there's a next page
                    if await self.has_next_page(page):
                        page_num += 1
                        print(f"\n⏳ Waiting 2s before next page...")
                        await asyncio.sleep(2)
                        
                        # Navigate to next page
                        next_url = f"{subcategory['url']}?p={page_num}"
                        print(f"📡 Loading page {page_num}: {next_url}")
                        response = await page.goto(next_url, wait_until='networkidle', timeout=30000)
                        if response and response.status == 404:
                            print(f"  ⚠ Page {page_num} returned 404, stopping pagination")
                            break
                    else:
                        print(f"\n✓ No more pages found. Reached last page: {page_num}")
                        break
                
                print("\n" + "="*70)
                print(f"✅ SUBCATEGORY COMPLETE: {subcategory['name']}")
                print("="*70)
                print(f"Total products scraped: {len(subcategory_products)}")
                print(f"Across {page_num} pages\n")
                
            except Exception as e:
                print(f"\n❌ Error scraping subcategory {subcategory['name']}: {e}")
                import traceback
                traceback.print_exc()
            
            finally:
                await page.close()
            
            return subcategory['slug'], subcategory['name'], subcategory_products
    
    async def scrape_all_subcategories(self):
        """Scrape all subcategories concurrently with semaphore control"""
        
        print("\n" + "="*70)
        print("SUPER FRIDAY SCRAPER - CONCURRENT SUBCATEGORIES")
        print("="*70)
        print(f"\nMain URL: {self.base_url}")
        print(f"Max Concurrent Subcategories: {self.max_concurrent}\n")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            try:
                # Load main page to get subcategories
                print("📡 Loading main category page...")
                response = await page.goto(self.base_url, wait_until='networkidle', timeout=30000)
                if response and response.status == 404:
                    print(f"❌ Main category page returned 404 - URL may have changed: {self.base_url}")
                    await page.close()
                    return
                print(f"✓ Page loaded: {await page.title()}\n")
                
                # Get subcategories
                subcategories = await self.get_subcategories(page)
                await page.close()
                
                if not subcategories:
                    print("❌ No subcategories found!")
                    return
                
                # Create semaphore for concurrency control
                semaphore = asyncio.Semaphore(self.max_concurrent)
                
                print("\n" + "="*70)
                print(f"🔄 STARTING CONCURRENT SCRAPING ({self.max_concurrent} at a time)")
                print("="*70)
                
                # Scrape all subcategories concurrently
                tasks = [
                    self.scrape_subcategory(context, subcategory, semaphore)
                    for subcategory in subcategories
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for result in results:
                    if isinstance(result, Exception):
                        print(f"❌ Subcategory scraping failed: {result}")
                        continue
                    
                    slug, name, products = result
                    
                    # Store products by subcategory
                    self.subcategories[slug] = {
                        'name': name,
                        'products': products
                    }
                    
                    # Add to all products list
                    self.all_products.extend(products)
                
                print("\n" + "="*70)
                print("✅ ALL SUBCATEGORIES SCRAPED SUCCESSFULLY")
                print("="*70)
                print(f"\nTotal subcategories: {len(subcategories)}")
                print(f"Total products across all subcategories: {len(self.all_products)}")
                
                # Print summary by subcategory
                print("\n📊 PRODUCTS BY SUBCATEGORY:")
                for slug, data in self.subcategories.items():
                    print(f"  • {data['name']}: {len(data['products'])} products")
                
            except Exception as e:
                print(f"\n❌ Error during scraping: {e}")
                import traceback
                traceback.print_exc()
                
            finally:
                await context.close()
                await browser.close()
    
    def download_image(self, image_url, product_id, image_index=0, upload_immediately=True):
        """Download product image and optionally upload to S3 immediately"""
        
        if not image_url:
            return None, None
        
        try:
            # Download image
            response = requests.get(image_url, timeout=10, stream=True)
            response.raise_for_status()
            
            # Detect proper extension from Content-Type header
            content_type = response.headers.get('Content-Type', '').lower()
            if 'jpeg' in content_type or 'jpg' in content_type:
                ext = '.jpg'
            elif 'png' in content_type:
                ext = '.png'
            elif 'gif' in content_type:
                ext = '.gif'
            elif 'webp' in content_type:
                ext = '.webp'
            else:
                # Fallback to URL extension or .jpg
                ext = os.path.splitext(urlparse(image_url).path)[1] or '.jpg'
            
            # Include index in filename for multiple images
            filename = f"{product_id}_{image_index}{ext}"
            
            # Save locally
            local_path = self.local_images_dir / filename
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Upload to S3 immediately if configured and requested
            s3_path = None
            if upload_immediately and self.s3_client:
                image_s3_key = f"sheeel_data/year={self.year}/month={self.month}/day={self.day}/{self.category}/images/{filename}"
                try:
                    self.s3_client.upload_file(str(local_path), self.s3_bucket, image_s3_key)
                    s3_path = f"s3://{self.s3_bucket}/{image_s3_key}"
                except Exception as e:
                    print(f"  ⚠ Error uploading image to S3: {e}")
            
            return str(local_path), s3_path
            
        except Exception as e:
            print(f"  ⚠ Error downloading image for product {product_id}: {e}")
            return None, None
    
    def download_all_images(self):
        """Download all product images and upload to S3 incrementally"""
        
        print("\n" + "="*70)
        print("📥 DOWNLOADING & UPLOADING PRODUCT IMAGES")
        print("="*70)
        
        total_products = len(self.all_products)
        total_images_downloaded = 0
        total_images_uploaded = 0
        
        for i, product in enumerate(self.all_products, 1):
            image_urls = product.get('image_urls', [])
            if not image_urls:
                continue
            
            local_image_paths = []
            s3_image_paths = []
            
            for idx, img_url in enumerate(image_urls):
                local_path, s3_path = self.download_image(
                    img_url, 
                    product['product_id'], 
                    idx,
                    upload_immediately=True  # Upload as we download
                )
                if local_path:
                    local_image_paths.append(local_path)
                    total_images_downloaded += 1
                if s3_path:
                    s3_image_paths.append(s3_path)
                    total_images_uploaded += 1
            
            # Store all local and S3 paths as arrays
            product['local_image_paths'] = local_image_paths
            product['s3_image_paths'] = s3_image_paths
                    
            if i % 10 == 0:
                if self.s3_client:
                    print(f"  Processed {i}/{total_products} products (Downloaded: {total_images_downloaded}, Uploaded: {total_images_uploaded})...")
                else:
                    print(f"  Processed {i}/{total_products} products (Downloaded: {total_images_downloaded})...")
        
        if self.s3_client:
            print(f"\n✓ Downloaded {total_images_downloaded} images and uploaded {total_images_uploaded} to S3")
        else:
            print(f"\n✓ Downloaded {total_images_downloaded} images from {total_products} products")
    
    def save_to_excel(self, include_s3_paths=False):
        """Save data to Excel file with multiple sheets (one per subcategory)"""
        
        print("\n" + "="*70)
        print("💾 SAVING TO EXCEL (MULTI-SHEET)")
        print("="*70)
        
        if not self.all_products:
            print("⚠ No products to save")
            return None
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"super_friday_{timestamp}.xlsx"
        local_path = self.local_data_dir / filename
        
        # Create Excel writer
        with pd.ExcelWriter(local_path, engine='openpyxl') as writer:
            
            # Create a sheet for each subcategory
            for slug, data in self.subcategories.items():
                if not data['products']:
                    continue
                
                # Create DataFrame for this subcategory
                df = pd.DataFrame(data['products'])
                
                # Remove local_image_paths column if S3 is configured
                if include_s3_paths and 'local_image_paths' in df.columns:
                    df = df.drop(columns=['local_image_paths'])
                
                # Clean ALL cells (including nested lists/dicts) to remove illegal Excel characters
                # Use map() to apply clean_for_excel to every single cell
                try:
                    df = df.map(clean_for_excel)  # pandas >= 2.1.0
                except AttributeError:
                    df = df.applymap(clean_for_excel)  # pandas < 2.1.0
                
                # Sheet name (max 31 characters for Excel)
                sheet_name = slug[:31]
                
                # Save to sheet
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"✓ Sheet '{sheet_name}': {len(df)} products")
            
            # Also create a summary sheet with all products
            df_all = pd.DataFrame(self.all_products)
            if include_s3_paths and 'local_image_paths' in df_all.columns:
                df_all = df_all.drop(columns=['local_image_paths'])
            
            # Clean ALL cells (including nested lists/dicts) to remove illegal Excel characters
            # Use map() to apply clean_for_excel to every single cell
            try:
                df_all = df_all.map(clean_for_excel)  # pandas >= 2.1.0
            except AttributeError:
                df_all = df_all.applymap(clean_for_excel)  # pandas < 2.1.0
            
            df_all.to_excel(writer, sheet_name='ALL_PRODUCTS', index=False)
            print(f"✓ Sheet 'ALL_PRODUCTS': {len(df_all)} products")
        
        print(f"\n✓ Saved to: {local_path}")
        print(f"  Total sheets: {len(self.subcategories) + 1}")
        print(f"  Total products: {len(self.all_products)}")
        
        return str(local_path)
    
    def upload_to_s3(self, local_file, s3_key):
        """Upload file to S3"""
        
        if not self.s3_client:
            print("⚠ S3 not configured, skipping upload")
            return False
        
        try:
            self.s3_client.upload_file(local_file, self.s3_bucket, s3_key)
            print(f"✓ Uploaded to s3://{self.s3_bucket}/{s3_key}")
            return True
        except Exception as e:
            print(f"❌ Error uploading to S3: {e}")
            return False
    
    def upload_results_to_s3(self):
        """Upload Excel to S3 (images already uploaded incrementally)"""
        
        if not self.s3_client:
            print("\n⚠ S3 not configured, skipping S3 upload")
            return None
        
        print("\n" + "="*70)
        print("☁️  UPLOADING EXCEL TO S3")
        print("="*70)
        
        # Images already uploaded incrementally during download
        # Just create Excel file with S3 paths (multi-sheet)
        print(f"\n📊 Creating multi-sheet Excel file with S3 paths...")
        excel_path = self.save_to_excel(include_s3_paths=True)
        
        # Upload to S3
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_filename = f"super_friday_{timestamp}.xlsx"
        excel_s3_key = f"sheeel_data/year={self.year}/month={self.month}/day={self.day}/{self.category}/excel-files/{excel_filename}"
        self.upload_to_s3(excel_path, excel_s3_key)
        
        return excel_path
    
    def run(self):
        """Main execution flow"""
        
        print("\n" + "="*70)
        print("SUPER FRIDAY SCRAPER - OPTIMIZED (CONCURRENT)")
        print("="*70)
        print(f"\nDate: {self.year}-{self.month}-{self.day}")
        print(f"Category: {self.category}")
        print(f"S3 Bucket: {self.s3_bucket or 'Not configured'}")
        print(f"Concurrency: {self.max_concurrent} subcategories at a time")
        print()
        
        # Step 1: Scrape all subcategories concurrently
        asyncio.run(self.scrape_all_subcategories())
        
        if not self.all_products:
            print("\n❌ No products scraped, exiting")
            return
        
        # Step 2: Download images
        # self.download_all_images()  # disabled: image download only enabled for LogicPulse/coupons
        
        # Step 3: Save to Excel and upload to S3
        if self.s3_client:
            # Use S3 upload which creates Excel with S3 paths
            excel_path = self.upload_results_to_s3()
        else:
            # Local only - save Excel with local paths
            excel_path = self.save_to_excel()
        
        # Summary
        print("\n" + "="*70)
        print("📊 FINAL SUMMARY")
        print("="*70)
        print(f"\n✅ Total subcategories: {len(self.subcategories)}")
        print(f"\n✅ Total products: {len(self.all_products)}")
        print(f"✅ Excel file: {excel_path}")
        
        if self.s3_client:
            print(f"\n☁️  S3 Paths:")
            print(f"  Excel: s3://{self.s3_bucket}/sheeel_data/year={self.year}/month={self.month}/day={self.day}/{self.category}/excel-files/")
            print(f"  Images: s3://{self.s3_bucket}/sheeel_data/year={self.year}/month={self.month}/day={self.day}/{self.category}/images/")
        
        print("\n" + "="*70)
        print("✅ SCRAPING COMPLETE!")
        print("="*70 + "\n")

if __name__ == "__main__":
    # Get configuration from environment or use defaults
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    # Concurrency setting (default: 3 subcategories at a time)
    max_concurrent = int(os.getenv('MAX_CONCURRENT_SUBCATEGORIES', '3'))
    
    # Run scraper
    scraper = SuperFridayScraper(
        s3_bucket=s3_bucket,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        max_concurrent_subcategories=max_concurrent
    )
    scraper.run()
