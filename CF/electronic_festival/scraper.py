"""
ELECTRONIC FESTIVAL Scraper - Cloudflare R2 Version (Concurrent Subcategory Scraping)
Scrapes https://www.sheeel.com/ar/electronic-festival.html and all subcategories
Uses asyncio + semaphore for parallel subcategory processing
Saves data to Cloudflare R2 with date partitioning and downloads images
Each subcategory is saved as a separate sheet in the Excel file
Falls back to scraping the main page directly if no subcategories are found
"""

from playwright.async_api import async_playwright
import asyncio
import json
import re
import os
import sys
import requests
from datetime import datetime
from pathlib import Path
import time
import pandas as pd
import boto3
from botocore.config import Config
from urllib.parse import urlparse
import hashlib

_CF_ROOT = Path(__file__).resolve().parent.parent
if str(_CF_ROOT) not in sys.path:
    sys.path.insert(0, str(_CF_ROOT))
from request_metrics import RequestMetricsTracker, build_daily_summary

# Helper function to clean illegal characters for Excel
def clean_for_excel(value):
    """Remove illegal characters that Excel/openpyxl cannot handle

    Handles strings, lists, and nested structures recursively
    """
    if value is None:
        return None

    if isinstance(value, str):
        cleaned = ''.join(char for char in value if ord(char) >= 32 or char in '\t\n\r')
        return cleaned
    elif isinstance(value, list):
        return [clean_for_excel(item) for item in value]
    elif isinstance(value, dict):
        return {k: clean_for_excel(v) for k, v in value.items()}

    return value

class ElectronicFestivalScraper:
    def __init__(self, r2_bucket=None, cf_access_key=None, cf_secret_key=None, cf_endpoint_url=None, max_concurrent_subcategories=3):
        self.base_url = "https://www.sheeel.com/ar/electronic-festival.html"
        self.category = "electronic_festival"
        self.subcategories = {}  # Will store {subcategory_name: [products]}
        self.all_products = []  # All products combined
        self.r2_bucket = r2_bucket
        self.max_concurrent = max_concurrent_subcategories

        if r2_bucket and cf_access_key and cf_secret_key and cf_endpoint_url:
            self.r2_client = boto3.client(
                's3',
                endpoint_url=cf_endpoint_url,
                aws_access_key_id=cf_access_key,
                aws_secret_access_key=cf_secret_key,
                region_name='us-east-1',
                config=Config(
                    signature_version='s3v4',
                    s3={'addressing_style': 'path'}
                )
            )
        else:
            self.r2_client = None

        now = datetime.now()
        self.year = now.strftime("%Y")
        self.month = now.strftime("%m")
        self.day = now.strftime("%d")

        self.local_data_dir = Path("data")
        self.local_images_dir = self.local_data_dir / "images"
        self.local_data_dir.mkdir(exist_ok=True)
        self.local_images_dir.mkdir(exist_ok=True)

        self.metrics = RequestMetricsTracker()

    def _track_playwright_response(self, response, name=None, slug=None):
        if response is None:
            self.metrics.record_failure(name=name, slug=slug, detail="no response")
            return
        self.metrics.record_http_response(response.status, name=name, slug=slug)

    async def get_subcategories(self, page):
        """Extract all subcategory links from the main category page"""

        print("\n" + "="*70)
        print("🔍 EXTRACTING SUBCATEGORIES")
        print("="*70)

        try:
            await page.wait_for_selector('.subcategory-link', timeout=10000)
            subcategory_elements = await page.query_selector_all('.subcategory-link')

            subcategories = []
            seen_slugs = set()

            for elem in subcategory_elements:
                url = await elem.get_attribute('href')
                name = await elem.inner_text()
                name = name.strip()

                if url and name and '/ar/electronic-festival/' in url:
                    slug = url.split('/')[-1].replace('.html', '')

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
            # If no subcategories found, treat the main page as a single category
            print("ℹ Treating main page as single category (no subcategories)")
            return []

    async def has_next_page(self, page):
        try:
            next_button = await page.query_selector('.pages-item-next a.next')
            return next_button is not None
        except Exception as e:
            print(f"  ⚠ Error checking next page: {e}")
            return False

    async def scrape_page(self, page, page_num, subcategory_name):
        print(f"\n{'='*70}")
        print(f"📄 SCRAPING PAGE {page_num} - {subcategory_name}")
        print("="*70)
        try:
            print(f"  ⏳ Waiting for product links...")
            await page.wait_for_selector('[id^="product-item-info_"] > a', timeout=10000)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
            product_links = await page.eval_on_selector_all('[id^="product-item-info_"] > a', 'elements => elements.map(e => e.href)')
            print(f"  ✓ Found {len(product_links)} product links on page {page_num}\n")

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
        try:
            detail_page = await context.new_page()
            response = await detail_page.goto(product_url, wait_until='networkidle', timeout=30000)
            self._track_playwright_response(response)
            if response and response.status == 404:
                print(f"       ⚠ Skipping (404 Not Found): {product_url}")
                await detail_page.close()
                return None
            await detail_page.wait_for_selector('#maincontent .product-info-main', timeout=10000)

            info = await detail_page.query_selector('#maincontent .product-info-main')
            product_data = {}

            product_id_input = await detail_page.query_selector('input[name="product"]')
            if product_id_input:
                product_data['product_id'] = int(await product_id_input.get_attribute('value'))
            else:
                product_data['product_id'] = None

            title_el = await info.query_selector('.page-title .base')
            product_data['name'] = (await title_el.inner_text()).strip() if title_el else None

            sku_el = await detail_page.query_selector('.product-info.sku')
            if sku_el:
                sku_text = await sku_el.inner_text()
                product_data['sku'] = sku_text.split(':')[0].strip()
            else:
                product_data['sku'] = None

            avail_el = await detail_page.query_selector('.availability-info')
            product_data['availability'] = (await avail_el.inner_text()).strip() if avail_el else None

            bought_el = await detail_page.query_selector('.x-bought-count')
            product_data['times_bought'] = (await bought_el.inner_text()).strip() if bought_el else None

            old_price_el = await detail_page.query_selector('.old-price .price')
            product_data['old_price'] = (await old_price_el.inner_text()).strip() if old_price_el else None

            special_price_el = await detail_page.query_selector('.special-price .price, .normal-price .price')
            product_data['special_price'] = (await special_price_el.inner_text()).strip() if special_price_el else None

            normal_price_el = await detail_page.query_selector('.normal-price .price')
            product_data['normal_price'] = (await normal_price_el.inner_text()).strip() if normal_price_el else None

            desc_el = await detail_page.query_selector('.product.attribute.overview .value')
            product_data['description'] = (await desc_el.inner_text()).strip() if desc_el else None

            brand_el = await detail_page.query_selector('a.amshopby-brand-title-link')
            product_data['brand'] = (await brand_el.inner_text()).strip() if brand_el else None

            image_elements = await detail_page.query_selector_all('.product-gallery-image')
            image_urls = []
            for img_el in image_elements:
                img_url = await img_el.get_attribute('data-src') or await img_el.get_attribute('src')
                if img_url:
                    image_urls.append(img_url)
            product_data['image_urls'] = image_urls

            timer_el = await detail_page.query_selector('#deal-timer .time')
            product_data['deal_time_left'] = (await timer_el.inner_text()).strip() if timer_el else None

            discount_el = await detail_page.query_selector('.discount-percent-item')
            product_data['discount_badge'] = (await discount_el.inner_text()).strip() if discount_el else None

            more_info_container = await detail_page.query_selector('#more-info')
            if more_info_container:
                attribute_labels = await more_info_container.query_selector_all('.attribute-info.label')

                for label_el in attribute_labels:
                    section_name = (await label_el.inner_text()).strip()
                    ul_element = await label_el.evaluate_handle('node => node.nextElementSibling')
                    section_features = []
                    try:
                        li_elements = await ul_element.as_element().query_selector_all('li')
                        for li in li_elements:
                            section_features.append((await li.inner_text()).strip())
                    except:
                        pass

                    if 'المميزات' in section_name or 'المواصفات' in section_name:
                        product_data['features_specs'] = section_features
                        for i, feature in enumerate(section_features):
                            product_data[f'feature_spec_{i}'] = feature
                    elif 'محتوى' in section_name or 'العلبة' in section_name:
                        product_data['box_contents'] = section_features[0] if section_features else None
                    elif 'الكفالة' in section_name or 'ضمان' in section_name:
                        product_data['warranty'] = section_features[0] if section_features else None
                    else:
                        key = section_name.replace(' ', '_').replace(':', '')
                        product_data[f'other_{key}'] = section_features

            product_data['url'] = product_url
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
        async with semaphore:
            print("\n" + "="*70)
            print(f"📱 SCRAPING SUBCATEGORY: {subcategory['name']}")
            print("="*70)
            print(f"URL: {subcategory['url']}\n")

            page = await browser_context.new_page()
            subcategory_products = []

            try:
                print("📡 Loading first page...")
                response = await page.goto(subcategory['url'], wait_until='networkidle', timeout=30000)
                self._track_playwright_response(
                    response, name=subcategory['name'], slug=subcategory['slug']
                )
                if response and response.status == 404:
                    print(f"⚠ Subcategory '{subcategory['name']}' returned 404 - URL may have changed. Skipping.")
                    await page.close()
                    return subcategory['slug'], subcategory['name'], []
                print(f"✓ Page loaded: {await page.title()}\n")

                page_num = 1

                while True:
                    page_products = await self.scrape_page(page, page_num, subcategory['name'])
                    subcategory_products.extend(page_products)

                    if await self.has_next_page(page):
                        page_num += 1
                        print(f"\n⏳ Waiting 2s before next page...")
                        await asyncio.sleep(2)
                        next_url = f"{subcategory['url']}?p={page_num}"
                        print(f"📡 Loading page {page_num}: {next_url}")
                        response = await page.goto(next_url, wait_until='networkidle', timeout=30000)
                        self._track_playwright_response(
                            response, name=subcategory['name'], slug=subcategory['slug']
                        )
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
                self.metrics.record_failure(
                    name=subcategory['name'],
                    slug=subcategory['slug'],
                    detail=str(e)[:120],
                )
                import traceback
                traceback.print_exc()

            finally:
                await page.close()

            return subcategory['slug'], subcategory['name'], subcategory_products

    async def scrape_main_page_only(self, browser_context):
        """Scrape the main page when no subcategories are found"""

        print("\n" + "="*70)
        print("📄 SCRAPING MAIN PAGE (NO SUBCATEGORIES)")
        print("="*70)

        page = await browser_context.new_page()
        all_products = []

        try:
            print("📡 Loading main page...")
            response = await page.goto(self.base_url, wait_until='networkidle', timeout=30000)
            self._track_playwright_response(response, name=self.category)
            if response and response.status == 404:
                print(f"❌ Main category page returned 404 - URL may have changed: {self.base_url}")
                await page.close()
                return all_products
            print(f"✓ Page loaded: {await page.title()}\n")

            page_num = 1

            while True:
                page_products = await self.scrape_page(page, page_num, self.category)
                all_products.extend(page_products)

                if await self.has_next_page(page):
                    page_num += 1
                    print(f"\n⏳ Waiting 2s before next page...")
                    await asyncio.sleep(2)
                    next_url = f"{self.base_url}?p={page_num}"
                    print(f"📡 Loading page {page_num}: {next_url}")
                    response = await page.goto(next_url, wait_until='networkidle', timeout=30000)
                    self._track_playwright_response(response, name=self.category)
                    if response and response.status == 404:
                        print(f"  ⚠ Page {page_num} returned 404, stopping pagination")
                        break
                else:
                    print(f"\n✓ No more pages found. Reached last page: {page_num}")
                    break

        except Exception as e:
            print(f"\n❌ Error scraping main page: {e}")
            import traceback
            traceback.print_exc()

        finally:
            await page.close()

        return all_products

    async def scrape_all_subcategories(self):
        print("\n" + "="*70)
        print("ELECTRONIC FESTIVAL SCRAPER - CONCURRENT SUBCATEGORIES")
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
                print("📡 Loading main category page...")
                response = await page.goto(self.base_url, wait_until='networkidle', timeout=30000)
                self._track_playwright_response(response, name=self.category)
                if response and response.status == 404:
                    print(f"❌ Main category page returned 404 - URL may have changed: {self.base_url}")
                    await page.close()
                    return
                print(f"✓ Page loaded: {await page.title()}\n")

                subcategories = await self.get_subcategories(page)
                await page.close()

                if not subcategories:
                    # No subcategories - scrape main page directly
                    print("ℹ No subcategories found. Scraping main page directly...")
                    products = await self.scrape_main_page_only(context)
                    if products:
                        self.subcategories[self.category] = {
                            'name': self.category,
                            'products': products
                        }
                        self.all_products.extend(products)
                    return

                semaphore = asyncio.Semaphore(self.max_concurrent)

                print("\n" + "="*70)
                print(f"🔄 STARTING CONCURRENT SCRAPING ({self.max_concurrent} at a time)")
                print("="*70)

                tasks = [
                    self.scrape_subcategory(context, subcategory, semaphore)
                    for subcategory in subcategories
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        print(f"❌ Subcategory scraping failed: {result}")
                        self.metrics.record_failure(detail=str(result)[:120])
                        continue

                    slug, name, products = result
                    self.subcategories[slug] = {
                        'name': name,
                        'products': products
                    }
                    self.all_products.extend(products)

                print("\n" + "="*70)
                print("✅ ALL SUBCATEGORIES SCRAPED SUCCESSFULLY")
                print("="*70)
                print(f"\nTotal subcategories: {len(subcategories)}")
                print(f"Total products across all subcategories: {len(self.all_products)}")

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
        if not image_url:
            return None, None

        try:
            response = requests.get(image_url, timeout=10, stream=True)
            self.metrics.record_http_response(response.status_code)
            response.raise_for_status()

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
                ext = os.path.splitext(urlparse(image_url).path)[1] or '.jpg'

            filename = f"{product_id}_{image_index}{ext}"
            local_path = self.local_images_dir / filename
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            r2_path = None
            if upload_immediately and self.r2_client:
                image_r2_key = f"sheeel_data/year={self.year}/month={self.month}/day={self.day}/{self.category}/images/{filename}"
                try:
                    self.r2_client.upload_file(str(local_path), self.r2_bucket, image_r2_key)
                    r2_path = f"r2://{self.r2_bucket}/{image_r2_key}"
                except Exception as e:
                    print(f"  ⚠ Error uploading image to R2: {e}")

            return str(local_path), r2_path

        except Exception as e:
            print(f"  ⚠ Error downloading image for product {product_id}: {e}")
            self.metrics.record_failure(detail=str(e)[:120])
            return None, None

    def download_all_images(self):
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
            r2_image_paths = []

            for idx, img_url in enumerate(image_urls):
                local_path, r2_path = self.download_image(
                    img_url,
                    product['product_id'],
                    idx,
                    upload_immediately=True
                )
                if local_path:
                    local_image_paths.append(local_path)
                    total_images_downloaded += 1
                if r2_path:
                    r2_image_paths.append(r2_path)
                    total_images_uploaded += 1

            product['local_image_paths'] = local_image_paths
            product['r2_image_paths'] = r2_image_paths

            if i % 10 == 0:
                if self.r2_client:
                    print(f"  Processed {i}/{total_products} products (Downloaded: {total_images_downloaded}, Uploaded: {total_images_uploaded})...")
                else:
                    print(f"  Processed {i}/{total_products} products (Downloaded: {total_images_downloaded})...")

        if self.r2_client:
            print(f"\n✓ Downloaded {total_images_downloaded} images and uploaded {total_images_uploaded} to R2")
        else:
            print(f"\n✓ Downloaded {total_images_downloaded} images from {total_products} products")

    def save_to_excel(self, include_r2_paths=False):
        print("\n" + "="*70)
        print("💾 SAVING TO EXCEL (MULTI-SHEET)")
        print("="*70)

        if not self.all_products:
            print("⚠ No products to save")
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"electronic_festival_{timestamp}.xlsx"
        local_path = self.local_data_dir / filename

        with pd.ExcelWriter(local_path, engine='openpyxl') as writer:
            for slug, data in self.subcategories.items():
                if not data['products']:
                    continue

                df = pd.DataFrame(data['products'])

                if include_r2_paths and 'local_image_paths' in df.columns:
                    df = df.drop(columns=['local_image_paths'])

                try:
                    df = df.map(clean_for_excel)
                except AttributeError:
                    df = df.applymap(clean_for_excel)

                sheet_name = slug[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"✓ Sheet '{sheet_name}': {len(df)} products")

            df_all = pd.DataFrame(self.all_products)
            if include_r2_paths and 'local_image_paths' in df_all.columns:
                df_all = df_all.drop(columns=['local_image_paths'])

            try:
                df_all = df_all.map(clean_for_excel)
            except AttributeError:
                df_all = df_all.applymap(clean_for_excel)

            df_all.to_excel(writer, sheet_name='ALL_PRODUCTS', index=False)
            print(f"✓ Sheet 'ALL_PRODUCTS': {len(df_all)} products")

        print(f"\n✓ Saved to: {local_path}")
        print(f"  Total sheets: {len(self.subcategories) + 1}")
        print(f"  Total products: {len(self.all_products)}")

        return str(local_path)

    def upload_to_r2(self, local_file, r2_key):
        if not self.r2_client:
            print("⚠ R2 not configured, skipping upload")
            return False

        try:
            self.r2_client.upload_file(local_file, self.r2_bucket, r2_key)
            print(f"✓ Uploaded to r2://{self.r2_bucket}/{r2_key}")
            return True
        except Exception as e:
            print(f"❌ Error uploading to R2: {e}")
            return False

    def upload_results_to_r2(self):
        if not self.r2_client:
            print("\n⚠ R2 not configured, skipping R2 upload")
            return None

        print("\n" + "="*70)
        print("☁️  UPLOADING EXCEL TO CLOUDFLARE R2")
        print("="*70)

        print(f"\n📊 Creating multi-sheet Excel file with R2 paths...")
        excel_path = self.save_to_excel(include_r2_paths=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_filename = f"electronic_festival_{timestamp}.xlsx"
        excel_r2_key = f"sheeel_data/year={self.year}/month={self.month}/day={self.day}/{self.category}/excel-files/{excel_filename}"
        self.upload_to_r2(excel_path, excel_r2_key)
        self.upload_excel_json_version_to_r2(excel_path, excel_filename)
        self.upload_json_summary(len(self.all_products))

        return excel_path


    def upload_excel_json_version_to_r2(self, excel_local_path, excel_filename):
        if not self.r2_client:
            return

        records = getattr(self, "products", None)
        if records is None:
            records = getattr(self, "all_products", [])

        json_filename = os.path.splitext(excel_filename)[0] + ".json"
        json_local_path = self.local_data_dir / json_filename

        with open(json_local_path, "w", encoding="utf-8") as fh:
            json.dump(records, fh, indent=2, ensure_ascii=False, default=str)

        json_r2_key = (
            f"sheeel_data/year={self.year}/month={self.month}/day={self.day}/"
            f"{self.category}/json version/{json_filename}"
        )
        self.upload_to_r2(str(json_local_path), json_r2_key)

    def upload_json_summary(self, total_listings):
        if not self.r2_client:
            return

        summary = build_daily_summary(
            total_listings=total_listings,
            request_metrics=self.metrics.build_block(),
        )
        datestamp = datetime.now().strftime("%Y%m%d")
        summary_filename = f"summary_{datestamp}.json"
        local_summary = self.local_data_dir / summary_filename
        with open(local_summary, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2, ensure_ascii=False)

        summary_r2_key = (
            f"sheeel_data/year={self.year}/month={self.month}/day={self.day}/"
            f"{self.category}/json-files/{summary_filename}"
        )
        self.upload_to_r2(str(local_summary), summary_r2_key)

    def run(self):
        print("\n" + "="*70)
        print("ELECTRONIC FESTIVAL SCRAPER - CLOUDFLARE R2 (CONCURRENT)")
        print("="*70)
        print(f"\nDate: {self.year}-{self.month}-{self.day}")
        print(f"Category: {self.category}")
        print(f"R2 Bucket: {self.r2_bucket or 'Not configured'}")
        print(f"Concurrency: {self.max_concurrent} subcategories at a time")
        print()

        asyncio.run(self.scrape_all_subcategories())

        if not self.all_products:
            print("\n❌ No products scraped, exiting")
            return

        # self.download_all_images()  # disabled: image download only enabled for LogicPulse/coupons

        if self.r2_client:
            excel_path = self.upload_results_to_r2()
        else:
            excel_path = self.save_to_excel()

        print("\n" + "="*70)
        print("📊 FINAL SUMMARY")
        print("="*70)
        print(f"\n✅ Total subcategories: {len(self.subcategories)}")
        print(f"\n✅ Total products: {len(self.all_products)}")
        print(f"✅ Excel file: {excel_path}")

        if self.r2_client:
            print(f"\n☁️  R2 Paths:")
            print(f"  Excel: r2://{self.r2_bucket}/sheeel_data/year={self.year}/month={self.month}/day={self.day}/{self.category}/excel-files/")
            print(f"  Images: r2://{self.r2_bucket}/sheeel_data/year={self.year}/month={self.month}/day={self.day}/{self.category}/images/")

        print("\n" + "="*70)
        print("✅ SCRAPING COMPLETE!")
        print("="*70 + "\n")


if __name__ == "__main__":
    r2_bucket = os.getenv('CF_R2_BUCKET_NAME')
    cf_access_key = os.getenv('CF_R2_ACCESS_KEY_ID')
    cf_secret_key = os.getenv('CF_R2_SECRET_ACCESS_KEY')
    cf_endpoint_url = os.getenv('CF_R2_ENDPOINT_URL')

    max_concurrent = int(os.getenv('MAX_CONCURRENT_SUBCATEGORIES', '3'))

    scraper = ElectronicFestivalScraper(
        r2_bucket=r2_bucket,
        cf_access_key=cf_access_key,
        cf_secret_key=cf_secret_key,
        cf_endpoint_url=cf_endpoint_url,
        max_concurrent_subcategories=max_concurrent
    )
    scraper.run()
