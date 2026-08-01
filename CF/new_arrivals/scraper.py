"""
New Arrivals Scraper - Cloudflare R2 Version
Scrapes https://www.sheeel.com/ar/new-arrivals.html with pagination
Saves data to Cloudflare R2 with date partitioning and downloads images
"""

from playwright.sync_api import sync_playwright
import json
import re
import os
import sys
import requests
from datetime import datetime
from pathlib import Path

_CF_ROOT = Path(__file__).resolve().parent.parent
if str(_CF_ROOT) not in sys.path:
    sys.path.insert(0, str(_CF_ROOT))
from request_metrics import RequestMetricsTracker, build_daily_summary
import time
import pandas as pd
import boto3
from botocore.config import Config
from urllib.parse import urlparse

class NewArrivalsScraper:
    def __init__(self, r2_bucket=None, cf_access_key=None, cf_secret_key=None, cf_endpoint_url=None):
        self.base_url = "https://www.sheeel.com/ar/new-arrivals.html"
        self.category = "new_arrivals"
        self.products = []
        self.r2_bucket = r2_bucket

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

    def _goto(self, page, url, timeout=60000, retries=3):
        """Navigate to a URL. Uses domcontentloaded because networkidle rarely settles on sheeel.com."""
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                response = page.goto(url, wait_until='domcontentloaded', timeout=timeout)
                self._track_playwright_response(response)
                return response
            except Exception as e:
                last_error = e
                if attempt < retries:
                    print(f"  ⚠ Navigation attempt {attempt}/{retries} failed, retrying in 2s...")
                    time.sleep(2)
        raise last_error

    def has_next_page(self, page):
        try:
            next_button = page.query_selector('.pages-item-next a.next')
            return next_button is not None
        except Exception as e:
            print(f"  ⚠ Error checking next page: {e}")
            return False

    def get_current_page_number(self, page):
        try:
            current_page = page.query_selector('.pages-items .item.current .page span:last-child')
            if current_page:
                return int(current_page.inner_text().strip())
            return 1
        except Exception as e:
            print(f"  ⚠ Error getting current page: {e}")
            return 1

    def scrape_page(self, page, page_num):
        print(f"\n{'='*70}")
        print(f"📄 SCRAPING PAGE {page_num}")
        print("="*70)
        try:
            print(f"  ⏳ Waiting for product links...")
            page.wait_for_selector('[id^="product-item-info_"] > a', timeout=10000)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            product_links = page.eval_on_selector_all('[id^="product-item-info_"] > a', 'elements => elements.map(e => e.href)')
            print(f"  ✓ Found {len(product_links)} product links on page {page_num}\n")
            page_products = []
            for i, product_url in enumerate(product_links, 1):
                print(f"  [{i}/{len(product_links)}] 🔗 {product_url.split('/')[-1][:50]}...")
                product_data = self.scrape_product_detail(page.context, product_url, i)
                if product_data:
                    product_data['page_number'] = page_num
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

    def scrape_product_detail(self, context, product_url, index):
        try:
            detail_page = context.new_page()
            response = self._goto(detail_page, product_url)
            if response and response.status == 404:
                print(f"       ⚠ Skipping (404 Not Found): {product_url}")
                detail_page.close()
                return None
            detail_page.wait_for_selector('#maincontent .product-info-main', timeout=10000)

            info = detail_page.query_selector('#maincontent .product-info-main')
            product_data = {}

            product_id_input = detail_page.query_selector('input[name="product"]')
            if product_id_input:
                product_data['product_id'] = int(product_id_input.get_attribute('value'))
            else:
                product_data['product_id'] = None

            title_el = info.query_selector('.page-title .base')
            product_data['name'] = title_el.inner_text().strip() if title_el else None

            sku_el = detail_page.query_selector('.product-info.sku')
            product_data['sku'] = sku_el.inner_text().split(':')[0].strip() if sku_el else None

            avail_el = detail_page.query_selector('.availability-info')
            product_data['availability'] = avail_el.inner_text().strip() if avail_el else None

            bought_el = detail_page.query_selector('.x-bought-count')
            product_data['times_bought'] = bought_el.inner_text().strip() if bought_el else None

            old_price_el = detail_page.query_selector('.old-price .price')
            product_data['old_price'] = old_price_el.inner_text().strip() if old_price_el else None

            special_price_el = detail_page.query_selector('.special-price .price, .normal-price .price')
            product_data['special_price'] = special_price_el.inner_text().strip() if special_price_el else None

            normal_price_el = detail_page.query_selector('.normal-price .price')
            product_data['normal_price'] = normal_price_el.inner_text().strip() if normal_price_el else None

            desc_el = detail_page.query_selector('.product.attribute.overview .value')
            product_data['description'] = desc_el.inner_text().strip() if desc_el else None

            brand_el = detail_page.query_selector('a.amshopby-brand-title-link')
            product_data['brand'] = brand_el.inner_text().strip() if brand_el else None

            image_elements = detail_page.query_selector_all('.product-gallery-image')
            image_urls = []
            for img_el in image_elements:
                img_url = img_el.get_attribute('data-src') or img_el.get_attribute('src')
                if img_url:
                    image_urls.append(img_url)
            product_data['image_urls'] = image_urls

            timer_el = detail_page.query_selector('#deal-timer .time')
            product_data['deal_time_left'] = timer_el.inner_text().strip() if timer_el else None

            discount_el = detail_page.query_selector('.discount-percent-item')
            product_data['discount_badge'] = discount_el.inner_text().strip() if discount_el else None

            more_info_container = detail_page.query_selector('#more-info')
            if more_info_container:
                attribute_labels = more_info_container.query_selector_all('.attribute-info.label')

                for label_el in attribute_labels:
                    section_name = label_el.inner_text().strip()
                    ul_element = label_el.evaluate_handle('node => node.nextElementSibling')
                    section_features = []
                    try:
                        li_elements = ul_element.as_element().query_selector_all('li')
                        for li in li_elements:
                            section_features.append(li.inner_text().strip())
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

            detail_page.close()
            return product_data
        except Exception as e:
            print(f"       ❌ Error: {str(e)[:50]}")
            try:
                detail_page.close()
            except:
                pass
            return None

    def scrape_all_pages(self):
        print("\n" + "="*70)
        print("🚀 NEW ARRIVALS SCRAPER - WITH PAGINATION")
        print("="*70)
        print(f"\nURL: {self.base_url}\n")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()

            try:
                print("📡 Loading first page...")
                response = self._goto(page, self.base_url)
                if response and response.status == 404:
                    print(f"❌ Main category page returned 404 - URL may have changed: {self.base_url}")
                    return
                print(f"✓ Page loaded: {page.title()}\n")

                page_num = 1

                while True:
                    page_products = self.scrape_page(page, page_num)
                    self.products.extend(page_products)

                    if self.has_next_page(page):
                        page_num += 1
                        print(f"\n⏳ Waiting 2s before next page...")
                        time.sleep(2)
                        next_url = f"{self.base_url}?p={page_num}"
                        print(f"📡 Loading page {page_num}: {next_url}")
                        response = self._goto(page, next_url)
                        if response and response.status == 404:
                            print(f"  ⚠ Page {page_num} returned 404, stopping pagination")
                            break
                    else:
                        print(f"\n✓ No more pages found. Reached last page: {page_num}")
                        break

                print("\n" + "="*70)
                print("✅ ALL PAGES SCRAPED SUCCESSFULLY")
                print("="*70)
                print(f"\nTotal products scraped: {len(self.products)}")
                print(f"Across {page_num} pages")

            except Exception as e:
                print(f"\n❌ Error during scraping: {e}")
                import traceback
                traceback.print_exc()

            finally:
                context.close()
                browser.close()

    def download_image(self, image_url, product_id, image_index=0):
        if not image_url:
            return None

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

            return str(local_path)

        except Exception as e:
            print(f"  ⚠ Error downloading image for product {product_id}: {e}")
            self.metrics.record_failure(detail=str(e)[:120])
            return None

    def download_all_images(self):
        print("\n" + "="*70)
        print("📥 DOWNLOADING PRODUCT IMAGES")
        print("="*70)

        total_products = len(self.products)
        total_images_downloaded = 0

        for i, product in enumerate(self.products, 1):
            image_urls = product.get('image_urls', [])
            if not image_urls:
                continue

            local_image_paths = []
            for idx, img_url in enumerate(image_urls):
                local_path = self.download_image(img_url, product['product_id'], idx)
                if local_path:
                    local_image_paths.append(local_path)
                    total_images_downloaded += 1

            product['local_image_paths'] = local_image_paths

            if i % 10 == 0:
                print(f"  Processed {i}/{total_products} products...")

        print(f"\n✓ Downloaded {total_images_downloaded} images from {total_products} products")

    def save_to_excel(self, include_r2_paths=False):
        print("\n" + "="*70)
        print("💾 SAVING TO EXCEL")
        print("="*70)

        if not self.products:
            print("⚠ No products to save")
            return None

        df = pd.DataFrame(self.products)

        if include_r2_paths and 'local_image_paths' in df.columns:
            df = df.drop(columns=['local_image_paths'])
            print("✓ Removed 'local_image_paths' column")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"new_arrivals_{timestamp}.xlsx"
        local_path = self.local_data_dir / filename

        df.to_excel(local_path, index=False, engine='openpyxl')
        print(f"✓ Saved to: {local_path}")
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {len(df.columns)}")

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
        print("☁️  UPLOADING TO CLOUDFLARE R2")
        print("="*70)

        print(f"\n📷 Uploading images...")
        total_images_uploaded = 0

        for product in self.products:
            local_image_paths = product.get('local_image_paths', [])
            if not local_image_paths:
                continue

            r2_image_paths = []
            for local_path in local_image_paths:
                if os.path.exists(local_path):
                    image_filename = os.path.basename(local_path)
                    image_r2_key = f"sheeel_data/year={self.year}/month={self.month}/day={self.day}/{self.category}/images/{image_filename}"

                    if self.upload_to_r2(local_path, image_r2_key):
                        r2_image_paths.append(f"r2://{self.r2_bucket}/{image_r2_key}")
                        total_images_uploaded += 1

                        if total_images_uploaded % 10 == 0:
                            print(f"  Uploaded {total_images_uploaded} images...")

            product['r2_image_paths'] = r2_image_paths

        print(f"\n✓ Uploaded {total_images_uploaded} images to R2")

        print(f"\n📊 Creating Excel file with R2 paths...")
        df = pd.DataFrame(self.products)

        if 'local_image_paths' in df.columns:
            df = df.drop(columns=['local_image_paths'])
            print("✓ Removed 'local_image_paths' column")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_filename = f"new_arrivals_{timestamp}.xlsx"
        excel_local_path = str(self.local_data_dir / excel_filename)
        df.to_excel(excel_local_path, index=False, engine='openpyxl')
        print(f"✓ Saved to: {excel_local_path}")
        print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")

        excel_r2_key = f"sheeel_data/year={self.year}/month={self.month}/day={self.day}/{self.category}/excel-files/{excel_filename}"
        self.upload_to_r2(excel_local_path, excel_r2_key)
        self.upload_excel_json_version_to_r2(excel_local_path, excel_filename)
        self.upload_json_summary(len(self.products))

        return excel_local_path


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
        print("🆕 NEW ARRIVALS SCRAPER - CLOUDFLARE R2")
        print("="*70)
        print(f"\nDate: {self.year}-{self.month}-{self.day}")
        print(f"Category: {self.category}")
        print(f"R2 Bucket: {self.r2_bucket or 'Not configured'}")
        print()

        self.scrape_all_pages()

        if not self.products:
            print("\n❌ No products scraped, exiting")
            return

        self.download_all_images()

        if self.r2_client:
            excel_path = self.upload_results_to_r2()
        else:
            excel_path = self.save_to_excel()

        print("\n" + "="*70)
        print("📊 FINAL SUMMARY")
        print("="*70)
        print(f"\n✅ Total products: {len(self.products)}")
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

    scraper = NewArrivalsScraper(
        r2_bucket=r2_bucket,
        cf_access_key=cf_access_key,
        cf_secret_key=cf_secret_key,
        cf_endpoint_url=cf_endpoint_url
    )
    scraper.run()
