import os
import sys
from datetime import datetime, timezone
import time
import csv
from typing import List, Dict, Optional
from dataclasses import dataclass
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from supabase import create_client

@dataclass
class RichListEntry:
    rank: int
    address: str
    label: str
    balance_xrp: float
    escrow_xrp: float
    percentage: float

class XRPLRichListScraper:
    def __init__(self):
        self.url = "https://xrpscan.com/balances"
        options = webdriver.ChromeOptions()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(20)

    def wait_for_rich_list_table(self):
        print("Waiting for rich list table to load...")
        try:
            wait = WebDriverWait(self.driver, 40)
            
            select_element = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "select#formGroupPage")))
            select = Select(select_element)
            select.select_by_value("10000")
            print("Changed display count to 10000 entries")
            
            time.sleep(10)
            
            table_header = wait.until(EC.presence_of_element_located(
                (By.XPATH, "//th[contains(text(), 'Top 10,000 XRP balances')]")))
            print("Rich list table header found")
            
            time.sleep(10)
            return True
            
        except Exception as e:
            print(f"Error while waiting for rich list table: {e}")
            return False

    def parse_xrp_amount(self, text: str) -> float:
        try:
            amount_str = text.replace('XRP', '').replace(',', '').strip()
            if not amount_str or amount_str == '-':
                return 0.0
            return float(amount_str)
        except (ValueError, AttributeError) as e:
            print(f"Error parsing XRP amount '{text}': {e}")
            return 0.0

    def parse_percentage(self, text: str) -> float:
        try:
            return float(text.replace('%', '').strip())
        except (ValueError, AttributeError):
            return 0.0

    def scrape_to_csv(self, output_path: str) -> bool:
        try:
            self.driver.get(self.url)
            if not self.wait_for_rich_list_table():
                return False

            table = self.driver.find_element(
                By.XPATH, 
                "//th[contains(text(), 'Top 10,000 XRP balances')]/ancestor::table"
            )
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['rank', 'address', 'label', 'balance_xrp', 'escrow_xrp', 'percentage', 'snapshot_date']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                snapshot_date = datetime.now(timezone.utc).isoformat()
                processed_count = 0

                for row in rows:
                    try:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) < 7:
                            continue

                        entry = {
                            'rank': int(cells[0].text.strip()),
                            'address': cells[1].find_element(By.TAG_NAME, "a").text.strip(),
                            'label': cells[3].text.strip() or "Unknown",
                            'balance_xrp': self.parse_xrp_amount(cells[4].text),
                            'escrow_xrp': self.parse_xrp_amount(cells[5].text) if cells[5].text.strip() else 0.0,
                            'percentage': self.parse_percentage(cells[6].text),
                            'snapshot_date': snapshot_date
                        }
                        writer.writerow(entry)
                        processed_count += 1

                        if processed_count % 100 == 0:
                            print(f"Processed {processed_count} entries...")

                    except Exception as e:
                        print(f"Error processing row: {e}")
                        continue

            print(f"Successfully saved {processed_count} entries to CSV")
            return True

        except Exception as e:
            print(f"Error scraping rich list: {e}")
            return False
        finally:
            self.driver.quit()

class SupabaseUploader:
    def __init__(self):
        supabase_url = os.environ["SUPABASE_URL"]
        supabase_key = os.environ["SUPABASE_KEY"]
        if not supabase_url or not supabase_key:
            raise ValueError("Supabase credentials not found in environment variables")
        
        self.supabase = create_client(supabase_url, supabase_key)
        self._test_connection()

    def _test_connection(self):
        try:
            response = self.supabase.table('xrpl_rich_list').select('count', count='exact').limit(1).execute()
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Supabase connection test failed: {response.error}")
            print("Successfully connected to Supabase")
        except Exception as e:
            print(f"Failed to connect to Supabase: {e}")
            raise

    def upload_from_csv(self, csv_path: str) -> bool:
        print(f"Starting upload from {csv_path}")
        try:
            batch_size = 100
            current_batch = []
            processed_count = 0
            total_count = 0

            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    current_batch.append({
                        'rank': int(row['rank']),
                        'address': row['address'],
                        'label': row['label'],
                        'balance_xrp': float(row['balance_xrp']),
                        'escrow_xrp': float(row['escrow_xrp']),
                        'percentage': float(row['percentage']),
                        'snapshot_date': row['snapshot_date']
                    })
                    
                    if len(current_batch) >= batch_size:
                        self._upload_batch(current_batch)
                        processed_count += len(current_batch)
                        print(f"Uploaded {processed_count} entries...")
                        current_batch = []
                    
                    total_count += 1

                # Upload remaining entries
                if current_batch:
                    self._upload_batch(current_batch)
                    processed_count += len(current_batch)

            print(f"Successfully uploaded {processed_count} entries to Supabase")
            return True

        except Exception as e:
            print(f"Error uploading to Supabase: {e}")
            return False

    def _upload_batch(self, batch: List[Dict]) -> None:
        max_retries = 3
        current_retry = 0
        
        while current_retry < max_retries:
            try:
                response = self.supabase.table('xrpl_rich_list').insert(batch).execute()
                if hasattr(response, 'error') and response.error:
                    raise Exception(f"Supabase error: {response.error}")
                break
            except Exception as e:
                current_retry += 1
                print(f"Error uploading batch (attempt {current_retry}/{max_retries}): {e}")
                if current_retry < max_retries:
                    print("Retrying...")
                    time.sleep(5)
                else:
                    raise Exception("Failed to upload batch after max retries")

def main():
    try:
        # 一時的なCSVファイルのパスを設定
        temp_csv_path = "rich_list_temp.csv"
        
        # スクレイピングを実行
        print("Starting scraping process...")
        scraper = XRPLRichListScraper()
        if not scraper.scrape_to_csv(temp_csv_path):
            raise Exception("Scraping failed")
        
        # Supabaseにアップロード
        print("Starting Supabase upload...")
        uploader = SupabaseUploader()
        if not uploader.upload_from_csv(temp_csv_path):
            raise Exception("Upload to Supabase failed")
        
        # 一時ファイルを削除
        try:
            os.remove(temp_csv_path)
            print("Temporary CSV file cleaned up")
        except Exception as e:
            print(f"Warning: Could not delete temporary CSV file: {e}")
        
        print("Process completed successfully")
        
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()