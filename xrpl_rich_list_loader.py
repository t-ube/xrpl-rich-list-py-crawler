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

    def update_summary_table(self) -> bool:
        try:
            sql_query = """
            INSERT INTO xrpl_rich_list_summary (grouped_label, count, total_balance, total_escrow, total_xrp, created_at)
            WITH latest_snapshot AS (
                SELECT snapshot_date 
                FROM xrpl_rich_list 
                ORDER BY snapshot_date DESC 
                LIMIT 1
            )
            SELECT 
                CASE 
                    WHEN label LIKE 'Ripple%' THEN 'Ripple'
                    WHEN label LIKE 'Coinbase%' THEN 'Coinbase'
                    WHEN label LIKE 'Bitrue%' THEN 'Bitrue'
                    WHEN label LIKE 'Binance%' THEN 'Binance'
                    WHEN label LIKE 'WhiteBIT%' THEN 'WhiteBIT'
                    WHEN label LIKE 'CoinCola%' THEN 'CoinCola'
                    WHEN label LIKE '%gatehub%' THEN 'gatehub'
                    WHEN label LIKE 'Crypto.com%' THEN 'Crypto.com'
                    ELSE REGEXP_REPLACE(
                        REGEXP_REPLACE(label, '^~', ''),
                        '\s*\([0-9]+\)$', ''
                    )
                END AS grouped_label,
                COUNT(*) as count,
                SUM(balance_xrp) as total_balance,
                SUM(escrow_xrp) as total_escrow,
                SUM(balance_xrp + escrow_xrp) as total_xrp,
                (SELECT snapshot_date FROM latest_snapshot) as created_at
            FROM xrpl_rich_list
            WHERE snapshot_date = (SELECT snapshot_date FROM latest_snapshot)
            GROUP BY 
                CASE 
                    WHEN label LIKE 'Ripple%' THEN 'Ripple'
                    WHEN label LIKE 'Coinbase%' THEN 'Coinbase'
                    WHEN label LIKE 'Bitrue%' THEN 'Bitrue'
                    WHEN label LIKE 'Binance%' THEN 'Binance'
                    WHEN label LIKE 'WhiteBIT%' THEN 'WhiteBIT'
                    WHEN label LIKE 'CoinCola%' THEN 'CoinCola'
                    WHEN label LIKE '%gatehub%' THEN 'gatehub'
                    WHEN label LIKE 'Crypto.com%' THEN 'Crypto.com'
                    ELSE REGEXP_REPLACE(
                        REGEXP_REPLACE(label, '^~', ''),
                        '\s*\([0-9]+\)$', ''
                    )
                END
            ORDER BY total_balance DESC
            """
            
            # 直接クエリを実行
            response = self.supabase.query(sql_query).execute()
            
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Summary table update failed: {response.error}")
                
            print("Successfully updated summary table")
            return True
            
        except Exception as e:
            print(f"Error updating summary table: {e}")
            return False

    def update_balance_changes(self) -> bool:
        try:
            # まず既存のデータを削除
            cleanup_query = """
            DELETE FROM xrpl_rich_list_changes;
            """

            # 新しいデータを挿入
            changes_query = """
            WITH current_totals AS (
                SELECT 
                    grouped_label,
                    total_xrp,
                    created_at
                FROM xrpl_rich_list_summary
                WHERE created_at = (
                    SELECT created_at 
                    FROM xrpl_rich_list_summary 
                    ORDER BY created_at DESC 
                    LIMIT 1
                )
            ),
            period_changes AS (
                SELECT 
                    c.grouped_label,
                    1 as period_days,
                    c.total_xrp - COALESCE(d1.total_xrp, c.total_xrp) as day_change,
                    CASE 
                        WHEN COALESCE(d1.total_xrp, c.total_xrp) = 0 THEN 0
                        ELSE ((c.total_xrp - COALESCE(d1.total_xrp, c.total_xrp)) / COALESCE(d1.total_xrp, c.total_xrp) * 100)
                    END as day_percentage,
                    c.total_xrp - COALESCE(d7.total_xrp, c.total_xrp) as week_change,
                    CASE 
                        WHEN COALESCE(d7.total_xrp, c.total_xrp) = 0 THEN 0
                        ELSE ((c.total_xrp - COALESCE(d7.total_xrp, c.total_xrp)) / COALESCE(d7.total_xrp, c.total_xrp) * 100)
                    END as week_percentage,
                    c.total_xrp - COALESCE(d30.total_xrp, c.total_xrp) as month_change,
                    CASE 
                        WHEN COALESCE(d30.total_xrp, c.total_xrp) = 0 THEN 0
                        ELSE ((c.total_xrp - COALESCE(d30.total_xrp, c.total_xrp)) / COALESCE(d30.total_xrp, c.total_xrp) * 100)
                    END as month_percentage
                FROM current_totals c
                LEFT JOIN xrpl_rich_list_summary d1 ON 
                    c.grouped_label = d1.grouped_label AND 
                    d1.created_at = (
                        SELECT created_at 
                        FROM xrpl_rich_list_summary 
                        WHERE created_at < c.created_at
                        ORDER BY created_at DESC 
                        LIMIT 1
                    )
                LEFT JOIN xrpl_rich_list_summary d7 ON 
                    c.grouped_label = d7.grouped_label AND 
                    d7.created_at = (
                        SELECT created_at 
                        FROM xrpl_rich_list_summary 
                        WHERE created_at <= c.created_at - INTERVAL '7 days'
                        ORDER BY created_at DESC 
                        LIMIT 1
                    )
                LEFT JOIN xrpl_rich_list_summary d30 ON 
                    c.grouped_label = d30.grouped_label AND 
                    d30.created_at = (
                        SELECT created_at 
                        FROM xrpl_rich_list_summary 
                        WHERE created_at <= c.created_at - INTERVAL '30 days'
                        ORDER BY created_at DESC 
                        LIMIT 1
                    )
            )
            INSERT INTO xrpl_rich_list_changes 
                (grouped_label, period_days, balance_change, percentage_change, calculated_at)
            SELECT 
                grouped_label,
                1,
                day_change,
                day_percentage,
                CURRENT_TIMESTAMP
            FROM period_changes
            UNION ALL
            SELECT 
                grouped_label,
                7,
                week_change,
                week_percentage,
                CURRENT_TIMESTAMP
            FROM period_changes
            UNION ALL
            SELECT 
                grouped_label,
                30,
                month_change,
                month_percentage,
                CURRENT_TIMESTAMP
            FROM period_changes;
            """
            
            # クリーンアップを実行
            cleanup_response = self.supabase.query(cleanup_query).execute()
            if hasattr(cleanup_response, 'error') and cleanup_response.error:
                raise Exception(f"Changes cleanup failed: {cleanup_response.error}")
            
            # 新しいデータを挿入
            changes_response = self.supabase.query(changes_query).execute()
            if hasattr(changes_response, 'error') and changes_response.error:
                raise Exception(f"Balance changes update failed: {changes_response.error}")
            
            print("Successfully updated balance changes")
            return True
            
        except Exception as e:
            print(f"Error updating balance changes: {e}")
            return False

    def cleanup_old_data(self) -> bool:
        try:
            # xrpl_rich_list の10日以上前のデータを削除
            rich_list_query = """
            DELETE FROM xrpl_rich_list
            WHERE snapshot_date < CURRENT_TIMESTAMP - INTERVAL '10 days';
            """
            
            # xrpl_rich_list_summary の370日以上前のデータを削除
            summary_query = """
            DELETE FROM xrpl_rich_list_summary
            WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '370 days';
            """
            
            # クエリを実行
            response1 = self.supabase.query(rich_list_query).execute()
            if hasattr(response1, 'error') and response1.error:
                raise Exception(f"Rich list cleanup failed: {response1.error}")
                
            response2 = self.supabase.query(summary_query).execute()
            if hasattr(response2, 'error') and response2.error:
                raise Exception(f"Summary table cleanup failed: {response2.error}")

            print("Successfully cleaned up old data")
            return True
            
        except Exception as e:
            print(f"Error cleaning up old data: {e}")
            return False

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

        # サマリーテーブルを更新
        print("Updating summary table...")
        if not uploader.update_summary_table():
            raise Exception("Summary table update failed")
        
        # 残高変化を計算
        print("Calculating balance changes...")
        if not uploader.update_balance_changes():
            raise Exception("Balance changes calculation failed")
        
        # 古いデータを削除
        print("Cleaning up old data...")
        if not uploader.cleanup_old_data():
            raise Exception("Data cleanup failed")
        
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