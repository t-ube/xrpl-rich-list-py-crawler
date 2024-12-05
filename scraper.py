import os
import sys
from datetime import datetime, timezone
import time
import csv
import asyncio
from typing import List, Dict
from dataclasses import dataclass

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from xrpl.clients import JsonRpcClient
from xrpl.models import AccountInfo, AccountObjects
from xrpl.asyncio.clients import AsyncJsonRpcClient

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


class XRPLBalanceValidator:
    def __init__(self, node_url="wss://s1.ripple.com"):
        self.node_url = node_url
        self.client = None

    async def setup_client(self):
        self.client = AsyncJsonRpcClient(self.node_url)

    async def cleanup_client(self):
        if self.client and hasattr(self.client, '_client'):
            await self.client._client.close()
        self.client = None

    async def get_account_info(self, address: str) -> tuple[float, float]:
        try:
            balance_response = await self.client.request(AccountInfo(
                account=address,
                ledger_index="validated"
            ))
            
            try:
                response_dict = balance_response.to_dict()
                
                if (isinstance(response_dict, dict) and 
                    response_dict.get('status') == 'success' and 
                    'result' in response_dict and 
                    'account_data' in response_dict['result'] and 
                    'Balance' in response_dict['result']['account_data']):
                    
                    balance = float(response_dict['result']['account_data']['Balance']) / 1000000
                    escrow_balance = await self.get_escrow_info(address)
                    return balance, escrow_balance
                
                print(f"Account {address} may not exist or is not accessible")
                return 0.0, 0.0
                    
            except Exception as e:
                print(f"Error processing response for {address}: {e}")
                return 0.0, 0.0
                
        except Exception as e:
            print(f"Error fetching balance for {address}: {str(e)}")
            return 0.0, 0.0

    async def get_escrow_info(self, address: str) -> float:
        try:
            response = await self.client.request(AccountObjects(
                account=address,
                type="escrow",
                ledger_index="validated"
            ))
            
            try:
                response_dict = response.to_dict()
                
                if (isinstance(response_dict, dict) and 
                    response_dict.get('status') == 'success' and 
                    'result' in response_dict and 
                    'account_objects' in response_dict['result']):
                    
                    escrows = response_dict['result']['account_objects']
                    return sum(
                        float(escrow['Amount']) / 1000000 
                        for escrow in escrows 
                        if isinstance(escrow, dict) and 'Amount' in escrow
                    )
                
                return 0.0
                    
            except Exception as e:
                print(f"Error processing escrow response for {address}: {e}")
                return 0.0
                
        except Exception as e:
            print(f"Error fetching escrow for {address}: {str(e)}")
            return 0.0

    async def validate_balances(self, csv_path: str, batch_size: int = 16):
        print("Starting balance validation...")
        temp_path = f"{csv_path}.temp"
        
        try:
            await self.setup_client()
            
            entries = []
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                entries = list(reader)

            total = len(entries)
            processed = 0
            verified_count = 0
            
            with open(temp_path, 'w', newline='', encoding='utf-8') as tempfile:
                fieldnames = ['rank', 'address', 'label', 'balance_xrp', 'escrow_xrp', 'percentage', 'snapshot_date']
                writer = csv.DictWriter(tempfile, fieldnames=fieldnames)
                writer.writeheader()

                for i in range(0, total, batch_size):
                    batch = entries[i:i + batch_size]
                    tasks = []
                    
                    for entry in batch:
                        tasks.append(self.get_account_info(entry['address']))
                    
                    try:
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        
                        for entry, result in zip(batch, results):
                            if isinstance(result, Exception) or result == (0.0, 0.0):
                                print(f"Could not verify {entry['address']}, keeping original values")
                                writer.writerow(entry)
                            else:
                                balance, escrow = result
                                if balance > 0 or escrow > 0:
                                    entry['balance_xrp'] = balance
                                    entry['escrow_xrp'] = escrow
                                    verified_count += 1
                                writer.writerow(entry)
                        
                        processed += len(batch)
                        if processed % 100 == 0:
                            print(f"Processed {processed}/{total} entries ({(processed/total)*100:.1f}%)")
                            print(f"Successfully verified: {verified_count} addresses")
                        
                    except Exception as e:
                        print(f"Error processing batch: {e}")
                        for entry in batch:
                            writer.writerow(entry)
                        continue
                    
                    if i + batch_size < total:
                        await asyncio.sleep(2)  # 待機時間も2秒

                os.replace(temp_path, csv_path)
                print(f"\nBalance validation completed:")
                print(f"Total processed: {total}")
                print(f"Successfully verified: {verified_count}")
                print(f"Unverified/kept original: {total - verified_count}")
                
        except Exception as e:
            print(f"Error during balance validation: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise
        finally:
            await self.cleanup_client()


class RichListScrapeProcessor:
    def __init__(self):
        self.validator = XRPLBalanceValidator()
        self.scraper = XRPLRichListScraper()

    async def process(self):
        temp_csv_path = "rich_list_temp.csv"
        
        try:
            print("Starting scraping process...")
            if not self.scraper.scrape_to_csv(temp_csv_path):
                raise Exception("Scraping failed")

            print("\nStarting full validation...")
            await self.validator.validate_balances(temp_csv_path)
            
            print("Process completed successfully")
            
        except Exception as e:
            print(f"Error during processing: {e}")
            if os.path.exists(temp_csv_path):
                try:
                    os.remove(temp_csv_path)
                except:
                    pass
            raise

def main():
    processor = RichListScrapeProcessor()
    try:
        asyncio.run(processor.process())
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()