import aiohttp
import asyncio
import csv
from datetime import datetime, timezone
from typing import List, Dict, Set
from dataclasses import dataclass
import json

@dataclass
class XRPAccount:
    account: str
    balance: float
    name: str = "Unknown"
    desc: str = ""
    domain: str = ""
    twitter: str = ""
    verified: bool = False
    escrow_xrp: float = 0.0

class XRPDataFetcher:
    def __init__(self):
        self.base_url = "https://api.xrpscan.com/api/v1"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'application/json'
        }

    async def fetch_data(self, endpoint: str) -> List[Dict]:
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for attempt in range(3):  # 3回までリトライ
                try:
                    async with session.get(f"{self.base_url}/{endpoint}") as response:
                        if response.status != 200:
                            raise Exception(f"API request failed with status: {response.status}")
                        
                        content_type = response.headers.get('Content-Type', '')
                        if 'application/json' not in content_type and 'text/json' not in content_type:
                            raw = await response.read()
                            if raw.lstrip().startswith((b"{", b"[")):
                                return json.loads(raw.decode("utf-8", errors="strict"))
                            if attempt < 2:
                                print(f"Unexpected content type: {content_type}, retrying... (attempt {attempt + 1}/3)")
                                await asyncio.sleep(5 * (attempt + 1)) 
                                continue
                            raise Exception(f"Unexpected content type: {content_type}")
                        
                        return await response.json()
                        
                except Exception as e:
                    if attempt < 2:
                        print(f"Error during API request: {e}, retrying... (attempt {attempt + 1}/3)")
                        await asyncio.sleep(5 * (attempt + 1))
                        continue
                    raise Exception(f"API request failed after 3 attempts: {e}")

    async def fetch_data(self, endpoint: str) -> List[Dict]:
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for attempt in range(3):  # 3回までリトライ
                try:
                    async with session.get(f"{self.base_url}/{endpoint}") as response:
                        if response.status != 200:
                            raise Exception(f"API request failed with status: {response.status}")
                        
                        content_type = response.headers.get('Content-Type', '')
                        if 'application/json' not in content_type and 'text/json' not in content_type:
                            if attempt < 2:
                                print(f"Unexpected content type: {content_type}, retrying... (attempt {attempt + 1}/3)")
                                await asyncio.sleep(5 * (attempt + 1)) 
                                continue
                            raise Exception(f"Unexpected content type: {content_type}")
                        
                        return await response.json()
                        
                except Exception as e:
                    if attempt < 2:
                        print(f"Error during API request: {e}, retrying... (attempt {attempt + 1}/3)")
                        await asyncio.sleep(5 * (attempt + 1))
                        continue
                    raise Exception(f"API request failed after 3 attempts: {e}")

    def convert_balance_to_xrp(self, drops: int) -> float:
        return drops / 1_000_000

    def format_label(self, name: str, desc: str) -> str:
        """Format label with name and description"""
        if not name or name == "Unknown":
            return "Unknown"
            
        if desc:
            return f"{name} ({desc})"
        return name

    async def get_rich_list(self) -> List[XRPAccount]:
        data = await self.fetch_data("balances")
        accounts = []
        
        for entry in data:
            # Safely handle the name field which might be None
            name_info = entry.get('name') or {}
            
            # name.nameがある場合はそれを使用し、ない場合はusernameを試す
            name = (name_info.get('name') or 
                   (name_info.get('username') if isinstance(name_info, dict) else None) or 
                   'Unknown')
            
            account = XRPAccount(
                account=entry['account'],
                balance=self.convert_balance_to_xrp(entry['balance']),
                name=name,
                desc=name_info.get('desc', ''),
                domain=name_info.get('domain', ''),
                twitter=name_info.get('twitter', '')
            )
            accounts.append(account)
            
        return accounts

    async def get_well_known_accounts(self) -> List[XRPAccount]:
        data = await self.fetch_data("names/well-known")
        accounts = []
        
        for entry in data:
            account = XRPAccount(
                account=entry['account'],
                balance=0,
                name=entry.get('name', 'Unknown'),
                desc=entry.get('desc', ""),
                domain=entry.get('domain', ""),
                twitter=entry.get('twitter', ""),
                verified=entry.get('verified', False)
            )
            accounts.append(account)
            
        return accounts

    def merge_accounts(self, rich_list: List[XRPAccount], well_known: List[XRPAccount]) -> List[XRPAccount]:
        well_known_dict = {acc.account: acc for acc in well_known}
        processed_accounts: Set[str] = set()
        merged_accounts = []
        
        # Process rich list first
        for rich_acc in rich_list:
            if rich_acc.account in processed_accounts:
                continue
                
            if rich_acc.account in well_known_dict:
                well_known_acc = well_known_dict[rich_acc.account]
                well_known_acc.balance = rich_acc.balance
                merged_accounts.append(well_known_acc)
            else:
                merged_accounts.append(rich_acc)
                
            processed_accounts.add(rich_acc.account)
        
        # Add remaining well-known accounts
        for well_known_acc in well_known:
            if well_known_acc.account not in processed_accounts:
                merged_accounts.append(well_known_acc)
                processed_accounts.add(well_known_acc.account)
        
        return sorted(merged_accounts, key=lambda x: x.balance, reverse=True)

    async def save_to_csv(self, output_path: str):
        try:
            print("Fetching rich list data...")
            rich_list = await self.get_rich_list()
            print(f"Found {len(rich_list)} accounts in rich list")
            
            print("Fetching well-known accounts...")
            well_known = await self.get_well_known_accounts()
            print(f"Found {len(well_known)} well-known accounts")
            
            print("Merging account data...")
            merged_accounts = self.merge_accounts(rich_list, well_known)
            
            snapshot_date = datetime.now(timezone.utc).isoformat()
            
            print(f"Saving data to {output_path}...")
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['rank', 'address', 'label', 'balance_xrp', 'escrow_xrp', 'percentage', 
                            'domain', 'twitter', 'verified', 'snapshot_date']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                total_xrp = sum(acc.balance for acc in merged_accounts)
                
                for rank, account in enumerate(merged_accounts, 1):
                    percentage = (account.balance / total_xrp * 100) if total_xrp > 0 else 0
                    
                    writer.writerow({
                        'rank': rank,
                        'address': account.account,
                        'label': self.format_label(account.name, account.desc),
                        'balance_xrp': account.balance,
                        'escrow_xrp': account.escrow_xrp,
                        'percentage': round(percentage, 6),
                        'domain': account.domain,
                        'twitter': account.twitter,
                        'verified': account.verified,
                        'snapshot_date': snapshot_date
                    })
            
            print(f"Successfully saved {len(merged_accounts)} entries to CSV")
            return True
            
        except Exception as e:
            print(f"Error creating CSV: {e}")
            return False

async def main():
    retries = 3
    for attempt in range(retries):
        try:
            fetcher = XRPDataFetcher()
            await fetcher.save_to_csv("rich_list_temp.csv")
            break
        except Exception as e:
            if attempt < retries - 1:
                print(f"Attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(10)  # 10秒待ってリトライ
                continue
            raise

if __name__ == "__main__":
    asyncio.run(main())