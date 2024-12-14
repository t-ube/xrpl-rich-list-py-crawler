import aiohttp
import asyncio
import csv
from datetime import datetime, timezone
from typing import List, Dict, Set
from dataclasses import dataclass

@dataclass
class XRPAccount:
    account: str
    balance: float
    name: str = "Unknown"
    desc: str = ""
    domain: str = ""
    twitter: str = ""
    verified: bool = False

class XRPDataFetcher:
    def __init__(self):
        self.base_url = "https://api.xrpscan.com/api/v1"
        
    async def fetch_data(self, endpoint: str) -> List[Dict]:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/{endpoint}") as response:
                if response.status != 200:
                    raise Exception(f"API request failed: {response.status}")
                return await response.json()

    def convert_balance_to_xrp(self, drops: int) -> float:
        return drops / 1_000_000

    def extract_name_info(self, name_data: Dict) -> tuple:
        """Extract name information from the API response safely"""
        if not name_data:
            return "Unknown", "", "", ""
            
        return (
            name_data.get('name', 'Unknown'),
            name_data.get('desc', ''),
            name_data.get('domain', ''),
            name_data.get('twitter', '')
        )

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
            name, desc, domain, twitter = self.extract_name_info(name_info)
            
            account = XRPAccount(
                account=entry['account'],
                balance=self.convert_balance_to_xrp(entry['balance']),
                name=name,
                desc=desc,
                domain=domain,
                twitter=twitter
            )
            accounts.append(account)
            
        return accounts

    async def get_well_known_accounts(self) -> List[XRPAccount]:
        data = await self.fetch_data("names/well-known")
        accounts = []
        
        for entry in data:
            account = XRPAccount(
                account=entry['account'],
                balance=0,  # Will be updated later if in rich list
                name=entry.get('name', 'Unknown'),
                desc=entry.get('desc', ""),
                domain=entry.get('domain', ""),
                twitter=entry.get('twitter', ""),
                verified=entry.get('verified', False)
            )
            accounts.append(account)
            
        return accounts

    def merge_accounts(self, rich_list: List[XRPAccount], well_known: List[XRPAccount]) -> List[XRPAccount]:
        # Create a lookup for well-known accounts
        well_known_dict = {acc.account: acc for acc in well_known}
        
        # Create a set to track processed accounts
        processed_accounts: Set[str] = set()
        
        # Final merged list
        merged_accounts = []
        
        # Process rich list first
        for rich_acc in rich_list:
            if rich_acc.account in processed_accounts:
                continue
                
            if rich_acc.account in well_known_dict:
                # Update balance in well-known account
                well_known_acc = well_known_dict[rich_acc.account]
                well_known_acc.balance = rich_acc.balance
                merged_accounts.append(well_known_acc)
            else:
                merged_accounts.append(rich_acc)
                
            processed_accounts.add(rich_acc.account)
        
        # Add remaining well-known accounts that weren't in rich list
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
            
            # Merge and sort accounts
            print("Merging account data...")
            merged_accounts = self.merge_accounts(rich_list, well_known)
            
            # Save to CSV
            snapshot_date = datetime.now(timezone.utc).isoformat()
            
            print(f"Saving data to {output_path}...")
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['rank', 'address', 'label', 'balance_xrp', 'percentage', 
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
    fetcher = XRPDataFetcher()
    await fetcher.save_to_csv("rich_list_temp.csv")

if __name__ == "__main__":
    asyncio.run(main())