import aiohttp
import asyncio
import csv
from datetime import datetime, timezone
from typing import Dict, List, Set
from dataclasses import dataclass

@dataclass
class WellKnownAccount:
    account: str
    name: str
    desc: str
    domain: str
    twitter: str
    verified: bool

class RLUSDDataEnricher:
    def __init__(self):
        self.base_url = "https://api.xrpscan.com/api/v1"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }

    def format_label(self, name: str, desc: str) -> str:
        """Format label with name and description"""
        if not name or name == "Unknown":
            return "Unknown"
            
        if desc:
            return f"{name} ({desc})"
        return name

    async def get_well_known_accounts(self) -> Dict[str, WellKnownAccount]:
        """Fetch well-known accounts from XRPScan API"""
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                url = f"{self.base_url}/names/well-known"
                
                async with session.get(url) as response:
                    if response.status != 200:
                        raise Exception(f"API request failed with status: {response.status}")
                    
                    data = await response.json()
                    accounts = {}
                    
                    for entry in data:
                        account = WellKnownAccount(
                            account=entry['account'],
                            name=entry.get('name', 'Unknown'),
                            desc=entry.get('desc', ""),
                            domain=entry.get('domain', ""),
                            twitter=entry.get('twitter', ""),
                            verified=entry.get('verified', False)
                        )
                        accounts[account.account] = account
                    
                    return accounts
        
        except Exception as e:
            print(f"Error fetching well-known accounts: {e}")
            return {}

    async def enrich_csv(self, input_path: str, output_path: str):
        """Enrich RLUSD holders data with well-known account information"""
        try:
            print("Fetching well-known accounts...")
            well_known = await self.get_well_known_accounts()
            print(f"Found {len(well_known)} well-known accounts")
            
            # Read existing CSV
            with open(input_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                entries = list(reader)
            
            print(f"Processing {len(entries)} RLUSD holder entries...")
            
            # Prepare enriched data
            snapshot_date = datetime.now(timezone.utc).isoformat()
            enriched_entries = []
            
            for entry in entries:
                address = entry['address']
                if address in well_known:
                    account = well_known[address]
                    entry.update({
                        'label': self.format_label(account.name, account.desc),
                        'domain': account.domain,
                        'twitter': account.twitter,
                        'verified': account.verified
                    })
                else:
                    entry.update({
                        'label': entry.get('label', 'Unknown'),
                        'domain': '',
                        'twitter': '',
                        'verified': False
                    })
                
                entry['snapshot_date'] = snapshot_date
                enriched_entries.append(entry)
            
            # Write enriched data
            fieldnames = ['rank', 'address', 'label', 'balance_rlusd', 'trust_limit', 
                         'percentage', 'domain', 'twitter', 'verified', 
                         'rippling_disabled', 'snapshot_date']
            
            print(f"Writing enriched data to {output_path}...")
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(enriched_entries)
            
            print(f"\nEnrichment completed:")
            print(f"Total entries: {len(enriched_entries)}")
            print(f"Enriched with well-known data: {sum(1 for e in enriched_entries if e['label'] != 'Unknown')}")
            
        except Exception as e:
            print(f"Error enriching data: {e}")
            raise

async def main():
    enricher = RLUSDDataEnricher()
    await enricher.enrich_csv("rlusd_holders.csv", "rlusd_holders_enriched.csv")

if __name__ == "__main__":
    asyncio.run(main())