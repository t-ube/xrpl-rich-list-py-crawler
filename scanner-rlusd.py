import aiohttp
import asyncio
import csv
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class TrustLine:
    address: str
    balance: float
    limit: str
    rippling_disabled: bool

class RLUSDScanner:
    def __init__(self):
        self.base_url = "https://api.xrpscan.com/api/v1"
        self.rlusd_issuer = "rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }

    async def get_trust_lines(self) -> List[TrustLine]:
        """Get all trust lines from XRPScan API"""
        trust_lines = []
        
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                url = f"{self.base_url}/account/{self.rlusd_issuer}/trustlines"
                
                async with session.get(url) as response:
                    if response.status != 200:
                        raise Exception(f"API request failed with status: {response.status}")
                    
                    data = await response.json()
                    
                    for line in data:
                        # Get specification and state from the trust line
                        spec = line.get('specification', {})
                        state = line.get('state', {})
                        counterparty = line.get('counterparty', {})
                        
                        # Only process RLUSD trust lines
                        if spec.get('currency') == "524C555344000000000000000000000000000000":
                            trust_line = TrustLine(
                                address=spec.get('counterparty', ''),
                                balance=abs(float(state.get('balance', 0))),
                                limit=str(counterparty.get('limit', '0')),  # 文字列として保存
                                rippling_disabled=bool(counterparty.get('ripplingDisabled', False))
                            )
                            
                            if trust_line.balance > 0:  # 残高があるものだけを追加
                                trust_lines.append(trust_line)
        
        except Exception as e:
            print(f"Error fetching trust lines: {e}")
        
        return trust_lines

    async def save_to_csv(self, output_path: str):
        """Save trust lines data to CSV"""
        try:
            print("Fetching RLUSD trust lines...")
            trust_lines = await self.get_trust_lines()
            
            if not trust_lines:
                print("No active RLUSD trust lines found.")
                return
            
            # Sort by balance in descending order
            trust_lines.sort(key=lambda x: x.balance, reverse=True)
            
            snapshot_date = datetime.now(timezone.utc).isoformat()
            total_supply = sum(line.balance for line in trust_lines)
            
            print(f"Writing {len(trust_lines)} trust lines to CSV...")
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['rank', 'address', 'label', 'balance_rlusd', 'trust_limit', 
                            'percentage', 'rippling_disabled', 'snapshot_date']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for rank, line in enumerate(trust_lines, 1):
                    percentage = (line.balance / total_supply * 100) if total_supply > 0 else 0
                    
                    writer.writerow({
                        'rank': rank,
                        'address': line.address,
                        'label': 'Unknown',
                        'balance_rlusd': line.balance,
                        'trust_limit': line.limit,
                        'percentage': round(percentage, 6),
                        'rippling_disabled': line.rippling_disabled,
                        'snapshot_date': snapshot_date
                    })
            
            print(f"\nSummary:")
            print(f"Total holders: {len(trust_lines)}")
            print(f"Total supply: {total_supply:,.2f} RLUSD")
            print(f"Data saved to {output_path}")
            
        except Exception as e:
            print(f"Error saving to CSV: {e}")
            raise

async def main():
    scanner = RLUSDScanner()
    await scanner.save_to_csv("rlusd_holders.csv")

if __name__ == "__main__":
    asyncio.run(main())