import asyncio
import csv
from typing import Optional, Tuple
from dataclasses import dataclass
import os

from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.models import AccountInfo, AccountObjects

@dataclass
class ValidatedAccount:
    address: str
    balance_xrp: float
    escrow_xrp: float
    exists: bool

class XRPLBalanceValidator:
    def __init__(self, node_url="wss://s1.ripple.com", max_retries=2, retry_delay=1):
        self.node_url = node_url
        self.client = None
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    async def setup_client(self):
        self.client = AsyncJsonRpcClient(self.node_url)

    async def cleanup_client(self):
        if self.client and hasattr(self.client, '_client'):
            await self.client._client.close()
        self.client = None

    async def get_escrow_info(self, address: str) -> Optional[float]:
        """Get escrow balance for an account"""
        for attempt in range(self.max_retries + 1):
            try:
                response = await self.client.request(AccountObjects(
                    account=address,
                    type="escrow",
                    ledger_index="validated"
                ))
                
                response_dict = response.to_dict()
                if (response_dict.get('status') == 'success' and 
                    'result' in response_dict and 
                    'account_objects' in response_dict['result']):
                    
                    escrows = response_dict['result']['account_objects']
                    return sum(
                        float(escrow['Amount']) / 1000000 
                        for escrow in escrows 
                        if isinstance(escrow, dict) and 'Amount' in escrow
                    )

                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay)
                    continue
                
                return 0
                    
            except Exception as e:
                if attempt < self.max_retries:
                    print(f"Retry {attempt + 1}/{self.max_retries} for escrow {address}")
                    await asyncio.sleep(self.retry_delay)
                    continue
                print(f"Error fetching escrow for {address}: {e}")
                return 0

    async def check_account(self, address: str) -> ValidatedAccount:
        """Validate a single account's current balance"""
        for attempt in range(self.max_retries + 1):
            try:
                response = await self.client.request(AccountInfo(
                    account=address,
                    ledger_index="validated"
                ))
                
                response_dict = response.to_dict()
                if (response_dict.get('status') == 'success' and 
                    'result' in response_dict and 
                    'account_data' in response_dict['result'] and 
                    'Balance' in response_dict['result']['account_data']):
                    
                    current_balance = float(response_dict['result']['account_data']['Balance']) / 1000000
                    escrow_balance = await self.get_escrow_info(address) or 0
                    
                    return ValidatedAccount(
                        address=address,
                        balance_xrp=current_balance,
                        escrow_xrp=escrow_balance,
                        exists=True
                    )
                
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay)
                    continue
                
                return ValidatedAccount(
                    address=address,
                    balance_xrp=0,
                    escrow_xrp=0,
                    exists=False
                )
                    
            except Exception as e:
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay)
                    continue
                print(f"Error checking account {address}: {e}")
                raise

    async def validate_balances(self, csv_path: str, batch_size: int = 16):
        """Validate balances for all accounts in the CSV"""
        print("Starting balance validation...")
        temp_path = f"{csv_path}.temp"
        
        try:
            await self.setup_client()
            
            # Read CSV
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                entries = list(reader)

            total = len(entries)
            processed = 0
            verified_count = 0
            
            # Prepare output CSV
            with open(temp_path, 'w', newline='', encoding='utf-8') as tempfile:
                fieldnames = ['rank', 'address', 'label', 'balance_xrp', 'escrow_xrp', 
                            'percentage', 'domain', 'twitter', 'verified', 'snapshot_date',
                            'exists']
                writer = csv.DictWriter(tempfile, fieldnames=fieldnames)
                writer.writeheader()

                # Process in batches
                for i in range(0, total, batch_size):
                    batch = entries[i:i + batch_size]
                    tasks = []
                    
                    for entry in batch:
                        tasks.append(self.check_account(entry['address']))
                    
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for entry, result in zip(batch, results):
                        if isinstance(result, Exception):
                            print(f"Error processing {entry['address']}: {result}")
                            writer.writerow(entry)  # Keep original data
                            continue
                            
                        if result.exists:
                            entry['balance_xrp'] = result.balance_xrp
                            entry['escrow_xrp'] = result.escrow_xrp
                            entry['exists'] = True
                            verified_count += 1
                        else:
                            entry['balance_xrp'] = 0
                            entry['escrow_xrp'] = 0
                            entry['exists'] = False
                        
                        writer.writerow(entry)
                    
                    processed += len(batch)
                    if processed % 100 == 0:
                        print(f"Processed {processed}/{total} entries ({(processed/total)*100:.1f}%)")
                        print(f"Successfully verified: {verified_count} addresses")
                    
                    if i + batch_size < total:
                        await asyncio.sleep(1)  # Rate limiting

            # Replace original file with validated data
            os.replace(temp_path, csv_path)
            print(f"\nBalance validation completed:")
            print(f"Total processed: {total}")
            print(f"Successfully verified: {verified_count}")
            
        except Exception as e:
            print(f"Error during balance validation: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise
        finally:
            await self.cleanup_client()

async def main():
    validator = XRPLBalanceValidator()
    await validator.validate_balances("rich_list.csv")

if __name__ == "__main__":
    asyncio.run(main())