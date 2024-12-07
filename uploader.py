import os
import sys
import time
import csv
from typing import List, Dict

from supabase import create_client

class SupabaseUploader:
    def __init__(self):
        supabase_url = os.environ["SUPABASE_URL"]
        supabase_key = os.environ["SUPABASE_KEY"]
        if not supabase_url or not supabase_key:
            raise ValueError("Supabase credentials not found in environment variables")
        
        self.supabase = create_client(
            supabase_url=supabase_url,
            supabase_key=supabase_key
        )
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
                    # exists を文字列の "True"/"False" から bool型に変換
                    exists_value = str(row.get('exists', 'True')).lower() == 'true'
                    
                    current_batch.append({
                        'rank': int(row['rank']),
                        'address': row['address'],
                        'label': row['label'],
                        'balance_xrp': float(row['balance_xrp']),
                        'escrow_xrp': float(row['escrow_xrp']),
                        'percentage': float(row['percentage']),
                        'snapshot_date': row['snapshot_date'],
                        'exists': exists_value
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
            # PostgreSQL関数を呼び出す
            response = self.supabase.rpc(
                'update_rich_list_summary'
            ).execute()
            
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Summary table update failed: {response.error}")
                
            print("Successfully updated summary table")
            return True
            
        except Exception as e:
            print(f"Error updating summary table: {e}")
            return False

    def update_balance_changes(self) -> bool:
        try:
            # PostgreSQL関数を呼び出す
            response = self.supabase.rpc(
                'update_balance_changes'
            ).execute()
            
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Balance changes update failed: {response.error}")
            
            print("Successfully updated balance changes")
            return True
            
        except Exception as e:
            print(f"Error updating balance changes: {e}")
            return False

    def cleanup_old_data(self) -> bool:
        try:
            # PostgreSQL関数を呼び出す
            response = self.supabase.rpc(
                'cleanup_old_rich_list_data'
            ).execute()
            
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Data cleanup failed: {response.error}")

            print("Successfully cleaned up old data")
            return True
            
        except Exception as e:
            print(f"Error cleaning up old data: {e}")
            return False


class RichListUploadProcessor:
    def __init__(self):
        self.uploader = None

    def process(self):
        temp_csv_path = "rich_list_temp.csv"
        
        try:
            print("Starting Supabase upload...")
            self.uploader = SupabaseUploader()
            if not self.uploader.upload_from_csv(temp_csv_path):
                raise Exception("Upload to Supabase failed")

            print("Updating summary table...")
            if not self.uploader.update_summary_table():
                raise Exception("Summary table update failed")
            
            print("Calculating balance changes...")
            if not self.uploader.update_balance_changes():
                raise Exception("Balance changes calculation failed")
            
            print("Cleaning up old data...")
            if not self.uploader.cleanup_old_data():
                raise Exception("Data cleanup failed")

            try:
                os.remove(temp_csv_path)
                print("Temporary CSV file cleaned up")
            except Exception as e:
                print(f"Warning: Could not delete temporary CSV file: {e}")

            print("Process completed successfully")
            
        except Exception as e:
            print(f"Error during processing: {e}")
            raise


def main():
    processor = RichListUploadProcessor()
    try:
        processor.process()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
