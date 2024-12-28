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
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                response = self.supabase.table('xrpl_rich_list').select('address').limit(1).execute()
                if hasattr(response, 'error') and response.error:
                    raise Exception(f"Supabase connection test failed: {response.error}")
                print("Successfully connected to Supabase")
                return
            except Exception as e:
                print(f"Connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    print("All connection attempts failed")
                    raise

    def update_balance_changes(self) -> bool:
        try:
            # PostgreSQL関数を呼び出す
            response = self.supabase.rpc(
                'update_balance_changes'
            ).execute()
            
            if hasattr(response, 'error') and response.error:
                # タイムアウトエラーの場合は無視
                if '57014' in str(response.error):
                    print("Warning: Balance changes calculation timed out, but continuing...")
                    return True
                raise Exception(f"Balance changes update failed: {response.error}")
            
            print("Successfully updated balance changes")
            return True
            
        except Exception as e:
            # タイムアウトエラーの場合は無視
            if '57014' in str(e):
                print("Warning: Balance changes calculation timed out, but continuing...")
                return True
            print(f"Error updating balance changes: {e}")
            return False

    def update_available_changes(self) -> bool:
        try:
            # PostgreSQL関数を呼び出す
            response = self.supabase.rpc(
                'update_available_changes'
            ).execute()
            
            if hasattr(response, 'error') and response.error:
                # タイムアウトエラーの場合は無視して続行
                if '57014' in str(response.error):
                    print("Warning: Available changes calculation timed out, but continuing...")
                    return True
                raise Exception(f"Available changes update failed: {response.error}")
            
            print("Successfully updated available changes")
            return True
            
        except Exception as e:
            # タイムアウトエラーの場合は無視して続行
            if '57014' in str(e):
                print("Warning: Available changes calculation timed out, but continuing...")
                return True
            print(f"Error updating available changes: {e}")
            return False

    def update_category_changes(self) -> bool:
        try:
            # PostgreSQL関数を呼び出す
            response = self.supabase.rpc(
                'update_category_changes'
            ).execute()
            
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Category changes update failed: {response.error}")
            
            print("Successfully updated category changes")
            return True
            
        except Exception as e:
            print(f"Error updating category changes: {e}")
            return False

    def update_country_changes(self) -> bool:
        try:
            # PostgreSQL関数を呼び出す
            response = self.supabase.rpc(
                'update_country_changes'
            ).execute()
            
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Country changes update failed: {response.error}")
            
            print("Successfully updated country changes")
            return True
            
        except Exception as e:
            print(f"Error updating country changes: {e}")
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
    
    def cleanup_old_statistics(self) -> bool:
        try:
            # PostgreSQL関数を呼び出す
            response = self.supabase.rpc(
                'delete_old_statistics'
            ).execute()
            
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Data cleanup failed: {response.error}")

            print("Successfully cleaned up old statistics")
            return True
            
        except Exception as e:
            print(f"Error cleaning up old statistics: {e}")
            return False
            
    def update_category_statistics(self) -> bool: #228
        try:
            # PostgreSQL関数を呼び出す
            response = self.supabase.rpc(
                'update_category_statistics'
            ).execute()
            
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Category statistics update failed: {response.error}")
            
            print("Successfully updated category statistics")
            return True
            
        except Exception as e:
            print(f"Error updating category statistics: {e}")
            return False

    def update_country_statistics(self) -> bool: #299
        try:
            # PostgreSQL関数を呼び出す
            response = self.supabase.rpc(
                'update_country_statistics'
            ).execute()
            
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Country statistics update failed: {response.error}")
            
            print("Successfully updated country statistics")
            return True
            
        except Exception as e:
            print(f"Error updating country statistics: {e}")
            return False

    def update_available_statistics(self) -> bool:
        try:
            # PostgreSQL関数を呼び出す
            response = self.supabase.rpc(
                'update_available_statistics'
            ).execute()
            
            if hasattr(response, 'error') and response.error:
                # タイムアウトエラーの場合は無視して続行
                if '57014' in str(response.error):
                    print("Warning: Available statistics calculation timed out, but continuing...")
                    return True
                raise Exception(f"Available statistics update failed: {response.error}")
            
            print("Successfully updated available statistics")
            return True
            
        except Exception as e:
            # タイムアウトエラーの場合は無視して続行
            if '57014' in str(e):
                print("Warning: Available statistics calculation timed out, but continuing...")
                return True
            print(f"Error updating available statistics: {e}")
            return False


class RichListUploadProcessor:
    def __init__(self):
        self.uploader = None

    def process(self):
        
        try:
            print("Starting Supabase upload...")
            self.uploader = SupabaseUploader()

            print("Calculating balance changes...")
            if not self.uploader.update_balance_changes():
                raise Exception("Balance changes calculation failed")

            print("Calculating available changes...")
            if not self.uploader.update_available_changes():
                raise Exception("Available changes calculation failed")

            print("Calculating category changes...")
            if not self.uploader.update_category_changes():
                raise Exception("Category changes calculation failed")

            print("Calculating country changes...")
            if not self.uploader.update_country_changes():
                raise Exception("Country changes calculation failed")
            
            print("Cleaning up old statistics...")
            if not self.uploader.cleanup_old_statistics():
                raise Exception("Statistics cleanup failed")
    
            print("Calculating category statistics...")
            if not self.uploader.update_category_statistics():
                raise Exception("Country statistics calculation failed")

            print("Calculating country statistics...")
            if not self.uploader.update_country_statistics():
                raise Exception("Country statistics calculation failed")

            print("Calculating available statistics...")
            if not self.uploader.update_available_statistics():
                raise Exception("Country statistics calculation failed")

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
