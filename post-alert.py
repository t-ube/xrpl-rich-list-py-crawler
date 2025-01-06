import os
import tweepy
from supabase import create_client
from dataclasses import dataclass
from typing import List, Optional, Optional

@dataclass
class ExchangeChange:
    name: str
    balance_change: float
    percentage_change: float

class SupabaseClient:
    def __init__(self):
        supabase_url = os.environ["SUPABASE_URL"]
        supabase_key = os.environ["SUPABASE_KEY"]
        if not supabase_url or not supabase_key:
            raise ValueError("Supabase credentials not found")
        
        self.supabase = create_client(supabase_url, supabase_key)

    def get_significant_changes(self, threshold_percentage: float = 5.0, threshold_amount: float = 1000000.0) -> List[ExchangeChange]:
        """
        1時間の変化量が閾値を超える取引所の変化を取得
        
        Args:
            threshold_percentage: パーセンテージの閾値（デフォルト: 5.0%）
            threshold_amount: 金額の閾値（デフォルト: 1,000,000 XRP）
        """
        try:
        # ストアドプロシージャを直接呼び出し
            response = self.supabase.rpc(
                'get_significant_changes', 
                {'percentage_threshold': threshold_percentage, 'amount_threshold': threshold_amount}
            ).execute()

            if hasattr(response, 'error') and response.error:
                raise Exception(f"Query failed: {response.error}")

            changes = []
            for row in response.data:
                changes.append(ExchangeChange(
                    name=row['grouped_label'],
                    balance_change=float(row['change_1h']),
                    percentage_change=float(row['percentage_1h'])
                ))
            
            return changes

        except Exception as e:
            print(f"Error fetching changes: {e}")
            return []

class XRPAlertBot:
    def __init__(self):
        self.supabase = SupabaseClient()
        self.twitter = self._init_twitter()

    def _init_twitter(self) -> tweepy.Client:
        """Twitter APIクライアントの初期化"""
        return tweepy.Client(
            consumer_key=os.environ["TWITTER_API_KEY"],
            consumer_secret=os.environ["TWITTER_API_SECRET"],
            bearer_token=os.environ["TWITTER_BEARER_TOKEN"],
            access_token=os.environ["TWITTER_ACCESS_TOKEN"],
            access_token_secret=os.environ["TWITTER_ACCESS_TOKEN_SECRET"]
        )

    def format_tweet(self, changes: List[ExchangeChange]) -> Optional[str]:
        """ツイート本文のフォーマット"""
        if not changes:
            return None

        lines = ["🚨 XRP Rich List Alert", "📊 Changes 1H", ""]
        
        for change in changes:
            sign = "+" if change.balance_change >= 0 else ""
            arrow = "↗️" if change.balance_change >= 0 else "↘️"
            
            lines.append(f"{change.name}")
            lines.append(f"  {arrow} {sign}{change.balance_change:,.0f} XRP ({sign}{change.percentage_change:.1f}%)")

        lines.append("\nhttp://xrp-rich-list-summary.shirome.net")
        return "\n".join(lines)

    def post_alert(self, threshold_percentage: float = 5.0, threshold_amount: float = 1000000.0):
        """アラートの投稿"""
        try:
            # significant changesの取得
            changes = self.supabase.get_significant_changes(threshold_percentage, threshold_amount)
            
            # ツイート本文の生成
            tweet_text = self.format_tweet(changes)
            if not tweet_text:
                print("No significant changes to report")
                return
            
            # ツイートの投稿
            self.twitter.create_tweet(text=tweet_text)
            print("Alert tweet posted successfully")

        except Exception as e:
            print(f"Error posting alert: {e}")

def main():
    try:
        bot = XRPAlertBot()
        bot.post_alert(threshold_percentage=10, threshold_amount=10000)
    except Exception as e:
        print(f"Fatal error: {e}")

if __name__ == "__main__":
    main()