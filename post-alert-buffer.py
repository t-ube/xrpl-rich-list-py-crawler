"""XRP Rich List のアラートを Buffer API 経由で X に投稿する。

必要な環境変数:
    SUPABASE_URL
    SUPABASE_KEY
    BUFFER_API_KEY
    BUFFER_CHANNEL_ID
"""

import json
import os
import time
from dataclasses import dataclass
from typing import List, Optional

import requests
from supabase import create_client

BUFFER_API_URL = "https://api.buffer.com"

# X の本文上限。URL は t.co で短縮されて 23 文字換算になる
X_CHARACTER_LIMIT = 280
X_URL_LENGTH = 23


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

    def get_significant_changes(
        self,
        threshold_percentage: float = 5.0,
        threshold_amount: float = 1000000.0,
    ) -> List[ExchangeChange]:
        """1時間の変化量が閾値を超える取引所の変化を取得

        Args:
            threshold_percentage: パーセンテージの閾値（デフォルト: 5.0%）
            threshold_amount: 金額の閾値（デフォルト: 1,000,000 XRP）
        """
        try:
            # ストアドプロシージャを直接呼び出し
            response = self.supabase.rpc(
                "get_significant_changes",
                {
                    "percentage_threshold": threshold_percentage,
                    "amount_threshold": threshold_amount,
                },
            ).execute()

            if hasattr(response, "error") and response.error:
                raise Exception(f"Query failed: {response.error}")

            return [
                ExchangeChange(
                    name=row["grouped_label"],
                    balance_change=float(row["change_1h"]),
                    percentage_change=float(row["percentage_1h"]),
                )
                for row in response.data
            ]

        except Exception as e:
            print(f"Error fetching changes: {e}")
            return []


class BufferError(RuntimeError):
    pass


class BufferClient:
    """Buffer GraphQL API (https://api.buffer.com) の薄いラッパー。

    エンドポイントは1本だけで、query / mutation を POST で投げる。
    """

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30):
        self.api_key = api_key or os.environ["BUFFER_API_KEY"]
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            }
        )

    def _request(self, query: str, variables: Optional[dict] = None) -> dict:
        response = self.session.post(
            BUFFER_API_URL,
            json={"query": query, "variables": variables or {}},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()

        # 構文エラーや認証エラーはトップレベルの errors に入る
        if payload.get("errors"):
            raise BufferError(f"GraphQL error: {payload['errors']}")

        return payload["data"]

    def get_organization_id(self) -> str:
        data = self._request(
            "query GetOrganizations { account { organizations { id name } } }"
        )
        organizations = data["account"]["organizations"]
        if not organizations:
            raise BufferError("この API キーに紐づく organization がありません")
        return organizations[0]["id"]

    def find_channel_id(self, service: str = "twitter") -> str:
        """接続済みチャンネルから service に一致するものの ID を返す。"""
        organization_id = self.get_organization_id()
        query = f"""
        query GetChannels {{
          channels(input: {{ organizationId: {json.dumps(organization_id)} }}) {{
            id
            name
            service
          }}
        }}
        """
        channels = self._request(query)["channels"]

        for channel in channels:
            if channel["service"] == service:
                return channel["id"]

        available = ", ".join(c["service"] for c in channels) or "(なし)"
        raise BufferError(
            f"service={service} のチャンネルが見つかりません。接続済み: {available}"
        )

    def create_post(self, channel_id: str, text: str, mode: str = "now") -> dict:
        """投稿を作成する。

        mode:
            now             即時投稿
            addToQueue      キューの次の空きスロットに入れる
            customScheduled dueAt での予約（本スクリプトでは未使用）
        """
        query = """
        mutation CreatePost($input: CreatePostInput!) {
          createPost(input: $input) {
            ... on PostActionSuccess {
              post { id text status dueAt }
            }
            ... on MutationError {
              message
            }
          }
        }
        """
        variables = {
            "input": {
                "text": text,
                "channelId": channel_id,
                "schedulingType": "automatic",
                "mode": mode,
            }
        }
        result = self._request(query, variables)["createPost"]

        # 失敗は HTTP ステータスではなく union の型で返ってくる
        if "message" in result:
            raise BufferError(f"createPost failed: {result['message']}")

        return result["post"]


class XRPAlertBot:
    def __init__(self):
        self.supabase = SupabaseClient()
        self.buffer = BufferClient()
        self.channel_id = os.environ.get("BUFFER_CHANNEL_ID")

    def _resolve_channel_id(self) -> str:
        """チャンネル ID を一度だけ解決してキャッシュする。"""
        if not self.channel_id:
            self.channel_id = self.buffer.find_channel_id("twitter")
            print(f"Resolved channel id: {self.channel_id}")
        return self.channel_id

    def format_tweet(self, changes: List[ExchangeChange]) -> Optional[str]:
        """ツイート本文のフォーマット（280 文字に収まるよう件数を絞る）"""
        if not changes:
            return None

        timestamp = int(time.time())
        url = f"http://xrp-rich-list-summary.shirome.net?timestamp={timestamp}"

        header = ["🚨 XRP Rich List Alert", "📊 Changes 1H", ""]
        # ヘッダ + 空行 + URL(23文字換算) + 改行ぶん
        budget = X_CHARACTER_LIMIT - len("\n".join(header)) - X_URL_LENGTH - 2

        body: List[str] = []
        used = 0
        for index, change in enumerate(changes):
            sign = "+" if change.balance_change >= 0 else ""
            arrow = "↗️" if change.balance_change >= 0 else "↘️"
            entry = (
                f"{change.name}\n"
                f"  {arrow} {sign}{change.balance_change:,.0f} XRP "
                f"({sign}{change.percentage_change:.1f}%)"
            )

            remaining = len(changes) - index
            # 途中で切る場合は「他N件」の行ぶんも確保しておく
            suffix_cost = len(f"…他{remaining}件") + 1 if remaining > 1 else 0

            if used + len(entry) + 1 + suffix_cost > budget:
                body.append(f"…他{remaining}件")
                break

            body.append(entry)
            used += len(entry) + 1

        return "\n".join(header + body + ["", url])

    def post_alert(
        self,
        threshold_percentage: float = 5.0,
        threshold_amount: float = 1000000.0,
    ):
        """アラートの投稿"""
        try:
            changes = self.supabase.get_significant_changes(
                threshold_percentage, threshold_amount
            )

            tweet_text = self.format_tweet(changes)
            if not tweet_text:
                print("No significant changes to report")
                return

            channel_id = self._resolve_channel_id()
            post = self.buffer.create_post(channel_id, tweet_text, mode="now")
            print(f"Alert posted via Buffer: id={post['id']} status={post['status']}")

        except Exception as e:
            print(f"Error posting alert: {e}")


def main():
    try:
        bot = XRPAlertBot()
        bot.post_alert(threshold_percentage=0.1, threshold_amount=500000)
    except Exception as e:
        print(f"Fatal error: {e}")


if __name__ == "__main__":
    main()
