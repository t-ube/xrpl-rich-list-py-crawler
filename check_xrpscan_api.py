#!/usr/bin/env python3
import asyncio
import aiohttp
import json
import re
from typing import Optional

API_URL = "https://api.xrpscan.com/api/v1/balances"

HEADERS = {
    # loader.py と同じ系統のUAに寄せる
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

def sniff_html(text: str) -> Optional[str]:
    """HTMLっぽい原因をざっくり推測"""
    t = text.lower()
    if "cf-ray" in t or "cloudflare" in t or "attention required" in t:
        return "Cloudflareブロック/チャレンジの可能性"
    if "<html" in t or "<!doctype html" in t:
        # XRPSCANの通常HTMLか、エラーページか、CDNブロックか
        if "xrpscan" in t:
            return "XRPSCANのHTMLページを返している（APIではなくWeb側/リダイレクトの可能性）"
        return "HTMLページを返している（WAF/エラーページ/リダイレクト先の可能性）"
    return None

async def fetch_once(session: aiohttp.ClientSession, url: str) -> None:
    async with session.get(url, allow_redirects=True) as resp:
        print("=== Request ===")
        print("URL:", url)
        print("Status:", resp.status)
        print("Final URL:", str(resp.url))
        print("\n=== Response headers ===")
        for k in ["Content-Type", "Server", "CF-RAY", "Location", "Retry-After"]:
            v = resp.headers.get(k)
            if v:
                print(f"{k}: {v}")

        body = await resp.read()
        print("\n=== Body preview ===")
        # 先頭 5000 bytes だけ表示（文字化け防止にreplace）
        preview = body[:5000].decode("utf-8", errors="replace")
        print(preview[:2000])  # 表示はさらに短く

        ct = resp.headers.get("Content-Type", "")
        if "application/json" in ct or "text/json" in ct:
            print("Content-Type はJSONっぽい")
            try:
                data = json.loads(body.decode("utf-8", errors="strict"))
                if isinstance(data, list):
                    print(f"JSON parse OK: list len={len(data)}")
                    if len(data) > 0:
                        print("First item keys:", list(data[0].keys()))
                else:
                    print("JSON parse OK:", type(data))
            except Exception as e:
                print("JSON parse FAILED:", e)
        else:
            print(f"Content-Type がJSONではない: {ct}")
            guess = sniff_html(preview)
            if guess:
                print("推測:", guess)

        # 保存（CIログだけでは足りない時用）
        out = "xrpscan_response_preview.txt"
        with open(out, "w", encoding="utf-8") as f:
            f.write(preview)
        print(f"\nSaved preview to: {out}")

async def main():
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
        await fetch_once(session, API_URL)

if __name__ == "__main__":
    asyncio.run(main())
