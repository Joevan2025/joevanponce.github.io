import requests
import csv
import os
from datetime import datetime
from dotenv import load_dotenv

# ============================================
# LOAD CREDENTIALS FROM .env FILE
# ============================================
load_dotenv()

LONG_LIVED_TOKEN = os.getenv("LONG_LIVED_TOKEN")
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
API_VERSION = os.getenv("API_VERSION", "v19.0")
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"
OUTPUT_DIR = "facebook_data"

# Validate credentials
if not LONG_LIVED_TOKEN:
    raise ValueError("❌ LONG_LIVED_TOKEN is missing in your .env file!")
if not APP_ID or not APP_SECRET:
    raise ValueError("❌ APP_ID or APP_SECRET is missing in your .env file!")

# Create output folder
os.makedirs(OUTPUT_DIR, exist_ok=True)


def save_to_csv(data, filename, fieldnames):
    """Save a list of dicts to a CSV file."""
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"✅ Saved: {filepath} ({len(data)} records)")


def paginate(url, params):
    """Handle Facebook API pagination automatically."""
    results = []
    while url:
        res = requests.get(url, params=params)
        data = res.json()
        if "error" in data:
            print(f"❌ API Error: {data['error']['message']}")
            break
        results.extend(data.get("data", []))
        # Get next page
        url = data.get("paging", {}).get("next")
        params = {}  # next URL already has params embedded
    return results


# ============================================
# 1. MY POSTS
# ============================================
def extract_my_posts():
    print("\n📄 Extracting My Posts...")
    url = f"{BASE_URL}/me/posts"
    params = {
        "access_token": LONG_LIVED_TOKEN,
        "fields": "id,message,story,created_time,likes.summary(true),comments.summary(true),shares",
        "limit": 100
    }
    posts = paginate(url, params)
    rows = []
    for p in posts:
        rows.append({
            "post_id": p.get("id", ""),
            "message": p.get("message", p.get("story", "")),
            "created_time": p.get("created_time", ""),
            "likes": p.get("likes", {}).get("summary", {}).get("total_count", 0),
            "comments": p.get("comments", {}).get("summary", {}).get("total_count", 0),
            "shares": p.get("shares", {}).get("count", 0),
        })
    save_to_csv(rows, "my_posts.csv", ["post_id", "message", "created_time", "likes", "comments", "shares"])


# ============================================
# 2. PAGE POSTS & INSIGHTS
# ============================================
def extract_page_data():
    print("\n📊 Extracting Page Posts & Insights...")
    # Get list of pages managed by user
    url = f"{BASE_URL}/me/accounts"
    res = requests.get(url, params={"access_token": LONG_LIVED_TOKEN})
    pages = res.json().get("data", [])

    if not pages:
        print("⚠️  No pages found for this account.")
        return

    all_posts = []
    all_insights = []

    for page in pages:
        page_id = page["id"]
        page_name = page["name"]
        page_token = page["access_token"]
        print(f"   → Processing page: {page_name}")

        # Page Posts
        posts_url = f"{BASE_URL}/{page_id}/posts"
        posts_params = {
            "access_token": page_token,
            "fields": "id,message,story,created_time,likes.summary(true),comments.summary(true),shares",
            "limit": 100
        }
        posts = paginate(posts_url, posts_params)
        for p in posts:
            all_posts.append({
                "page_name": page_name,
                "page_id": page_id,
                "post_id": p.get("id", ""),
                "message": p.get("message", p.get("story", "")),
                "created_time": p.get("created_time", ""),
                "likes": p.get("likes", {}).get("summary", {}).get("total_count", 0),
                "comments": p.get("comments", {}).get("summary", {}).get("total_count", 0),
                "shares": p.get("shares", {}).get("count", 0),
            })

        # Page Insights
        insights_url = f"{BASE_URL}/{page_id}/insights"
        insights_params = {
            "access_token": page_token,
            "metric": "page_impressions,page_reach,page_engaged_users,page_fans",
            "period": "day",
            "limit": 100
        }
        insights = paginate(insights_url, insights_params)
        for i in insights:
            for val in i.get("values", []):
                all_insights.append({
                    "page_name": page_name,
                    "page_id": page_id,
                    "metric": i.get("name", ""),
                    "value": val.get("value", 0),
                    "end_time": val.get("end_time", ""),
                })

    if all_posts:
        save_to_csv(all_posts, "page_posts.csv",
                    ["page_name", "page_id", "post_id", "message", "created_time", "likes", "comments", "shares"])
    if all_insights:
        save_to_csv(all_insights, "page_insights.csv",
                    ["page_name", "page_id", "metric", "value", "end_time"])


# ============================================
# 3. AD CAMPAIGN DATA
# ============================================
def extract_ad_data():
    print("\n📢 Extracting Ad Campaign Data...")
    # Get ad accounts
    url = f"{BASE_URL}/me/adaccounts"
    res = requests.get(url, params={
        "access_token": LONG_LIVED_TOKEN,
        "fields": "id,name,account_status,currency"
    })
    ad_accounts = res.json().get("data", [])

    if not ad_accounts:
        print("⚠️  No ad accounts found.")
        return

    all_campaigns = []

    for account in ad_accounts:
        account_id = account["id"]
        account_name = account.get("name", "")
        print(f"   → Processing ad account: {account_name}")

        campaigns_url = f"{BASE_URL}/{account_id}/campaigns"
        campaigns_params = {
            "access_token": LONG_LIVED_TOKEN,
            "fields": "id,name,status,objective,daily_budget,lifetime_budget,start_time,stop_time,insights{impressions,clicks,spend,reach,ctr,cpc}",
            "limit": 100
        }
        campaigns = paginate(campaigns_url, campaigns_params)
        for c in campaigns:
            insights = c.get("insights", {}).get("data", [{}])[0] if c.get("insights") else {}
            all_campaigns.append({
                "account_name": account_name,
                "account_id": account_id,
                "campaign_id": c.get("id", ""),
                "campaign_name": c.get("name", ""),
                "status": c.get("status", ""),
                "objective": c.get("objective", ""),
                "daily_budget": c.get("daily_budget", ""),
                "lifetime_budget": c.get("lifetime_budget", ""),
                "start_time": c.get("start_time", ""),
                "stop_time": c.get("stop_time", ""),
                "impressions": insights.get("impressions", 0),
                "clicks": insights.get("clicks", 0),
                "spend": insights.get("spend", 0),
                "reach": insights.get("reach", 0),
                "ctr": insights.get("ctr", 0),
                "cpc": insights.get("cpc", 0),
            })

    if all_campaigns:
        save_to_csv(all_campaigns, "ad_campaigns.csv", [
            "account_name", "account_id", "campaign_id", "campaign_name",
            "status", "objective", "daily_budget", "lifetime_budget",
            "start_time", "stop_time", "impressions", "clicks", "spend",
            "reach", "ctr", "cpc"
        ])


# ============================================
# 4. FRIENDS LIST
# ============================================
def extract_friends():
    print("\n👥 Extracting Friends List...")
    url = f"{BASE_URL}/me/friends"
    params = {
        "access_token": LONG_LIVED_TOKEN,
        "fields": "id,name",
        "limit": 100
    }
    friends = paginate(url, params)

    if not friends:
        print("⚠️  No friends data returned. Note: Facebook only returns friends")
        print("   who also use your app. Full friends list requires special permissions.")
        return

    rows = [{"friend_id": f.get("id", ""), "name": f.get("name", "")} for f in friends]
    save_to_csv(rows, "friends_list.csv", ["friend_id", "name"])


# ============================================
# MAIN
# ============================================
if __name__ == "__main__":
    print("=" * 50)
    print("  Facebook Data Extractor")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    extract_my_posts()
    extract_page_data()
    extract_ad_data()
    extract_friends()

    print("\n✅ All done! CSV files saved in the 'facebook_data' folder.")