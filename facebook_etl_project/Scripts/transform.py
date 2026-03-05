import pandas as pd
import os
from datetime import datetime

# ============================================
# CONFIG
# ============================================
INPUT_DIR = "facebook_data"
OUTPUT_DIR = r"C:\Users\ADMIN\projects\etl\facebook_etl_local\facebook_data_transformed"
DATE_FROM = "2024-01-01"   # ← Change your start date
DATE_TO   = "2025-12-31"   # ← Change your end date

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================
# HELPERS
# ============================================
def load_csv(filename):
    filepath = os.path.join(INPUT_DIR, filename)
    if not os.path.exists(filepath):
        print(f"⚠️  File not found: {filepath}")
        return None
    df = pd.read_csv(filepath)
    print(f"📂 Loaded: {filename} ({len(df)} rows)")
    return df


def save_csv(df, filename):
    filepath = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(filepath, index=False)
    print(f"✅ Saved: {filepath} ({len(df)} rows)")


def clean_dates(df, col):
    """Parse and normalize datetime column."""
    df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    df[col] = df[col].dt.tz_localize(None)  # Remove timezone for clean CSV
    return df


def filter_by_date(df, col, date_from, date_to):
    """Filter rows within a date range."""
    before = len(df)
    df = df[(df[col] >= date_from) & (df[col] <= date_to)]
    print(f"   🔍 Date filter: {before} → {len(df)} rows ({before - len(df)} removed)")
    return df


def clean_text(df, cols):
    """Strip whitespace and fill nulls in text columns."""
    for col in cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()
    return df


def clean_numeric(df, cols):
    """Fill nulls with 0 in numeric columns."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


# ============================================
# 1. MY POSTS
# ============================================
def transform_my_posts():
    print("\n📄 Transforming My Posts...")
    df = load_csv("my_posts.csv")
    if df is None:
        return

    # Clean
    df = clean_text(df, ["post_id", "message"])
    df = clean_numeric(df, ["likes", "comments", "shares"])
    df = clean_dates(df, "created_time")

    # Filter by date
    df = filter_by_date(df, "created_time", DATE_FROM, DATE_TO)

    # Calculated fields
    df["total_interactions"] = df["likes"] + df["comments"] + df["shares"]
    df["engagement_score"] = (
        df["likes"] * 1 + df["comments"] * 2 + df["shares"] * 3
    )  # Weighted: shares > comments > likes
    df["has_text"] = df["message"].str.len() > 0

    # Aggregation: daily post summary
    df["date"] = df["created_time"].dt.date
    daily = (
        df.groupby("date")
        .agg(
            total_posts=("post_id", "count"),
            total_likes=("likes", "sum"),
            total_comments=("comments", "sum"),
            total_shares=("shares", "sum"),
            avg_engagement_score=("engagement_score", "mean"),
        )
        .reset_index()
    )
    daily["avg_engagement_score"] = daily["avg_engagement_score"].round(2)

    # Drop unneeded columns
    df = df.drop(columns=["message"], errors="ignore")

    save_csv(df, "my_posts_transformed.csv")
    save_csv(daily, "my_posts_daily_summary.csv")


# ============================================
# 2. PAGE POSTS
# ============================================
def transform_page_posts():
    print("\n📄 Transforming Page Posts...")
    df = load_csv("page_posts.csv")
    if df is None:
        return

    # Clean
    df = clean_text(df, ["page_name", "page_id", "post_id", "message"])
    df = clean_numeric(df, ["likes", "comments", "shares"])
    df = clean_dates(df, "created_time")

    # Filter by date
    df = filter_by_date(df, "created_time", DATE_FROM, DATE_TO)

    # Calculated fields
    df["total_interactions"] = df["likes"] + df["comments"] + df["shares"]
    df["engagement_rate"] = (
        df["total_interactions"] / df["total_interactions"].sum() * 100
    ).round(4)

    # Aggregation: per page summary (before dropping page_name)
    page_summary = (
        df.groupby("page_name")
        .agg(
            total_posts=("post_id", "count"),
            total_likes=("likes", "sum"),
            total_comments=("comments", "sum"),
            total_shares=("shares", "sum"),
            avg_interactions=("total_interactions", "mean"),
        )
        .reset_index()
    )
    page_summary["avg_interactions"] = page_summary["avg_interactions"].round(2)

    # Drop unneeded columns
    df = df.drop(columns=["page_name", "page_id", "message"], errors="ignore")

    save_csv(df, "page_posts_transformed.csv")
    save_csv(page_summary, "page_posts_summary.csv")


# ============================================
# 3. PAGE INSIGHTS
# ============================================
def transform_page_insights():
    print("\n📊 Transforming Page Insights...")
    df = load_csv("page_insights.csv")
    if df is None:
        return

    # Clean
    df = clean_text(df, ["page_name", "page_id", "metric"])
    df = clean_numeric(df, ["value"])
    df = clean_dates(df, "end_time")

    # Filter by date
    df = filter_by_date(df, "end_time", DATE_FROM, DATE_TO)

    # Aggregation: pivot metrics into columns per page per day
    df["date"] = df["end_time"].dt.date
    pivot = df.pivot_table(
        index=["page_name", "date"],
        columns="metric",
        values="value",
        aggfunc="sum",
    ).reset_index()
    pivot.columns.name = None
    pivot = pivot.fillna(0)

    # Daily totals per metric
    daily_totals = (
        df.groupby(["date", "metric"])
        .agg(total_value=("value", "sum"))
        .reset_index()
    )

    # Drop unneeded columns
    df = df.drop(columns=["page_name", "page_id"], errors="ignore")

    save_csv(df, "page_insights_transformed.csv")
    save_csv(pivot, "page_insights_pivot.csv")
    save_csv(daily_totals, "page_insights_daily_totals.csv")


# ============================================
# 4. AD CAMPAIGNS
# ============================================
def transform_ad_campaigns():
    print("\n📢 Transforming Ad Campaigns...")
    df = load_csv("ad_campaigns.csv")
    if df is None:
        return

    # Clean
    df = clean_text(df, ["account_name", "campaign_name", "status", "objective"])
    df = clean_numeric(df, ["impressions", "clicks", "spend", "reach", "ctr", "cpc",
                             "daily_budget", "lifetime_budget"])
    df = clean_dates(df, "start_time")

    # Filter: only active/paused campaigns within date range
    df = df[df["status"].isin(["ACTIVE", "PAUSED", "COMPLETED"])]
    df = filter_by_date(df, "start_time", DATE_FROM, DATE_TO)

    # Calculated fields
    df["ctr_calculated"] = (
        (df["clicks"] / df["impressions"].replace(0, pd.NA)) * 100
    ).round(4).fillna(0)

    df["cpc_calculated"] = (
        (df["spend"] / df["clicks"].replace(0, pd.NA))
    ).round(4).fillna(0)

    df["cpm"] = (
        (df["spend"] / df["impressions"].replace(0, pd.NA)) * 1000
    ).round(4).fillna(0)  # Cost per 1000 impressions

    df["conversion_efficiency"] = (
        df["clicks"] / df["reach"].replace(0, pd.NA)
    ).round(4).fillna(0)  # Click-through from reached users

    # Aggregation: summary per objective
    objective_summary = (
        df.groupby("objective")
        .agg(
            total_campaigns=("campaign_id", "count"),
            total_spend=("spend", "sum"),
            total_impressions=("impressions", "sum"),
            total_clicks=("clicks", "sum"),
            total_reach=("reach", "sum"),
            avg_ctr=("ctr_calculated", "mean"),
            avg_cpc=("cpc_calculated", "mean"),
            avg_cpm=("cpm", "mean"),
        )
        .reset_index()
    )
    for col in ["total_spend", "avg_ctr", "avg_cpc", "avg_cpm"]:
        objective_summary[col] = objective_summary[col].round(2)

    # Aggregation: summary per account
    account_summary = (
        df.groupby("account_name")
        .agg(
            total_campaigns=("campaign_id", "count"),
            total_spend=("spend", "sum"),
            total_impressions=("impressions", "sum"),
            total_clicks=("clicks", "sum"),
            avg_ctr=("ctr_calculated", "mean"),
        )
        .reset_index()
    )

    save_csv(df, "ad_campaigns_transformed.csv")
    save_csv(objective_summary, "ad_campaigns_by_objective.csv")
    save_csv(account_summary, "ad_campaigns_by_account.csv")


# ============================================
# 5. FRIENDS LIST
# ============================================
def transform_friends():
    print("\n👥 Transforming Friends List...")
    df = load_csv("friends_list.csv")
    if df is None:
        return

    # Clean
    df = clean_text(df, ["friend_id", "name"])

    # Remove duplicates
    before = len(df)
    df = df.drop_duplicates(subset="friend_id")
    print(f"   🧹 Removed {before - len(df)} duplicate entries")

    # Add name breakdown
    df["first_name"] = df["name"].str.split().str[0]
    df["last_name"]  = df["name"].str.split().str[-1]

    # Summary
    summary = pd.DataFrame({
        "metric": ["Total Friends"],
        "value":  [len(df)]
    })

    save_csv(df, "friends_transformed.csv")
    save_csv(summary, "friends_summary.csv")


# ============================================
# MAIN
# ============================================
if __name__ == "__main__":
    print("=" * 50)
    print("  Facebook Data Transformer")
    print(f"  Date Range: {DATE_FROM} → {DATE_TO}")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    transform_my_posts()
    transform_page_posts()
    transform_page_insights()
    transform_ad_campaigns()
    transform_friends()

    print("\n✅ All transformations complete!")
    print(f"📁 Output saved in: '{OUTPUT_DIR}/' folder")