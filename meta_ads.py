import requests
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import csv
import io
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Load environment variables
load_dotenv()


# Facebook API configuration
GRAPH_API_URL = "https://graph.facebook.com/v22.0"
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("AD_ACCOUNT_ID")

# Validate Facebook API credentials
if not ACCESS_TOKEN or not AD_ACCOUNT_ID:
    raise ValueError("Access Token or Ad Account ID not found in .env file")

# Define date range for the past year from today
START_DATE = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
END_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")  # Yesterday

# Define insight fields
insight_fields = "impressions,clicks,inline_link_clicks,spend,reach,cpc,cpm,conversions,cost_per_conversion,frequency"

# Define FIELDS without breakdowns
FIELDS = f"name,created_time,status,insights.time_range({{'since':'{START_DATE}','until':'{END_DATE}'}}){{{insight_fields}}}"

# Filter ads by created_time > START_DATE
filtering = f"[{{'field':'created_time','operator':'GREATER_THAN','value':'{START_DATE}'}}]"
url = f"{GRAPH_API_URL}/{AD_ACCOUNT_ID}/ads?fields={FIELDS}&filtering={filtering}&limit=100&access_token={ACCESS_TOKEN}"

all_ads = []

# Fetch ads from Facebook Graph API
while url:
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            ads = data.get("data", [])
            all_ads.extend(ads)
            url = data.get("paging", {}).get("next")
        else:
            error_data = response.json()
            print(f"Error: {response.status_code}, {error_data}")
            
            # Provide helpful error messages
            if response.status_code == 400:
                error_msg = error_data.get('error', {}).get('message', '')
                if 'permissions' in error_msg.lower():
                    print("\n🔑 Permission Error Detected:")
                    print("   - Your access token may not have permission to access this ad account")
                    print("   - Check if the Ad Account ID is correct")
                    print("   - Verify your token has 'ads_read' permission")
                elif 'does not exist' in error_msg.lower():
                    print("\n❌ Ad Account Not Found:")
                    print("   - The Ad Account ID '334634203' may not exist")
                    print("   - Check your Facebook Ads Manager for the correct account ID")
                elif 'expired' in error_msg.lower():
                    print("\n⏰ Token Expired:")
                    print("   - Your access token may have expired")
                    print("   - Generate a new token from Facebook Developer Console")
            break
    except requests.exceptions.RequestException as e:
        print(f"Network error: {e}")
        break

# Manually filter ads to ensure created_time <= END_DATE
filtered_ads = []
for ad in all_ads:
    created_time = ad.get("created_time")
    if created_time:
        created_dt = datetime.strptime(created_time, "%Y-%m-%dT%H:%M:%S%z")
        if created_dt <= datetime.strptime(END_DATE + "T23:59:59+0000", "%Y-%m-%dT%H:%M:%S%z") and created_dt >= datetime.strptime(START_DATE + "T00:00:00+0000", "%Y-%m-%dT%H:%M:%S%z"):
            filtered_ads.append(ad)

# Debugging: Check the creation dates of retrieved ads
print(f"Ads retrieved between ({START_DATE} and {END_DATE}): {len(filtered_ads)}")

# Define CSV header (no breakdowns for now)
header = ["Date", "AD Name", "Created Time", "Status", "Impressions", "Clicks", "Link Clicks", "Spend", "Reach", "CPC", "Cost per 1000 Impressions", "Conversions", "Cost per Conversion", "Ad Views per Person"]

# Create CSV content in memory
output = io.StringIO()
writer = csv.writer(output)
writer.writerow(header)

# Process ads and write to CSV
for ad in filtered_ads:
    date = datetime.now().date()
    ad_name = ad.get("name", "[No name]")
    created_time = ad.get("created_time")
    status = ad.get("status")
    insights_list = ad.get("insights", {}).get("data", [])
    
    if not insights_list:
        # No insights data; write a single row with default values
        data = [date, ad_name, created_time, status, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        writer.writerow(data)
        continue
    
    for insights in insights_list:
        impressions = f"{round(float(insights.get('impressions', 0)), 2):,.1f}"
        clicks = f"{round(float(insights.get('clicks', 0)), 2):,.0f}"
        link_clicks = f"{round(float(insights.get('inline_link_clicks', 0)), 2):,.0f}"
        spend = f"${round(float(insights.get('spend', 0)), 2):,.2f}"
        reach = f"{round(float(insights.get('reach', 0)), 2):,.0f}"
        cpc = f"${round(float(insights.get('cpc', 0)), 2):,.2f}"
        cpm = f"${round(float(insights.get('cpm', 0)), 2):,.2f}"
        conversions_list = insights.get("conversions", [])
        conversions_total = sum(float(conv.get("value", 0)) for conv in conversions_list) if conversions_list else 0
        conversions = f"{round(conversions_total, 2):,.0f}"
        cost_per_conversion_list = insights.get("cost_per_conversion", [])
        cost_per_conversion_total = sum(float(cpc.get("value", 0)) for cpc in cost_per_conversion_list) if cost_per_conversion_list else 0
        cost_per_conversion = f"${round(cost_per_conversion_total, 2):,.2f}"
        frequency = f"{round(float(insights.get('frequency', 0)), 2):,.0f}"
        
        data = [date, ad_name, created_time, status, impressions, clicks, link_clicks, spend, reach, cpc, cpm, conversions, cost_per_conversion, frequency]
        writer.writerow(data)

# Get CSV content
csv_content = output.getvalue()
output.close()

# Save CSV to file
with open("social_metrics/facebook/fb_ad_metrics.csv", "w", newline="", encoding="utf-8") as f:
    f.write(csv_content)
print("CSV file saved successfully as 'fb_ad_metrics.csv'")