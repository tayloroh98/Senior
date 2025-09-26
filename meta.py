import requests
import json

# Meta Marketing API 설정값
ACCESS_TOKEN = "EAAS2XKrZCIIABPuVG3aFrD3Mawc3fSNcqHLJqqm8HI9DBd5VCed5EYov0TZBMYBlZC8ZAQ1QvMAHmywKyn3OpETkU4Ljgx1v8YtCDHZBiIt0iXXcHnIeUowA2MnoZBfoYmGUMkkArD6dFfYwH8tkk2J9UdJJqZA5i6pZAIi1XZBgNiC9VKnX36ZBuszeVTLpMAaNNlJACFW9ZAf"   # 발급받은 User/Client 토큰
AD_ACCOUNT_ID = "act_34634203"      # "act_" 포함 (예: act_1234567890)
API_VERSION = "v23.0"

# Insights API 엔드포인트
url = f"https://graph.facebook.com/{API_VERSION}/{AD_ACCOUNT_ID}/insights"

# 요청 파라미터
params = {
    "fields": "campaign_name,adset_name,ad_name,impressions,clicks,spend,ctr,cpc,actions",
    "date_preset": "last_7d",    # 최근 7일 데이터
    "level": "ad",               # 집계 레벨: account / campaign / adset / ad
    "access_token": ACCESS_TOKEN
}

# API 요청
response = requests.get(url, params=params)

# 응답 처리
if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=2))  # JSON 보기 쉽게 출력
else:
    print("Error:", response.status_code, response.text)
