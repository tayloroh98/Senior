# Google Cloud Functions 배포 명령어

## 1. Secret Manager에 시크릿 저장

```bash
# Google Ads 설정
echo "YOUR_GOOGLE_ADS_DEVELOPER_TOKEN" | gcloud secrets create google-ads-developer-token --data-file=-
echo "YOUR_GOOGLE_ADS_CLIENT_ID" | gcloud secrets create google-ads-client-id --data-file=-
echo "YOUR_GOOGLE_ADS_CLIENT_SECRET" | gcloud secrets create google-ads-client-secret --data-file=-
echo "YOUR_GOOGLE_ADS_REFRESH_TOKEN" | gcloud secrets create google-ads-refresh-token --data-file=-
echo "YOUR_GOOGLE_ADS_LOGIN_CUSTOMER_ID" | gcloud secrets create google-ads-login-customer-id --data-file=-
echo "YOUR_GOOGLE_ADS_CUSTOMER_ID" | gcloud secrets create google-ads-customer-id --data-file=-

# Gmail 설정
echo "YOUR_GMAIL_CLIENT_ID" | gcloud secrets create gmail-client-id --data-file=-
echo "YOUR_GMAIL_CLIENT_SECRET" | gcloud secrets create gmail-client-secret --data-file=-
echo "YOUR_GMAIL_REFRESH_TOKEN" | gcloud secrets create gmail-refresh-token --data-file=-

# 기타 설정
echo "YOUR_GEMINI_API_KEY" | gcloud secrets create gemini-api-key --data-file=-
echo "YOUR_EMAIL_ADDRESS" | gcloud secrets create report-recipient-email --data-file=-
```

## 2. Cloud Functions 배포

```bash
gcloud functions deploy marketing-pipeline \
  --runtime python311 \
  --trigger-http \
  --allow-unauthenticated \
  --memory 1024MB \
  --timeout 540s \
  --max-instances 10 \
  --set-env-vars GCP_PROJECT_ID=YOUR_PROJECT_ID,BIGQUERY_DATASET_ID=marketing_data,GOOGLE_ADS_ENABLED=true,GMAIL_ENABLED=true,FACEBOOK_ADS_ENABLED=false,LINKEDIN_ADS_ENABLED=false \
  --set-secrets GOOGLE_ADS_DEVELOPER_TOKEN=google-ads-developer-token:latest,GOOGLE_ADS_CLIENT_ID=google-ads-client-id:latest,GOOGLE_ADS_CLIENT_SECRET=google-ads-client-secret:latest,GOOGLE_ADS_REFRESH_TOKEN=google-ads-refresh-token:latest,GOOGLE_ADS_LOGIN_CUSTOMER_ID=google-ads-login-customer-id:latest,GOOGLE_ADS_CUSTOMER_ID=google-ads-customer-id:latest,GMAIL_CLIENT_ID=gmail-client-id:latest,GMAIL_CLIENT_SECRET=gmail-client-secret:latest,GMAIL_REFRESH_TOKEN=gmail-refresh-token:latest,GEMINI_API_KEY=gemini-api-key:latest,REPORT_RECIPIENT_EMAIL=report-recipient-email:latest \
  --source .
```

## 3. Cloud Scheduler 설정

```bash
gcloud scheduler jobs create http marketing-report-scheduler \
  --schedule="0 9 * * *" \
  --time-zone="Asia/Seoul" \
  --uri="https://your-region-your-project.cloudfunctions.net/marketing-pipeline" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{"report_date":""}' \
  --description="Daily marketing report automation"
```

## 4. 로컬 테스트

```bash
# .env 파일 설정 후
python main.py
```

## 5. 필요한 권한

```bash
# Cloud Functions 서비스 계정에 필요한 권한
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:YOUR_PROJECT_ID@appspot.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:YOUR_PROJECT_ID@appspot.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

## 주의사항

- 모든 `YOUR_*` 플레이스홀더를 실제 값으로 교체하세요
- 민감한 정보는 절대 코드에 직접 포함하지 마세요
- Secret Manager를 사용하여 보안을 유지하세요