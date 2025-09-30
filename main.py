import functions_framework
import logging
import json
import os
import base64
from datetime import datetime, timedelta
from typing import Dict, Any
import traceback
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import google.generativeai as genai
import requests
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# .env 파일 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# BigQuery 설정
BIGQUERY_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'marketing-automation-473220')
BIGQUERY_DATASET_ID = os.getenv('BIGQUERY_DATASET_ID', 'marketing_data')

# Gemini API 설정
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', None)

# Gmail API 스코프
GMAIL_SCOPES = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/userinfo.email',
    'openid'
]

def authenticate_gmail():
    """
    Gmail API 인증을 수행합니다.
    OAuth2를 사용하여 사용자 인증을 받습니다.
    """
    creds = None
    
    # token.json 파일이 있으면 기존 인증 정보 사용
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', GMAIL_SCOPES)
    
    # 유효한 인증 정보가 없거나 refresh_token이 없는 경우 새로 인증
    if not creds or not creds.valid or not getattr(creds, 'refresh_token', None):
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # credentials.json 파일이 필요합니다 (Google Cloud Console에서 다운로드)
            if not os.path.exists('credentials.json'):
                logger.error("credentials.json 파일이 없습니다. Google Cloud Console에서 OAuth 2.0 클라이언트 ID를 다운로드하세요.")
                return None
            
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', GMAIL_SCOPES)
            # Force consent to ensure refresh_token issuance
            creds = flow.run_local_server(
                port=8080,
                access_type='offline',
                prompt='consent'
            )
        
        # 인증 정보를 token.json에 저장
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    
    return creds

def create_gmail_message(sender, to, subject, message_text, html_content=None):
    """
    Gmail API용 메시지를 생성합니다.
    """
    message = MIMEMultipart('alternative')
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    
    # 텍스트 버전
    text_part = MIMEText(message_text, 'plain', 'utf-8')
    message.attach(text_part)
    
    # HTML 버전 (있는 경우)
    if html_content:
        html_part = MIMEText(html_content, 'html', 'utf-8')
        message.attach(html_part)
    
    # Base64 인코딩
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
    return {'raw': raw_message}

def create_bigquery_dataset_if_not_exists(client, dataset_id):
    """
    BigQuery 데이터셋이 존재하지 않으면 생성
    """
    try:
        dataset_ref = client.dataset(dataset_id)
        dataset = client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_id} already exists")
        return dataset
    except NotFound:
        logger.info(f"Creating dataset {dataset_id}")
        dataset_ref = client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        logger.info(f"Dataset {dataset_id} created successfully")
        return dataset

def create_bigquery_table_if_not_exists(client, dataset_id, table_id, schema):
    """
    BigQuery 테이블이 존재하지 않으면 생성
    """
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        logger.info(f"Table {dataset_id}.{table_id} already exists")
        return table
    except NotFound:
        logger.info(f"Creating table {dataset_id}.{table_id}")
        table_ref = client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        logger.info(f"Table {dataset_id}.{table_id} created successfully")
        return table

def get_bigquery_data_for_analysis(report_date: str = None) -> pd.DataFrame:
    """
    BigQuery에서 Google Ads 데이터를 조회 (분석용)
    """
    try:
        if not report_date:
            report_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info(f"Fetching Google Ads data from BigQuery for analysis on {report_date}")
        
        # BigQuery 클라이언트 초기화
        client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
        
        # 쿼리 작성
        query = f"""
            SELECT 
                channel,
                campaign_name,
                impressions,
                clicks,
                spend,
                cpc,
                conversions,
                cost_per_conversion,
                report_date
            FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.google_ads_daily`
            WHERE report_date = '{report_date}'
            ORDER BY impressions DESC
        """
        
        # 쿼리 실행
        query_job = client.query(query)
        results = query_job.result()
        
        # DataFrame으로 변환
        df = pd.DataFrame([dict(row) for row in results])
        
        logger.info(f"Retrieved {len(df)} rows from BigQuery for analysis")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching data from BigQuery for analysis: {str(e)}")
        raise

def analyze_marketing_data_with_gemini(data: pd.DataFrame, report_date: str) -> str:
    """
    Gemini를 사용해서 마케팅 데이터 분석
    """
    try:
        if GEMINI_API_KEY is None:
            logger.warning("GEMINI_API_KEY not set, skipping LLM analysis")
            return "LLM analysis skipped - API key not configured"
        
        # Gemini API 설정
        genai.configure(api_key=GEMINI_API_KEY)
        
        # 모델 초기화 (지원되는 최신 모델 사용)
        model = genai.GenerativeModel('models/gemini-2.5-flash')
        
        # 데이터를 문자열로 변환 (간단한 요약 형태로)
        if len(data) == 0:
            data_summary = "No data available for the specified date."
        else:
            # 주요 지표 요약
            total_impressions = data['impressions'].sum()
            total_clicks = data['clicks'].sum()
            total_spend = data['spend'].sum()
            total_conversions = data['conversions'].sum()
            avg_cpc = data['cpc'].mean()
            avg_cost_per_conversion = data['cost_per_conversion'].mean()
            
            # 상위 캠페인 정보
            top_campaigns = data.head(3)[['campaign_name', 'impressions', 'clicks', 'spend', 'conversions']].to_string(index=False)
            
            data_summary = f"""
            Marketing Performance Summary for {report_date}:
            
            Overall Performance:
            - Total Impressions: {total_impressions:,}
            - Total Clicks: {total_clicks:,}
            - Total Spend: ${total_spend:.2f}
            - Total Conversions: {total_conversions}
            - Average CPC: ${avg_cpc:.2f}
            - Average Cost per Conversion: ${avg_cost_per_conversion:.2f}
            
            Top 3 Campaigns by Impressions:
            {top_campaigns}
            """
        
        # 프롬프트 작성
        prompt = f"""
        You are a marketing data analyst. You need to create a daily marketing performance summary report based on the following data.

        Your task:
        1. Analyze the marketing data and provide key insights
        2. Create a concise daily summary report (keep it brief and clear)
        3. Highlight up to 3 key insights or areas of attention (if there are fewer than 3 meaningful insights, provide only the available ones)
        4. Write everything in English
        5. Focus on actionable insights and trends

        Marketing Data:
        {data_summary}

        Please provide your analysis in the following format:
        **Daily Marketing Performance Report - {report_date}**

        **Key Insights:**
        1. [First insight]
        2. [Second insight] 
        3. [Third insight (if applicable)]

        **Summary:** [Brief overall assessment]
        """
        
        logger.info("Sending request to Gemini API for marketing analysis...")
        
        # Gemini API 호출
        response = model.generate_content(prompt)
        
        logger.info("Received marketing analysis from Gemini API")
        return response.text
        
    except Exception as e:
        logger.error(f"Error analyzing marketing data with Gemini: {str(e)}")
        return f"LLM analysis failed: {str(e)}"

def extract_google_ads_data(report_date: str) -> Dict[str, Any]:
    """
    Google Ads API에서 데이터 추출 (어제 하루 데이터만)
    """
    try:
        import pandas as pd
        import yaml
        from google.ads.googleads.client import GoogleAdsClient
        from google.ads.googleads.errors import GoogleAdsException
        
        logger.info(f"Extracting Google Ads data for {report_date}...")
        
        # Google Ads 클라이언트 초기화
        googleads_client = GoogleAdsClient.load_from_storage(path="./google-ads.yaml")
        ga_service = googleads_client.get_service("GoogleAdsService")
        
        # customer_id 가져오기
        with open("google-ads.yaml", "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        customer_id = config["customer_id"]
        
        # Google Ads 쿼리 언어 (GAQL) - 어제 하루 데이터만
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,         -- 비용 (마이크로 단위)
                metrics.average_cpc,         -- 평균 클릭당비용 (CPC)
                metrics.conversions,         -- 전환수
                metrics.cost_per_conversion  -- 전환당비용
            FROM
                campaign
            WHERE
                segments.date = '{report_date}'
                AND campaign.status = 'ENABLED'
            ORDER BY
                metrics.impressions DESC
        """
        
        # 데이터를 저장할 리스트
        data_rows = []
        
        # 검색 스트림을 사용하여 데이터 처리
        stream = ga_service.search_stream(customer_id=customer_id, query=query)
        
        # 결과 반복 처리
        for batch in stream:
            for row in batch.results:
                campaign = row.campaign
                metrics = row.metrics
                
                # API에서 받은 비용(cost)과 CPC는 '마이크로' 단위이므로 실제 통화 단위로 변환
                spend = metrics.cost_micros / 1_000_000
                cpc = metrics.average_cpc / 1_000_000
                cost_per_conversion = metrics.cost_per_conversion / 1_000_000
                
                # 데이터 행 생성
                data_row = {
                    "channel": "google ads",
                    "campaign_name": campaign.name,
                    "impressions": metrics.impressions,
                    "clicks": metrics.clicks,
                    "spend": round(spend, 2),
                    "cpc": round(cpc, 2),
                    "conversions": metrics.conversions,
                    "cost_per_conversion": round(cost_per_conversion, 2)
                }
                data_rows.append(data_row)
        
        # pandas DataFrame으로 변환
        df = pd.DataFrame(data_rows)
        
        logger.info(f"Successfully extracted {len(df)} rows of Google Ads data for {report_date}")
        
        return {
            "status": "success", 
            "data": df,
            "row_count": len(df),
            "report_date": report_date
        }
        
    except GoogleAdsException as ex:
        logger.error(f"Google Ads API error: {ex.error.code().name} - {str(ex)}")
        raise
    except FileNotFoundError:
        logger.error("google-ads.yaml file not found")
        raise
    except Exception as e:
        logger.error(f"Error extracting Google Ads data: {str(e)}")
        raise

# def extract_meta_ads_data(report_date: str) -> Dict[str, Any]:
#     """
#     Meta Marketing API에서 데이터 추출
#     """
#     try:
#         logger.info(f"Extracting Meta Ads data for {report_date}...")
#         # Meta API 호출 로직 구현
#         # from meta_ads import get_meta_data
#         # return get_meta_data(report_date)
#         return {"status": "success", "data": "meta_ads_data"}
#     except Exception as e:
#         logger.error(f"Error extracting Meta Ads data: {str(e)}")
#         raise


def load_data_to_warehouse(data: Dict[str, Any], source_name: str, report_date: str = None) -> Dict[str, Any]:
    try:
        logger.info(f"Loading {source_name} data to BigQuery...")
        
        # 입력 데이터에서 DataFrame 추출
        if "data" not in data:
            raise ValueError("Input data must contain 'data' field with DataFrame")
        
        df = data["data"]
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Data must be a pandas DataFrame")
        
        # BigQuery 클라이언트 초기화
        client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
        logger.info(f"Initialized BigQuery client for project: {BIGQUERY_PROJECT_ID}")
        
        # 데이터셋 생성 (존재하지 않는 경우)
        create_bigquery_dataset_if_not_exists(client, BIGQUERY_DATASET_ID)
        
        # 테이블 이름 생성 (소스별로 다른 테이블)
        table_id = f"{source_name.lower().replace(' ', '_')}_daily"
        
        # 테이블 스키마 정의
        schema = [
            bigquery.SchemaField("channel", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("campaign_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("impressions", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("clicks", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("spend", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("cpc", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("conversions", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("cost_per_conversion", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("report_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("uploaded_at", "TIMESTAMP", mode="REQUIRED")
        ]
        
        # 테이블 생성 (존재하지 않는 경우)
        table = create_bigquery_table_if_not_exists(client, BIGQUERY_DATASET_ID, table_id, schema)
        
        # DataFrame에 추가 컬럼 추가
        df_upload = df.copy()
        if report_date:
            df_upload['report_date'] = pd.to_datetime(report_date).date()
        else:
            df_upload['report_date'] = datetime.now().date()
        df_upload['uploaded_at'] = datetime.now()
        
        # BigQuery에 업로드
        table_ref = client.dataset(BIGQUERY_DATASET_ID).table(table_id)
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND"
        )
        
        job = client.load_table_from_dataframe(df_upload, table_ref, job_config=job_config)
        job.result()  # 작업 완료 대기
        
        logger.info(f"Successfully uploaded {len(df_upload)} rows to {BIGQUERY_DATASET_ID}.{table_id}")
        
        return {
            "status": "success",
            "source": source_name,
            "rows_uploaded": len(df_upload),
            "table_id": f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_id}",
            "write_disposition": "WRITE_APPEND"
        }
        
    except Exception as e:
        logger.error(f"Error loading {source_name} data to BigQuery: {str(e)}")
        return {
            "status": "error",
            "source": source_name,
            "error": str(e)
        }

def run_analysis_and_anomaly_detection(report_date: str) -> Dict[str, Any]:
    """
    웨어하우스 데이터를 기반으로 시계열 분석 및 이상치 탐지 실행 (Gemini LLM 사용)
    메트릭 계산 및 캠페인별 데이터 생성 포함
    """
    try:
        logger.info(f"Running LLM-powered analysis for {report_date}...")
        
        # 1. BigQuery에서 데이터 조회
        df = get_bigquery_data_for_analysis(report_date)
        
        # 2. 전체 메트릭 계산
        if len(df) > 0:
            total_cost = df['spend'].sum()
            total_clicks = df['clicks'].sum()
            total_conversions = df['conversions'].sum()
            
            # 평균 CPC 계산 (총 비용 / 총 클릭수)
            avg_cpc = total_cost / total_clicks if total_clicks > 0 else 0
            
            # 평균 전환당 비용 계산 (총 비용 / 총 전환수)
            avg_cost_per_conversion = total_cost / total_conversions if total_conversions > 0 else 0
            
            # ROA 계산 (임시로 conversions를 기반으로 계산, 실제로는 revenue 필요)
            # ROA = (전환수 * 가정 전환가치) / 총비용
            # 여기서는 N/A로 두거나, 간단히 conversions/spend 비율로 표시
            roa = f"{(total_conversions / total_cost * 100):.2f}%" if total_cost > 0 else "N/A"
            
            # 포맷팅
            metrics = {
                "total_cost": f"${total_cost:,.2f}",
                "cpc": f"${avg_cpc:.2f}",
                "cpc_per_conversion": f"${avg_cost_per_conversion:.2f}",
                "roa": roa
            }
            
            # 3. 캠페인별 데이터를 HTML 테이블 행으로 생성
            campaign_rows_html = ""
            for _, row in df.iterrows():
                # 각 캠페인의 메트릭 계산
                campaign_cost = row['spend']
                campaign_cpc = row['cpc']
                campaign_clicks = row['clicks']
                campaign_conversions = row['conversions']
                campaign_cost_per_conv = row['cost_per_conversion']
                
                # 캠페인별 ROA 계산
                campaign_roa = f"{(campaign_conversions / campaign_cost * 100):.2f}%" if campaign_cost > 0 else "N/A"
                
                campaign_rows_html += f"""
                                    <tr>
                                        <td>{row['channel']}</td>
                                        <td>{row['campaign_name']}</td>
                                        <td>${campaign_cost:,.2f}</td>
                                        <td>${campaign_cpc:.2f}</td>
                                        <td>${campaign_cost_per_conv:.2f}</td>
                                        <td>{campaign_roa}</td>
                                    </tr>"""
        else:
            # 데이터가 없는 경우 기본값
            metrics = {
                "total_cost": "N/A",
                "cpc": "N/A",
                "cpc_per_conversion": "N/A",
                "roa": "N/A"
            }
            campaign_rows_html = """
                                    <tr>
                                        <td colspan="6" style="text-align: center; color: #64748b;">No campaign data available for this date</td>
                                    </tr>"""
        
        # 4. Gemini로 마케팅 데이터 분석
        llm_analysis = analyze_marketing_data_with_gemini(df, report_date)
        
        # 5. 모든 결과 반환
        return {
            "status": "success", 
            "analysis_results": llm_analysis,
            "data_rows_analyzed": len(df),
            "report_date": report_date,
            # 메트릭 추가
            "total_cost": metrics["total_cost"],
            "cpc": metrics["cpc"],
            "cpc_per_conversion": metrics["cpc_per_conversion"],
            "roa": metrics["roa"],
            # 캠페인 행 HTML 추가
            "campaign_rows": campaign_rows_html
        }
    except Exception as e:
        logger.error(f"Error running LLM analysis: {str(e)}")
        return {
            "status": "error",
            "error": str(e),
            "analysis_results": f"Analysis failed: {str(e)}",
            "total_cost": "N/A",
            "cpc": "N/A",
            "cpc_per_conversion": "N/A",
            "roa": "N/A",
            "campaign_rows": ""
        }

def generate_report_content(analysis_results: Dict[str, Any]) -> str:
    """
    Generates HTML report content based on analysis results.
    Applies the structure from `report_content_test.html`.
    """
    try:
        logger.info("Generating HTML report content...")

        # Extract values with safe defaults
        report_date = analysis_results.get("report_date") or datetime.now().strftime("%Y-%m-%d")
        analysis_text = analysis_results.get("analysis_results", "No analysis results available.")

        # Metrics may come top-level or under a 'metrics' key
        metrics = analysis_results.get("metrics", {})
        total_cost = analysis_results.get("total_cost", metrics.get("total_cost", "N/A"))
        cpc = analysis_results.get("cpc", metrics.get("cpc", "N/A"))
        cpc_per_conversion = analysis_results.get("cpc_per_conversion", metrics.get("cpc_per_conversion", "N/A"))
        roa = analysis_results.get("roa", metrics.get("roa", "N/A"))

        # Campaign rows should be pre-rendered HTML rows, otherwise leave empty
        campaign_rows = analysis_results.get("campaign_rows", "")

        # Generate the HTML content (escaped braces for f-string CSS)
        html_content = f"""<!doctype html>
<html lang=\"ko\">
        <head>
            <meta charset=\"utf-8\">
            <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
            <title>Daily Marketing Data Analysis Report</title>
            <style>
                /* Base */
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen,
                       Ubuntu, Cantarell, 'Helvetica Neue', Arial, 'Apple Color Emoji', 'Segoe UI Emoji';
                       line-height: 1.6; color: #222; background: #f7f9fb; margin: 0; }}
                .container {{ max-width: 800px; margin: 0 auto; padding: 24px; }}
                .card {{ background: #ffffff; border: 1px solid #e6ecf2; border-radius: 10px; box-shadow: 0 2px 8px rgba(16,24,40,0.06); overflow: hidden; }}
                .section {{ padding: 20px 24px; }}
                .header {{ background: linear-gradient(90deg, #005A9C 0%, #1a86d0 100%); color: #fff; }}
                .header h1 {{ margin: 0 0 6px; font-size: 20px; }}
                .muted {{ color: #6b7280; }}
                .meta {{ display: flex; gap: 12px; flex-wrap: wrap; font-size: 13px; color: #3b82f6; }}
                .meta strong {{ color: #e0f2fe; font-weight: 600; }}
                h2 {{ font-size: 16px; margin: 0 0 10px; color: #0f172a; }}
                p {{ margin: 0 0 12px; }}
                hr {{ border: 0; height: 1px; background: #eef2f7; margin: 16px 0; }}

                /* Preformatted analysis */
                pre {{ background: #f4f6f8; color: inherit; padding: 16px; border-radius: 8px; border: 1px solid #e6ecf2;
                      overflow: auto; white-space: pre-wrap; word-wrap: break-word; margin: 0; font-size: 13px;
                      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, \"Liberation Mono\", \"Courier New\", monospace; }}

                /* Footer */
                .footer {{ font-size: 12px; color: #64748b; text-align: center; padding: 16px 24px; }}

                /* Email client safety */
                a {{ color: #0ea5e9; text-decoration: none; }}
                a:hover {{ text-decoration: underline; }}
                /* KPI grid */
                .kpis {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; }}
                @media (min-width: 560px) {{ .kpis {{ grid-template-columns: repeat(4, 1fr); }} }}
                .kpi {{ border: 1px solid #eef2f7; border-radius: 8px; padding: 12px; background: #fafcff; }}
                .kpi .label {{ font-size: 12px; color: #64748b; margin-bottom: 6px; }}
                .kpi .value {{ font-size: 18px; font-weight: 700; color: #0f172a; }}
                /* Table */
                .table-wrap {{ overflow-x: auto; border: 1px solid #eef2f7; border-radius: 8px; }}
                table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
                thead {{ background: #f1f5f9; }}
                th, td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid #eef2f7; white-space: nowrap; }}
                th {{ color: #334155; font-weight: 600; }}
                tbody tr:hover {{ background: #f8fafc; }}
            </style>
        </head>
        <body>
            <div class=\"container\">
                <div class=\"card\">
                    <div class=\"section header\">
                        <h1>Daily Marketing Data Analysis Report</h1>
                        <div class=\"meta\">
                            <div><strong>Report Date:</strong> <span>{report_date}</span></div>
                        </div>
                    </div>
                    <div class=\"section\">
                        <h2>Key Metrics</h2>
                        <div class=\"kpis\">
                            <div class=\"kpi\">
                                <div class=\"label\">Total Cost</div>
                                <div class=\"value\">{total_cost}</div>
                            </div>
                            <div class=\"kpi\">
                                <div class=\"label\">CPC</div>
                                <div class=\"value\">{cpc}</div>
                            </div>
                            <div class=\"kpi\">
                                <div class=\"label\">CPC (Click per conversion)</div>
                                <div class=\"value\">{cpc_per_conversion}</div>
                            </div>
                            <div class=\"kpi\">
                                <div class=\"label\">ROA</div>
                                <div class=\"value\">{roa}</div>
                            </div>
                        </div>
                        <hr>
                        <h2>Campaign Performance</h2>
                        <div class=\"table-wrap\">
                            <table role=\"table\" aria-label=\"Campaign performance\">
                                <thead>
                                    <tr>
                                        <th scope=\"col\">Channel</th>
                                        <th scope=\"col\">Campaign</th>
                                        <th scope=\"col\">Total Cost</th>
                                        <th scope=\"col\">CPC</th>
                                        <th scope=\"col\">CPC (Click per conversion)</th>
                                        <th scope=\"col\">ROA</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {campaign_rows}
                                </tbody>
                            </table>
                        </div>
                        <hr>
                        <h2>LLM Analysis</h2>
                        <pre>{analysis_text}</pre>
                    </div>
                    <div class=\"footer\">© {report_date} • Automated Report</div>
                </div>
            </div>
        </body>
        </html>"""

        return html_content
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        return ""

def send_email_via_gmail(to_email: str, subject: str, html_content: str, message_text: str = None):
    """
    Gmail API를 사용하여 이메일을 전송합니다.
    """
    try:
        # Gmail API 인증
        creds = authenticate_gmail()
        if not creds:
            logger.error("Gmail API 인증 실패")
            return f"Gmail API authentication failed"
        
        # Gmail API 서비스 빌드
        service = build('gmail', 'v1', credentials=creds)
        
        # 발신자 이메일 (인증된 사용자의 이메일)
        sender_email = creds.token_response.get('email') if hasattr(creds, 'token_response') else None
        if not sender_email:
            # 사용자 정보 가져오기
            user_info = service.users().getProfile(userId='me').execute()
            sender_email = user_info['emailAddress']
        
        logger.info(f"Gmail API로 이메일 전송 시도: {sender_email} -> {to_email}")
        
        # 텍스트 버전이 없으면 HTML에서 추출
        if not message_text:
            # 간단한 HTML 태그 제거
            import re
            message_text = re.sub(r'<[^>]+>', '', html_content)
            message_text = re.sub(r'\s+', ' ', message_text).strip()
        
        # 메시지 생성
        message = create_gmail_message(sender_email, to_email, subject, message_text, html_content)
        
        # 이메일 전송
        sent_message = service.users().messages().send(
            userId='me', 
            body=message
        ).execute()
        
        logger.info(f"이메일 전송 성공. 메시지 ID: {sent_message['id']}")
        return f"Email sent successfully via Gmail API. Message ID: {sent_message['id']}"
        
    except HttpError as error:
        logger.error(f"Gmail API 오류: {error}")
        error_details = f"Gmail API error: {str(error)}"
        
        # 구체적인 오류 메시지 추가
        if "access_denied" in str(error):
            error_details += "\n\n해결 방법:\n1. Google Cloud Console에서 OAuth 동의 화면을 설정하세요\n2. 앱을 '테스트' 상태에서 '프로덕션' 상태로 변경하세요\n3. 또는 테스트 사용자로 자신의 이메일을 추가하세요"
        elif "insufficient_authentication_scopes" in str(error):
            error_details += "\n\n해결 방법:\n1. token.json 파일을 삭제하고 다시 인증하세요\n2. 더 넓은 스코프 권한이 필요할 수 있습니다"
        elif "quotaExceeded" in str(error):
            error_details += "\n\n해결 방법:\n1. Gmail API 할당량을 확인하세요\n2. 잠시 후 다시 시도하세요"
            
        return error_details
    except Exception as e:
        logger.error(f"이메일 전송 실패: {str(e)}")
        return f"Email sending failed: {str(e)}"

def marketing_report_pipeline(report_date: str = None) -> Dict[str, Any]:
    """
    마케팅 성과 보고서 파이프라인 실행
    """
    pipeline_start_time = datetime.now()
    
    try:
        # 날짜 설정 (기본값: 어제)
        if not report_date:
            report_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info(f"Starting marketing report pipeline for {report_date}")
        
        # 파이프라인 단계별 결과 저장
        pipeline_results = {
            "data_extraction": {},
            "data_loading": [],
            "analysis": {},
            "report_generation": "",
            "email_sending": ""
        }
        
        # 1. 데이터 추출 단계
        logger.info("Step 1: Extracting Google Ads data...")
        try:
            google_data = extract_google_ads_data(report_date)
            pipeline_results["data_extraction"] = {
                "status": "success",
                "source": "Google Ads",
                "rows_extracted": google_data.get("row_count", 0),
                "report_date": google_data.get("report_date", report_date)
            }
            logger.info(f"✓ Google Ads data extraction completed: {google_data.get('row_count', 0)} rows")
        except Exception as e:
            logger.error(f"✗ Google Ads data extraction failed: {str(e)}")
            pipeline_results["data_extraction"] = {
                "status": "error",
                "source": "Google Ads",
                "error": str(e)
            }
            # 데이터 추출 실패 시에도 분석은 계속 진행 (기존 데이터 사용)
        
        # 2. 데이터 적재 단계
        logger.info("Step 2: Loading data to warehouse...")
        load_results = []
        if pipeline_results["data_extraction"].get("status") == "success":
            try:
                load_result = load_data_to_warehouse(google_data, "Google Ads", report_date)
                load_results.append(load_result)
                pipeline_results["data_loading"] = load_results
                logger.info(f"✓ Data loading completed: {load_result.get('rows_uploaded', 0)} rows uploaded")
            except Exception as e:
                logger.error(f"✗ Data loading failed: {str(e)}")
                pipeline_results["data_loading"] = [{
                    "status": "error",
                    "source": "Google Ads",
                    "error": str(e)
                }]
        else:
            logger.warning("Skipping data loading due to extraction failure")
            pipeline_results["data_loading"] = [{"status": "skipped", "reason": "extraction_failed"}]
        
        # 3. 분석 실행 단계
        logger.info("Step 3: Running analysis and anomaly detection...")
        try:
            analysis_results = run_analysis_and_anomaly_detection(report_date)
            pipeline_results["analysis"] = analysis_results
            logger.info(f"✓ Analysis completed: {analysis_results.get('data_rows_analyzed', 0)} rows analyzed")
        except Exception as e:
            logger.error(f"✗ Analysis failed: {str(e)}")
            pipeline_results["analysis"] = {
                "status": "error",
                "error": str(e),
                "analysis_results": f"Analysis failed: {str(e)}"
            }
        
        # 4. 보고서 생성 단계
        logger.info("Step 4: Generating report content...")
        try:
            report_content = generate_report_content(pipeline_results["analysis"])
            pipeline_results["report_generation"] = report_content
            logger.info(f"✓ Report generation completed: {len(report_content)} characters")
        except Exception as e:
            logger.error(f"✗ Report generation failed: {str(e)}")
            pipeline_results["report_generation"] = f"Report generation failed: {str(e)}"
        
        # 5. 이메일 전송 단계
        logger.info("Step 5: Sending email report via Gmail API...")
        recipient_email = os.getenv('REPORT_RECIPIENT_EMAIL')
        if recipient_email and pipeline_results["report_generation"]:
            try:
                # HTML 콘텐츠에서 텍스트 버전 생성
                import re
                text_content = re.sub(r'<[^>]+>', '', pipeline_results["report_generation"])
                text_content = re.sub(r'\s+', ' ', text_content).strip()
                
                email_result = send_email_via_gmail(
                    to_email=recipient_email,
                    subject=f"Daily Marketing Report - {report_date}",
                    html_content=pipeline_results["report_generation"],
                    message_text=text_content
                )
                pipeline_results["email_sending"] = email_result
                logger.info(f"✓ Email sending completed: {email_result}")
            except Exception as e:
                logger.error(f"✗ Email sending failed: {str(e)}")
                pipeline_results["email_sending"] = f"Email sending failed: {str(e)}"
        else:
            if not recipient_email:
                logger.warning("REPORT_RECIPIENT_EMAIL not set, skipping email sending")
                pipeline_results["email_sending"] = "Email sending skipped - recipient email not configured"
            else:
                logger.warning("Report content empty, skipping email sending")
                pipeline_results["email_sending"] = "Email sending skipped - no report content"
        
        # 파이프라인 완료 시간 계산
        pipeline_end_time = datetime.now()
        pipeline_duration = (pipeline_end_time - pipeline_start_time).total_seconds()
        
        logger.info(f"Marketing report pipeline completed in {pipeline_duration:.2f} seconds")
        
        return {
            "status": "success",
            "report_date": report_date,
            "pipeline_completed": True,
            "pipeline_duration_seconds": pipeline_duration,
            "pipeline_start_time": pipeline_start_time.isoformat(),
            "pipeline_end_time": pipeline_end_time.isoformat(),
            "results": pipeline_results
        }
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc(),
            "pipeline_duration_seconds": (datetime.now() - pipeline_start_time).total_seconds()
        }

@functions_framework.http
def main(request):
    """
    Google Cloud Functions HTTP 트리거 엔드포인트
    Cloud Scheduler에서 이 함수를 호출
    """
    try:
        # CORS 헤더 설정 (필요한 경우)
        if request.method == 'OPTIONS':
            headers = {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Max-Age': '3600'
            }
            return ('', 204, headers)
        
        # 요청 데이터 파싱
        if request.method == 'POST':
            try:
                request_json = request.get_json(silent=True)
                report_date = request_json.get('report_date') if request_json else None
            except Exception:
                report_date = None
        else:
            # GET 요청인 경우 쿼리 파라미터에서 날짜 추출
            report_date = request.args.get('report_date')
        
        logger.info(f"Received request: method={request.method}, report_date={report_date}")
        
        # 파이프라인 실행
        result = marketing_report_pipeline(report_date)
        
        # 응답 헤더 설정
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
        }
        
        if result["status"] == "success":
            return (json.dumps(result, ensure_ascii=False), 200, headers)
        else:
            return (json.dumps(result, ensure_ascii=False), 500, headers)
            
    except Exception as e:
        logger.error(f"Function error: {str(e)}")
        logger.error(traceback.format_exc())
        
        error_response = {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }
        
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
        }
        
        return (json.dumps(error_response, ensure_ascii=False), 500, headers)

# 로컬 테스트용 (Cloud Functions에서는 사용되지 않음)
if __name__ == "__main__":
    # 로컬에서 테스트할 때 사용
    result = marketing_report_pipeline()
    print(json.dumps(result, ensure_ascii=False, indent=2))
