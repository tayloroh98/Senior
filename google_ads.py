import csv
import sys
from datetime import datetime, timedelta

import yaml
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


def main(client, customer_id):
    """
    지정된 고객 ID의 캠페인 데이터를 가져와 CSV 파일로 저장합니다.
    """
    ga_service = client.get_service("GoogleAdsService")

    # 날짜 범위 계산 (지난 7일)
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    # Google Ads 쿼리 언어 (GAQL)
    # 요청한 모든 지표(spend, cpc, conversions 등)를 포함하도록 쿼리 수정
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
            segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND campaign.status = 'ENABLED'
        ORDER BY
            metrics.impressions DESC
        LIMIT 10
    """

    try:
        # CSV 파일 준비
        csv_filename = "google_ads_report.csv"
        with open(csv_filename, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)

            # CSV 헤더(열 이름) 작성
            header = [
                "channel", "campaign name", "Impressions", "Clicks", 
                "spend", "cpc", "conversions", "cost per conversion"
            ]
            writer.writerow(header)

            print(f"--- Customer ID: {customer_id}의 캠페인 데이터 (지난 7일) ---")
            print(f"'{csv_filename}' 파일에 저장 중...")

            # 검색 스트림을 사용하여 대용량 데이터 처리
            stream = ga_service.search_stream(customer_id=customer_id, query=query)

            # 결과 반복 처리
            for batch in stream:
                for row in batch.results:
                    campaign = row.campaign
                    metrics = row.metrics

                    # API에서 받은 비용(cost)과 CPC는 '마이크로' 단위(1/1,000,000)이므로
                    # 실제 통화 단위로 변환하기 위해 1,000,000으로 나눕니다.
                    spend = metrics.cost_micros / 1_000_000
                    cpc = metrics.average_cpc / 1_000_000
                    cost_per_conversion = metrics.cost_per_conversion / 1_000_000

                    # CSV 파일에 쓸 데이터 행 생성
                    data_row = [
                        "google ads",
                        campaign.name,
                        metrics.impressions,
                        metrics.clicks,
                        f"{spend:.2f}",  # 소수점 2자리까지 표시
                        f"{cpc:.2f}",
                        metrics.conversions,
                        f"{cost_per_conversion:.2f}"
                    ]
                    
                    # CSV 파일에 한 줄 쓰기
                    writer.writerow(data_row)
            
            print(f"\n성공적으로 '{csv_filename}' 파일을 저장했습니다.")

    except GoogleAdsException as ex:
        print(
            f'요청 ID "{ex.request_id}"의 요청이 실패했습니다. '
            f'오류 코드: "{ex.error.code().name}".\n'
            f'상세 메시지: "{str(ex)}"'
        )
        sys.exit(1)
    except Exception as e:
        print(f"알 수 없는 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        # google-ads.yaml 파일에서 인증 정보를 로드합니다.
        googleads_client = GoogleAdsClient.load_from_storage(path="./google-ads.yaml")

        with open("google-ads.yaml", "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        CUSTOMER_ID = config["customer_id"]
        main(googleads_client, CUSTOMER_ID)

    except FileNotFoundError:
        print("="*50)
        print("오류: 'google-ads.yaml' 파일을 찾을 수 없습니다.")
        print("스크립트 실행 위치나 홈 디렉터리에 해당 파일이 있는지,")
        print("또는 'load_from_storage(path=...)'에 정확한 경로를 지정했는지 확인하세요.")
        print("="*50)
        sys.exit(1)