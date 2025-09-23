import sys
from datetime import datetime, timedelta
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import yaml
# 데이터를 조회할 대상 고객 ID (하이픈 '-' 제외)
# 이 ID가 google-ads.yaml의 login_customer_id와 다를 수 있습니다. (예: MCC로 하위 계정 조회)
CUSTOMER_ID = "customer_id"

def main(client, customer_id):
    """
    지정된 고객 ID의 캠페인 데이터를 가져옵니다.
    """
    ga_service = client.get_service("GoogleAdsService")

    # 날짜 범위 계산 (지난 7일)
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    # Google Ads 쿼리 언어 (GAQL)
    # 지난 7일간의 캠페인별 이름, ID, 노출수, 클릭수 조회
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            metrics.impressions,
            metrics.clicks
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
        # 검색 스트림을 사용하여 대용량 데이터 처리
        stream = ga_service.search_stream(customer_id=customer_id, query=query)

        print(f"--- Customer ID: {customer_id}의 캠페인 데이터 (지난 7일) ---")
        
        # 결과 반복 처리
        for batch in stream:
            for row in batch.results:
                campaign = row.campaign
                metrics = row.metrics
                print(
                    f"캠페인 ID: {campaign.id}, "
                    f"이름: '{campaign.name}', "
                    f"노출수: {metrics.impressions}, "
                    f"클릭수: {metrics.clicks}"
                )

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
        # 현재 스크립트와 같은 디렉터리의 google-ads.yaml 파일을 사용합니다.
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