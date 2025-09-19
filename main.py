from prefect import flow, task
from datetime import timedelta
import pendulum

@task(retries=3, retry_delay_seconds=60)
def extract_google_ads_data(report_date: str):
    # Google Ads API에서 데이터 추출 로직
    print(f"Extracting Google Ads data for {report_date}...")
    # return data

@task(retries=3, retry_delay_seconds=60)
def extract_meta_ads_data(report_date: str):
    # Meta Marketing API에서 데이터 추출 로직
    print(f"Extracting Meta Ads data for {report_date}...")
    # return data

#... Spotify, Snapchat, LinkedIn 데이터 추출 태스크 정의

@task
def load_data_to_warehouse(data, source_name: str):
    # 추출된 데이터를 데이터 웨어하우스에 적재하는 로직
    print(f"Loading {source_name} data to warehouse...")
    # return load_status

@task
def run_analysis_and_anomaly_detection(report_date: str):
    # 웨어하우스 데이터를 기반으로 시계열 분석 및 이상치 탐지 실행
    print(f"Running analysis for {report_date}...")
    # return analysis_results

@task
def generate_report_content(analysis_results):
    # 분석 결과를 바탕으로 보고서 내용(HTML/PDF) 생성
    print("Generating report content...")
    # return report_file

@task
def send_email_report(report_file):
    # 생성된 보고서 파일을 이메일에 첨부하여 발송
    print("Sending email report...")
    # return send_status

@flow(name="Daily Marketing Performance Report")
def marketing_report_pipeline(date: str = "today"):
    if date == "today":
        report_date = pendulum.today("UTC").subtract(days=1).to_date_string()
    else:
        report_date = date

    # 1. 데이터 병렬 추출
    google_data = extract_google_ads_data.submit(report_date)
    meta_data = extract_meta_ads_data.submit(report_date)
    #... 다른 플랫폼 데이터 추출

    # 2. 데이터 병렬 적재 (각 추출 작업이 완료된 후 실행)
    load_google = load_data_to_warehouse.submit(google_data, "Google Ads", wait_for=[google_data])
    load_meta = load_data_to_warehouse.submit(meta_data, "Meta Ads", wait_for=[meta_data])
    #... 다른 플랫폼 데이터 적재

    # 3. 모든 데이터 적재가 완료된 후 분석 실행
    all_loads = [load_google, load_meta,...]
    analysis_results = run_analysis_and_anomaly_detection.submit(report_date, wait_for=all_loads)

    # 4. 보고서 생성 및 발송
    report_file = generate_report_content.submit(analysis_results)
    send_email_report.submit(report_file)

if __name__ == "__main__":
    # 매일 아침 9시(KST)에 실행되도록 스케줄링 (Prefect 배포 시 설정)
    # 예: cron="0 9 * * *" tz="Asia/Seoul"
    marketing_report_pipeline()
