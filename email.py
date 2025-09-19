import boto3
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

def send_report_email(sender, recipient, subject, body_text, file_path, aws_region="us-east-1"):
    """
    Amazon SES를 사용하여 첨부 파일이 있는 이메일을 보냅니다.
    """
    client = boto3.client('ses', region_name=aws_region)
    
    msg = MIMEMultipart()
    msg = subject
    msg['From'] = sender
    msg = recipient
    
    # 이메일 본문
    msg.attach(MIMEText(body_text))
    
    # 파일 첨부
    with open(file_path, 'rb') as f:
        part = MIMEApplication(f.read(), Name=os.path.basename(file_path))
    part = f'attachment; filename="{os.path.basename(file_path)}"'
    msg.attach(part)
    
    try:
        response = client.send_raw_email(
            Source=sender,
            Destinations=[recipient],
            RawMessage={'Data': msg.as_string()}
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
        return False
    else:
        print(f"Email sent! Message ID: {response['MessageId']}")
        return True

# 사용 예시
# send_report_email(
#     sender="report-automation@example.com",
#     recipient="manager@example.com",
#     subject="Daily Marketing Performance Report - 2025-09-18",
#     body_text="Please find the attached daily performance report.",
#     file_path="/path/to/report_2025-09-18.pdf"
# )
