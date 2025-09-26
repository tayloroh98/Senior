"""
Gmail API를 사용한 이메일 전송 테스트 파일
Mailgun 대신 Gmail API를 사용하여 이메일을 전송하는 기능을 테스트합니다.
"""

import os
import base64
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Gmail API 스코프 - openid 포함
SCOPES = [
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
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    # 유효한 인증 정보가 없으면 새로 인증
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # credentials.json 파일이 필요합니다 (Google Cloud Console에서 다운로드)
            if not os.path.exists('credentials.json'):
                logger.error("credentials.json 파일이 없습니다. Google Cloud Console에서 OAuth 2.0 클라이언트 ID를 다운로드하세요.")
                return None
            
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=8080)
        
        # 인증 정보를 token.json에 저장
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    
    return creds

def create_message(sender, to, subject, message_text, html_content=None):
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

def send_email_via_gmail(to_email, subject, message_text, html_content=None):
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
        
        # 메시지 생성
        message = create_message(sender_email, to_email, subject, message_text, html_content)
        
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

def test_gmail_send():
    """
    Gmail API 이메일 전송 테스트
    """
    # 테스트용 이메일 내용
    test_subject = "Gmail API 테스트 - 마케팅 보고서"
    test_message = """
안녕하세요!

이것은 Gmail API를 사용한 이메일 전송 테스트입니다.

테스트 내용:
- Gmail API 인증
- 이메일 전송 기능
- HTML 및 텍스트 형식 지원

테스트가 성공적으로 완료되었습니다.

감사합니다.
"""
    
    # HTML 버전
    test_html = """
    <html>
    <head>
        <style>
            body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
            .container { max-width: 600px; margin: 0 auto; padding: 20px; }
            .header { background-color: #4285f4; color: white; padding: 20px; text-align: center; }
            .content { padding: 20px; background-color: #f9f9f9; }
            .footer { padding: 20px; text-align: center; font-size: 12px; color: #666; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Gmail API 테스트</h1>
            </div>
            <div class="content">
                <h2>마케팅 보고서 테스트</h2>
                <p>안녕하세요!</p>
                <p>이것은 Gmail API를 사용한 이메일 전송 테스트입니다.</p>
                
                <h3>테스트 내용:</h3>
                <ul>
                    <li>Gmail API 인증</li>
                    <li>이메일 전송 기능</li>
                    <li>HTML 및 텍스트 형식 지원</li>
                </ul>
                
                <p><strong>테스트가 성공적으로 완료되었습니다.</strong></p>
            </div>
            <div class="footer">
                <p>이 이메일은 Gmail API를 통해 자동으로 전송되었습니다.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # 수신자 이메일 (환경 변수에서 가져오거나 기본값 사용)
    recipient_email = os.getenv('REPORT_RECIPIENT_EMAIL', 'analyticsbyui@gmail.com')
    
    logger.info(f"Gmail API 테스트 시작 - 수신자: {recipient_email}")
    
    # 이메일 전송
    result = send_email_via_gmail(
        to_email=recipient_email,
        subject=test_subject,
        message_text=test_message,
        html_content=test_html
    )
    
    logger.info(f"테스트 결과: {result}")
    return result

if __name__ == "__main__":
    print("Gmail API 이메일 전송 테스트를 시작합니다...")
    print("=" * 50)
    
    result = test_gmail_send()
    print(f"\n테스트 결과: {result}")
    print("=" * 50)
