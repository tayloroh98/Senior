# Gmail API 설정 가이드

Gmail API를 사용하기 위한 설정 단계입니다.

## 1. Google Cloud Console 설정

### 1.1 프로젝트 생성/선택
1. [Google Cloud Console](https://console.cloud.google.com/)에 접속
2. 새 프로젝트 생성 또는 기존 프로젝트 선택

### 1.2 Gmail API 활성화
1. "API 및 서비스" > "라이브러리"로 이동
2. "Gmail API" 검색 후 활성화

### 1.3 OAuth 2.0 클라이언트 ID 생성
1. "API 및 서비스" > "사용자 인증 정보"로 이동
2. "사용자 인증 정보 만들기" > "OAuth 클라이언트 ID" 선택
3. 애플리케이션 유형: "데스크톱 애플리케이션"
4. 이름: "Marketing Pipeline Gmail"
5. "만들기" 클릭

### 1.4 인증 정보 다운로드
1. 생성된 OAuth 클라이언트 ID의 다운로드 버튼 클릭
2. JSON 파일을 다운로드하여 `credentials.json`으로 저장
3. 프로젝트 루트 디렉토리에 `credentials.json` 파일 배치

## 2. 환경 변수 설정

`.env` 파일에 다음 추가:
```env
# Gmail API 설정
GMAIL_SENDER_EMAIL=your_gmail@gmail.com
```

## 3. 테스트 실행

### 3.1 첫 실행
```bash
python gmail_test.py
```

### 3.2 인증 과정
1. 브라우저가 자동으로 열림
2. Google 계정으로 로그인
3. 권한 승인
4. `token.json` 파일이 자동 생성됨

### 3.3 성공 확인
- 콘솔에 "이메일 전송 성공" 메시지 출력
- 지정된 이메일 주소로 테스트 이메일 수신

## 4. 문제 해결

### 4.1 일반적인 오류
- **403 Forbidden**: Gmail API가 활성화되지 않음
- **401 Unauthorized**: 인증 정보가 잘못됨
- **credentials.json 없음**: OAuth 클라이언트 ID 다운로드 필요

### 4.2 토큰 갱신
- `token.json` 파일 삭제 후 재실행
- 새로운 인증 과정 진행

## 5. 보안 고려사항

- `credentials.json`과 `token.json`을 `.gitignore`에 추가
- 프로덕션 환경에서는 서비스 계정 사용 권장
- 이메일 전송 권한만 필요한 최소 권한 사용

## 6. 다음 단계

테스트 성공 후 `main.py`에 Gmail API 통합:
1. `send_email_via_gmail()` 함수를 `main.py`에 추가
2. Mailgun 대신 Gmail API 사용하도록 수정
3. 환경 변수 설정 업데이트
