# Gmail API Access Blocked 해결 방법

## 현재 오류
"Access blocked: marketing automation has not completed the Google verification process"

## 해결 방법

### 방법 1: 테스트 사용자 추가 (권장)
1. Google Cloud Console 접속: https://console.cloud.google.com/
2. 프로젝트 선택
3. "API 및 서비스" > "OAuth 동의 화면" 클릭
4. "테스트 사용자" 섹션에서 "+ 추가" 클릭
5. `analyticsbyui@gmail.com` 입력
6. "저장" 클릭

### 방법 2: 앱 이름 변경
1. OAuth 동의 화면에서 "앱 정보" 편집
2. 앱 이름을 "Gmail API Test" 또는 "Email Sender"로 변경
3. 저장

### 방법 3: 프로덕션으로 게시
1. OAuth 동의 화면에서 "게시 상태" 섹션
2. "앱을 프로덕션으로 만들기" 클릭
3. 확인

## 확인 사항
- 앱이 "테스트" 상태인지 확인
- 테스트 사용자 목록에 자신의 이메일이 있는지 확인
- 앱 이름이 적절한지 확인

## 다음 단계
위 방법 중 하나를 완료한 후 다시 Gmail API 테스트를 실행하세요.
