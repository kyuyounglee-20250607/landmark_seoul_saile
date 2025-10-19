# 서울시 임대차 정보 대시보드

서울시 임대차 정보를 조회하고 분석할 수 있는 Streamlit 웹 애플리케이션입니다.

## 🌟 주요 기능

- 구별 임대차 정보 조회
- 데이터 시각화 및 분석
- 지도 기반 위치 정보 표시
- 상세 데이터 필터링

## 🚀 Streamlit Cloud 배포 가이드

### 1. 사전 준비사항

#### 1.1 필요한 파일들
- `app.py`: 메인 애플리케이션 코드
- `requirements.txt`: 필요한 패키지 목록
- `code.csv`: 법정동 코드 데이터
- `.gitignore`: 민감한 파일 제외 설정
- `.streamlit/secrets.toml`: API 키 설정 (로컬에서는 .env 파일 사용)

#### 1.2 .gitignore 설정
```
.env
.streamlit/secrets.toml
__pycache__/
*.pyc
```

### 2. Streamlit Cloud 설정

#### 2.1 API 키 설정
1. Streamlit Cloud 대시보드 접속
2. 앱 설정에서 "Secrets" 섹션 이동
3. 다음 형식으로 API 키 추가:
```toml
SEOUL_LANDMARK_API = "your_seoul_api_key"
REST_API = "your_kakao_api_key"
KAKAO_JAVA_SCRIPT_KEY = "your_kakao_js_key"
```

#### 2.2 배포 설정
1. GitHub 저장소 연결
2. Main 브랜치 선택
3. Python 버전: 3.9
4. 앱 경로: `seoul_rent_data/app.py`

### 3. 환경 변수 전환 가이드

현재 로컬에서 `.env` 파일을 사용중이므로, Streamlit Cloud에서는 다음과 같이 환경 변수를 로드하도록 코드를 수정해야 합니다:

```python
# API 키 로드 함수
def load_api_keys():
    # 로컬 환경 (.env 파일)
    if os.path.exists(".env"):
        load_dotenv()
        return {
            "SEOUL_LANDMARK_API": os.getenv("SEOUL_LANDMARK_API"),
            "REST_API": os.getenv("REST_API"),
            "KAKAO_JAVA_SCRIPT_KEY": os.getenv("KAKAO_JAVA_SCRIPT_KEY")
        }
    # Streamlit Cloud 환경
    else:
        return {
            "SEOUL_LANDMARK_API": st.secrets["SEOUL_LANDMARK_API"],
            "REST_API": st.secrets["REST_API"],
            "KAKAO_JAVA_SCRIPT_KEY": st.secrets["KAKAO_JAVA_SCRIPT_KEY"]
        }
```

### 4. 자동 배포 설정

현재 GitHub 저장소에 코드를 push하면 자동으로 배포되도록 설정되어 있습니다. 배포 시 다음 사항을 확인하세요:

1. 코드 변경 시 테스트
2. requirements.txt 업데이트
3. .gitignore 파일 확인
4. API 키 노출 여부 확인

### 5. 문제 해결

#### 5.1 일반적인 문제
- API 키 오류: Streamlit Cloud의 Secrets 설정 확인
- 패키지 오류: requirements.txt 내용 확인
- 메모리 초과: 데이터 처리 방식 최적화 필요

#### 5.2 로그 확인
1. Streamlit Cloud 대시보드 접속
2. 해당 앱의 "Manage app" 선택
3. "View logs" 에서 오류 확인

## 📝 참고사항

1. API 키 관리
   - 절대로 API 키를 GitHub에 커밋하지 마세요
   - 항상 환경 변수나 Streamlit Secrets로 관리하세요

2. 성능 최적화
   - 캐시 기능 활용
   - 데이터 분할 처리
   - 메모리 사용량 모니터링

3. 배포 전 체크리스트
   - [ ] 모든 API 키가 안전하게 관리되는지 확인
   - [ ] requirements.txt 최신화
   - [ ] 테스트 완료
   - [ ] .gitignore 설정 확인