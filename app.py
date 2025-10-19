import streamlit as st
import pandas as pd
import requests
import json
from dotenv import load_dotenv
import os
import asyncio
import aiohttp
import time
from datetime import datetime
import folium
from streamlit_folium import folium_static
from folium import plugins

# API 키 로드 함수
def load_api_keys():
    """환경에 따라 적절한 방식으로 API 키 로드"""
    if os.path.exists(".env"):
        load_dotenv()
        return {
            "SEOUL_LANDMARK_API": os.getenv("SEOUL_LANDMARK_API"),
            "REST_API": os.getenv("REST_API"),
            "KAKAO_JAVA_SCRIPT_KEY": os.getenv("KAKAO_JAVA_SCRIPT_KEY")
        }
    else:
        return {
            "SEOUL_LANDMARK_API": st.secrets["SEOUL_LANDMARK_API"],
            "REST_API": st.secrets["REST_API"],
            "KAKAO_JAVA_SCRIPT_KEY": st.secrets["KAKAO_JAVA_SCRIPT_KEY"]
        }

# API 키 로드
API_KEYS = load_api_keys()
SEOUL_API_KEY = API_KEYS["SEOUL_LANDMARK_API"]
KAKAO_API_KEY = API_KEYS["REST_API"]
KAKAO_JAVA_SCRIPT_KEY = API_KEYS["KAKAO_JAVA_SCRIPT_KEY"]

# 페이지 설정
st.set_page_config(
    page_title="서울시 임대차 정보",
    page_icon="🏢",
    layout="wide"
)

# 주소로 위경도 조회 함수
def get_coordinates(address):
    url = 'https://dapi.kakao.com/v2/local/search/address.json'
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    params = {'query': address}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            result = response.json()
            if result.get('documents'):
                doc = result['documents'][0]
                return float(doc['x']), float(doc['y'])
    except Exception as e:
        pass
    
    return None, None

# URL 파라미터 생성 함수
def build_api_params(gu_code=None, dong_code=None):
    """API URL 파라미터 문자열 생성
    서울시 OpenAPI는 선택적 파라미터를 순서대로 전달
    형식: /START/END 또는 /START/END/접수연도/자치구코드/자치구명/법정동코드/...
    """
    if not gu_code and not dong_code:
        return ""
    
    # 접수연도는 빈 값으로, 자치구코드만 또는 자치구코드+법정동코드 전달
    params = []
    
    # 접수연도 (선택사항이지만 순서상 필요할 수 있음)
    # params.append("")  # 비워두기
    
    if gu_code:
        params.append(str(gu_code))
    
    if dong_code:
        params.append(str(dong_code))
    
    return "/" + "/".join(params) if params else ""

# 전체 데이터 개수 조회 (필터 적용)
async def get_total_count(gu_code=None, dong_code=None):
    """구/동 필터를 적용한 전체 데이터 개수 조회 (1/1로 요청)"""
    # 필터링은 URL이 아닌 데이터 수신 후 처리
    # 서울시 API는 URL 파라미터로 필터링을 지원하지 않을 수 있음
    
    url = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/json/tbLnOpendataRentV/1/1000"
    
    print(f"[DEBUG] Total Count URL: {url}")  # 디버깅용
    
    timeout = aiohttp.ClientTimeout(total=30)
    
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if 'tbLnOpendataRentV' in data:
                        result = data['tbLnOpendataRentV']
                        
                        if 'list_total_count' in result:
                            total_count = int(result['list_total_count'])
                            
                            # 샘플 데이터로 필드명 확인
                            if 'row' in result and len(result['row']) > 0:
                                sample = result['row'][0]
                                print(f"[DEBUG] Sample data keys: {sample.keys()}")
                            
                            return total_count, None
                        
                        if 'RESULT' in result:
                            code = result['RESULT'].get('CODE')
                            msg = result['RESULT'].get('MESSAGE', '알 수 없는 오류')
                            return None, f"API 오류: {code} - {msg}"
                    
                    return None, f"잘못된 응답 형식: {data}"
                else:
                    return None, f"HTTP 오류: {response.status}"
                    
    except asyncio.TimeoutError:
        return None, "요청 시간 초과"
    except Exception as e:
        return None, f"오류 발생: {str(e)}"

# 비동기 데이터 조회 함수
async def fetch_data_async(session, start_idx, end_idx, gu_code=None, dong_code=None, max_retries=3):
    """비동기로 단일 범위 데이터 조회 (필터링 없이 전체 조회)"""
    # 서울시 API는 URL 파라미터 필터링을 지원하지 않으므로 전체 조회 후 필터링
    url = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/json/tbLnOpendataRentV/{start_idx}/{end_idx}"
    
    for retry in range(max_retries):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if 'tbLnOpendataRentV' in data:
                        result = data['tbLnOpendataRentV']
                        
                        if 'row' in result:
                            rows = result['row']
                            
                            # 클라이언트 사이드 필터링
                            if gu_code or dong_code:
                                filtered_rows = []
                                for row in rows:
                                    # CGG_CD 필드로 자치구 필터링
                                    if gu_code and row.get('CGG_CD') != str(gu_code):
                                        continue
                                    # STDG_CD 필드로 법정동 필터링
                                    if dong_code and row.get('STDG_CD') != str(dong_code):
                                        continue
                                    filtered_rows.append(row)
                                return filtered_rows, None
                            
                            return rows, None
                        
                        if 'RESULT' in result:
                            code = result['RESULT'].get('CODE')
                            if code == 'INFO-200':  # 데이터 없음
                                return [], None
                            msg = result['RESULT'].get('MESSAGE', '알 수 없는 오류')
                            
                            # 재시도 가능한 오류인 경우
                            if retry < max_retries - 1:
                                await asyncio.sleep(2 ** retry)
                                continue
                            
                            return None, f"API 오류: {code} - {msg}"
                    
                    return None, "잘못된 응답 형식"
                else:
                    if retry < max_retries - 1:
                        await asyncio.sleep(2 ** retry)
                        continue
                    return None, f"HTTP 오류: {response.status}"
                    
        except asyncio.TimeoutError:
            if retry < max_retries - 1:
                await asyncio.sleep(2 ** retry)
                continue
            return None, f"요청 시간 초과 (범위: {start_idx}-{end_idx})"
            
        except Exception as e:
            if retry < max_retries - 1:
                await asyncio.sleep(2 ** retry)
                continue
            return None, f"오류 발생: {str(e)}"
    
    return None, "최대 재시도 횟수 초과"

# 순차적 비동기 데이터 수집
async def collect_data_sequential(total_count, gu_code=None, dong_code=None, progress_callback=None):
    """비동기로 순차적으로 데이터 수집 (100건씩)"""
    all_data = []
    batch_size = 100
    current_idx = 1
    
    # 타임아웃 설정 (30초로 증가)
    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while current_idx <= total_count:
            end_idx = min(current_idx + batch_size - 1, total_count)
            
            if progress_callback:
                progress_callback(
                    current_idx, 
                    end_idx, 
                    total_count, 
                    len(all_data)
                )
            
            # 비동기로 데이터 조회 (파라미터 전달)
            data, error = await fetch_data_async(session, current_idx, end_idx, gu_code, dong_code)
            
            if error:
                # 에러 발생 시 로깅하고 계속 진행
                if progress_callback:
                    progress_callback(
                        current_idx, 
                        end_idx, 
                        total_count, 
                        len(all_data),
                        error=f"⚠️ 범위 {current_idx}-{end_idx} 조회 실패: {error}"
                    )
                # 다음 배치로 이동
                current_idx = end_idx + 1
                await asyncio.sleep(1)  # 에러 후 대기
                continue
            
            if data:
                all_data.extend(data)
            
            # 다음 범위로 이동
            current_idx = end_idx + 1
            
            # API 부하 방지를 위한 대기 (성공 시에만)
            await asyncio.sleep(0.2)
    
    return all_data

# 동기 래퍼 함수
def get_all_rent_data(gu_code=None, dong_code=None, progress_callback=None):
    """전체 데이터 수집 (동기 래퍼)"""
    
    # 1단계: 전체 개수 조회
    if progress_callback:
        progress_callback(0, 0, 0, 0, status="전체 데이터 개수 조회 중...")
    
    total_count, error = asyncio.run(get_total_count(gu_code, dong_code))
    
    if error:
        return None, error
    
    if not total_count or total_count == 0:
        return None, "조회된 데이터가 없습니다."
    
    if progress_callback:
        progress_callback(0, 0, total_count, 0, status=f"총 {total_count:,}건의 데이터 수집 시작...")
    
    # 2단계: 비동기로 순차적 데이터 수집 (파라미터 전달)
    try:
        all_data = asyncio.run(collect_data_sequential(total_count, gu_code, dong_code, progress_callback))
        
        if all_data:
            return pd.DataFrame(all_data), None
        return None, "데이터 수집 실패"
        
    except Exception as e:
        return None, f"데이터 수집 중 오류: {str(e)}"

def preprocess_data(df):
    """데이터 전처리 함수"""
    if df is None or df.empty:
        return None
    
    try:
        # 숫자형 컬럼 변환
        numeric_columns = ['GRFE', 'RTFE', 'MNO', 'SNO', 'FLR', 'RENT_AREA']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 컬럼명 한글 변환
        column_mapping = {
            'STDG_NM': '법정동명',
            'LOTNO_SE_NM': '지번구분명',
            'MNO': '본번',
            'SNO': '부번',
            'FLR': '층',
            'CTRT_DAY': '계약일',
            'RENT_SE': '전월세구분',
            'RENT_AREA': '임대면적(㎡)',
            'GRFE': '보증금(만원)',
            'RTFE': '임대료(만원)',
            'BLDG_NM': '건물명',
            'ARCH_YR': '건축년도',
            'BLDG_USG': '건물용도',
            'CTRT_PRD': '계약기간',
            'NEW_UPDT_YN': '신규갱신여부',
            'CTRT_UPDT_USE_YN': '계약갱신권사용여부',
            'BFR_GRFE': '종전보증금',
            'BFR_RTFE': '종전임대료'
        }
        df = df.rename(columns=column_mapping)
        
        return df
    except Exception as e:
        st.error(f"데이터 전처리 중 오류 발생: {str(e)}")
        return None

def create_address(row, gu_name):
    """주소 생성 함수"""
    address = f"서울특별시 {gu_name} {row['법정동명']}"
    if row['지번구분명'] == '산':
        address += f" {row['지번구분명']}"
    try:
        address += f" {int(row['본번'])}"
    except:
        pass
    try:
        if row['부번'] != 0:
            address += f"-{int(row['부번'])}"
    except:
        pass
    return address

def create_folium_map(data_df, center_lat, center_lng):
    """Folium 지도 생성 함수"""
    m = folium.Map(
        location=[center_lat, center_lng],
        zoom_start=14,
        tiles='OpenStreetMap'
    )
    
    marker_cluster = plugins.MarkerCluster().add_to(m)
    
    for _, row in data_df.iterrows():
        if pd.notna(row['위도']) and pd.notna(row['경도']):
            popup_content = f"""
                <div style='width:200px'>
                <b>{row['건물명'] if pd.notna(row['건물명']) else row['주소']}</b><br>
                전월세구분: {row['전월세구분']}<br>
                보증금: {int(row['보증금(만원)']):,}만원<br>
                임대료: {int(row['임대료(만원)']):,}만원<br>
                면적: {row['임대면적(㎡)']}㎡<br>
                계약일: {row['계약일']}
                </div>
            """
            
            color = 'red' if row['전월세구분'] == '전세' else 'blue'
            
            folium.Marker(
                location=[row['위도'], row['경도']],
                popup=folium.Popup(popup_content, max_width=300),
                icon=folium.Icon(color=color, icon='info-sign'),
                tooltip=f"{row['건물명'] if pd.notna(row['건물명']) else row['주소']}"
            ).add_to(marker_cluster)
    
    return m

def filter_and_display_data(df):
    """필터링 및 데이터 표시 함수"""
    if df is None or df.empty:
        st.warning("표시할 데이터가 없습니다.")
        return

    st.subheader("필터링 옵션")
    
    col1, col2 = st.columns(2)
    
    with col1:
        min_deposit_value = int(df['보증금(만원)'].fillna(0).min())
        max_deposit_value = int(df['보증금(만원)'].fillna(0).max())
        deposit_range = st.slider(
            "보증금 범위 (만원)",
            min_value=min_deposit_value,
            max_value=max_deposit_value,
            value=(min_deposit_value, max_deposit_value)
        )
    
    with col2:
        min_rent_value = int(df['임대료(만원)'].fillna(0).min())
        max_rent_value = int(df['임대료(만원)'].fillna(0).max())
        rent_range = st.slider(
            "임대료 범위 (만원)",
            min_value=min_rent_value,
            max_value=max_rent_value,
            value=(min_rent_value, max_rent_value)
        )

    # 필터링 적용
    filtered_df = df[
        (df['보증금(만원)'] >= deposit_range[0]) &
        (df['보증금(만원)'] <= deposit_range[1]) &
        (df['임대료(만원)'] >= rent_range[0]) &
        (df['임대료(만원)'] <= rent_range[1])
    ]

    st.subheader("조회 결과")
    st.write(f"총 {len(filtered_df):,}건의 데이터가 조회되었습니다.")

    if not filtered_df.empty:
        # 지도 표시
        valid_coords = filtered_df[filtered_df['위도'].notna() & filtered_df['경도'].notna()]
        
        if not valid_coords.empty:
            center_lat = valid_coords['위도'].mean()
            center_lng = valid_coords['경도'].mean()
            
            with st.spinner("지도를 생성중입니다..."):
                map_obj = create_folium_map(valid_coords, center_lat, center_lng)
                folium_static(map_obj, width=1200, height=600)

        # 데이터 테이블 표시
        st.subheader("상세 데이터")
        st.dataframe(filtered_df, use_container_width=True, height=400)
        
        # CSV 다운로드 버튼
        csv_data = filtered_df.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            label="📥 CSV 파일 다운로드",
            data=csv_data,
            file_name=f"서울시_임대_정보_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    else:
        st.warning("조건에 맞는 데이터가 없습니다.")

def initialize_session_state():
    """세션 상태 초기화"""
    if 'full_data_df' not in st.session_state:
        st.session_state.full_data_df = None
    if 'selected_gu_info' not in st.session_state:
        st.session_state.selected_gu_info = None
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False

def load_location_codes():
    """법정동 코드 데이터 로드"""
    try:
        codes_df = pd.read_csv('code.csv')
        return codes_df
    except Exception as e:
        st.error(f"법정동 코드 파일 로드 중 오류 발생: {e}")
        return None

def main():
    st.title("🏢 서울시 임대차 정보 조회")
    
    initialize_session_state()
    
    # 사이드바 설정
    with st.sidebar:
        st.header("📍 데이터 조회 설정")
        
        codes_df = load_location_codes()
        if codes_df is None:
            return
            
        # 자치구 선택
        gu_options = codes_df[['자치구코드', '자치구명']].drop_duplicates()
        selected_gu = st.selectbox(
            "자치구 선택",
            options=gu_options.values.tolist(),
            format_func=lambda x: x[1]
        )
        
        # 법정동 선택
        dong_options = codes_df[
            codes_df['자치구코드'] == selected_gu[0]
        ][['법정동코드', '법정동명']].drop_duplicates()
        
        dong_options = pd.concat([
            pd.DataFrame([['', '전체']], columns=['법정동코드', '법정동명']),
            dong_options
        ])
        
        selected_dong = st.selectbox(
            "법정동 선택",
            options=dong_options.values.tolist(),
            format_func=lambda x: x[1]
        )

        st.divider()
        load_data = st.button("🔍 데이터 조회", type="primary", use_container_width=True)
    
    # 데이터 조회
    if load_data:
        status_placeholder = st.empty()
        progress_bar = st.progress(0)
        detail_placeholder = st.empty()
        
        try:
            # 진행 상태 콜백 함수
            def update_progress(start_idx, end_idx, total, collected, status=None, error=None):
                if error:
                    detail_placeholder.warning(error)
                    return
                
                if status:
                    status_placeholder.info(f"📥 {status}")
                    return
                
                if total > 0:
                    progress = min(end_idx / total, 1.0)
                    progress_bar.progress(progress)
                    
                    status_msg = f"📥 데이터 수집 중: {start_idx:,} ~ {end_idx:,} / {total:,}건"
                    status_placeholder.info(status_msg)
                    detail_placeholder.text(f"✅ 현재까지 수집된 데이터: {collected:,}건")
            
            # 데이터 수집 시작
            df, error_msg = get_all_rent_data(
                gu_code=selected_gu[0],
                dong_code=selected_dong[0] if selected_dong[0] else None,
                progress_callback=update_progress
            )
            
            if error_msg:
                status_placeholder.error(f"❌ {error_msg}")
                progress_bar.empty()
                detail_placeholder.empty()
                return
            
            if df is None or df.empty:
                status_placeholder.warning("⚠️ 조회된 데이터가 없습니다.")
                progress_bar.empty()
                detail_placeholder.empty()
                return
            
            progress_bar.progress(1.0)
            status_placeholder.success(f"✅ {len(df):,}건의 데이터를 수집했습니다.")
            detail_placeholder.empty()
            
            # 데이터 전처리
            status_placeholder.info("⚙️ 데이터를 전처리하고 있습니다...")
            
            df = preprocess_data(df)
            if df is None:
                status_placeholder.error("❌ 데이터 전처리 실패")
                progress_bar.empty()
                return
            
            # 주소 생성
            df['주소'] = df.apply(lambda x: create_address(x, selected_gu[1]), axis=1)
            
            # 위치 정보 조회
            status_placeholder.info("🌍 위치 정보를 조회하고 있습니다...")
            
            coordinates = []
            total = len(df)
            
            for idx, address in enumerate(df['주소']):
                lng, lat = get_coordinates(address)
                coordinates.append((lat, lng))
                
                if (idx + 1) % 50 == 0:  # 50건마다 업데이트
                    progress = (idx + 1) / total
                    progress_bar.progress(progress)
                    status_placeholder.info(f"🌍 위치 정보 조회 중... ({idx + 1:,}/{total:,})")
            
            df['위도'] = [coord[0] for coord in coordinates]
            df['경도'] = [coord[1] for coord in coordinates]
            
            progress_bar.progress(1.0)
            status_placeholder.success("✅ 모든 데이터 처리가 완료되었습니다!")
            
            # 세션 상태 저장
            st.session_state.full_data_df = df
            st.session_state.selected_gu_info = selected_gu
            st.session_state.data_loaded = True
            
            time.sleep(1)
            progress_bar.empty()
            status_placeholder.empty()
            
        except Exception as e:
            status_placeholder.error(f"❌ 오류 발생: {str(e)}")
            progress_bar.empty()
            detail_placeholder.empty()
            return
    
    # 로딩 완료 후 데이터 표시
    if st.session_state.data_loaded and st.session_state.full_data_df is not None:
        df = st.session_state.full_data_df
        
        st.success(f"✅ {st.session_state.selected_gu_info[1]} 데이터 로드 완료")
        
        # 기본 통계
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("총 데이터", f"{len(df):,}건")
        with col2:
            st.metric("평균 보증금", f"{df['보증금(만원)'].mean():,.0f}만원")
        with col3:
            st.metric("평균 임대료", f"{df['임대료(만원)'].mean():,.0f}만원")
        with col4:
            valid_coords = df[df['위도'].notna()].shape[0]
            st.metric("좌표 확인", f"{valid_coords:,}건")
        
        st.divider()
        
        # 탭 생성
        tab1, tab2, tab3 = st.tabs(["📊 데이터 분석", "🗺️ 지도 보기", "📋 상세 데이터"])
        
        with tab1:
            if '계약일' in df.columns:
                st.subheader("📅 월별 계약 현황")
                df_copy = df.copy()
                df_copy['계약월'] = pd.to_datetime(df_copy['계약일'], errors='coerce').dt.strftime('%Y-%m')
                monthly_stats = df_copy.groupby('계약월').agg({
                    '보증금(만원)': 'mean',
                    '임대료(만원)': 'mean'
                }).round(0)
                st.line_chart(monthly_stats)
            
            if '법정동명' in df.columns:
                st.subheader("📍 지역별 평균 가격")
                dong_stats = df.groupby('법정동명').agg({
                    '보증금(만원)': 'mean',
                    '임대료(만원)': 'mean'
                }).round(0).sort_values('보증금(만원)', ascending=False)
                st.bar_chart(dong_stats)
        
        with tab2:
            filter_and_display_data(df)
        
        with tab3:
            st.dataframe(df, use_container_width=True, height=500)
            
            csv_data = df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 전체 데이터 CSV 다운로드",
                data=csv_data,
                file_name=f"서울시_임대_정보_전체_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    else:
        st.info("👆 사이드바에서 자치구를 선택하고 '데이터 조회' 버튼을 클릭하세요.")

if __name__ == "__main__":
    main()