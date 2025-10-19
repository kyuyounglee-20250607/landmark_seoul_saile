```
VSCODE + WINDOW 환경
인증키는 .EVN에 저장  SEOUL_LANDMARK_API

API를 호출해서 정보를 추출
URL : http://openapi.seoul.go.kr:8088/(인증키)/json/tbLnOpendataRentV/1/5/

1/5/  페이지 시작과 끝번호
전체 페이지정보를 가져와서 10페이지단위로 호출

출력값은 다음과 같고 전체정보를 추출하면 데이터프레임형태로 출력
No	출력명	출력설명
공통	list_total_count	총 데이터 건수 (정상조회 시 출력됨)
공통	RESULT.CODE	요청결과 코드 (하단 메세지설명 참고)
공통	RESULT.MESSAGE	요청결과 메시지 (하단 메세지설명 참고)
1	RCPT_YR	접수연도
2	CGG_CD	자치구코드
3	CGG_NM	자치구명
4	STDG_CD	법정동코드
5	STDG_NM	법정동명
6	LOTNO_SE	지번구분
7	LOTNO_SE_NM	지번구분명
8	MNO	본번
9	SNO	부번
10	FLR	층
11	CTRT_DAY	계약일
12	RENT_SE	전월세 구분
13	RENT_AREA	임대면적(㎡)
14	GRFE	보증금(만원)
15	RTFE	임대료(만원)
16	BLDG_NM	건물명
17	ARCH_YR	건축년도
18	BLDG_USG	건물용도
19	CTRT_PRD	계약기간
20	NEW_UPDT_YN	신규갱신여부
21	CTRT_UPDT_USE_YN	계약갱신권사용여부
22	BFR_GRFE	종전 보증금
23	BFR_RTFE	종전 임대료
```