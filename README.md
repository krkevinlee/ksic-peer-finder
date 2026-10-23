# 업종 피어 파인더

기업명 → 표준산업분류(KSIC) → 같은 업종의 코스피·코스닥 상장사, 최근 4개 분기 실적(매출액·영업이익·지배주주순이익)과 LTM, 시가총액(어제·1개월·3개월 평균), PER을 보여주고 엑셀로 내려받는 정적 페이지.

- `pipeline/build.py` — KRX KIND 상장법인목록, DART(기업개황·정기보고서), KRX OpenAPI(일별 시가총액)에서 `site/data/*.json` 생성. 호출 결과는 `cache/`(data 브랜치)에 쌓고 바뀐 것만 다시 부름.
- `.github/workflows/daily.yml` — 매일 09:30 KST 빌드 후 GitHub Pages 배포. 저장소 Secrets `DART_API_KEY`, `KRX_API_KEY` 필요.
- 비상장사 업종코드는 남는 DART 호출 한도로 매일 조금씩 수집.
- 로컬 실행: `python pipeline/run_local.py --budget 17000`
