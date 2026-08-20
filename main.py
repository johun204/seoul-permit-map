import os
import asyncio
import sys
import json
import aiohttp
from datetime import date, timedelta, datetime, timezone

# 1. 설정
KAKAO_API_KEY = os.getenv('KAKAO_API_KEY')
CACHE_FILE_PATH = 'data/address_cache.json'
DATA_FILE_PATH = 'data/data.json'
# SEOUL_CONTRACT_URL 시크릿이 설정되어 있으면 그걸 우선 사용한다 (엔드포인트가 바뀔 경우 대비)
CONTRACT_LIST_URL = os.getenv('SEOUL_CONTRACT_URL') or 'https://land.seoul.go.kr/land/wsklis/getContractList.do'
KAKAO_KEYWORD_SEARCH_URL = 'https://dapi.kakao.com/v2/local/search/keyword.json'

SEOUL_DISTRICTS = [
    {'code': '11110', 'eng_name': 'Jongno-gu', 'kor_name': '종로구'},
    {'code': '11140', 'eng_name': 'Jung-gu', 'kor_name': '중구'},
    {'code': '11170', 'eng_name': 'Yongsan-gu', 'kor_name': '용산구'},
    {'code': '11200', 'eng_name': 'Seongdong-gu', 'kor_name': '성동구'},
    {'code': '11215', 'eng_name': 'Gwangjin-gu', 'kor_name': '광진구'},
    {'code': '11230', 'eng_name': 'Dongdaemun-gu', 'kor_name': '동대문구'},
    {'code': '11260', 'eng_name': 'Jungnang-gu', 'kor_name': '중랑구'},
    {'code': '11290', 'eng_name': 'Seongbuk-gu', 'kor_name': '성북구'},
    {'code': '11305', 'eng_name': 'Gangbuk-gu', 'kor_name': '강북구'},
    {'code': '11320', 'eng_name': 'Dobong-gu', 'kor_name': '도봉구'},
    {'code': '11350', 'eng_name': 'Nowon-gu', 'kor_name': '노원구'},
    {'code': '11380', 'eng_name': 'Eunpyeong-gu', 'kor_name': '은평구'},
    {'code': '11410', 'eng_name': 'Seodaemun-gu', 'kor_name': '서대문구'},
    {'code': '11440', 'eng_name': 'Mapo-gu', 'kor_name': '마포구'},
    {'code': '11470', 'eng_name': 'Yangcheon-gu', 'kor_name': '양천구'},
    {'code': '11500', 'eng_name': 'Gangseo-gu', 'kor_name': '강서구'},
    {'code': '11530', 'eng_name': 'Guro-gu', 'kor_name': '구로구'},
    {'code': '11545', 'eng_name': 'Geumcheon-gu', 'kor_name': '금천구'},
    {'code': '11560', 'eng_name': 'Yeongdeungpo-gu', 'kor_name': '영등포구'},
    {'code': '11590', 'eng_name': 'Dongjak-gu', 'kor_name': '동작구'},
    {'code': '11620', 'eng_name': 'Gwanak-gu', 'kor_name': '관악구'},
    {'code': '11650', 'eng_name': 'Seocho-gu', 'kor_name': '서초구'},
    {'code': '11680', 'eng_name': 'Gangnam-gu', 'kor_name': '강남구'},
    {'code': '11710', 'eng_name': 'Songpa-gu', 'kor_name': '송파구'},
    {'code': '11740', 'eng_name': 'Gangdong-gu', 'kor_name': '강동구'},
]

# 구별/날짜별로 조회 가능한 최대 기간이 달라서(초과 시 에러 응답) 탐색으로 찾는다.
# 60일에서 10일 단위로 좁혀가며 첫 성공 지점을 찾고, 거기서 1일 단위로 넓혀가며
# 실제 최대 허용 기간을 찾는다.
INITIAL_LOOKBACK_DAYS = 60
COARSE_STEP_DAYS = 10
FINE_STEP_DAYS = 1

# 카카오 지오코딩 API 동시 요청 수 제한 (레이트리밋 대비)
GEOCODE_CONCURRENCY = 8


# -------------------- HTTP 요청 -------------------- #
async def fetch_post(session, url, data=None, json_body=None, headers=None):
    try:
        kwargs = {"timeout": 10}
        if headers:
            kwargs["headers"] = headers

        if json_body is not None:
            kwargs["json"] = json_body
        elif data is not None:
            kwargs["data"] = data

        async with session.post(url, **kwargs) as response:
            return await response.text()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"요청 실패 ({url}): {e}")
        return None


# -------------------- 캐시 파일 입출력 -------------------- #
def load_address_cache(path):
    if not os.path.exists(path):
        print("기존 캐시 파일 없음. 새로 시작합니다.")
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            cache = json.load(f)
        print(f"기존 캐시 로드 완료: {len(cache)}개 주소")
        return cache
    except (json.JSONDecodeError, OSError) as e:
        print(f"캐시 파일 로드 실패 (새로 시작): {e}")
        return {}


def save_address_cache(path, cache):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    print(f"캐시 파일 저장 완료: {path}")


# -------------------- 카카오 좌표 변환 (캐시 적용) -------------------- #
def pick_best_document(documents):
    for keyword in ('아파트', '주거시설', '부동산'):
        matches = [doc for doc in documents if keyword in doc['category_name']]
        if matches:
            return matches[0]
    return documents[0] if documents else None


async def get_lat_lon(session, address, cache):
    if address in cache:
        return cache[address]

    headers = {'Authorization': f'KakaoAK {KAKAO_API_KEY}'}
    query = f"서울시 {address}"

    result = await fetch_post(session, KAKAO_KEYWORD_SEARCH_URL, data={'query': query}, headers=headers)
    if result is None:
        # 네트워크 오류는 캐시하지 않아 다음 실행에서 재시도되게 한다.
        return None, None, None

    try:
        data = json.loads(result)
        doc = pick_best_document(data['documents'])
    except (json.JSONDecodeError, KeyError) as e:
        print(f"지오코딩 응답 파싱 실패 ({address}): {e}")
        return None, None, None

    # 정상 응답인데 매칭되는 장소가 없는 경우만 (None, None, None)으로 캐시한다.
    result_tuple = (doc.get('place_name'), doc['y'], doc['x']) if doc else (None, None, None)
    cache[address] = result_tuple
    return result_tuple


# -------------------- 서울시 토지거래허가 조회 -------------------- #
async def _try_fetch_permits(session, district, today, lookback_days):
    """지정한 조회기간으로 1회 시도. 실패/에러/빈 결과면 None."""
    begin_date = (today - timedelta(days=lookback_days)).strftime("%Y%m%d")
    end_date = today.strftime("%Y%m%d")
    payload = {"sggCd": district["code"], "beginDate": begin_date, "endDate": end_date}

    result = await fetch_post(session, CONTRACT_LIST_URL, data=payload)
    if result is None:
        return None

    try:
        content = json.loads(result)
    except json.JSONDecodeError as e:
        print(f"{district['code']} {district['kor_name']} {lookback_days}일 응답 파싱 실패: {e}")
        return None

    return content.get("result") or None


async def find_widest_successful_window(attempt, initial_days, coarse_step, fine_step=FINE_STEP_DAYS):
    """attempt(days) -> 성공 시 결과, 실패 시 None을 반환하는 콜백.

    굵은 단위(coarse_step)로 좁혀가며 처음 성공하는 지점을 찾은 뒤,
    그 지점에서 1일 단위로 넓혀가며 실제로 성공하는 가장 넓은 기간을 찾는다.
    (지역구/날짜마다 허용되는 최대 조회기간이 달라서 상수로 고정할 수 없다.)
    """
    coarse_days = initial_days
    result = None
    while coarse_days > 0:
        result = await attempt(coarse_days)
        if result is not None:
            break
        coarse_days -= coarse_step

    if result is None:
        return 0, None

    best_days, best_result = coarse_days, result
    for fine_days in range(coarse_days + fine_step, min(coarse_days + coarse_step, initial_days), fine_step):
        candidate = await attempt(fine_days)
        if candidate is None:
            break
        best_days, best_result = fine_days, candidate

    return best_days, best_result


async def fetch_district_permits(session, district, today):
    async def attempt(lookback_days):
        result = await _try_fetch_permits(session, district, today, lookback_days)
        if result is None:
            print(f"{district['code']} {district['kor_name']} {lookback_days}일 로드 실패")
        return result

    best_days, best_result = await find_widest_successful_window(
        attempt, INITIAL_LOOKBACK_DAYS, COARSE_STEP_DAYS
    )
    if best_result:
        print(f"{district['code']} {district['kor_name']} 최대 조회기간 {best_days}일")
    return best_result or []


def filter_residential_permits(records):
    return [
        x for x in records
        if x["USE_PURP"] == "주거용" and x["JOB_GBN_NM"] == "허가" and x["JIMOK"] == "대"
    ]


async def geocode_permits(session, records, cache, stats, semaphore):
    unique_addresses = list(dict.fromkeys(r["ADDRESS"] for r in records))
    for address in unique_addresses:
        if address in cache:
            stats["cache_hit"] += 1
        else:
            stats["api_call"] += 1

    async def geocode_one(address):
        async with semaphore:
            return address, await get_lat_lon(session, address, cache)

    resolved = dict(await asyncio.gather(*(geocode_one(a) for a in unique_addresses)))

    geocoded = []
    for record in records:
        place_name, lat, lng = resolved[record["ADDRESS"]]
        if place_name and lat and lng:
            geocoded.append({
                "address": record["ADDRESS"],
                "place_name": place_name,
                "lat": lat,
                "lng": lng,
                "date": record["HNDL_YMD"],
                "sggCd": record["SGG_CD"],
            })
    return geocoded


def save_data(path, data):
    last_updated = (datetime.now(timezone.utc) + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump({"last_updated": last_updated, "data": data}, f, ensure_ascii=False, indent=2)
    print(f"작업 완료: {last_updated}")


async def main():
    if not KAKAO_API_KEY:
        raise SystemExit("KAKAO_API_KEY 환경변수가 설정되지 않았습니다.")

    cache = load_address_cache(CACHE_FILE_PATH)
    stats = {"api_call": 0, "cache_hit": 0}
    today = date.today()
    data = []
    geocode_semaphore = asyncio.Semaphore(GEOCODE_CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        for district in SEOUL_DISTRICTS:
            records = await fetch_district_permits(session, district, today)
            residential = filter_residential_permits(records)
            geocoded = await geocode_permits(session, residential, cache, stats, geocode_semaphore)
            data.extend(geocoded)
            print(f"[{district['code']}] {district['kor_name']} 토지거래허가 {len(geocoded)}건")

    print(f"작업 완료: API 호출 {stats['api_call']}회, 캐시 사용 {stats['cache_hit']}회")

    save_data(DATA_FILE_PATH, data)
    save_address_cache(CACHE_FILE_PATH, cache)


# -------------------- 실행 -------------------- #
if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
