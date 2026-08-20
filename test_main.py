import asyncio
from unittest.mock import patch

import main
from main import pick_best_document, filter_residential_permits, find_widest_successful_window


def test_pick_best_document_prefers_apartment():
    docs = [
        {'category_name': '부동산 > 원룸'},
        {'category_name': '부동산 > 아파트'},
    ]
    assert pick_best_document(docs) is docs[1]


def test_pick_best_document_falls_back_to_first():
    docs = [{'category_name': '음식점 > 카페'}]
    assert pick_best_document(docs) is docs[0]


def test_pick_best_document_empty_list():
    assert pick_best_document([]) is None


def test_filter_residential_permits():
    records = [
        {"USE_PURP": "주거용", "JOB_GBN_NM": "허가", "JIMOK": "대", "id": "keep"},
        {"USE_PURP": "상업용", "JOB_GBN_NM": "허가", "JIMOK": "대", "id": "drop_purpose"},
        {"USE_PURP": "주거용", "JOB_GBN_NM": "신고", "JIMOK": "대", "id": "drop_job"},
        {"USE_PURP": "주거용", "JOB_GBN_NM": "허가", "JIMOK": "전", "id": "drop_jimok"},
    ]
    result = filter_residential_permits(records)
    assert [r["id"] for r in result] == ["keep"]


def test_find_widest_successful_window_finds_exact_boundary():
    # 실제 API가 39일까지만 조회를 허용한다고 가정 (구/날짜마다 달라지는 상황을 흉내)
    threshold = 39

    async def attempt(days):
        return {"days": days} if days <= threshold else None

    best_days, best_result = asyncio.run(
        find_widest_successful_window(attempt, initial_days=60, coarse_step=10)
    )
    assert best_days == threshold
    assert best_result == {"days": threshold}


def test_find_widest_successful_window_boundary_at_initial_days():
    async def attempt(days):
        return {"days": days}  # 항상 성공 (60일까지 전부 허용되는 경우)

    best_days, _ = asyncio.run(
        find_widest_successful_window(attempt, initial_days=60, coarse_step=10)
    )
    assert best_days == 60  # initial_days를 넘어서 탐색하지 않아야 함


def test_find_widest_successful_window_never_succeeds():
    async def attempt(days):
        return None

    best_days, best_result = asyncio.run(
        find_widest_successful_window(attempt, initial_days=60, coarse_step=10)
    )
    assert best_days == 0
    assert best_result is None


def test_geocode_permits_dedupes_concurrent_addresses():
    # 같은 주소가 동시성 배치 안에서 중복 요청되지 않아야 한다 (레이스 방지)
    call_log = []

    async def fake_get_lat_lon(session, address, cache):
        call_log.append(address)
        await asyncio.sleep(0)
        return (f'place-{address}', 37.0, 127.0)

    records = [
        {'ADDRESS': 'A동 1', 'HNDL_YMD': '20260101', 'SGG_CD': '111'},
        {'ADDRESS': 'A동 1', 'HNDL_YMD': '20260102', 'SGG_CD': '111'},
        {'ADDRESS': 'B동 2', 'HNDL_YMD': '20260103', 'SGG_CD': '111'},
    ]
    cache = {}
    stats = {'api_call': 0, 'cache_hit': 0}

    async def run():
        with patch('main.get_lat_lon', new=fake_get_lat_lon):
            return await main.geocode_permits(None, records, cache, stats, asyncio.Semaphore(4))

    result = asyncio.run(run())

    assert len(result) == 3
    assert call_log.count('A동 1') == 1
    assert stats == {'api_call': 2, 'cache_hit': 0}


if __name__ == "__main__":
    test_pick_best_document_prefers_apartment()
    test_pick_best_document_falls_back_to_first()
    test_pick_best_document_empty_list()
    test_filter_residential_permits()
    test_find_widest_successful_window_finds_exact_boundary()
    test_find_widest_successful_window_boundary_at_initial_days()
    test_find_widest_successful_window_never_succeeds()
    test_geocode_permits_dedupes_concurrent_addresses()
    print("모든 테스트 통과")
