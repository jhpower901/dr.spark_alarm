"""pricing.repost.find_previous: 같은 판매자가 같은 물건을 다시 올린 글 찾기(메모리 seen 테이블, 합성 글)."""
from __future__ import annotations

import pytest

from pricing import repost as R
from pricing.enrich import RepostInfo
from tests.conftest import DAY, TS, add_seen, make_item

S9 = "2526 아토믹 레드스터 S9 FIS 165"
SELLER = "판매자A"


def _item(title=S9, price=1_500_000, author=SELLER, post_id="200009", **kw):
    return make_item(title, price, id=post_id, author=author, **kw)


def find(con, item, price="item"):
    return R.find_previous(con, item, item["raw_price"] if price == "item" else price)


def test_same_seller_same_item_earlier(seen_con):
    add_seen(seen_con, "200001", TS - 5 * DAY, S9, 1_700_000)
    r = find(seen_con, _item())
    assert isinstance(r, RepostInfo)
    assert r.prev_post_id == "200001"
    assert r.n_previous == 1
    assert r.first_seen_at == r.prev_seen_at == TS - 5 * DAY
    assert r.prev_price == 1_700_000
    assert r.change_pct == round((1_500_000 / 1_700_000 - 1) * 100, 1) == -11.8


def test_several_previous_posts(seen_con):
    add_seen(seen_con, "200001", TS - 30 * DAY, S9, 1_800_000)
    add_seen(seen_con, "200002", TS - 12 * DAY, S9, 1_700_000)
    add_seen(seen_con, "200003", TS - 3 * DAY, S9, 1_600_000)
    r = find(seen_con, _item())
    assert r.n_previous == 3
    assert r.prev_post_id == "200003" and r.prev_seen_at == TS - 3 * DAY
    assert r.first_seen_at == TS - 30 * DAY
    assert r.prev_price == 1_600_000
    assert r.change_pct == round((1_500_000 / 1_600_000 - 1) * 100, 1)


def test_previous_price_unit_is_corrected(seen_con):
    add_seen(seen_con, "200001", TS - 5 * DAY, S9, 170)  # 만원 단위 입력
    r = find(seen_con, _item())
    assert r.prev_price == 1_700_000


def test_same_price_gives_zero_change(seen_con):
    add_seen(seen_con, "200001", TS - 5 * DAY, S9, 1_500_000)
    assert find(seen_con, _item()).change_pct == 0.0


def test_unknown_current_price_keeps_previous_price(seen_con):
    add_seen(seen_con, "200001", TS - 5 * DAY, S9, 1_700_000)
    r = find(seen_con, _item(), price=None)
    assert r.prev_price == 1_700_000 and r.change_pct is None


def test_different_author(seen_con):
    add_seen(seen_con, "200001", TS - 5 * DAY, S9, 1_700_000, author="판매자B")
    assert find(seen_con, _item()) is None


def test_empty_author_is_never_matched(seen_con):
    add_seen(seen_con, "200001", TS - 5 * DAY, S9, 1_700_000, author="")
    assert find(seen_con, _item(author="")) is None
    assert find(seen_con, _item(author=None)) is None
    assert find(seen_con, _item(author="   ")) is None


@pytest.mark.parametrize("old_title, new_title", [
    ("2526 아토믹 레드스터 G9 FIS GS 165", "2526 아토믹 레드스터 S9 FIS SL 165"),  # 회전 vs 대회전
    ("2526 아토믹 레드스터 S9 FIS 157cm", "2526 아토믹 레드스터 S9 FIS 165cm"),    # 길이 다름
    ("2425 아토믹 레드스터 S9 FIS 165", S9),                                       # 시즌 다름
    ("2526 아토믹 레드스터 G9 FIS 183", S9),                                       # 모델 다름
    ("카브 하드쉘 세트 L", S9),                                                     # 전혀 다른 물건
    ("아토믹 레드스터 S9 FIS 165 + 레드스터 CS 130 부츠 일괄", "아토믹 레드스터 S9 FIS 165"),  # 일괄 vs 낱개
])
def test_different_item_same_seller(seen_con, old_title, new_title):
    add_seen(seen_con, "200001", TS - 5 * DAY, old_title, 1_500_000)
    assert find(seen_con, _item(new_title)) is None


def test_same_item_with_minor_title_change(seen_con):
    add_seen(seen_con, "200001", TS - 5 * DAY, "2526 아토믹 레드스터 S9 FIS SL 165 판매", 1_600_000)
    r = find(seen_con, _item("2526 아토믹 레드스터 S9 FIS SL 165"))
    assert r is not None and r.prev_post_id == "200001"


def test_lookback_window(seen_con):
    add_seen(seen_con, "200001", TS - (R.LOOKBACK_DAYS + 1) * DAY, S9, 1_700_000)
    assert find(seen_con, _item()) is None
    add_seen(seen_con, "200002", TS - (R.LOOKBACK_DAYS - 1) * DAY, S9, 1_600_000)
    r = find(seen_con, _item())
    assert r.n_previous == 1 and r.prev_post_id == "200002"


def test_lookback_boundary_is_inclusive(seen_con):
    add_seen(seen_con, "200001", TS - R.LOOKBACK_DAYS * DAY, S9, 1_700_000)
    assert find(seen_con, _item()).prev_post_id == "200001"


def test_ignores_itself_and_later_posts(seen_con):
    # run_once는 보강 전에 save_item을 하므로 seen에 자기 글이 이미 있다
    add_seen(seen_con, "200009", TS, S9, 1_500_000)
    add_seen(seen_con, "200010", TS + 2 * DAY, S9, 1_400_000)
    assert find(seen_con, _item(post_id="200009")) is None


def test_newer_post_saved_first_is_not_previous(seen_con):
    # 한 번의 run_once에 같은 판매자 글 2건: 목록은 최신 글부터라 300002가 먼저 저장된다.
    # 나중에 처리하는 더 오래된 글(300001)의 observed_at이 몇 초 늦어도 300002를 '이전 글'로 잡으면 안 된다
    add_seen(seen_con, "300002", TS, S9, 1_500_000)
    add_seen(seen_con, "300001", TS + 3, S9, 1_500_000)
    assert find(seen_con, _item(post_id="300001", observed_at=TS + 3)) is None
    # 그 뒤에 올라온 글에게 직전 글은 관측 순서가 아니라 게시 순서(글번호)로 가장 최근인 300002다
    r = find(seen_con, _item(post_id="300003", observed_at=TS + 5))
    assert r.prev_post_id == "300002" and r.n_previous == 2
    assert r.prev_seen_at == TS and r.first_seen_at == TS


def test_same_observed_at_tie_breaks_by_post_number(seen_con):
    add_seen(seen_con, "200005", TS - 2 * DAY, S9, 1_600_000)
    add_seen(seen_con, "200003", TS - 2 * DAY, S9, 1_700_000)
    r = find(seen_con, _item())
    assert r.prev_post_id == "200005" and r.prev_price == 1_600_000
    assert r.first_seen_at == r.prev_seen_at == TS - 2 * DAY


def test_non_numeric_post_id_only_excludes_itself(seen_con):
    add_seen(seen_con, "200001", TS - 5 * DAY, S9, 1_700_000)
    add_seen(seen_con, "abc", TS, S9, 1_500_000)
    r = find(seen_con, _item(post_id="abc"))
    assert r.prev_post_id == "200001"


def test_huge_previous_body_is_clipped(seen_con):
    add_seen(seen_con, "200001", TS - 5 * DAY, S9, 1_700_000, body_content="1" * 200_000)
    r = find(seen_con, _item(body_content="1" * 200_000))
    assert r is not None and r.prev_post_id == "200001"


def test_price_jump_is_not_a_repost(seen_con):
    add_seen(seen_con, "200001", TS - 5 * DAY, S9, 1_000_000)
    assert find(seen_con, _item(price=1_600_000)) is None  # 1.5배 넘게 인상


def test_price_gap_over_two_times_is_not_a_repost(seen_con):
    add_seen(seen_con, "200001", TS - 5 * DAY, S9, 3_100_000)
    assert find(seen_con, _item(price=1_500_000)) is None


def test_concurrent_posts_with_different_prices(seen_con):
    add_seen(seen_con, "200001", TS - 30 * 60, S9, 1_400_000)
    assert find(seen_con, _item()) is None  # 1시간 안 다른 가격 = 비슷한 물건 여러 개


def test_concurrent_posts_with_same_price(seen_con):
    add_seen(seen_con, "200001", TS - 30 * 60, S9, 1_500_000)
    assert find(seen_con, _item()).prev_post_id == "200001"


def test_wanted_and_sale_are_not_linked(seen_con):
    add_seen(seen_con, "200001", TS - 5 * DAY, "[삽니다] 아토믹 레드스터 S9 FIS 165", 1_500_000)
    assert find(seen_con, _item("아토믹 레드스터 S9 FIS 165")) is None


def test_observed_at_missing_uses_now(seen_con, monkeypatch):
    monkeypatch.setattr(R.time, "time", lambda: TS)
    add_seen(seen_con, "200001", TS - 5 * DAY, S9, 1_700_000)
    r = find(seen_con, _item(observed_at=None))
    assert r.prev_post_id == "200001"


def test_no_rows_returns_none(seen_con):
    assert find(seen_con, _item()) is None
