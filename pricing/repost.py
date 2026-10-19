"""재게시 탐지: 같은 판매자가 같은 물건을 다시 올린 글인지 seen 테이블에서 찾는다.

분석 파이프라인의 매물 묶기 규칙(drspark.listing_units.posts_match: 정규화 제목 동일 또는 토큰 유사도 ≥ 90,
스펙·품목·묶음 호환, 호가 2배 차이·1시간 내 동시 등록·1.5배 인상 제외)을 그대로 쓴다.
이번 글보다 글번호가 작은(먼저 게시된) 글만 보고, 같은 매물로 보이는 글 중 가장 최근 글(글번호 최대)을
직전 글로 보아 호가 변화를 계산한다.
"""
from __future__ import annotations

import time

LOOKBACK_DAYS = 180


def _post_key(L, P, pid, author, t, title, ms, spec, release_year, body, raw_price):
    from .enrich import clip_text  # 보강과 같은 길이 제한(아주 긴 본문 한 건이 알림을 늦추지 않게)

    title, ms, spec, body = (clip_text(k, v) for k, v in
                             (("title", title), ("model_serial", ms), ("spec", spec), ("body_content", body)))
    price = P.parse_price(raw_price, title, body)
    lt = L.classify_listing(title, body, price.value)
    bf = L.bundle_flags(title, ms, spec, body, price.value)
    sig = L.spec_signature(title, ms, spec)
    if not sig["season"]:
        sig["season"] = L.season_from_year_field(release_year)
    valid = price.value is not None and L.price_is_valid(price.value) and lt.listing_type == "sale"
    key = L.PostKey(post_id=str(pid), author=author or "", t=int(t), listing_type=lt.listing_type,
                    title_norm=L.normalize_title(title), mtext=L.match_text(title, ms), sig=sig,
                    cats=[c for c in (bf.categories or "").split(",") if c], tsig=L.spec_signature(title),
                    is_bundle=bool(bf.is_bundle), price=int(price.value) if valid else None)
    return key, price.value


def find_previous(con, item: dict, price_krw: int | None):
    """item(source.py 매물 dict)과 같은 매물인 이전 글을 seen에서 찾아 RepostInfo를 돌려준다. 없으면 None."""
    from drspark import listing_units as L
    from drspark import parse as P

    from .enrich import RepostInfo

    author = (item.get("author") or "").strip()
    if not author:
        return None
    now = int(item.get("observed_at") or time.time())
    post_id = str(item.get("id") or "")
    # 드스파크 글번호는 사이트 전체에서 하나씩 늘어난다. run_once는 목록을 최신 글부터 처리하고 보강 전에 저장하므로,
    # 한 번에 같은 판매자 글이 2건 들어오면 먼저 저장된 '더 새 글'이 observed_at 조건만으로는 이전 글처럼 보인다.
    # 그래서 글번호가 더 작은 글만 이전 글 후보로 본다(글번호가 숫자가 아니면 자기 글만 뺀다).
    if post_id.isdigit():
        id_cond, id_arg = "CAST(post_id AS INTEGER) < ?", int(post_id)
    else:
        id_cond, id_arg = "post_id != ?", post_id
    rows = con.execute(
        f"""SELECT post_id, observed_at, title, model_serial, spec, release_year, body_content, price
             FROM seen WHERE author = ? AND {id_cond} AND observed_at >= ? AND observed_at <= ?
             ORDER BY observed_at DESC""",
        (author, id_arg, now - LOOKBACK_DAYS * 86400, now)).fetchall()
    if not rows:
        return None
    new_key, _ = _post_key(L, P, item.get("id"), author, now, item.get("title"), item.get("model_serial"),
                           item.get("spec"), item.get("release_year"), item.get("body_content"), item.get("raw_price"))
    matches = []
    for pid, t, title, ms, spec, ry, body, raw in rows:
        old_key, old_price = _post_key(L, P, pid, author, t, title, ms, spec, ry, body, raw)
        ok, _ratio, _why = L.posts_match(new_key, old_key)
        if ok:
            pid = str(pid)
            matches.append((int(pid) if pid.isdigit() else -1, int(t), pid, old_price))
    if not matches:
        return None
    # 직전 글 = 글번호가 가장 큰 글(같은 실행에서 저장 순서가 뒤바뀌어도 게시 순서를 따른다). 글번호가 없으면 관측 시각 순
    matches.sort()
    _, prev_t, prev_id, prev_price = matches[-1]
    change = None
    if prev_price and price_krw:
        change = round((price_krw / prev_price - 1) * 100, 1)
    return RepostInfo(prev_post_id=prev_id, n_previous=len(matches), first_seen_at=min(m[1] for m in matches),
                      prev_seen_at=prev_t, prev_price=prev_price, change_pct=change)
