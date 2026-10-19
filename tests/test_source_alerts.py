"""source.py: DB 초기화(기존 스키마 호환), 보강 단계, 알림 필터, Discord 페이로드(기존 동작 보존), run_once 흐름.

Discord로는 아무것도 보내지 않는다(requests.post는 conftest에서 막고, 필요한 테스트에서만 가짜로 바꾼다).
"""
from __future__ import annotations

import copy
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys

import pytest

from pricing import discord_fields as DF
from pricing.enrich import ENRICHMENT_DDL, Enrichment, RepostInfo
from tests.conftest import (DAY, FAKE_AUTHOR, FAKE_EMAIL, FAKE_PHONE, ORIGINAL_SEEN_COLUMNS, ORIGINAL_SEEN_DDL, ROOT,
                            TS, make_item)

S9 = "2526 아토믹 레드스터 S9 FIS 165"
FAKE_WEBHOOK = "https://discord.invalid/api/webhooks/0/test-token"


# ------------------------------------------------------------------------------------ 원래 동작(fc71c35) 기준
def original_payload(item: dict) -> dict:
    """commit fc71c35의 discord_send()가 만들던 페이로드(그대로 옮긴 기준 구현)."""
    title = (item.get("title") or "").strip()
    url = item.get("url") or ""
    price = f"{item.get('raw_price'):,}원" if isinstance(item.get("raw_price"), int) else "—"
    status_full = " / ".join(item.get("status_list") or []) or "—"
    ts = item.get("observed_at")
    time_text = f"<t:{ts}:f> (<t:{ts}:R>)"
    author = item.get("author") or "—"
    fields = [
        {"name": "가격", "value": price, "inline": True},
        {"name": "상태", "value": status_full, "inline": True},
        {"name": "작성자", "value": author, "inline": False},
        {"name": "작성시간", "value": time_text, "inline": False},
    ]
    detail_lines = []
    if item.get("release_year"):
        detail_lines.append(f"출시연도: {item['release_year']}")
    if item.get("model_serial"):
        detail_lines.append(f"Model/Serial: {item['model_serial']}")
    if item.get("spec"):
        detail_lines.append(f"제품스펙: {item['spec']}")
    if item.get("purchased_at"):
        detail_lines.append(f"구입처/시기: {item['purchased_at']}")
    if item.get("usage_count"):
        detail_lines.append(f"사용횟수: {item['usage_count']}")
    if item.get("features"):
        detail_lines.append(f"특장점: {item['features']}")
    if item.get("phone"):
        detail_lines.append(f"전화번호: {item['phone']}")
    if item.get("email"):
        detail_lines.append(f"E-mail: {item['email']}")
    if detail_lines:
        fields.append({"name": "판매 정보", "value": "\n".join(detail_lines)[:900], "inline": False})
    if item.get("region"):
        fields.append({"name": "거래지역", "value": item["region"][:200], "inline": False})
    if item.get("body_content"):
        fields.append({"name": "본문내용", "value": item["body_content"][:500], "inline": False})
    embed = {"title": title, "url": url, "color": 0x3498DB, "fields": fields}
    if item.get("thumb"):
        embed["thumbnail"] = {"url": item["thumb"]}
    return {"content": None, "embeds": [embed]}


ITEMS = {
    "minimal": make_item("스키복 구합니다", None, status_list=[], author="", body_content=None),
    "full": make_item(S9, 1_500_000, release_year="2025", model_serial="Redster S9 FIS", spec="165cm R12.7",
                      purchased_at="2025.11 공식 대리점", usage_count="5회", features="엣지 튜닝 완료",
                      phone=FAKE_PHONE, email=FAKE_EMAIL, region="서울 송파", thumb="https://img.example/a.jpg",
                      body_content="상태 좋습니다.\n직거래 우선입니다."),
    "long": make_item("카브 하드쉘 세트 L " * 3, 500_000, features="좋음 " * 400, region="경기 " * 100,
                      body_content="본문 " * 400),
}


def total_chars(embed: dict) -> int:
    return len(embed.get("title") or "") + sum(len(f["name"]) + len(f["value"]) for f in embed["fields"])


def deal(**kw) -> Enrichment:
    base = dict(listing_type="sale", category="ski", subcategory="ski_race_sl", brand="atomic",
                family_id="atomic/redster", model_id="atomic/redster/s9-fis", model_name="Atomic Redster S9 FIS M",
                season_start=2025, id_grade="A", price_krw=600_000, fair_p10=780_000, fair_p50=971_000,
                fair_p90=1_224_000, basis_level="model", n_basis=12, explain="동일 모델 12건 중앙값 55만원 → 97만원",
                deal_flag="undervalued", deal_ratio=0.618, deal_confidence="high", msrp_krw=2_530_000)
    base.update(kw)
    return Enrichment(**base)


def cut(change=-10.0) -> RepostInfo:
    return RepostInfo("300001", 1, TS - 7 * DAY, TS - 7 * DAY, 700_000, change)


# ------------------------------------------------------------------------------------ init_db
def _schema(path):
    con = sqlite3.connect(path)
    try:
        objs = con.execute("SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY name").fetchall()
        cols = {t: [r[1:] for r in con.execute(f"PRAGMA table_info({t})")]
                for t in [o[1] for o in objs if o[0] == "table"]}
        return objs, cols
    finally:
        con.close()


def test_init_db_fresh(db_path, src):
    objs, cols = _schema(db_path)
    assert {o[1] for o in objs if o[0] == "table"} == {"seen", "enrichment", "alert_outbox"}
    assert [c[0] for c in cols["seen"]] == ORIGINAL_SEEN_COLUMNS
    assert [c[0] for c in cols["enrichment"]] == list(Enrichment().to_row("x"))
    assert [c[0] for c in cols["alert_outbox"]] == ["post_id", "status", "payload", "attempts", "last_error",
                                                    "created_at", "updated_at"]
    con = sqlite3.connect(db_path)
    assert con.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    con.close()
    src.init_db()  # 두 번 불러도 그대로
    assert _schema(db_path) == (objs, cols)


def test_init_db_upgrades_original_schema(tmp_path, monkeypatch, src):
    p = tmp_path / "old.db"
    con = sqlite3.connect(p)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute(ORIGINAL_SEEN_DDL)
    con.execute("INSERT INTO seen(post_id, observed_at, title, price, author, phone, email) VALUES (?,?,?,?,?,?,?)",
                ("100001", TS - DAY, S9, 1_500_000, FAKE_AUTHOR, FAKE_PHONE, FAKE_EMAIL))
    con.commit()
    con.close()
    before_objs, before_cols = _schema(p)
    assert {o[1] for o in before_objs} == {"seen", "sqlite_autoindex_seen_1"}

    monkeypatch.setattr(src, "DB", str(p))
    src.init_db()

    after_objs, after_cols = _schema(p)
    seen_before = [o for o in before_objs if o[2] == "seen"]
    seen_after = [o for o in after_objs if o[2] == "seen"]
    assert seen_after == seen_before  # seen의 CREATE 문·인덱스가 바뀌지 않음
    assert after_cols["seen"] == before_cols["seen"]
    assert "enrichment" in after_cols
    con = sqlite3.connect(p)
    assert con.execute("SELECT post_id, title, price, author, phone, email FROM seen").fetchall() == \
        [("100001", S9, 1_500_000, FAKE_AUTHOR, FAKE_PHONE, FAKE_EMAIL)]
    assert con.execute("SELECT COUNT(*) FROM enrichment").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM alert_outbox").fetchone()[0] == 0
    con.close()
    assert src.is_known("100001") and not src.is_known("100002")


# 이전 배포의 enrichment 테이블(열이 모자람)
OLD_ENRICHMENT_DDL = """CREATE TABLE enrichment (post_id TEXT PRIMARY KEY, computed_at INTEGER, pricing_version TEXT,
    listing_type TEXT, category TEXT, model_id TEXT, id_grade TEXT, price_krw INTEGER, fair_p50 INTEGER,
    deal_flag TEXT)"""


def test_init_db_adds_missing_enrichment_columns(tmp_path, monkeypatch, src, enricher):
    p = tmp_path / "prev.db"
    con = sqlite3.connect(p)
    con.execute(ORIGINAL_SEEN_DDL)
    con.execute(OLD_ENRICHMENT_DDL)
    con.execute("INSERT INTO enrichment(post_id, model_id, fair_p50, deal_flag) VALUES ('100001', 'm', 900000, 'fair')")
    con.commit()
    con.close()
    monkeypatch.setattr(src, "DB", str(p))
    src.init_db()
    _, cols = _schema(p)
    names = [c[0] for c in cols["enrichment"]]
    assert set(names) == set(Enrichment().to_row("x"))  # 빠진 열을 모두 붙였다
    assert _rows(p, "SELECT post_id, model_id, fair_p50, deal_flag, explain FROM enrichment") == \
        [("100001", "m", 900_000, "fair", None)]  # 기존 행은 그대로
    src.init_db()  # 두 번째는 바꿀 것이 없다
    assert [c[0] for c in _schema(p)[1]["enrichment"]] == names
    # 새 열까지 저장된다
    monkeypatch.setattr(src, "ENRICHER", enricher)
    it = make_item(S9, 1_500_000)
    src.save_item(it)
    e = src.enrich_item(it)
    assert e.error is None
    assert _rows(p, f"SELECT price_kind, fair_p50, explain IS NOT NULL FROM enrichment WHERE post_id = '{it['id']}'") \
        == [("field", e.fair_p50, 1)]


def test_enrichment_table_has_no_personal_columns(db_path):
    _, cols = _schema(db_path)
    names = {c[0] for c in cols["enrichment"]}
    assert not names & {"author", "phone", "email", "title", "body_content", "features", "region"}


def test_save_item_and_is_known_unchanged(db_path, src):
    it = make_item(S9, 1_500_000, phone=FAKE_PHONE, email=FAKE_EMAIL)
    assert not src.is_known(it["id"])
    src.save_item(it)
    assert src.is_known(it["id"])
    con = sqlite3.connect(db_path)
    row = con.execute("SELECT post_id, observed_at, title, price, author, status FROM seen").fetchone()
    con.close()
    assert row == (it["id"], TS, S9, 1_500_000, FAKE_AUTHOR, "A급")


# ------------------------------------------------------------------------------------ enrich_item
def test_enrich_item_off_returns_none(db_path, src):
    it = make_item(S9, 1_500_000)
    src.save_item(it)
    assert src.enrich_item(it) is None
    con = sqlite3.connect(db_path)
    assert con.execute("SELECT COUNT(*) FROM enrichment").fetchone()[0] == 0
    con.close()


def test_enrich_item_saves_row(db_path, src, enricher, monkeypatch):
    monkeypatch.setattr(src, "ENRICHER", enricher)
    it = make_item(S9, 1_500_000, phone=FAKE_PHONE, email=FAKE_EMAIL)
    src.save_item(it)
    e = src.enrich_item(it)
    assert isinstance(e, Enrichment) and e.error is None
    con = sqlite3.connect(db_path)
    row = con.execute("SELECT post_id, model_id, id_grade, fair_p50, deal_flag, repost_n FROM enrichment").fetchall()
    dump = con.execute("SELECT * FROM enrichment").fetchall()
    con.close()
    assert row == [(it["id"], "atomic/redster/s9-fis", "A", e.fair_p50, e.deal_flag, 0)]
    text = " ".join(str(v) for v in dump[0])
    assert FAKE_PHONE not in text and FAKE_EMAIL not in text and FAKE_AUTHOR not in text


def test_enrich_item_finds_repost_in_db(db_path, src, enricher, monkeypatch):
    monkeypatch.setattr(src, "ENRICHER", enricher)
    src.save_item(make_item(S9, 1_700_000, id="9900000", observed_at=TS - 3 * DAY))
    it = make_item(S9, 1_500_000)
    src.save_item(it)
    e = src.enrich_item(it)
    assert e.repost is not None and e.repost.prev_post_id == "9900000" and e.repost.n_previous == 1
    assert DF.is_price_cut(e)


class _Raising:
    version = "test"

    def enrich(self, item, con=None):
        raise RuntimeError("enricher crashed")


def test_enrich_item_swallows_enricher_exception(db_path, src, monkeypatch):
    monkeypatch.setattr(src, "ENRICHER", _Raising())
    it = make_item(S9, 1_500_000)
    src.save_item(it)
    assert src.enrich_item(it) is None
    assert src.should_alert(it, None) is True


def test_enrich_item_without_enrichment_table(tmp_path, src, enricher, monkeypatch):
    # init_db 없이 옛 DB를 그대로 쓰는 경우: 저장만 실패하고, 계산한 보강 결과는 알림에 그대로 쓴다
    p = tmp_path / "old.db"
    con = sqlite3.connect(p)
    con.execute(ORIGINAL_SEEN_DDL)
    con.close()
    monkeypatch.setattr(src, "DB", str(p))
    monkeypatch.setattr(src, "ENRICHER", enricher)
    it = make_item(S9, 1_500_000)
    src.save_item(it)
    e = src.enrich_item(it)
    assert isinstance(e, Enrichment) and e.error is None and e.fair_p50
    assert [f["name"] for f in src.build_payload(it, e)["embeds"][0]["fields"]][4:6] == ["모델", "적정가"]


@pytest.mark.parametrize("exc", [sqlite3.OperationalError("database is locked"),
                                 sqlite3.OperationalError("table enrichment has no column named x")])
def test_enrich_item_save_failure_keeps_filter(db_path, src, enricher, fake_predict, monkeypatch, exc):
    # 저장이 실패해도 deals 필터는 계산한 판정으로 걸러야 한다(예전에는 None → 필터 없이 알림)
    fake_predict(1_200_000, 1_500_000, 1_800_000)  # 호가 150만원 = 적정
    monkeypatch.setattr(src, "ENRICHER", enricher)
    monkeypatch.setattr(src, "ALERT_MODE", "deals")

    def broken_save(*a, **kw):
        raise exc
    monkeypatch.setattr(src, "save_enrichment", broken_save)
    it = make_item(S9, 1_500_000)
    src.save_item(it)
    e = src.enrich_item(it)
    assert e is not None and e.deal_flag == "fair" and e.error is None
    assert src.should_alert(it, e) is False


def test_enrich_item_with_locked_db_keeps_result(db_path, src, enricher, fake_predict, monkeypatch):
    fake_predict(1_200_000, 1_500_000, 1_800_000)
    monkeypatch.setattr(src, "ENRICHER", enricher)
    monkeypatch.setattr(src, "ALERT_MODE", "deals")
    it = make_item(S9, 1_500_000)
    src.save_item(it)
    real_connect = sqlite3.connect
    monkeypatch.setattr(src.sqlite3, "connect", lambda *a, **kw: real_connect(*a, **{**kw, "timeout": 0.1}))
    locker = real_connect(db_path, isolation_level=None)
    locker.execute("BEGIN IMMEDIATE")  # 다른 연결이 쓰기 잠금을 잡고 있음
    try:
        e = src.enrich_item(it)
    finally:
        locker.execute("ROLLBACK")
        locker.close()
    assert e is not None and e.deal_flag == "fair"
    assert src.should_alert(it, e) is False
    assert _rows(db_path, "SELECT COUNT(*) FROM enrichment") == [(0,)]


def test_enrich_item_returns_error_enrichment(db_path, src, enricher, monkeypatch):
    def boom(*a, **kw):
        raise RuntimeError("모델 고장")
    monkeypatch.setattr(enricher.model, "predict", boom)
    monkeypatch.setattr(src, "ENRICHER", enricher)
    it = make_item(S9, 1_500_000)
    src.save_item(it)
    e = src.enrich_item(it)
    assert e is not None and e.error == "RuntimeError: 모델 고장"
    assert src.should_alert(it, e) is True
    con = sqlite3.connect(db_path)
    assert con.execute("SELECT error FROM enrichment").fetchone()[0] == "RuntimeError: 모델 고장"
    con.close()
    assert src.build_payload(it, e) == original_payload(it)


# ------------------------------------------------------------------------------------ should_alert
SALE_SKI = deal()
FAIR_SKI = deal(deal_flag="fair", deal_confidence="high")
LOW_DEAL = deal(deal_confidence="low", id_grade="C")
FAIR_CUT = deal(deal_flag="fair", repost=cut())
WANTED = Enrichment(listing_type="wanted", category="apparel", skipped_reason="구매 희망 글")
BOOTS = deal(category="boots", deal_flag="fair")
ERROR = Enrichment(error="RuntimeError: x", listing_type="wanted", category="boots")


@pytest.mark.parametrize("settings, e, expected", [
    ({}, None, True),
    ({}, SALE_SKI, True),
    ({}, FAIR_SKI, True),
    ({}, WANTED, True),                                  # 기본값: 모든 새 글(기존 동작)
    ({"ALERT_SKIP_WANTED": True}, WANTED, False),
    ({"ALERT_SKIP_WANTED": True}, FAIR_SKI, True),
    ({"ALERT_SKIP_WANTED": True}, None, True),           # 보강 없으면 놓치지 않게 알림
    ({"ALERT_CATEGORIES": {"ski"}}, SALE_SKI, True),
    ({"ALERT_CATEGORIES": {"ski"}}, BOOTS, False),
    ({"ALERT_CATEGORIES": {"ski", "boots"}}, BOOTS, True),
    ({"ALERT_CATEGORIES": {"ski"}}, WANTED, False),
    ({"ALERT_CATEGORIES": {"ski"}}, Enrichment(category=None), False),
    ({"ALERT_CATEGORIES": {"ski"}}, None, True),
    ({"ALERT_MODE": "deals"}, SALE_SKI, True),
    ({"ALERT_MODE": "deals"}, deal(deal_confidence="medium"), True),
    ({"ALERT_MODE": "deals"}, LOW_DEAL, False),
    ({"ALERT_MODE": "deals"}, FAIR_SKI, False),
    ({"ALERT_MODE": "deals"}, deal(deal_flag="price_typo"), False),
    ({"ALERT_MODE": "deals"}, FAIR_CUT, True),           # 가격 인하 재게시
    ({"ALERT_MODE": "deals"}, deal(deal_flag="fair", repost=cut(5.0)), False),
    ({"ALERT_MODE": "deals"}, WANTED, False),
    ({"ALERT_MODE": "deals"}, None, True),
    ({"ALERT_MODE": "deals"}, ERROR, True),              # 보강 실패도 알림
    ({"ALERT_MODE": "deals", "ALERT_SKIP_WANTED": True, "ALERT_CATEGORIES": {"ski"}}, ERROR, True),
    ({"ALERT_MODE": "deals", "ALERT_CATEGORIES": {"boots"}}, SALE_SKI, False),
    ({"ALERT_MODE": "all"}, LOW_DEAL, True),
    ({"ALERT_MODE": "unknown-mode"}, FAIR_SKI, True),
    # deals 모드의 가격 인하: 5% 이상 내렸고, 내린 값이 적정가(P50) 이하이며 고평가·가격 확인 필요가 아닐 때만
    ({"ALERT_MODE": "deals"}, deal(deal_flag="fair", price_krw=971_000, repost=cut(-10.0)), True),   # = P50
    ({"ALERT_MODE": "deals"}, deal(deal_flag="fair", price_krw=1_100_000, repost=cut(-10.0)), False),  # P50 초과
    ({"ALERT_MODE": "deals"}, deal(deal_flag="overvalued", price_krw=1_595_000, repost=cut(-12.0)), False),
    ({"ALERT_MODE": "deals"}, deal(deal_flag="price_typo", price_krw=200_000, repost=cut(-50.0)), False),
    ({"ALERT_MODE": "deals"}, deal(deal_flag="fair", repost=cut(-0.3)), False),                     # 소폭 인하
    ({"ALERT_MODE": "deals"}, deal(deal_flag="fair", repost=cut(-4.9)), False),
    ({"ALERT_MODE": "deals"}, deal(deal_flag="fair", repost=cut(-5.0)), True),
    ({"ALERT_MODE": "deals"}, deal(deal_flag="undervalued", deal_confidence="low", repost=cut(-6.0)), True),
    # 적정가가 없는 글(묶음 등): 10% 이상 내린 것만
    ({"ALERT_MODE": "deals"}, Enrichment(category="ski", skipped_reason="여러 물건 묶음", price_krw=900_000,
                                         repost=cut(-12.0)), True),
    ({"ALERT_MODE": "deals"}, Enrichment(category="ski", skipped_reason="여러 물건 묶음", price_krw=900_000,
                                         repost=cut(-6.0)), False),
    # 가격 단위·오기가 불확실한 저평가(신뢰 낮춤)는 deals 모드에서 빠진다
    ({"ALERT_MODE": "deals"}, deal(deal_confidence="low", extra={"price_uncertain": True}), False),
])
def test_should_alert(src, monkeypatch, settings, e, expected):
    for k, v in settings.items():
        monkeypatch.setattr(src, k, v)
    assert src.should_alert(make_item(S9, 1_500_000), e) is expected


# ------------------------------------------------------------------------------------ build_payload
@pytest.mark.parametrize("key", list(ITEMS))
def test_build_payload_without_enrichment_matches_original(src, key):
    item = copy.deepcopy(ITEMS[key])
    assert src.build_payload(item, None) == original_payload(item)
    assert src.build_payload(item) == original_payload(item)
    json.dumps(src.build_payload(item))


@pytest.mark.parametrize("key", list(ITEMS))
def test_build_payload_with_failed_enrichment_matches_original(src, key):
    item = copy.deepcopy(ITEMS[key])
    assert src.build_payload(item, Enrichment(error="RuntimeError: x")) == original_payload(item)


def test_build_payload_does_not_mutate_item(src):
    item = copy.deepcopy(ITEMS["full"])
    snapshot = copy.deepcopy(item)
    src.build_payload(item, deal(repost=cut()))
    assert item == snapshot


def test_build_payload_with_enrichment(src):
    item = copy.deepcopy(ITEMS["full"])
    e = deal(repost=cut())
    p = src.build_payload(item, e)
    orig = original_payload(item)["embeds"][0]
    emb = p["embeds"][0]
    names = [f["name"] for f in emb["fields"]]
    assert names == ["가격", "상태", "작성자", "작성시간", "모델", "적정가", "적정가 근거", "재게시",
                     "판매 정보", "거래지역", "본문내용"]
    # 기존 필드는 그대로, 보강 필드는 작성시간과 판매 정보 사이에 끼운다
    assert emb["fields"][:4] == orig["fields"][:4]
    assert emb["fields"][8:] == orig["fields"][4:]
    assert emb["fields"][4:8] == DF.fields(e)
    assert emb["title"] == "[저평가] [가격인하] " + item["title"]
    assert emb["color"] == DF.COLOR["undervalued"]
    assert emb["url"] == item["url"] and emb["thumbnail"] == {"url": item["thumb"]}
    assert p["content"] is None  # DEAL_MENTION 미설정
    json.dumps(p)


@pytest.mark.parametrize("e, colour, prefix", [
    (deal(deal_flag="fair"), 0x3498DB, ""),
    (deal(deal_flag="overvalued"), 0xE67E22, ""),
    (deal(deal_flag="price_typo"), 0x95A5A6, ""),
    (deal(), 0x2ECC71, "[저평가] "),
    (deal(deal_confidence="low"), 0x2ECC71, ""),
    (Enrichment(listing_type="wanted", skipped_reason="구매 희망 글"), 0x3498DB, ""),
])
def test_build_payload_colour_and_prefix(src, e, colour, prefix):
    emb = src.build_payload(copy.deepcopy(ITEMS["full"]), e)["embeds"][0]
    assert emb["color"] == colour
    assert emb["title"] == prefix + ITEMS["full"]["title"]


def test_build_payload_skipped_enrichment_fields(src):
    e = Enrichment(listing_type="wanted", category="apparel", skipped_reason="구매 희망 글")
    names = [f["name"] for f in src.build_payload(copy.deepcopy(ITEMS["minimal"]), e)["embeds"][0]["fields"]]
    assert names == ["가격", "상태", "작성자", "작성시간", "모델", "적정가"]


def test_build_payload_title_limit(src):
    item = make_item("가" * 300, 1_500_000)
    assert len(src.build_payload(item, deal())["embeds"][0]["title"]) == 256


@pytest.mark.parametrize("e, content", [
    (deal(), "@here"),
    (deal(deal_confidence="medium"), "@here"),
    (deal(deal_confidence="low"), None),
    (deal(deal_flag="fair"), None),
    (deal(deal_flag="fair", repost=cut()), None),  # 가격 인하만으로는 멘션하지 않는다
    (Enrichment(error="x", deal_flag="undervalued", deal_confidence="high"), None),
    (None, None),
])
def test_deal_mention(src, monkeypatch, e, content):
    monkeypatch.setattr(src, "DEAL_MENTION", "@here")
    assert src.build_payload(copy.deepcopy(ITEMS["full"]), e)["content"] == content


def test_no_mention_when_unset(src):
    assert src.build_payload(copy.deepcopy(ITEMS["full"]), deal())["content"] is None


def test_uncertain_price_gets_no_deal_tag_or_mention(src, enricher, fake_predict, monkeypatch):
    # 360만원 입력이 36만원으로 보정돼 P50 100만원보다 한참 싸 보여도 [저평가]·멘션을 내지 않는다
    fake_predict(800_000, 1_000_000, 1_200_000)
    monkeypatch.setattr(src, "DEAL_MENTION", "@here")
    it = make_item(S9 + " 신품", 3_600_000)
    e = enricher.enrich(it)
    assert e.deal_flag == "undervalued" and e.deal_confidence == "low"
    assert not DF.is_deal(e)
    p = src.build_payload(it, e)
    assert p["content"] is None
    emb = p["embeds"][0]
    assert not emb["title"].startswith("[저평가]")
    fm = {f["name"]: f["value"] for f in emb["fields"]}
    assert fm["가격"] == "3,600,000원"  # 가격 필드는 입력값 그대로(기존 동작)
    assert fm["적정가"].startswith("판정 호가 36만원(입력값 보정")  # 판정에 쓴 값을 함께 보여 준다
    monkeypatch.setattr(src, "ALERT_MODE", "deals")
    assert src.should_alert(it, e) is False


# ------------------------------------------------------------------------------------ _fit_embed
def _embed(**sizes):
    fields = [{"name": "가격", "value": "150만원", "inline": True}]
    for name, n in sizes.items():
        fields.append({"name": name.replace("_", " "), "value": "가" * n, "inline": False})
    return {"title": "제목" * 100, "url": "https://example.com", "color": 1, "fields": fields}


def test_fit_embed_keeps_small_embed(src):
    e = _embed(본문내용=500, 판매_정보=900)
    assert src._fit_embed(copy.deepcopy(e)) == e


def test_fit_embed_trims_body_first(src):
    e = src._fit_embed(_embed(본문내용=5000, 판매_정보=900, 적정가_근거=400))
    v = {f["name"]: f["value"] for f in e["fields"]}
    assert total_chars(e) <= src.EMBED_TOTAL_LIMIT
    assert v["본문내용"].endswith("…") and len(v["본문내용"]) < 5000
    assert v["판매 정보"] == "가" * 900 and v["적정가 근거"] == "가" * 400


def test_fit_embed_trims_in_order_when_body_is_not_enough(src):
    e = src._fit_embed(_embed(본문내용=3000, 판매_정보=4000, 적정가_근거=2000))
    v = {f["name"]: f["value"] for f in e["fields"]}
    assert total_chars(e) <= src.EMBED_TOTAL_LIMIT
    assert len(v["본문내용"]) == 21  # 최소 20자 + …
    assert v["판매 정보"].endswith("…")
    assert v["가격"] == "150만원"


def test_fit_embed_trims_all_three(src):
    e = src._fit_embed(_embed(본문내용=3000, 판매_정보=3000, 적정가_근거=7000))
    v = {f["name"]: f["value"] for f in e["fields"]}
    assert total_chars(e) <= src.EMBED_TOTAL_LIMIT
    assert len(v["본문내용"]) == len(v["판매 정보"]) == 21
    assert v["적정가 근거"].endswith("…") and len(v["적정가 근거"]) < 7000


def test_fit_embed_stops_once_under_limit(src):
    e = src._fit_embed(_embed(본문내용=4000, 판매_정보=4000, 적정가_근거=4000))
    v = {f["name"]: f["value"] for f in e["fields"]}
    assert total_chars(e) == src.EMBED_TOTAL_LIMIT
    assert v["적정가 근거"] == "가" * 4000  # 앞의 두 필드로 충분하면 근거는 그대로


def test_build_payload_huge_texts_fit_discord_limits(src):
    item = make_item(S9 * 20, 1_500_000, status_list=["상태 " * 300, "네고 " * 300], author="작" * 900,
                     features="특 " * 3000, spec="스펙 " * 2000, body_content="본문 " * 5000,
                     region="지역 " * 500, release_year="2025", usage_count="5회")
    e = deal(model_name="모델" * 2000, explain="근거 " * 3000, repost=cut())
    p = src.build_payload(item, e)
    emb = p["embeds"][0]
    assert total_chars(emb) <= src.EMBED_TOTAL_LIMIT
    assert len(emb["title"]) <= 256
    assert len(emb["fields"]) <= 25
    for f in emb["fields"]:
        if f["name"] not in ("상태", "작성자"):  # 이 둘은 기존 코드 그대로(길이 제한 없음)
            assert len(f["value"]) <= 1024
    json.dumps(p)


# ------------------------------------------------------------------------------------ discord_send
class _Resp:
    def __init__(self, status=204):
        self.status_code = status
        self.text = ""

    def json(self):
        return {}

    def raise_for_status(self):
        pass


def _capture_posts(monkeypatch, src, status=204):
    posts = []

    def fake_post(url, json=None, timeout=None):
        posts.append({"url": url, "json": json, "timeout": timeout})
        return _Resp(status)
    monkeypatch.setattr(src.requests, "post", fake_post)
    return posts


def test_discord_send_without_webhook_does_nothing(src, monkeypatch):
    posts = _capture_posts(monkeypatch, src)
    assert src.discord_send(copy.deepcopy(ITEMS["full"]), deal()) is False
    assert posts == []


def test_discord_send_returns_true_and_accepts_prebuilt_payload(src, monkeypatch):
    monkeypatch.setattr(src, "DISCORD_WEBHOOK", FAKE_WEBHOOK)
    posts = _capture_posts(monkeypatch, src)
    payload = src.build_payload(copy.deepcopy(ITEMS["full"]), deal())
    assert src.discord_send(None, payload=payload) is True
    assert [p["json"] for p in posts] == [payload]


def test_discord_send_raises_after_repeated_rate_limit(src, monkeypatch):
    # 429만 3번: 예전에는 조용히 끝나 알림을 잃었다 → 이제 실패로 알려 대기열에 남긴다
    monkeypatch.setattr(src, "DISCORD_WEBHOOK", FAKE_WEBHOOK)
    posts = _capture_posts(monkeypatch, src, status=429)
    with pytest.raises(src.requests.exceptions.RequestException):
        src.discord_send(copy.deepcopy(ITEMS["full"]))
    assert len(posts) == 3


def test_discord_send_posts_built_payload(src, monkeypatch):
    monkeypatch.setattr(src, "DISCORD_WEBHOOK", FAKE_WEBHOOK)
    posts = _capture_posts(monkeypatch, src)
    item = copy.deepcopy(ITEMS["full"])
    src.discord_send(item)  # 기존 호출 형태(보강 인자 없음)
    src.discord_send(item, deal())
    assert [p["url"] for p in posts] == [FAKE_WEBHOOK, FAKE_WEBHOOK]
    assert posts[0]["json"] == original_payload(item)
    assert posts[1]["json"] == src.build_payload(item, deal())
    assert posts[0]["timeout"] == 10


def test_discord_send_retries_rate_limit(src, monkeypatch):
    monkeypatch.setattr(src, "DISCORD_WEBHOOK", FAKE_WEBHOOK)
    statuses = iter([429, 204])
    posts = []

    def fake_post(url, json=None, timeout=None):
        posts.append(json)
        return _Resp(next(statuses))
    monkeypatch.setattr(src.requests, "post", fake_post)
    src.discord_send(copy.deepcopy(ITEMS["full"]))
    assert len(posts) == 2


# ------------------------------------------------------------------------------------ run_once 흐름
POST_ID = "9912345"


def _list_html(title=S9, price_text="150만원", pid=POST_ID):
    return f"""<html><body><div class="simple-board__webzine">
      <div class="item"><a class="item__container" href="/ski_sell2/{pid}">
        <div class="item__thumbnail"><img src="//img.example/thumb/{pid}.jpg"></div>
        <div class="item__inner item__subject"><span class="subject">{title}</span></div>
        <div class="item__inner item__etc-wrp"><span style="font-size:13px">{price_text}</span></div>
        <span class="status_icon">판매중</span>
        <div class="item__author"><span>{FAKE_AUTHOR}</span></div>
      </a></div></div></body></html>"""


def _detail_html(body="상태 좋습니다. 직거래 우선.", **et):
    et = {"판매가격": "1,500,000", "장비상태": "A급", "네고": "네고 가능", "거래방법": "직거래", "출시연도": "2025",
          "전화번호": FAKE_PHONE, "E-mail": FAKE_EMAIL, "거래지역": "서울 송파", **et}
    items = "".join(f'<!--<div class="item"><div class="item__label">{k}</div>'
                    f'<div class="item__value">{v}</div></div>-->' for k, v in et.items())
    return f"""<html><head><meta name="description" content="{body}"></head><body>
      <div class="rhymix_content xe_content"><p>{body}</p></div>
      <div class="simple-board__read__extravars">{items}</div>
      <div class="et_vars"></div></body></html>"""


@pytest.fixture
def site_posts(src, monkeypatch, db_path):
    """가짜 사이트(목록 1건 + 상세) + 가짜 웹후크. 반환: 보낸 페이로드 기록."""
    pages = {src.LIST_URL: _list_html(), f"{src.BASE}/ski_sell2/{POST_ID}": _detail_html()}
    monkeypatch.setattr(src, "fetch_html", lambda url: pages[url])
    monkeypatch.setattr(src, "DISCORD_WEBHOOK", FAKE_WEBHOOK)
    return _capture_posts(monkeypatch, src)


def _rows(db_path, sql):
    con = sqlite3.connect(db_path)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_run_once_enrichment_off_sends_original_payload(src, site_posts, db_path):
    src.run_once()
    assert len(site_posts) == 1
    p = site_posts[0]["json"]
    emb = p["embeds"][0]
    assert p["content"] is None
    assert emb["title"] == S9 and emb["color"] == 0x3498DB
    assert [f["name"] for f in emb["fields"]] == ["가격", "상태", "작성자", "작성시간", "판매 정보", "거래지역", "본문내용"]
    assert emb["fields"][0]["value"] == "1,500,000원"
    assert emb["fields"][1]["value"] == "A급 / 네고 가능 / 직거래"
    assert emb["thumbnail"] == {"url": f"https://img.example/thumb/{POST_ID}.jpg"}
    assert _rows(db_path, "SELECT post_id, price, author FROM seen") == [(POST_ID, 1_500_000, FAKE_AUTHOR)]
    assert _rows(db_path, "SELECT COUNT(*) FROM enrichment") == [(0,)]
    src.run_once()  # 이미 본 글은 다시 보내지 않는다
    assert len(site_posts) == 1


def test_run_once_alerts_even_if_enricher_raises(src, site_posts, db_path, monkeypatch):
    monkeypatch.setattr(src, "ENRICHER", _Raising())
    monkeypatch.setattr(src, "ALERT_MODE", "deals")
    src.run_once()
    assert len(site_posts) == 1
    assert site_posts[0]["json"]["embeds"][0]["color"] == 0x3498DB
    assert _rows(db_path, "SELECT post_id FROM seen") == [(POST_ID,)]
    assert _rows(db_path, "SELECT COUNT(*) FROM enrichment") == [(0,)]


def test_run_once_with_enrichment(src, site_posts, db_path, enricher, monkeypatch):
    monkeypatch.setattr(src, "ENRICHER", enricher)
    src.run_once()
    assert len(site_posts) == 1
    names = [f["name"] for f in site_posts[0]["json"]["embeds"][0]["fields"]]
    assert names[:5] == ["가격", "상태", "작성자", "작성시간", "모델"]
    assert "적정가" in names
    rows = _rows(db_path, "SELECT post_id, model_id, id_grade, price_krw, error FROM enrichment")
    assert rows == [(POST_ID, "atomic/redster/s9-fis", "A", 1_500_000, None)]
    dump = " ".join(str(v) for v in _rows(db_path, "SELECT * FROM enrichment")[0])
    assert FAKE_PHONE not in dump and FAKE_EMAIL not in dump and FAKE_AUTHOR not in dump


def test_run_once_deals_mode_filters_fair_listing(src, site_posts, db_path, enricher, fake_predict, monkeypatch):
    fake_predict(1_200_000, 1_500_000, 1_800_000)  # 호가 150만원 = 적정
    monkeypatch.setattr(src, "ENRICHER", enricher)
    monkeypatch.setattr(src, "ALERT_MODE", "deals")
    src.run_once()
    assert site_posts == []
    # 알림은 걸러도 기록은 남는다
    assert _rows(db_path, "SELECT post_id FROM seen") == [(POST_ID,)]
    assert _rows(db_path, "SELECT deal_flag FROM enrichment") == [("fair",)]


def test_run_once_deals_mode_sends_deal_with_mention(src, site_posts, db_path, enricher, fake_predict, monkeypatch):
    fake_predict(1_600_000, 2_000_000, 2_400_000)  # 호가 150만원 < P10 → 저평가(A, 동일 모델 12건 → 신뢰 높음)
    monkeypatch.setattr(src, "ENRICHER", enricher)
    monkeypatch.setattr(src, "ALERT_MODE", "deals")
    monkeypatch.setattr(src, "DEAL_MENTION", "<@&123>")
    src.run_once()
    assert len(site_posts) == 1
    p = site_posts[0]["json"]
    assert p["content"] == "<@&123>"
    assert p["embeds"][0]["title"] == "[저평가] " + S9
    assert p["embeds"][0]["color"] == DF.COLOR["undervalued"]


def test_run_once_skip_wanted(src, monkeypatch, db_path, enricher):
    title = "스키복 구합니다"
    pages = {src.LIST_URL: _list_html(title=title, price_text="0"),
             f"{src.BASE}/ski_sell2/{POST_ID}": _detail_html(body="사이즈 L 스키복 구합니다", 판매가격="")}
    monkeypatch.setattr(src, "fetch_html", lambda url: pages[url])
    monkeypatch.setattr(src, "DISCORD_WEBHOOK", FAKE_WEBHOOK)
    posts = _capture_posts(monkeypatch, src)
    monkeypatch.setattr(src, "ENRICHER", enricher)
    monkeypatch.setattr(src, "ALERT_SKIP_WANTED", True)
    src.run_once()
    assert posts == []
    assert _rows(db_path, "SELECT listing_type, skipped_reason FROM enrichment") == [("wanted", "구매 희망 글")]


def test_run_once_detail_failure_still_alerts(src, monkeypatch, db_path, enricher):
    def fake_fetch(url):
        if url == src.LIST_URL:
            return _list_html()
        raise src.requests.exceptions.ConnectionError("detail down")
    monkeypatch.setattr(src, "fetch_html", fake_fetch)
    monkeypatch.setattr(src, "DISCORD_WEBHOOK", FAKE_WEBHOOK)
    monkeypatch.setattr(src, "ENRICHER", enricher)
    posts = _capture_posts(monkeypatch, src)
    src.run_once()
    assert len(posts) == 1
    assert posts[0]["json"]["embeds"][0]["fields"][0]["value"] == "150원"  # 목록 가격 그대로(기존 동작)


# ------------------------------------------------------------------------------------ 설정값(F3·OPS-2·OPS-4)
def test_env_or_treats_blank_as_unset(src, monkeypatch):
    for v in ("", "   "):
        monkeypatch.setenv("DRSPARK_DB", v)
        assert src._env_or("DRSPARK_DB", "drspark_seen.db") == "drspark_seen.db"
    monkeypatch.delenv("DRSPARK_DB", raising=False)
    assert src._env_or("DRSPARK_DB", "drspark_seen.db") == "drspark_seen.db"
    monkeypatch.setenv("DRSPARK_DB", " /data/x.db ")
    assert src._env_or("DRSPARK_DB", "drspark_seen.db") == "/data/x.db"


def _import_source_with_env(tmp_path, **env) -> dict:
    """새 프로세스에서 이 환경변수로 source를 import해 모듈 설정값을 읽는다(작업 폴더는 tmp_path)."""
    code = ("import json, sys; sys.path.insert(0, sys.argv[1]); import source as s; "
            "print('RESULT ' + json.dumps({'DB': s.DB, 'cats': sorted(s.ALERT_CATEGORIES), 'mode': s.ALERT_MODE, "
            "'retry': s.ALERT_RETRY_HOURS, 'warnings': len(s._SETTING_WARNINGS)}))")
    full_env = {k: v for k, v in os.environ.items()
                if k not in ("DRSPARK_DB", "ALERT_CATEGORIES", "ALERT_MODE", "ALERT_RETRY_HOURS")}
    full_env.update(env)
    r = subprocess.run([sys.executable, "-c", code, str(ROOT)], cwd=tmp_path, env=full_env, capture_output=True,
                       text=True, encoding="utf-8", timeout=120)
    assert r.returncode == 0, r.stderr[-2000:]
    line = [ln for ln in r.stdout.splitlines() if ln.startswith("RESULT ")][-1]
    return json.loads(line[len("RESULT "):])


def test_blank_env_values_use_defaults_at_import(tmp_path):
    # .env에 'DRSPARK_DB='처럼 값 없이 남은 줄: 예전에는 sqlite3.connect('')(연결마다 임시 DB) → 매분 수집 실패
    got = _import_source_with_env(tmp_path, DRSPARK_DB="", ALERT_CATEGORIES="", ALERT_RETRY_HOURS="")
    assert got == {"DB": "drspark_seen.db", "cats": [], "mode": "all", "retry": 6.0, "warnings": 0}


def test_env_values_are_normalised_at_import(tmp_path):
    got = _import_source_with_env(tmp_path, DRSPARK_DB=str(tmp_path / "x.db"), ALERT_CATEGORIES="Ski, BOOTS,,",
                                  ALERT_MODE=" Deals ", ALERT_RETRY_HOURS="abc")
    assert got == {"DB": str(tmp_path / "x.db"), "cats": ["boots", "ski"], "mode": "deals", "retry": 6.0,
                   "warnings": 1}
    assert _import_source_with_env(tmp_path, ALERT_RETRY_HOURS="1.5")["retry"] == 1.5


@pytest.mark.parametrize("cats, mode, exp_cats, exp_mode, n_warn", [
    ({"ski", "boots"}, "deals", {"ski", "boots"}, "deals", 0),
    ({"skis", "boot"}, "all", set(), "all", 2),          # 모두 오타 → 필터를 끄고(전체 알림) 경고
    ({"ski", "skis"}, "deal", {"ski"}, "all", 2),        # 모르는 값만 빼고, 모드 오타는 all로
    (set(), "deals_only", set(), "all", 1),
])
def test_validate_alert_settings(src, monkeypatch, caplog, cats, mode, exp_cats, exp_mode, n_warn):
    monkeypatch.setattr(src.logger, "handlers", [])  # 로그는 caplog로만(컨테이너의 /dev/log 없음 소음 방지)
    monkeypatch.setattr(src, "ALERT_CATEGORIES", set(cats))
    monkeypatch.setattr(src, "ALERT_MODE", mode)
    monkeypatch.setattr(src, "_SETTING_WARNINGS", [])
    with caplog.at_level(logging.WARNING, logger="drspark"):
        src._validate_alert_settings()
    assert src.ALERT_CATEGORIES == exp_cats and src.ALERT_MODE == exp_mode
    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == n_warn
    if exp_mode == "all" and not exp_cats:
        assert src.should_alert(make_item(S9, 1_500_000), deal(category="boots", deal_flag="fair")) is True


def test_validate_alert_settings_reports_bad_numbers(src, monkeypatch, caplog):
    monkeypatch.setattr(src.logger, "handlers", [])
    monkeypatch.setattr(src, "_SETTING_WARNINGS", ["ALERT_RETRY_HOURS='abc'는 0 이상의 숫자가 아니라 기본값 6를 쓴다"])
    with caplog.at_level(logging.WARNING, logger="drspark"):
        src._validate_alert_settings()
    assert any("ALERT_RETRY_HOURS" in r.getMessage() for r in caplog.records)


# ------------------------------------------------------------------------------------ pricing/ 없이 단독 배포(OPS-3)
SOLO_SCRIPT = r'''
import importlib.util, json, runpy, sqlite3, sys, types
if importlib.util.find_spec("pricing") is not None:
    print("SKIP pricing importable"); sys.exit(0)
path, item_path = sys.argv[1], sys.argv[2]
spec = importlib.util.spec_from_file_location("watcher", path)
w = importlib.util.module_from_spec(spec)
spec.loader.exec_module(w)
item = json.load(open(item_path, encoding="utf-8"))
w.init_db()
con = sqlite3.connect(w.DB)
tables = sorted(r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'"))
con.close()
w._validate_alert_settings()
out = {"import_error": type(w.PRICING_IMPORT_ERROR).__name__, "tables": tables, "enricher_none": w.ENRICHER is None,
       "enrich_item_none": w.enrich_item(item) is None, "should_alert": w.should_alert(item, None),
       "payload": w.build_payload(item, None)}
print("RESULT " + json.dumps(out, ensure_ascii=True))
sys.stdout.flush()


class FakeScheduler:  # __main__ 시작 경로만 확인(스케줄러는 돌리지 않는다)
    def add_job(self, *a, **kw):
        pass

    def start(self):
        print("STARTED")


mod = types.ModuleType("apscheduler.schedulers.blocking")
mod.BlockingScheduler = FakeScheduler
sys.modules["apscheduler.schedulers.blocking"] = mod
runpy.run_path(path, run_name="__main__")
'''


def test_source_alone_without_pricing_package(tmp_path):
    # 원래처럼 source.py 파일 하나만(watcher.py로) 배포한 경우: pricing/이 없어도 보강 없이 기존 알림대로 돈다
    shutil.copy(ROOT / "source.py", tmp_path / "watcher.py")
    item = copy.deepcopy(ITEMS["full"])
    (tmp_path / "item.json").write_text(json.dumps(item, ensure_ascii=True), encoding="utf-8")
    env = {**os.environ, "DRSPARK_PRICING": "0", "DRSPARK_DB": str(tmp_path / "seen.db"),
           "ALERT_CATEGORIES": "Ski,skis"}
    env.pop("DISCORD_WEBHOOK_URL", None)
    r = subprocess.run([sys.executable, "-I", "-c", SOLO_SCRIPT, str(tmp_path / "watcher.py"),
                        str(tmp_path / "item.json")], cwd=tmp_path, env=env, capture_output=True, text=True,
                       encoding="utf-8", timeout=180)
    assert r.returncode == 0, r.stderr[-3000:]
    if r.stdout.startswith("SKIP"):
        pytest.skip("이 환경에서는 pricing 패키지가 다른 경로에서 import된다")
    line = [ln for ln in r.stdout.splitlines() if ln.startswith("RESULT ")][-1]
    got = json.loads(line[len("RESULT "):])
    assert got["import_error"] == "ModuleNotFoundError"
    assert got["tables"] == ["alert_outbox", "seen"]  # enrichment 테이블은 만들지 않는다
    assert got["enricher_none"] and got["enrich_item_none"] and got["should_alert"]
    assert got["payload"] == original_payload(item)
    assert "STARTED" in r.stdout
    assert "pricing package not importable" in r.stderr
    assert "Pricing enrichment: off" in r.stderr and "Watcher started" in r.stderr


# ----------------------------------------------------------------- 거래 상태(판매중 / 거래완료)
def test_legacy_label_still_sets_price():
    """예전 라벨 '판매가격'도 그대로 읽는다."""
    import source
    it = {"raw_price": 1, "et_vars": {"판매가격": "450,000원", "거래": "판매중"}}
    source.normalize_item_from_et_vars(it)
    assert it["raw_price"] == 450_000 and it["trade_status"] == "판매중"


def test_missing_trade_field_is_none():
    import source
    it = {"raw_price": 300_000, "et_vars": {"장비상태": "A급"}}
    source.normalize_item_from_et_vars(it)
    assert it["trade_status"] is None and it["raw_price"] == 300_000


def test_completed_trade_shown_in_status_and_not_a_deal(monkeypatch):
    import source
    from pricing import discord_fields as DF
    from pricing.enrich import Enrichment

    e = Enrichment(deal_flag="undervalued", deal_confidence="high", fair_p10=500_000, fair_p50=600_000,
                   fair_p90=700_000, price_krw=400_000, trade_status="거래완료")
    assert not DF.is_deal(e)
    item = {"id": "1", "title": "t", "url": "u", "raw_price": 400_000, "status_list": ["S급", "네고X"],
            "observed_at": 1_790_000_000, "trade_status": "거래완료"}
    payload = source.build_payload(item, e)
    status = next(f["value"] for f in payload["embeds"][0]["fields"] if f["name"] == "상태")
    assert status.startswith("거래완료 / ")
    assert "[저평가]" not in payload["embeds"][0]["title"]
    monkeypatch.setattr(source, "ALERT_MODE", "deals")
    assert source.should_alert(item, e) is False


def test_on_sale_status_unchanged():
    import source
    item = {"id": "1", "title": "t", "url": "u", "raw_price": 1000, "status_list": ["S급"], "observed_at": 1_790_000_000,
            "trade_status": "판매중"}
    status = next(f["value"] for f in source.build_payload(item, None)["embeds"][0]["fields"] if f["name"] == "상태")
    assert status == "S급"


def test_trade_status_saved_in_enrichment(tmp_path, monkeypatch):
    import sqlite3

    import source
    from pricing.enrich import Enrichment, save_enrichment
    monkeypatch.setattr(source, "DB", str(tmp_path / "t.db"))
    source.init_db()
    with sqlite3.connect(source.DB) as con:
        save_enrichment(con, "9", Enrichment(trade_status="거래완료"))
        assert con.execute("SELECT trade_status FROM enrichment WHERE post_id='9'").fetchone()[0] == "거래완료"
