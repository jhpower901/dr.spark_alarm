"""pricing.enrich: 규칙 파싱 + 별칭 리졸버 + 적정가(P10/P50/P90) + 판정 + 실패 격리.

실제 사전·모델(pricing_data/)을 쓰는 테스트는 모델이 다시 학습돼도 깨지지 않도록 정확한 금액 대신
관계(P10 ≤ P50 ≤ P90, 판정 문턱)를 본다. 판정 문턱·신뢰도 규칙의 경계값은 모델 출력을 고정(fake_predict)해 확인한다.
"""
from __future__ import annotations

import csv
import datetime as dt
import sqlite3
from dataclasses import fields as dc_fields

import pytest

from pricing import discord_fields as DF
from pricing import enrich as EN
from pricing.enrich import ENRICHMENT_DDL, Enricher, Enrichment, RepostInfo, as_dict, save_enrichment
from tests.conftest import DATA_DIR, DAY, FAKE_AUTHOR, FAKE_EMAIL, FAKE_PHONE, KST, ROOT, TS, add_seen, make_item

S9 = "2526 아토믹 레드스터 S9 FIS 165"           # 모델+시즌 → A
S9_NO_SEASON = "아토믹 레드스터 S9 FIS 165"      # 모델만 → B
FAMILY_ONLY = "아토믹 레드스터 스키 165"          # 패밀리만 → B
BRAND_ONLY = "아토믹 스키 165"                    # 브랜드만 → C


def _msrp(model_id: str) -> int | None:
    with open(DATA_DIR / "canonical_models.csv", encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            if r["canonical_model_id"] == model_id:
                return int(float(r["msrp_krw"])) if r["msrp_krw"] else None
    return None


def test_vendored_drspark_package_is_used(enricher):
    import drspark
    assert drspark.__file__.startswith(str(ROOT / "drspark"))


def test_enricher_loaded(enricher):
    assert enricher.version.startswith("fairprice-")
    assert "@" in enricher.version
    assert len(enricher.resolver.aliases) > 1000
    assert enricher.model_name["atomic/redster/s9-fis"]


# ------------------------------------------------------------------------------------ 모델 식별
@pytest.mark.parametrize("title, kw, model_id, family_id, brand, category, grade", [
    (S9, {}, "atomic/redster/s9-fis", "atomic/redster", "atomic", "ski", "A"),
    ("헤드 월드컵 레벨스 e-SL 165 24/25", {}, "head/worldcup-rebels/e-sl", "head/worldcup-rebels", "head", "ski", "A"),
    ("2425 로시뇰 히어로 월드컵 ZA 26.5", {}, "rossignol/hero-world-cup/za", "rossignol/hero-world-cup",
     "rossignol", "boots", "A"),
    ("카브 하드쉘 세트 L", {"release_year": "2025"}, "carve/hard-shell/chs", "carve/hard-shell", "carve",
     "apparel", "A"),
    (S9_NO_SEASON, {}, "atomic/redster/s9-fis", "atomic/redster", "atomic", "ski", "B"),
    ("카브 하드쉘 세트 L", {}, "carve/hard-shell/chs", "carve/hard-shell", "carve", "apparel", "B"),
    (FAMILY_ONLY, {}, None, "atomic/redster", "atomic", "ski", "B"),
    (BRAND_ONLY, {}, None, None, "atomic", "ski", "C"),
])
def test_model_family_resolution_and_grade(enricher, title, kw, model_id, family_id, brand, category, grade):
    e = enricher.enrich(make_item(title, 900_000, **kw))
    assert e.error is None
    assert e.model_id == model_id
    assert e.family_id == family_id
    assert e.brand == brand
    assert e.category == category
    assert e.id_grade == grade
    if model_id:
        assert e.model_name == enricher.model_name[model_id]
    else:
        assert e.model_name is None


def test_known_model_details(enricher):
    e = enricher.enrich(make_item(S9, 1_500_000))
    assert e.listing_type == "sale"
    assert e.subcategory == "ski_race_sl"
    assert "Redster S9 FIS" in e.model_name
    assert e.season_start == 2025 and e.season_label == "25/26"
    assert e.category_ko == "스키"
    assert e.msrp_krw == _msrp("atomic/redster/s9-fis")
    assert e.pricing_version == enricher.version
    assert e.elapsed_ms > 0


def test_korean_hero_master_title_keeps_brand_and_season(enricher):
    # 제목만으로는 모델까지 못 잡더라도(사전 별칭 범위) 브랜드·시즌·카테고리는 남아야 한다
    e = enricher.enrich(make_item("24/25 로시뇰 히어로 마스터 R22 183cm", 900_000))
    assert e.brand == "rossignol"
    assert e.season_start == 2024
    assert e.category == "ski"
    assert e.id_grade in ("A", "B", "C")


def test_english_title_resolves_model_without_season(enricher):
    e = enricher.enrich(make_item("Rossignol Hero Master R22 183", 1_500_000))
    assert e.model_id == "rossignol/hero/master-r22"
    assert e.id_grade == "B"  # 시즌 표기가 없으면 정확한 모델이어도 B


# ------------------------------------------------------------------------------------ 시즌
@pytest.mark.parametrize("title, kw, season", [
    ("24/25 로시뇰 히어로 마스터 R22 183cm", {}, 2024),
    (S9, {}, 2025),
    ("헤드 월드컵 레벨스 e-SL 165 24/25", {}, 2024),
    (S9_NO_SEASON, {"release_year": "2025"}, 2025),
    (S9_NO_SEASON, {"release_year": "23/24"}, 2023),
    (S9_NO_SEASON, {}, None),
])
def test_season_parsing(enricher, title, kw, season):
    e = enricher.enrich(make_item(title, 900_000, **kw))
    assert e.season_start == season
    assert e.season_label == (None if season is None else f"{season % 100:02d}/{(season + 1) % 100:02d}")


def test_season_from_release_year_upgrades_grade(enricher):
    b = enricher.enrich(make_item(S9_NO_SEASON, 900_000))
    a = enricher.enrich(make_item(S9_NO_SEASON, 900_000, release_year="2025"))
    assert (b.id_grade, a.id_grade) == ("B", "A")


# ------------------------------------------------------------------------------------ 적정가
PRICED = [
    (S9, {}), (S9_NO_SEASON, {}), (FAMILY_ONLY, {}), (BRAND_ONLY, {}),
    ("2425 로시뇰 히어로 월드컵 ZA 26.5", {}), ("카브 하드쉘 세트 L", {}), ("오클리 고글 판매", {}),
    ("헤드 월드컵 레벨스 e-SL 165 24/25", {}), ("24/25 로시뇰 히어로 마스터 R22 183cm", {}),
]


@pytest.mark.parametrize("title, kw", PRICED)
def test_quantiles_are_ordered(enricher, title, kw):
    e = enricher.enrich(make_item(title, 500_000, **kw))
    assert e.error is None and e.skipped_reason is None
    for v in (e.fair_p10, e.fair_p50, e.fair_p90):
        assert isinstance(v, int) and v > 0 and v % 1000 == 0  # 천원 단위 반올림
    assert e.fair_p10 <= e.fair_p50 <= e.fair_p90
    assert e.basis_level in ("model_season", "model", "family", "brand_subcat", "subcategory", "category")
    assert e.n_basis is None or e.n_basis >= 0
    assert e.explain and "→" in e.explain
    assert e.deal_flag in ("undervalued", "fair", "overvalued", "price_typo")
    assert e.deal_confidence in ("high", "medium", "low")
    # 비율은 반올림 전 P50 기준이라 표시용(천원 단위) P50으로 나눈 값과 조금 다를 수 있다
    assert e.deal_ratio == pytest.approx(e.price_krw / e.fair_p50, rel=0.02)


def test_asking_price_does_not_change_fair_price(enricher):
    a = enricher.enrich(make_item(S9, 600_000))
    b = enricher.enrich(make_item(S9, 1_400_000))
    assert (a.fair_p10, a.fair_p50, a.fair_p90) == (b.fair_p10, b.fair_p50, b.fair_p90)


def test_deal_flags_relative_to_real_model(enricher):
    base = enricher.enrich(make_item(S9, 1_000_000))
    p10, p50, p90 = base.fair_p10, base.fair_p50, base.fair_p90

    def flag(price):
        # 3.5M 초과 호가는 parse_price가 '0 하나 더'로 고친다 → 그 아래에서만 고른다
        assert 5_000 <= price <= 3_500_000
        e = enricher.enrich(make_item(S9, price))
        assert e.price_krw == price
        return e.deal_flag

    assert flag(p50) == "fair"
    low = (p10 - 20_000) // 10_000 * 10_000
    assert low > 0.3 * p50, "모델 구간이 예상보다 넓어 저평가 테스트 가격을 고를 수 없음"
    assert flag(low) == "undervalued"
    high = (p90 + 20_000 + 9_999) // 10_000 * 10_000
    assert high < 3 * p50
    assert flag(high) == "overvalued"
    assert flag(max(int(0.2 * p50) // 1000 * 1000, 5_000)) == "price_typo"
    typo_high = (int(3.2 * p50) // 10_000 + 1) * 10_000
    if typo_high <= 3_500_000:
        assert flag(typo_high) == "price_typo"


@pytest.mark.parametrize("price, expected", [
    (290_000, "price_typo"),     # 호가/P50 0.29 < 0.3
    (300_000, "undervalued"),    # 0.3은 오기 아님, P10 미만
    (799_000, "undervalued"),
    (800_000, "fair"),           # P10과 같으면 적정
    (1_000_000, "fair"),
    (1_200_000, "fair"),         # P90과 같으면 적정
    (1_201_000, "overvalued"),
    (3_000_000, "overvalued"),   # 3.0은 오기 아님
    (3_010_000, "price_typo"),   # 3.01 > 3.0
])
def test_deal_flag_thresholds(enricher, fake_predict, price, expected):
    fake_predict(800_000, 1_000_000, 1_200_000)
    e = enricher.enrich(make_item(S9, price))
    assert e.price_krw == price
    assert e.deal_flag == expected
    assert e.deal_ratio == round(price / 1_000_000, 3)


def test_zero_p50_gives_no_verdict(enricher, fake_predict):
    fake_predict(0, 0, 0)
    e = enricher.enrich(make_item(S9, 900_000))
    assert e.error is None
    assert e.deal_flag is None and e.deal_ratio is None and e.deal_confidence is None


@pytest.mark.parametrize("title, kw, basis, n, conf", [
    (S9, {}, "model", 12, "high"),
    (S9, {}, "model_season", 3, "high"),
    (S9, {}, "model", 2, "medium"),            # 근거 3건 미만
    (S9, {}, "family", 50, "medium"),
    (S9, {}, "brand_subcat", 50, "low"),
    (S9_NO_SEASON, {}, "model", 12, "medium"),  # B
    (FAMILY_ONLY, {}, "family", 100, "medium"),  # B
    (FAMILY_ONLY, {}, "subcategory", 100, "low"),
    (BRAND_ONLY, {}, "brand_subcat", 30, "low"),  # C
    (BRAND_ONLY, {}, "category", 500, "low"),
])
def test_deal_confidence_rules(enricher, fake_predict, title, kw, basis, n, conf):
    fake_predict(800_000, 1_000_000, 1_200_000, basis_level=basis, n=n)
    e = enricher.enrich(make_item(title, 700_000, **kw))
    assert e.basis_level == basis and e.n_basis == n
    assert e.deal_confidence == conf


@pytest.mark.parametrize("title", [S9, S9_NO_SEASON, FAMILY_ONLY, BRAND_ONLY])
def test_real_model_confidence_follows_rule(enricher, title):
    e = enricher.enrich(make_item(title, 700_000))
    strong = e.basis_level in ("model_season", "model") and (e.n_basis or 0) >= 3
    if e.id_grade == "A" and strong:
        expected = "high"
    elif e.id_grade in ("A", "B") and e.basis_level in ("model_season", "model", "family"):
        expected = "medium"
    else:
        expected = "low"
    assert e.deal_confidence == expected
    if e.id_grade == "C":
        assert e.deal_confidence == "low"


def test_price_in_man_won_units(enricher):
    e = enricher.enrich(make_item(S9, 150))  # 150 → 150만원
    assert e.price_krw == 1_500_000
    assert e.price_kind == "field_man"
    assert e.fair_p50


def test_price_from_body_when_field_missing(enricher):
    e = enricher.enrich(make_item(S9_NO_SEASON, None, body_content="상태 좋습니다. 가격은 120만원입니다."))
    assert e.price_krw == 1_200_000
    assert e.price_kind == "body"
    assert e.skipped_reason is None and e.fair_p50


def test_model_input_frame(enricher, fake_predict):
    calls = fake_predict(800_000, 1_000_000, 1_200_000)
    enricher.enrich(make_item(S9, 900_000, usage_count="5회"))
    assert len(calls) == 1
    row = calls[0].iloc[0]
    assert row["model_id"] == "atomic/redster/s9-fis"
    assert row["id_grade"] == "A"
    assert row["season_start"] == 2025
    assert row["observed_month"] == "2026-10"
    assert "price_krw" not in calls[0].columns  # 호가는 입력이 아니다(표적 누수 방지)
    for col in ("author", "phone", "email", "title", "body_content"):
        assert col not in calls[0].columns


# ------------------------------------------------------------------------------------ 적정가를 내지 않는 경우
@pytest.mark.parametrize("title, price, kw, listing_type, reason", [
    ("스키복 구합니다", None, {}, "wanted", "구매 희망 글"),
    ("스키복 구합니다", 300_000, {}, "wanted", "구매 희망 글"),
    ("스키 부츠 구매합니다 270", 300_000, {}, "wanted", "구매 희망 글"),
    ("[삽니다] 아토믹 레드스터 S9 FIS 165", 1_000_000, {}, "wanted", "구매 희망 글"),
    ("거래 시 주의사항 안내 공지", None, {}, "non_sale", "판매 글 아님"),
    ("아토믹 레드스터 S9 FIS 165 + 레드스터 CS 130 부츠 일괄", 1_500_000, {}, "sale", "여러 물건 묶음"),
    ("스키 + 부츠 + 바인딩 일괄 판매", 500_000, {}, "sale", "여러 물건 묶음"),
    ("하이원 시즌권 양도합니다", 400_000, {}, "sale", "장비 외 품목"),
    ("스키장 리프트권 판매", 50_000, {}, "sale", "장비 외 품목"),
    (S9_NO_SEASON, None, {}, "sale", "가격 없음"),
    (S9_NO_SEASON, 3_000, {}, "sale", "가격 없음"),
    ("오클리 고글 판매", 4_999, {}, "sale", "가격 없음"),
])
def test_skipped_reasons(enricher, fake_predict, title, price, kw, listing_type, reason):
    calls = fake_predict(800_000, 1_000_000, 1_200_000)
    e = enricher.enrich(make_item(title, price, **kw))
    assert e.error is None
    assert e.listing_type == listing_type
    assert e.skipped_reason == reason
    assert calls == []  # 적정가 모델을 부르지 않는다
    assert (e.fair_p10, e.fair_p50, e.fair_p90, e.basis_level, e.n_basis, e.explain) == (None,) * 6
    assert (e.deal_flag, e.deal_ratio, e.deal_confidence) == (None, None, None)


def test_wanted_post_still_identifies_model(enricher):
    e = enricher.enrich(make_item("[삽니다] 아토믹 레드스터 S9 FIS 165", 1_000_000))
    assert e.skipped_reason == "구매 희망 글"
    assert e.model_id == "atomic/redster/s9-fis"


def test_pass_ticket_is_other_category(enricher):
    e = enricher.enrich(make_item("하이원 시즌권 양도합니다", 400_000))
    assert e.category == "other" and e.category_ko == "기타"
    assert e.price_krw == 400_000


def test_min_price_boundary(enricher):
    e = enricher.enrich(make_item("오클리 고글 판매", EN.MIN_PRICE))
    assert e.skipped_reason is None and e.fair_p50


# ------------------------------------------------------------------------------------ 재게시(DB 연결)
def test_enrich_with_connection_finds_repost(enricher, seen_con):
    add_seen(seen_con, "100001", TS - 5 * DAY, S9, 1_700_000, author=FAKE_AUTHOR)
    e = enricher.enrich(make_item(S9, 1_500_000, id="100009"), seen_con)
    assert e.error is None
    assert isinstance(e.repost, RepostInfo)
    assert e.repost.prev_post_id == "100001" and e.repost.n_previous == 1
    assert e.repost.change_pct == round((1_500_000 / 1_700_000 - 1) * 100, 1)


def test_enrich_without_connection_has_no_repost(enricher):
    assert enricher.enrich(make_item(S9, 1_500_000)).repost is None


def test_repost_lookup_runs_for_skipped_listing(enricher, seen_con):
    add_seen(seen_con, "100001", TS - 5 * DAY, "스키복 구합니다", None, author=FAKE_AUTHOR)
    e = enricher.enrich(make_item("스키복 구합니다", None, id="100009"), seen_con)
    assert e.error is None and e.skipped_reason == "구매 희망 글"


# ------------------------------------------------------------------------------------ 저장 행(개인정보 없음)
def _table_columns(ddl: str, table: str) -> list[str]:
    con = sqlite3.connect(":memory:")
    con.execute(ddl)
    return [r[1] for r in con.execute(f"PRAGMA table_info({table})")]


PERSONAL = {"author", "author_hash", "nickname", "phone", "email", "title", "body_content", "features",
            "purchased_at", "url", "thumb", "region"}


def test_to_row_matches_table_and_has_no_personal_fields(enricher):
    item = make_item(S9, 1_500_000, phone=FAKE_PHONE, email=FAKE_EMAIL,
                     body_content=f"연락은 {FAKE_PHONE} 또는 {FAKE_EMAIL} 으로 주세요", features="엣지 상태 좋음",
                     region="서울 강남")
    e = enricher.enrich(item)
    row = e.to_row(item["id"])
    assert list(row) == _table_columns(ENRICHMENT_DDL, "enrichment")
    assert not (set(row) & PERSONAL)
    text = " ".join(str(v) for v in row.values())
    for secret in (FAKE_PHONE, FAKE_EMAIL, FAKE_AUTHOR, "서울 강남"):
        assert secret not in text
    assert row["post_id"] == item["id"]
    assert row["repost_of"] is None and row["repost_n"] == 0


def test_to_row_with_repost():
    e = Enrichment(price_krw=600_000, repost=RepostInfo("100002", 2, TS - 9 * DAY, TS - 2 * DAY, 700_000, -14.3))
    row = e.to_row("100009")
    assert (row["repost_of"], row["repost_n"], row["prev_price"], row["price_change_pct"]) == \
        ("100002", 2, 700_000, -14.3)


def test_save_enrichment_upserts(enricher):
    con = sqlite3.connect(":memory:")
    con.execute(ENRICHMENT_DDL)
    e = enricher.enrich(make_item(S9, 1_500_000))
    save_enrichment(con, "9900001", e)
    e.deal_flag = "fair"
    save_enrichment(con, "9900001", e)
    rows = con.execute("SELECT post_id, model_id, deal_flag, fair_p50 FROM enrichment").fetchall()
    assert rows == [("9900001", "atomic/redster/s9-fis", "fair", e.fair_p50)]


def test_as_dict_adds_labels():
    d = as_dict(Enrichment(category="boots", season_start=2024))
    assert d["category_ko"] == "부츠" and d["season_label"] == "24/25"
    assert {f.name for f in dc_fields(Enrichment)} <= set(d)


def test_default_enrichment_labels():
    e = Enrichment()
    assert e.category_ko is None and e.season_label is None and e.id_grade == "C"
    assert Enrichment(category="weird").category_ko == "weird"


# ------------------------------------------------------------------------------------ 실패 격리
def test_predict_failure_is_captured(enricher, monkeypatch):
    def boom(*a, **kw):
        raise RuntimeError("모델 고장")
    monkeypatch.setattr(enricher.model, "predict", boom)
    e = enricher.enrich(make_item(S9, 1_500_000))
    assert isinstance(e, Enrichment)
    assert e.error == "RuntimeError: 모델 고장"
    assert e.fair_p50 is None and e.deal_flag is None
    assert e.model_id == "atomic/redster/s9-fis"  # 식별 결과는 남는다
    assert e.elapsed_ms >= 0
    # 실패한 보강은 알림 필드·색·접두어에 영향을 주지 않는다
    assert DF.fields(e) == [] and DF.color(e) == DF.DEFAULT_COLOR and DF.title_prefix(e) == ""
    assert e.to_row("x")["error"] == "RuntimeError: 모델 고장"


def test_resolver_failure_is_captured(enricher, monkeypatch):
    def boom(*a, **kw):
        raise KeyError("alias")
    monkeypatch.setattr(enricher.resolver, "resolve", boom)
    e = enricher.enrich(make_item(S9, 1_500_000))
    assert e.error.startswith("KeyError")


def test_error_message_is_truncated(enricher, monkeypatch):
    def boom(*a, **kw):
        raise ValueError("x" * 2000)
    monkeypatch.setattr(enricher.model, "predict", boom)
    assert len(enricher.enrich(make_item(S9, 1_500_000)).error) <= 300


def test_broken_connection_is_captured(enricher):
    # 재게시 조회만 실패: 계산한 식별·적정가는 그대로 쓰고(error 없음) 사유는 extra에 남긴다
    con = sqlite3.connect(":memory:")
    con.close()
    e = enricher.enrich(make_item(S9, 1_500_000), con)
    assert e.error is None and e.repost is None
    assert e.fair_p50 and e.deal_flag
    assert e.extra["repost_error"].startswith("ProgrammingError")
    assert [f["name"] for f in DF.fields(e)][:2] == ["모델", "적정가"]  # 알림에도 적정가가 나간다


def test_missing_seen_table_is_captured(enricher):
    con = sqlite3.connect(":memory:")
    e = enricher.enrich(make_item(S9, 1_500_000), con)
    assert e.error is None and e.repost is None and e.fair_p50
    assert "seen" in e.extra["repost_error"]


def test_repost_lookup_exception_keeps_fair_price(enricher, seen_con, monkeypatch):
    from pricing import repost as R

    def boom(*a, **kw):
        raise RuntimeError("repost broken")
    monkeypatch.setattr(R, "find_previous", boom)
    e = enricher.enrich(make_item(S9, 1_500_000), seen_con)
    assert e.error is None and e.repost is None and e.fair_p50
    assert e.extra["repost_error"] == "RuntimeError: repost broken"
    assert e.to_row("x")["error"] is None


def test_price_failure_still_runs_repost_lookup(enricher, seen_con, monkeypatch):
    def boom(*a, **kw):
        raise RuntimeError("모델 고장")
    monkeypatch.setattr(enricher.model, "predict", boom)
    add_seen(seen_con, "100001", TS - 5 * DAY, S9, 1_700_000, author=FAKE_AUTHOR)
    e = enricher.enrich(make_item(S9, 1_500_000, id="100009"), seen_con)
    assert e.error == "RuntimeError: 모델 고장"
    assert e.repost is not None and e.repost.prev_post_id == "100001"


@pytest.mark.parametrize("item", [
    {},
    {"id": "1", "title": None},
    {"id": "2", "title": S9_NO_SEASON, "raw_price": "abc"},
    {"id": "3", "title": S9, "raw_price": 1_500_000, "observed_at": None},
    {"id": "4", "title": "", "raw_price": 0, "body_content": None},
])
def test_odd_items_never_raise(enricher, item):
    e = enricher.enrich(item)
    assert isinstance(e, Enrichment)
    assert e.error is None or isinstance(e.error, str)


# ------------------------------------------------------------------------------------ from_env
def test_from_env_disabled(monkeypatch):
    for v in ("0", "false", "off", "no", " OFF "):
        monkeypatch.setenv("DRSPARK_PRICING", v)
        assert Enricher.from_env() is None


def test_from_env_missing_dir(monkeypatch, tmp_path):
    monkeypatch.delenv("DRSPARK_PRICING", raising=False)
    monkeypatch.setenv("DRSPARK_PRICING_DIR", str(tmp_path / "nope"))
    assert Enricher.from_env() is None


def test_from_env_broken_model_file(monkeypatch, tmp_path):
    (tmp_path / "fairprice.pkl").write_bytes(b"not a pickle")
    monkeypatch.delenv("DRSPARK_PRICING", raising=False)
    monkeypatch.setenv("DRSPARK_PRICING_DIR", str(tmp_path))
    assert Enricher.from_env() is None  # 읽기 실패도 알림을 막지 않는다


def test_from_env_uses_dir(monkeypatch, tmp_path):
    (tmp_path / "fairprice.pkl").write_bytes(b"x")
    seen = []
    monkeypatch.setattr(Enricher, "__init__", lambda self, d: seen.append(d))
    monkeypatch.setenv("DRSPARK_PRICING", "1")
    monkeypatch.setenv("DRSPARK_PRICING_DIR", str(tmp_path))
    assert isinstance(Enricher.from_env(), Enricher)
    assert [str(p) for p in seen] == [str(tmp_path)]


def test_from_env_default_dir(monkeypatch):
    if not (DATA_DIR / "fairprice.pkl").exists():
        pytest.skip("pricing_data 없음")
    seen = []
    monkeypatch.setattr(Enricher, "__init__", lambda self, d: seen.append(d))
    monkeypatch.delenv("DRSPARK_PRICING", raising=False)
    monkeypatch.delenv("DRSPARK_PRICING_DIR", raising=False)
    assert isinstance(Enricher.from_env(), Enricher)
    assert [str(p) for p in seen] == [str(DATA_DIR)]


@pytest.mark.parametrize("value", ["", "   "])
def test_from_env_empty_dir_means_default(monkeypatch, value):
    # .env에 'DRSPARK_PRICING_DIR='만 남아 있어도 현재 폴더('.')가 아니라 기본 폴더를 쓴다
    if not (DATA_DIR / "fairprice.pkl").exists():
        pytest.skip("pricing_data 없음")
    seen = []
    monkeypatch.setattr(Enricher, "__init__", lambda self, d: seen.append(d))
    monkeypatch.delenv("DRSPARK_PRICING", raising=False)
    monkeypatch.setenv("DRSPARK_PRICING_DIR", value)
    assert isinstance(Enricher.from_env(), Enricher)
    assert [str(p) for p in seen] == [str(DATA_DIR)]


# ------------------------------------------------------------------------------------ 불확실한 호가(F1·AQ-1)
def test_extra_zero_fixed_price_is_reference_only(enricher, fake_predict):
    # 360만원 입력 → 파서가 0 하나 더 붙은 것으로 보고 36만원으로 고침. 학습에서는 이런 글을 뺐다
    fake_predict(800_000, 1_000_000, 1_200_000)
    e = enricher.enrich(make_item(S9 + " 신품", 3_600_000))
    assert e.price_krw == 360_000 and e.price_kind == "field_fixed"
    assert "extra_zero_fixed" in e.extra["price_flags"] and e.price_uncertain
    assert e.deal_flag == "undervalued" and e.deal_ratio == 0.36  # 판정 자체는 남긴다
    assert e.deal_confidence == "low"
    assert not DF.is_deal(e) and DF.title_prefix(e) == ""
    row = e.to_row("x")
    assert (row["price_kind"], row["price_flags"], row["deal_confidence"]) == ("field_fixed", "extra_zero_fixed", "low")
    # 임베드에 판정에 쓴 호가(36만원)를 밝힌다(가격 필드는 입력값 3,600,000원 그대로)
    v = {f["name"]: f["value"] for f in DF.fields(e)}["적정가"]
    assert v.startswith("판정 호가 36만원(입력값 보정 · 단위·오기 불확실 → 판정은 참고용)\n")
    assert "· 참고용" in v and "신뢰 높음" not in v


def test_correct_low_price_is_still_a_deal(enricher, fake_predict):
    fake_predict(800_000, 1_000_000, 1_200_000)
    e = enricher.enrich(make_item(S9 + " 신품", 360_000))
    assert e.price_kind == "field" and not e.price_uncertain
    assert (e.deal_flag, e.deal_confidence) == ("undervalued", "high")
    assert DF.is_deal(e)
    assert DF.price_basis_line(e) is None


@pytest.mark.parametrize("title, raw, kw, price, kind, uncertain", [
    (S9, 50, {}, 500_000, "field_man", True),                                      # '50' → 50만원으로 가정
    (S9, 65, {"body_content": "65만원에 팝니다"}, 650_000, "field_man", False),     # 본문이 단위를 확인
    (S9, None, {"body_content": "가격은 70만원입니다"}, 700_000, "body", False),    # 가격 칸 없음 → 본문 가격
    (S9, 100_000, {"body_content": "정가 253만원 짜리 팝니다"}, 100_000, "field", True),  # 정가 대비 너무 낮음
])
def test_price_uncertain_rule_matches_training(enricher, fake_predict, title, raw, kw, price, kind, uncertain):
    fake_predict(800_000, 1_000_000, 1_200_000)
    e = enricher.enrich(make_item(title, raw, **kw))
    assert (e.price_krw, e.price_kind, e.price_uncertain) == (price, kind, uncertain)
    assert e.deal_confidence == ("low" if uncertain else "high")


def test_suspect_low_only_uncertain_for_race_gear():
    assert EN.is_price_uncertain(("suspect_low",), "ski_race_sl")
    assert EN.is_price_uncertain(("suspect_low",), "boots_race")
    assert not EN.is_price_uncertain(("suspect_low",), "ski_allmountain")
    assert not EN.is_price_uncertain(("suspect_low",), None)
    assert not EN.is_price_uncertain(("body_differs", "trailing_noise_fixed", "unit_body_confirmed"), "ski_race_sl")
    assert EN.is_price_uncertain(("unit_assumed",), None) and EN.is_price_uncertain(("retail_gap",), "apparel")


# ------------------------------------------------------------------------------------ 아주 긴 본문(SEC-2)
@pytest.mark.parametrize("body", ["1" * 200_000, "1." * 100_000, "가" * 100_000], ids=["digits", "dots", "hangul"])
def test_huge_body_without_spaces_is_fast(enricher, body):
    import time as _time
    t0 = _time.perf_counter()
    e = enricher.enrich(make_item(S9, 1_500_000, body_content=body, features=body))
    elapsed = _time.perf_counter() - t0
    assert e.error is None and e.model_id == "atomic/redster/s9-fis"
    assert elapsed < 5, f"{elapsed:.1f}s"  # 자르기 전에는 '1'×200k 본문 하나에 1분 가까이 걸렸다


def test_clip_text_limits():
    assert EN.clip_text("body_content", "a" * 9000) == "a" * EN.SCRUB_LIMIT["body_content"]
    assert EN.clip_text("title", "a" * 9000) == "a" * EN.SCRUB_LIMIT_DEFAULT
    assert EN.clip_text("title", None) is None and EN.clip_text("title", 5) == 5


# ------------------------------------------------------------------------------------ 학습 밖 게시 월 라벨(AQ-4)
MONTHS = ["2026-03", "2026-04", "2026-10"]


@pytest.mark.parametrize("explain, month, expected", [
    ("기준 50만원 · 11월 게시 +14% → 60만원", "2026-11", "기준 50만원 · 게시 월(10월 기준) +14% → 60만원"),
    ("1월 게시 +14% · 길이 165cm +3%", "2027-01", "게시 월(10월 기준) +14% · 길이 165cm +3%"),
    ("3월 게시 +14%", "2027-03", "게시 월(10월 기준) +14%"),             # 학습한 3월과 이름만 같은 다른 해
    ("10월 게시 +14%", "2026-10", "10월 게시 +14%"),                     # 학습 범위 안: 그대로
    ("4월 게시 −11%", "2026-04", "4월 게시 −11%"),
    ("길이 165cm +3%", "2026-11", "길이 165cm +3%"),                    # 월 요인이 근거에 없음
    ("게시 월(10월 기준) +14%", "2026-11", "게시 월(10월 기준) +14%"),   # 분석 저장소가 고친 뒤: 그대로
    (None, "2026-11", None),
])
def test_fix_month_label(explain, month, expected):
    assert EN.fix_month_label(explain, month, MONTHS) == expected
    assert EN.fix_month_label(explain, month, None) == explain


def test_month_label_after_training_period(enricher):
    nov = int(dt.datetime(2026, 11, 15, 12, 0, tzinfo=KST).timestamp())
    e = enricher.enrich(make_item(S9, 1_500_000, observed_at=nov))
    oct_ = enricher.enrich(make_item(S9, 1_500_000))
    assert e.fair_p50 == oct_.fair_p50  # 값은 10월 효과 그대로(라벨만 바뀐다)
    assert "11월 게시" not in e.explain
    if "10월 게시" in oct_.explain:
        assert "게시 월(10월 기준)" in e.explain
