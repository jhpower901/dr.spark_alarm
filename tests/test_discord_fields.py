"""pricing.discord_fields: 만원 표기, 임베드 필드·길이 제한, 색, 제목 접두어, 저평가·가격인하 판정."""
from __future__ import annotations

import datetime as dt

import pytest

from pricing import discord_fields as DF
from pricing.enrich import Enrichment, RepostInfo
from tests.conftest import KST


def _ts(month, day):
    return int(dt.datetime(2026, month, day, 12, 0, tzinfo=KST).timestamp())


def full(**kw) -> Enrichment:
    """모델+시즌 확인(A), 동일 모델 12건 근거, 저평가·신뢰 높음."""
    base = dict(
        listing_type="sale", category="ski", subcategory="ski_race_sl", brand="atomic", family_id="atomic/redster",
        model_id="atomic/redster/s9-fis", model_name="Atomic Redster S9 FIS M", season_start=2025, id_grade="A",
        price_krw=600_000, price_kind="field", fair_p10=780_000, fair_p50=971_000, fair_p90=1_224_000,
        basis_level="model", n_basis=12,
        explain="동일 모델 12건 중앙값 55만원 · 기준 59만원 · 당해 시즌 +52% · A급 +6% → 97만원",
        deal_flag="undervalued", deal_ratio=0.618, deal_confidence="high", msrp_krw=2_530_000,
        pricing_version="fairprice-1.0@test")
    base.update(kw)
    return Enrichment(**base)


def repost(**kw) -> RepostInfo:
    base = dict(prev_post_id="300002", n_previous=2, first_seen_at=_ts(10, 1), prev_seen_at=_ts(10, 7),
                prev_price=700_000, change_pct=-14.3)
    base.update(kw)
    return RepostInfo(**base)


def by_name(fs):
    return {f["name"]: f["value"] for f in fs}


# ------------------------------------------------------------------------------------ won()
@pytest.mark.parametrize("v, text", [
    (None, "—"),
    (0, "0원"),
    (5_000, "5,000원"),
    (9_999, "9,999원"),
    (10_000, "1만원"),
    (15_000, "1.5만원"),
    (600_000, "60만원"),
    (625_000, "62.5만원"),
    (625_000.0, "62.5만원"),
    (971_000, "97.1만원"),
    (1_000_000, "100만원"),
    (1_250_000, "125만원"),
    (2_530_000, "253만원"),
    (12_340_000, "1,234만원"),
    (-50_000, "-5만원"),
])
def test_won(v, text):
    assert DF.won(v) == text


# ------------------------------------------------------------------------------------ fields()
def test_fields_none_or_error():
    assert DF.fields(None) == []
    assert DF.fields(full(error="RuntimeError: boom")) == []


def test_fields_full():
    fs = DF.fields(full())
    assert [f["name"] for f in fs] == ["모델", "적정가", "적정가 근거"]
    assert all(f["inline"] is False for f in fs)
    v = by_name(fs)
    assert v["모델"] == "Atomic Redster S9 FIS M · 25/26 시즌 · 스키 · 식별 A(모델·시즌 확인)"
    assert v["적정가"] == ("**97.1만원** (80% 범위 78만원 ~ 122만원)\n"
                          "▼ 저평가 · 호가/적정가 0.62 · 신뢰 높음\n"
                          "정가 253만원 대비 24%")
    assert v["적정가 근거"].startswith("같은 모델 12건 기준 · 동일 모델 12건 중앙값 55만원")
    assert v["적정가 근거"].endswith("(요인별 %는 같은 종류 평균 매물 대비)")


@pytest.mark.parametrize("flag, conf, verdict", [
    ("fair", "medium", "● 적정 범위 · 호가/적정가 0.62 · 신뢰 보통"),
    ("overvalued", "low", "▲ 고평가 · 호가/적정가 0.62 · 참고용"),
    ("price_typo", "high", "? 가격 확인 필요(오기·다른 품목 의심) · 호가/적정가 0.62 · 신뢰 높음"),
    (None, None, "— · 호가/적정가 0.62 · 참고용"),
])
def test_fields_verdict_line(flag, conf, verdict):
    lines = by_name(DF.fields(full(deal_flag=flag, deal_confidence=conf)))["적정가"].split("\n")
    assert lines[1] == verdict


def test_fields_without_ratio_msrp_or_explain():
    fs = DF.fields(full(deal_ratio=None, msrp_krw=None, explain=None))
    assert [f["name"] for f in fs] == ["모델", "적정가"]
    assert by_name(fs)["적정가"] == "**97.1만원** (80% 범위 78만원 ~ 122만원)\n▼ 저평가 · 신뢰 높음"


def test_fields_msrp_needs_price():
    v = by_name(DF.fields(full(price_krw=None)))["적정가"]
    assert "대비" not in v and len(v.split("\n")) == 2


def test_fields_explain_without_basis():
    v = by_name(DF.fields(full(basis_level=None, n_basis=None)))["적정가 근거"]
    assert v.startswith("동일 모델 12건 중앙값")


def test_fields_skipped():
    e = Enrichment(listing_type="wanted", category="apparel", subcategory="apparel_set",
                   skipped_reason="구매 희망 글")
    fs = DF.fields(e)
    assert [f["name"] for f in fs] == ["모델", "적정가"]
    v = by_name(fs)
    assert v["모델"] == "모델 미확인 · 의류 · 식별 C(브랜드·종류만)"
    assert v["적정가"] == "산출 안 함(구매 희망 글)"


def test_fields_no_price_and_no_reason_only_model():
    assert [f["name"] for f in DF.fields(Enrichment(category="ski", brand="atomic"))] == ["모델"]


@pytest.mark.parametrize("kw, name", [
    (dict(family_id="atomic/redster", brand="atomic"), "Atomic Redster"),
    (dict(family_id="rossignol/hero-world-cup", brand="rossignol"), "Rossignol Hero World Cup"),
    (dict(brand="rossignol"), "Rossignol"),
    (dict(brand="sweet-protection"), "Sweet Protection"),
    (dict(), "모델 미확인"),
])
def test_model_line_fallbacks(kw, name):
    e = Enrichment(id_grade="B" if kw.get("family_id") else "C", **kw)
    assert DF.model_line(e).split(" · ")[0] == name


def test_model_line_grades():
    assert DF.model_line(Enrichment(id_grade="B", family_id="a/b", category="boots")) == \
        "A B · 부츠 · 식별 B(모델군 확인)"


def test_fields_repost_price_cut():
    fs = DF.fields(full(repost=repost()))
    assert [f["name"] for f in fs][-1] == "재게시"
    assert by_name(fs)["재게시"] == ("같은 판매자의 이전 글 2건 · 처음 10/01 · 직전 10/07\n"
                                   "직전 호가 70만원 → 이번 60만원 (-14.3%)")


def test_fields_repost_same_price_has_no_percent():
    v = by_name(DF.fields(full(price_krw=700_000, repost=repost(change_pct=0.0))))["재게시"]
    assert v.endswith("직전 호가 70만원 → 이번 70만원")


def test_fields_repost_small_change_shows_exact_won():
    # 160만원 → 159.5만원(−0.3%)을 만원으로 반올림하면 '160만원 → 160만원'이 되어 읽을 수 없다
    v = by_name(DF.fields(full(price_krw=1_595_000, repost=repost(prev_price=1_600_000, change_pct=-0.3))))["재게시"]
    assert v.endswith("직전 호가 1,600,000원 → 이번 1,595,000원 (-0.3%)")


# ------------------------------------------------------------------------------------ 판정에 쓴 호가 줄
@pytest.mark.parametrize("kw, line", [
    (dict(price_kind="field"), None),
    (dict(price_kind=None), None),
    (dict(price_kind="field_fixed", price_krw=360_000, extra={"price_uncertain": True}),
     "판정 호가 36만원(입력값 보정 · 단위·오기 불확실 → 판정은 참고용)"),
    (dict(price_kind="field_man", price_krw=650_000), "판정 호가 65만원(만원 단위로 해석)"),
    (dict(price_kind="field_cheon", price_krw=65_000), "판정 호가 6.5만원(천원 단위로 해석)"),
    (dict(price_kind="body", price_krw=700_000), "판정 호가 70만원(본문 가격)"),
    (dict(price_kind="field", price_krw=100_000, extra={"price_uncertain": True}),
     "판정 호가 10만원(단위·오기 불확실 → 판정은 참고용)"),
    (dict(price_kind="something_new", price_krw=100_000), "판정 호가 10만원(보정)"),
    (dict(price_kind="body", price_krw=None), None),
])
def test_price_basis_line(kw, line):
    e = full(**kw)
    assert DF.price_basis_line(e) == line
    v = by_name(DF.fields(e))["적정가"]
    if line:
        assert v.split("\n")[0] == line and v.split("\n")[1].startswith("**97.1만원**")
    else:
        assert v.startswith("**97.1만원**")


def test_fields_repost_price_rise():
    v = by_name(DF.fields(full(price_krw=750_000, repost=repost(change_pct=7.1))))["재게시"]
    assert v.endswith("(+7.1%)")


def test_fields_repost_without_prices():
    v = by_name(DF.fields(full(repost=repost(prev_price=None, change_pct=None))))["재게시"]
    assert v == "같은 판매자의 이전 글 2건 · 처음 10/01 · 직전 10/07"
    v = by_name(DF.fields(full(price_krw=None, repost=repost())))["재게시"]
    assert "\n" not in v


def test_fields_repost_on_skipped_listing():
    e = Enrichment(listing_type="sale", skipped_reason="가격 없음", repost=repost(first_seen_at=None))
    names = [f["name"] for f in DF.fields(e)]
    assert names == ["모델", "적정가", "재게시"]
    assert "처음 —" in by_name(DF.fields(e))["재게시"]


def test_field_length_limits():
    e = full(model_name="X" * 3000, explain="가" * 5000, repost=repost(n_previous=10 ** 6))
    fs = DF.fields(e)
    assert len(fs) <= 25
    for f in fs:
        assert 0 < len(f["name"]) <= 256
        assert 0 < len(f["value"]) <= 1024
    v = by_name(fs)
    assert len(v["모델"]) == 1024
    assert len(v["적정가 근거"]) <= 500
    assert v["적정가 근거"].endswith("(요인별 %는 같은 종류 평균 매물 대비)")


# ------------------------------------------------------------------------------------ color / is_deal / prefix
@pytest.mark.parametrize("e, colour", [
    (None, 0x3498DB),
    (full(error="x"), 0x3498DB),
    (full(deal_flag=None), 0x3498DB),
    (full(deal_flag="undervalued"), 0x2ECC71),
    (full(deal_flag="fair"), 0x3498DB),
    (full(deal_flag="overvalued"), 0xE67E22),
    (full(deal_flag="price_typo"), 0x95A5A6),
    (full(deal_flag="something-new"), 0x3498DB),
])
def test_color(e, colour):
    assert DF.color(e) == colour


def test_default_color_is_original_blue():
    assert DF.DEFAULT_COLOR == 0x3498DB


@pytest.mark.parametrize("e, deal", [
    (None, False),
    (full(), True),
    (full(deal_confidence="medium"), True),
    (full(deal_confidence="low"), False),
    (full(deal_confidence=None), False),
    (full(deal_flag="fair"), False),
    (full(deal_flag="price_typo"), False),
    (full(error="RuntimeError: x"), False),
])
def test_is_deal(e, deal):
    assert DF.is_deal(e) is deal


@pytest.mark.parametrize("e, cut", [
    (None, False),
    (full(), False),
    (full(repost=repost(change_pct=-5.0)), True),        # 5% 이상 인하부터
    (full(repost=repost(change_pct=-14.3)), True),
    (full(repost=repost(change_pct=-4.9)), False),       # 소폭 인하(끌어올리기용 재게시)는 아님
    (full(repost=repost(change_pct=-0.3)), False),
    (full(repost=repost(change_pct=0.0)), False),
    (full(repost=repost(change_pct=3.0)), False),
    (full(repost=repost(change_pct=None)), False),
    (full(error="x", repost=repost(change_pct=-20.0)), False),
])
def test_is_price_cut(e, cut):
    assert DF.is_price_cut(e) is cut


@pytest.mark.parametrize("e, prefix", [
    (None, ""),
    (full(error="x"), ""),
    (full(), "[저평가] "),
    (full(deal_confidence="low"), ""),
    (full(deal_flag="fair", repost=repost()), "[가격인하] "),
    (full(repost=repost()), "[저평가] [가격인하] "),
    (full(deal_flag="overvalued", repost=repost(change_pct=10.0)), ""),
    (full(deal_flag="overvalued", repost=repost(change_pct=-0.3)), ""),
    (full(deal_confidence="low", extra={"price_uncertain": True}, repost=repost()), "[가격인하] "),
])
def test_title_prefix(e, prefix):
    assert DF.title_prefix(e) == prefix
