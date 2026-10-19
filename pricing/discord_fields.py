"""Enrichment → Discord 임베드 필드·색·제목 접두어.

Discord 제한: 필드 값 ≤ 1024자, 필드 ≤ 25개, 임베드 전체 ≤ 6000자. 여기서는 필드마다 짧게 자른다.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .enrich import Enrichment

KST = timezone(timedelta(hours=9))

FLAG_LABEL = {
    "undervalued": "▼ 저평가",
    "fair": "● 적정 범위",
    "overvalued": "▲ 고평가",
    "price_typo": "? 가격 확인 필요(오기·다른 품목 의심)",
}
COLOR = {
    "undervalued": 0x2ECC71,  # 초록
    "fair": 0x3498DB,         # 기존 알림 색(파랑)
    "overvalued": 0xE67E22,   # 주황
    "price_typo": 0x95A5A6,   # 회색
}
DEFAULT_COLOR = 0x3498DB
CONF_LABEL = {"high": "신뢰 높음", "medium": "신뢰 보통", "low": "참고용"}
# 판정에 쓴 호가가 '가격' 필드(입력값 그대로)와 다를 때 붙이는 설명(price_kind별)
PRICE_KIND_NOTE = {"field_man": "만원 단위로 해석", "field_cheon": "천원 단위로 해석", "field_fixed": "입력값 보정",
                   "body": "본문 가격"}
# 이만큼(%) 넘게 내린 재게시만 [가격인하]로 본다(소폭 인하·끌어올리기용 재게시 제외)
PRICE_CUT_MIN_PCT = 5.0
GRADE_LABEL = {"A": "모델·시즌 확인", "B": "모델군 확인", "C": "브랜드·종류만"}
BASIS_LABEL = {"model_season": "같은 모델·시즌", "model": "같은 모델", "family": "같은 모델군",
               "brand_subcat": "같은 브랜드·종류", "subcategory": "같은 종류", "category": "같은 카테고리"}


def won(v) -> str:
    """원 → 만원 표기(625,000 → 62.5만원, 1,250,000 → 125만원)."""
    if v is None:
        return "—"
    v = float(v)
    if abs(v) < 10_000:
        return f"{v:,.0f}원"
    m = v / 10_000
    return f"{m:,.0f}만원" if m >= 100 or float(m).is_integer() else f"{m:,.1f}만원"


def _date(ts: int | None) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(ts, tz=KST).strftime("%m/%d")


def _pretty_id(x: str | None) -> str | None:
    if not x:
        return None
    return " ".join(p.replace("-", " ").title() for p in x.split("/"))


def model_line(e: Enrichment) -> str:
    name = e.model_name or _pretty_id(e.family_id) or _pretty_id(e.brand) or "모델 미확인"
    parts = [name]
    if e.season_label:
        parts.append(f"{e.season_label} 시즌")
    if e.category_ko:
        parts.append(e.category_ko)
    parts.append(f"식별 {e.id_grade}({GRADE_LABEL.get(e.id_grade, '')})")
    return " · ".join(parts)


def price_basis_line(e: Enrichment) -> str | None:
    """판정에 쓴 호가가 '가격' 필드와 다르거나(단위 보정·본문 가격) 불확실하면 그 값을 밝히는 줄. 아니면 None."""
    if not e.price_krw:
        return None
    notes = []
    if e.price_kind and e.price_kind != "field":
        notes.append(PRICE_KIND_NOTE.get(e.price_kind, "보정"))
    if e.price_uncertain:
        notes.append("단위·오기 불확실 → 판정은 참고용")
    if not notes:
        return None
    return f"판정 호가 {won(e.price_krw)}({' · '.join(notes)})"


def _won_pair(a, b) -> tuple[str, str]:
    """두 값이 만원 표기로 같아 보이면(160만원 → 160만원) 원 단위로 쓴다."""
    if won(a) == won(b) and a != b:
        return f"{int(a):,}원", f"{int(b):,}원"
    return won(a), won(b)


def fields(e: Enrichment | None) -> list[dict]:
    """알림에 덧붙일 필드 목록. e가 없거나 보강이 실패했으면 빈 목록."""
    if e is None or e.error:
        return []
    out = [{"name": "모델", "value": model_line(e)[:1024], "inline": False}]
    if e.fair_p50:
        lines = [f"**{won(e.fair_p50)}** (80% 범위 {won(e.fair_p10)} ~ {won(e.fair_p90)})"]
        basis_line = price_basis_line(e)
        if basis_line:
            lines.insert(0, basis_line)
        verdict = FLAG_LABEL.get(e.deal_flag, "—")
        if e.deal_ratio is not None:
            verdict += f" · 호가/적정가 {e.deal_ratio:.2f}"
        verdict += f" · {CONF_LABEL.get(e.deal_confidence, '참고용')}"
        lines.append(verdict)
        if e.msrp_krw and e.price_krw:
            lines.append(f"정가 {won(e.msrp_krw)} 대비 {e.price_krw / e.msrp_krw:.0%}")
        out.append({"name": "적정가", "value": "\n".join(lines)[:1024], "inline": False})
        if e.explain:
            basis = BASIS_LABEL.get(e.basis_level, "")
            head = f"{basis} {e.n_basis}건 기준 · " if basis and e.n_basis else ""
            note = "\n(요인별 %는 같은 종류 평균 매물 대비)"
            out.append({"name": "적정가 근거", "value": (head + e.explain)[: 500 - len(note)] + note, "inline": False})
    elif e.skipped_reason:
        out.append({"name": "적정가", "value": f"산출 안 함({e.skipped_reason})", "inline": False})
    r = e.repost
    if r is not None:
        text = f"같은 판매자의 이전 글 {r.n_previous}건 · 처음 {_date(r.first_seen_at)} · 직전 {_date(r.prev_seen_at)}"
        if r.prev_price and e.price_krw:
            prev_txt, now_txt = _won_pair(r.prev_price, e.price_krw)
            text += f"\n직전 호가 {prev_txt} → 이번 {now_txt}"
            if r.change_pct is not None and r.change_pct != 0:
                text += f" ({r.change_pct:+.1f}%)"
        out.append({"name": "재게시", "value": text[:1024], "inline": False})
    return out


def color(e: Enrichment | None) -> int:
    if e is None or e.error or not e.deal_flag:
        return DEFAULT_COLOR
    return COLOR.get(e.deal_flag, DEFAULT_COLOR)


def is_deal(e: Enrichment | None) -> bool:
    """알림을 강조할 만한 저평가: 판정이 저평가이고 신뢰가 '참고용'(C등급 등)이 아니며 이미 거래완료가 아닌 글."""
    return bool(e and not e.error and e.deal_flag == "undervalued" and e.deal_confidence in ("high", "medium")
                and e.trade_status != "거래완료")


def is_price_cut(e: Enrichment | None) -> bool:
    """같은 판매자가 직전 글보다 PRICE_CUT_MIN_PCT% 이상 내려 다시 올린 글."""
    return bool(e and not e.error and e.repost and e.repost.change_pct is not None
                and e.repost.change_pct <= -PRICE_CUT_MIN_PCT)


def title_prefix(e: Enrichment | None) -> str:
    tags = []
    if is_deal(e):
        tags.append("[저평가]")
    if is_price_cut(e):
        tags.append("[가격인하]")
    return (" ".join(tags) + " ") if tags else ""
