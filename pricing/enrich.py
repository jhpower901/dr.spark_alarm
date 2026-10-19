"""새 매물 1건 → 모델 식별 + 적정가(P10/P50/P90) + 저평가·고평가 판정 + 재게시 이력.

drspark 패키지(분석 저장소에서 export_alarm_bundle.py로 동기화한 규칙·리졸버·적정가 모듈)와
pricing_data/(canonical 사전, 별칭, 적정가 모델)를 쓴다. LLM은 부르지 않는다.

이 모듈의 실패가 알림을 막으면 안 된다: Enricher.enrich()는 예외를 밖으로 던지지 않고
식별·적정가 단계 실패는 Enrichment.error에, 재게시 조회 실패는 Enrichment.extra['repost_error']에 담아 돌려준다
(재게시 조회가 실패해도 계산한 적정가는 그대로 알림에 쓴다).
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger("drspark.pricing")

KST = timezone(timedelta(hours=9))  # 한국은 서머타임이 없어 고정 오프셋으로 충분하다(Windows에 tzdata가 없어도 동작)
MIN_PRICE = 5_000
# 적정가 모델 학습에서 뺀 카테고리(장비가 아닌 품목·회원권 등이 섞여 정확도가 낮다)
NO_PRICE_CATEGORIES = {"other"}
# 호가/P50이 이 범위를 벗어나면 저평가·고평가가 아니라 가격 오기·품목 오인 가능성이 크다(분석 보고서 '저평가·고평가 매물')
TYPO_LOW, TYPO_HIGH = 0.3, 3.0
# 학습에서 '가격 불확실'로 뺀 호가(분석 저장소 p2_build_listings의 price_uncertain과 같은 규칙):
# ÷10 보정(extra_zero_fixed)·만원 단위 가정(unit_assumed)·본문 정가 대비 지나치게 낮음(retail_gap),
# 레이스 스키·부츠의 5만원 미만 호가(suspect_low). 이런 호가로 낸 판정은 신뢰를 '참고용'으로 낮춘다.
UNCERTAIN_PRICE_FLAGS = {"extra_zero_fixed", "unit_assumed", "retail_gap"}
_RACE_SUBCATEGORY = re.compile(r"(?:ski|boots)_race")
# privacy.scrub_text의 이메일 정규식은 공백 없이 긴 문자열에서 시간이 제곱으로 는다. 스크럽하기 전에 필드 길이를 자른다
# (과거 5,584건 최대: 본문 1,250자, 나머지 필드 236자 이하 → 결과는 바뀌지 않는다).
SCRUB_LIMIT = {"body_content": 5_000}
SCRUB_LIMIT_DEFAULT = 1_000


def clip_text(key: str, v):
    """보강에 넘기는 글 필드의 길이를 제한한다(문자열이 아니면 그대로)."""
    return v[: SCRUB_LIMIT.get(key, SCRUB_LIMIT_DEFAULT)] if isinstance(v, str) else v


def is_price_uncertain(flags, subcategory: str | None) -> bool:
    """학습의 price_uncertain 규칙: 단위 보정·가정 또는 오기 의심 호가."""
    f = set(flags or ())
    return bool(f & UNCERTAIN_PRICE_FLAGS) or ("suspect_low" in f and bool(_RACE_SUBCATEGORY.match(subcategory or "")))


def fix_month_label(explain: str | None, month: str | None, months) -> str | None:
    """학습에 없던 게시 월은 모델이 마지막 학습 월의 효과로 계산하는데(drspark.fairprice._Adjust),
    근거 줄은 실제 달 이름('11월 게시 +14%')을 붙인다. 값은 그대로 두고 라벨만 '게시 월(10월 기준)'으로 바꾼다.
    분석 저장소에서 라벨을 고쳐 다시 내보내면 바꿀 문구가 없어져 아무 일도 하지 않는다."""
    if not explain or not month or not months or month in months:
        return explain
    try:
        mm, last = int(str(month)[-2:]), int(str(months[-1])[-2:])
    except ValueError:
        return explain
    return re.sub(rf"(?<!\d){mm}월 게시", f"게시 월({last}월 기준)", explain)

CATEGORY_KO = {
    "ski": "스키", "boots": "부츠", "binding": "바인딩/플레이트", "snowboard": "스노보드", "apparel": "의류",
    "gloves": "장갑", "helmet": "헬멧", "goggle": "고글", "protector": "보호대", "poles": "폴", "bag": "가방",
    "tuning": "튜닝용품", "other": "기타",
}


@dataclass
class RepostInfo:
    prev_post_id: str            # 같은 매물로 판단한 직전 글
    n_previous: int              # 같은 판매자가 같은 매물로 올린 이전 글 수(180일 이내)
    first_seen_at: int           # 그중 가장 이른 관측 시각(epoch)
    prev_seen_at: int
    prev_price: int | None       # 직전 글 호가(단위 보정 후)
    change_pct: float | None     # (이번 호가 / 직전 호가 − 1) × 100


@dataclass
class Enrichment:
    listing_type: str = "sale"                 # sale / wanted / non_sale
    category: str | None = None
    subcategory: str | None = None
    brand: str | None = None
    family_id: str | None = None
    model_id: str | None = None
    model_name: str | None = None              # 공식 모델명(사전) 또는 None
    season_start: int | None = None            # 24/25 → 2024
    id_grade: str = "C"                        # A 모델+시즌 / B 패밀리 / C 브랜드·카테고리
    price_krw: int | None = None               # 단위 보정 호가(판정에 쓴 값)
    price_kind: str | None = None              # field(입력값 그대로) / field_man / field_cheon / field_fixed / body …
    fair_p10: int | None = None
    fair_p50: int | None = None
    fair_p90: int | None = None
    basis_level: str | None = None             # model_season / model / family / brand_subcat / subcategory / category
    n_basis: int | None = None                 # 근거 수준의 학습 매물 수
    explain: str | None = None                 # "동일 모델 12건 중앙값 62만원 · … → 58만원"
    deal_flag: str | None = None               # undervalued / fair / overvalued / price_typo
    deal_ratio: float | None = None            # 호가 / P50
    deal_confidence: str | None = None         # high / medium / low
    msrp_krw: int | None = None
    repost: RepostInfo | None = None
    skipped_reason: str | None = None          # 적정가를 내지 않은 이유
    error: str | None = None
    pricing_version: str | None = None
    # 상세 페이지 '거래' 칸(판매중 / 거래완료 …). 거래완료는 마감 신호라 재학습용으로 기록하고 저평가 강조에서 뺀다
    trade_status: str | None = None
    elapsed_ms: float = 0.0
    # price_flags(가격 파서 표시), price_uncertain(학습 제외 규칙에 걸린 호가), repost_error(재게시 조회 실패 사유)
    extra: dict = field(default_factory=dict)

    @property
    def price_uncertain(self) -> bool:
        return bool(self.extra.get("price_uncertain"))

    @property
    def category_ko(self) -> str | None:
        return CATEGORY_KO.get(self.category, self.category) if self.category else None

    @property
    def season_label(self) -> str | None:
        s = self.season_start
        return None if s is None else f"{s % 100:02d}/{(s + 1) % 100:02d}"

    def to_row(self, post_id: str) -> dict:
        r = self.repost
        return {
            "post_id": post_id, "computed_at": int(time.time()), "pricing_version": self.pricing_version,
            "listing_type": self.listing_type, "category": self.category, "subcategory": self.subcategory,
            "brand": self.brand, "family_id": self.family_id, "model_id": self.model_id,
            "season_start": self.season_start, "id_grade": self.id_grade, "price_krw": self.price_krw,
            "price_kind": self.price_kind, "price_flags": ",".join(self.extra.get("price_flags") or ()) or None,
            "fair_p10": self.fair_p10, "fair_p50": self.fair_p50, "fair_p90": self.fair_p90,
            "basis_level": self.basis_level, "n_basis": self.n_basis, "deal_flag": self.deal_flag,
            "deal_ratio": self.deal_ratio, "deal_confidence": self.deal_confidence, "msrp_krw": self.msrp_krw,
            "repost_of": r.prev_post_id if r else None, "repost_n": r.n_previous if r else 0,
            "prev_price": r.prev_price if r else None, "price_change_pct": r.change_pct if r else None,
            "skipped_reason": self.skipped_reason, "error": self.error, "explain": self.explain,
            "trade_status": self.trade_status,
        }


# enrichment 테이블 열(post_id 제외). 열을 더하면 source.init_db가 기존 DB에 ALTER TABLE ADD COLUMN으로 붙인다.
# 열을 지우거나 이름·뜻을 바꾸지는 않는다(옛 DB의 열은 NULL로 남을 뿐 INSERT를 막지 않는다).
ENRICHMENT_COLUMNS = {
    "computed_at": "INTEGER",
    "pricing_version": "TEXT",
    "listing_type": "TEXT",
    "category": "TEXT",
    "subcategory": "TEXT",
    "brand": "TEXT",
    "family_id": "TEXT",
    "model_id": "TEXT",
    "season_start": "INTEGER",
    "id_grade": "TEXT",
    "price_krw": "INTEGER",
    "price_kind": "TEXT",
    "price_flags": "TEXT",
    "fair_p10": "INTEGER",
    "fair_p50": "INTEGER",
    "fair_p90": "INTEGER",
    "basis_level": "TEXT",
    "n_basis": "INTEGER",
    "deal_flag": "TEXT",
    "deal_ratio": "REAL",
    "deal_confidence": "TEXT",
    "msrp_krw": "INTEGER",
    "repost_of": "TEXT",
    "repost_n": "INTEGER",
    "prev_price": "INTEGER",
    "price_change_pct": "REAL",
    "skipped_reason": "TEXT",
    "error": "TEXT",
    "explain": "TEXT",
    "trade_status": "TEXT",
}

ENRICHMENT_DDL = (
    "\nCREATE TABLE IF NOT EXISTS enrichment\n(\n    post_id          TEXT PRIMARY KEY,\n"
    + ",\n".join(f"    {name:<16} {typ}" for name, typ in ENRICHMENT_COLUMNS.items())
    + "\n)\n"
)


def save_enrichment(con, post_id: str, e: Enrichment) -> None:
    """seen과 같은 DB의 enrichment 테이블에 기록(같은 post_id면 덮어씀). 파생값만 저장한다(연락처·작성자 없음)."""
    row = e.to_row(post_id)
    cols = ", ".join(row)
    marks = ", ".join("?" for _ in row)
    con.execute(f"INSERT OR REPLACE INTO enrichment({cols}) VALUES ({marks})", tuple(row.values()))


def _int(v) -> int | None:
    try:
        if v is None:
            return None
        f = float(v)
        return None if f != f else int(round(f))
    except (TypeError, ValueError):
        return None


class Enricher:
    """사전·모델을 한 번 읽어 두고 매물마다 enrich()를 부른다."""

    def __init__(self, data_dir: str | os.PathLike):
        import numpy as np  # noqa: F401  (지연 import: 보강 기능을 끈 환경에서는 무거운 의존성을 읽지 않는다)
        import pandas as pd

        from drspark.fairprice import FairPriceModel
        from drspark.resolver import ModelResolver

        t0 = time.time()
        self.data_dir = Path(data_dir)
        self.resolver = ModelResolver(self.data_dir)
        self.model = FairPriceModel.load(self.data_dir / "fairprice.pkl")
        meta_path = self.data_dir / "fairprice.json"
        self.meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
        self.version = f"{self.meta.get('version', self.model.version)}@{self.meta.get('trained_at', 'unknown')}"
        cm = pd.read_csv(self.data_dir / "canonical_models.csv")
        self.model_name = dict(zip(cm["canonical_model_id"], cm["official_name"].fillna(cm["model"])))
        self.msrp = {r.canonical_model_id: (r.msrp_krw, r.msrp_basis) for r in cm.itertuples()
                     if r.msrp_krw == r.msrp_krw}
        logger.info(f"Enricher loaded {self.version} from {self.data_dir} in {time.time() - t0:.1f}s "
                    f"(models={len(cm)}, aliases={len(self.resolver.aliases)})")

    @classmethod
    def from_env(cls) -> "Enricher | None":
        """DRSPARK_PRICING=0이면 끈다. 폴더(DRSPARK_PRICING_DIR, 기본 ./pricing_data)나 의존성이 없으면 None."""
        if os.getenv("DRSPARK_PRICING", "1").strip().lower() in ("0", "false", "off", "no"):
            logger.info("Pricing enrichment disabled by DRSPARK_PRICING")
            return None
        # 빈 값(DRSPARK_PRICING_DIR=)은 설정하지 않은 것으로 본다(Path('')는 현재 폴더라 보강이 조용히 꺼진다)
        d = Path((os.getenv("DRSPARK_PRICING_DIR") or "").strip()
                 or Path(__file__).resolve().parent.parent / "pricing_data")
        if not (d / "fairprice.pkl").exists():
            logger.warning(f"Pricing data not found in {d}; alerts will be sent without fair prices")
            return None
        try:
            return cls(d)
        except Exception:
            logger.exception("Pricing enrichment failed to load; alerts will be sent without fair prices")
            return None

    # ------------------------------------------------------------------ 본체
    def enrich(self, item: dict, con=None) -> Enrichment:
        """item: source.py의 매물 dict(id, title, raw_price, author, status, body_content, release_year,
        model_serial, spec, purchased_at, usage_count, features, observed_at). con: seen DB 연결(재게시 조회용, 선택)."""
        t0 = time.perf_counter()  # 벽시계 보정에 흔들리지 않는 경과 시간
        e = Enrichment(pricing_version=self.version)
        e.trade_status = (item.get("trade_status") or "").strip() or None
        try:
            frame, e = self._features(item, e)
            if frame is not None:
                self._price(frame, e)
        except Exception as ex:  # 보강 실패는 알림을 막지 않는다. error는 식별·적정가 단계 실패에만 쓴다
            logger.exception(f"Enrichment failed for {item.get('id')}")
            e.error = f"{type(ex).__name__}: {ex}"[:300]
        if con is not None:
            # 재게시 조회가 실패해도 이미 계산한 식별·적정가는 그대로 쓴다(repost=None, 사유는 extra에)
            try:
                from .repost import find_previous
                e.repost = find_previous(con, item, e.price_krw)
            except Exception as ex:
                logger.warning(f"Repost lookup failed for {item.get('id')}: {type(ex).__name__}: {ex}")
                e.extra["repost_error"] = f"{type(ex).__name__}: {ex}"[:300]
        e.elapsed_ms = (time.perf_counter() - t0) * 1000
        return e

    def _features(self, item: dict, e: Enrichment):
        import pandas as pd

        from drspark import listing_units as L
        from drspark import parse as P
        from drspark import privacy
        from drspark import taxonomy as T
        from drspark.schema import id_grade

        def txt(k):
            v = clip_text(k, item.get(k))
            return privacy.scrub_text(v) if isinstance(v, str) else v

        title, ms, spec, body = txt("title"), txt("model_serial"), txt("spec"), txt("body_content")
        features, usage_raw, status = txt("features"), txt("usage_count"), txt("status")
        seen_at = datetime.fromtimestamp(int(item.get("observed_at") or time.time()), tz=KST)

        cls = T.classify(title, ms, spec, features, body)
        brand_rule, _alias, _conf, _field = T.detect_brand_fields(title, ms, spec, features, body)
        price = P.parse_price(item.get("raw_price"), title, body, category_hint=cls.category)
        lt = L.classify_listing(title, body, price.value)
        bf = L.bundle_flags(title, ms, spec, body, price.value)
        cf = L.condition_flags(title, body, features, usage_raw, status, ms, spec)
        season = P.parse_season(item.get("release_year"), title, ms, body, spec=spec, observed_year=seen_at.year)
        lengths = P.parse_lengths(" ".join(x for x in (title, ms, spec) if x), category_hint=cls.category)
        usage = P.parse_usage(usage_raw)
        grade = P.parse_grade(status, title, body)
        res = self.resolver.resolve(title, ms, spec, brand_hint=brand_rule, category_hint=cls.category,
                                    subcategory_hint=cls.subcategory, gender_hint=cls.gender)

        model_resolved = res.level == "model" and res.method in ("exact", "exact_ambiguous", "fuzzy")
        season_resolved = season.start is not None and season.kind in ("two_year", "model_code", "single_year")
        e.listing_type = lt.listing_type
        e.category, e.subcategory = cls.category, cls.subcategory
        e.brand = res.brand or brand_rule
        e.family_id = res.family_id
        e.model_id = res.model_id if model_resolved else None
        e.model_name = self.model_name.get(e.model_id) if e.model_id else None
        e.season_start = season.start
        e.id_grade = id_grade(model_resolved, season_resolved, res.family_id)
        e.price_krw = price.value
        e.price_kind = price.kind
        e.extra["price_flags"] = list(price.flags)
        if price.value is not None and is_price_uncertain(price.flags, cls.subcategory):
            e.extra["price_uncertain"] = True
        msrp, msrp_basis = self.msrp.get(e.model_id, (None, None))
        e.msrp_krw = _int(msrp)

        # 적정가를 내지 않는 경우
        if lt.listing_type != "sale":
            e.skipped_reason = "구매 희망 글" if lt.listing_type == "wanted" else "판매 글 아님"
        elif bf.is_bundle:
            e.skipped_reason = "여러 물건 묶음"
        elif L.is_pass_ticket(title, ms) or cls.category in NO_PRICE_CATEGORIES:
            e.skipped_reason = "장비 외 품목"
        elif price.value is None or price.value < MIN_PRICE:
            e.skipped_reason = "가격 없음"
        if e.skipped_reason:
            return None, e

        obs_season = seen_at.year if seen_at.month >= 7 else seen_at.year - 1
        row = {
            "category": cls.category, "subcategory": cls.subcategory, "brand": e.brand, "family_id": e.family_id,
            "model_id": e.model_id, "season_start": season.start, "id_grade": e.id_grade, "grade": grade.grade,
            "grade_ord": grade.grade_ord, "age_seasons": max(obs_season - season.start, 0) if season.start else None,
            "usage_bucket": usage.bucket, "usage_uses": usage.uses, "with_binding": bool(bf.with_binding),
            "is_new_claim": bool(grade.is_new_claim), "like_new_claim": bool(grade.like_new_claim),
            "defect_level": cf.defect_level, "gender": cls.gender, "ski_length_cm": lengths.ski_length_cm,
            "radius_m": lengths.radius_m, "boot_flex": lengths.boot_flex, "boot_mondo_mm": lengths.boot_mondo_mm,
            "apparel_size": lengths.apparel_size if cls.category == "apparel" else None,
            "observed_month": seen_at.strftime("%Y-%m"), "msrp_krw": msrp, "msrp_basis": msrp_basis,
            "nego_open": bool(cf.nego_open), "nego_refused": bool(cf.nego_refused),
            "urgent_sale": bool(cf.urgent_sale), "ship_parcel": bool(cf.ship_parcel),
            "is_apparel_set": bool(bf.is_apparel_set),
        }
        return pd.DataFrame([row]), e

    def _price(self, frame, e: Enrichment) -> None:
        out = self.model.predict(frame, explain=True).iloc[0]
        e.fair_p10, e.fair_p50, e.fair_p90 = (_int(round(out[c], -3)) for c in ("fair_p10", "fair_p50", "fair_p90"))
        e.basis_level = out["basis_level"]
        e.n_basis = _int(out.get(f"n_{e.basis_level}"))
        e.explain = fix_month_label(out.get("explain_ko"), frame["observed_month"].iloc[0],
                                    getattr(getattr(self.model, "adj", None), "months", None))
        if not e.fair_p50:
            return
        ratio = e.price_krw / float(out["fair_p50"])
        e.deal_ratio = round(ratio, 3)
        if ratio < TYPO_LOW or ratio > TYPO_HIGH:
            e.deal_flag = "price_typo"
        elif e.price_krw < out["fair_p10"]:
            e.deal_flag = "undervalued"
        elif e.price_krw > out["fair_p90"]:
            e.deal_flag = "overvalued"
        else:
            e.deal_flag = "fair"
        strong = e.basis_level in ("model_season", "model") and (e.n_basis or 0) >= 3
        if e.id_grade == "A" and strong:
            e.deal_confidence = "high"
        elif e.id_grade in ("A", "B") and e.basis_level in ("model_season", "model", "family"):
            e.deal_confidence = "medium"
        else:
            e.deal_confidence = "low"
        # 학습이 '가격 불확실'로 뺀 호가(÷10 보정·단위 가정·오기 의심)는 판정을 참고용으로만 둔다.
        # is_deal()이 신뢰 높음·보통만 보므로 [저평가]·멘션·deals 모드에서 빠진다(판정 자체는 기록해 둔다).
        if e.price_uncertain:
            e.deal_confidence = "low"


def as_dict(e: Enrichment) -> dict:
    d = asdict(e)
    d["category_ko"] = e.category_ko
    d["season_label"] = e.season_label
    return d
