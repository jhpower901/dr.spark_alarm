"""별칭 사전 기반 모델 리졸버(결정적, LLM 없이 동작).

새 매물 텍스트(제목·모델명·스펙)에서 canonical model/family를 찾는다. 서비스 통합 시 이 모듈과
data/curated/model_aliases.csv, canonical_models.csv, canonical_families.csv만 있으면 된다.

매칭 순서
1) 정확 매칭: 정규화한 별칭이 텍스트에 단어 경계로 나타나는지(공백·하이픈 제거판도 비교 — 제품코드용).
   점수 = 별칭 길이 × 신뢰도 × 수준 가중(model 1.0, family 0.7) + 브랜드 일치 가산.
2) 모호 별칭(여러 대상)은 브랜드·카테고리 힌트로 좁히고, 못 좁히면 패밀리 수준으로만 쓴다.
3) 정확 매칭이 없으면 같은 브랜드의 모델 별칭에 퍼지 매칭(같은 토큰 수 창끼리 rapidfuzz ratio ≥ 95, 길이 ≥ 8, 길이 차 ≤ 2 — 긴 이름의 오타만).
4) (선택, use_defaults=True) 그래도 모델이 없으면 (패밀리|브랜드, 하위 카테고리) 대표 모델 사전을 쓴다.
   표본 내 정밀도가 25~34%에 그쳐 기본값은 끈다.
   대표 모델은 라벨 분포에서 share ≥ 0.7, n ≥ 5인 것만 두며, method가 *_default라 신뢰도를 낮춰 써야 한다
   (적정가 산출 시 패밀리 수준 B로 취급).
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from functools import lru_cache
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz

from . import config

LEVEL_W = {"model": 1.0, "family": 0.7}
# 단독으로는 너무 일반적인 별칭(오탐 방지). 사전 생성 단계에서 걸러지지 않은 것만 막는다.
GENERIC = {"ski", "skis", "스키", "부츠", "boots", "boot", "jacket", "자켓", "pants", "바지", "helmet", "헬멧",
           "goggle", "고글", "race", "레이스", "pro", "프로", "sl", "gs", "wc", "fis", "demo", "데모", "world cup",
           "월드컵", "team", "팀", "jr", "junior", "주니어", "women", "w", "m", "l", "xl", "s"}


def norm(s: str | None) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[\[\](){}<>\"'`~!@#$%^&*=+|\\;:,?]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def compact(s: str) -> str:
    return re.sub(r"[\s\-_./]", "", s)


@dataclass
class Resolution:
    brand: str | None
    family_id: str | None
    model_id: str | None
    level: str  # model | family | brand | none
    method: str  # exact | exact_ambiguous | fuzzy | family_default | brand_default | brand_only | none
    matched_alias: str | None
    score: float

    def to_dict(self) -> dict:
        return asdict(self)


class ModelResolver:
    def __init__(self, curated_dir: Path | str | None = None):
        d = Path(curated_dir or config.CURATED)
        al = pd.read_csv(d / "model_aliases.csv")
        cm = pd.read_csv(d / "canonical_models.csv")
        self.model_family = dict(zip(cm["canonical_model_id"], cm["family_id"]))
        self.model_cat = dict(zip(cm["canonical_model_id"], cm["category"]))
        fam = pd.read_csv(d / "canonical_families.csv")
        self.family_cat = dict(zip(fam["family_id"], fam["category"]))
        al = al[al["canonical_model_id"].notna()].copy()
        al["alias_norm"] = al["alias_norm"].astype(str).map(norm)
        al = al[(al["alias_norm"].str.len() >= 3) & ~al["alias_norm"].isin(GENERIC)]
        al["level"] = al["canonical_model_id"].map(lambda x: "model" if x.count("/") == 2 else "family" if x.count("/") == 1 else "brand")
        # 브랜드 수준 별칭(예: '콜마르' → colmar)은 브랜드 감지에만 쓴다
        br = al[(al["level"] == "brand") & ~al["ambiguous"].astype(bool)]
        self.brand_aliases = sorted(zip(br["alias_norm"], br["canonical_model_id"]), key=lambda x: -len(x[0]))
        self._brand_patterns = [(re.compile(rf"(?<![0-9a-z가-힣]){re.escape(a)}"), b) for a, b in self.brand_aliases]
        al = al[al["level"] != "brand"]
        al["brand"] = al["canonical_model_id"].str.split("/").str[0]
        al["compact"] = al["alias_norm"].map(compact)
        self.aliases = al.reset_index(drop=True)
        self._patterns = [re.compile(rf"(?<![0-9a-z가-힣]){re.escape(a)}(?![0-9a-z가-힣])") for a in self.aliases["alias_norm"]]
        self._by_brand = {b: g for b, g in self.aliases[self.aliases["level"] == "model"].groupby("brand")}
        # (패밀리|브랜드, 하위 카테고리) → 대표 모델. 라벨 분포에서 배운 사전(share·n 조건을 만족한 것만)
        self.defaults: dict[tuple[str, str], tuple[str, float]] = {}
        dp = d / "resolver_defaults.csv"
        if dp.exists():
            for r in pd.read_csv(dp).itertuples(index=False):
                self.defaults[(r.scope_id, r.subcategory)] = (r.model_id, float(r.share))

    def detect_brand(self, text: str) -> str | None:
        """브랜드 수준 별칭 중 가장 긴 일치. 브랜드 힌트가 없을 때 약한 힌트로 쓴다."""
        t = norm(text)
        for pat, b in self._brand_patterns:
            if pat.search(t):
                return b
        return None

    def _category_of(self, target: str) -> str | None:
        return self.model_cat.get(target) or self.family_cat.get(target)

    def resolve(self, title: str | None, model_serial: str | None = None, spec: str | None = None,
                brand_hint: str | None = None, category_hint: str | None = None,
                subcategory_hint: str | None = None, gender_hint: str | None = None,
                use_defaults: bool = False) -> Resolution:
        text = norm(" ".join(x for x in (title, model_serial, spec) if x))
        ctext = compact(text)
        soft_brand = None if brand_hint else self.detect_brand(text)
        cands = []
        for i, row in enumerate(self.aliases.itertuples(index=False)):
            hit = bool(self._patterns[i].search(text))
            if not hit and len(row.compact) >= 5 and re.search(r"\d", row.compact) and row.compact in ctext:
                hit = True  # 제품코드(공백·하이픈 차이)
            if not hit:
                continue
            s = len(row.alias_norm) * float(row.confidence) * LEVEL_W[row.level]
            if brand_hint and row.brand == brand_hint:
                s += 3
            elif brand_hint and row.brand != brand_hint:
                s -= 4
            elif soft_brand and row.brand == soft_brand:
                s += 1.5
            cat = self._category_of(row.canonical_model_id)
            if category_hint and cat and cat != category_hint:
                s -= 3
            if row.level == "model":
                tid = row.canonical_model_id
                if tid.endswith("-w"):
                    s += 2 if gender_hint == "W" else -1
                if tid.endswith("-jr"):
                    s += 2 if gender_hint == "J" else -1
            cands.append((s, row))
        if cands:
            cands.sort(key=lambda x: -x[0])
            best_s, best = cands[0]
            ambiguous = bool(best.ambiguous) and not brand_hint
            # 같은 별칭이 여러 모델을 가리키고 힌트로 못 가르면 패밀리 수준으로 낮춘다
            if ambiguous:
                same = [r for s, r in cands if r.alias_norm == best.alias_norm]
                fams = {self.model_family.get(r.canonical_model_id, r.canonical_model_id) for r in same}
                if len(fams) == 1:
                    fid = fams.pop()
                    return Resolution(fid.split("/")[0], fid, None, "family", "exact_ambiguous", best.alias_norm, best_s)
            tid = best.canonical_model_id
            if best.level == "family":
                # 같은 패밀리의 모델 별칭이 함께 걸렸으면 더 구체적인 모델을 고른다
                inner = [(s, r) for s, r in cands if r.level == "model"
                         and self.model_family.get(r.canonical_model_id, r.canonical_model_id.rsplit("/", 1)[0]) == tid]
                if inner:
                    s2, r2 = inner[0]
                    mid = r2.canonical_model_id
                    return Resolution(mid.split("/")[0], tid, mid, "model", "exact", r2.alias_norm, s2)
            if best.level == "model":
                # 더 긴 모델 별칭이 있으면 그것을 우선(위에서 길이로 이미 반영)
                return Resolution(tid.split("/")[0], self.model_family.get(tid, tid.rsplit("/", 1)[0]), tid, "model",
                                  "exact", best.alias_norm, best_s)
            # 패밀리만 맞았으면 같은 패밀리 모델 별칭에 퍼지로 한 번 더
            fz = self._fuzzy(text, tid.split("/")[0], family=tid)
            if fz:
                return fz
            dm = self._default(tid, subcategory_hint) if use_defaults else None
            if dm:
                return Resolution(tid.split("/")[0], tid, dm, "model", "family_default", best.alias_norm, best_s)
            return Resolution(tid.split("/")[0], tid, None, "family", "exact", best.alias_norm, best_s)
        brand_hint = brand_hint or soft_brand
        if brand_hint:
            fz = self._fuzzy(text, brand_hint)
            if fz:
                return fz
            dm = self._default(brand_hint, subcategory_hint) if use_defaults else None
            if dm:
                return Resolution(brand_hint, self.model_family.get(dm, dm.rsplit("/", 1)[0]), dm, "model", "brand_default", None, 0.0)
            return Resolution(brand_hint, None, None, "brand", "brand_only", None, 0.0)
        return Resolution(None, None, None, "none", "none", None, 0.0)

    def _default(self, scope_id: str, subcategory: str | None) -> str | None:
        if not subcategory:
            return None
        hit = self.defaults.get((scope_id, subcategory))
        return hit[0] if hit else None

    def _fuzzy(self, text: str, brand: str, family: str | None = None) -> Resolution | None:
        g = self._by_brand.get(brand)
        if g is None or not text:
            return None
        if family:
            g = g[g["canonical_model_id"].str.startswith(family + "/")]
        best = None
        toks = text.split()
        for row in g.itertuples(index=False):
            a = row.alias_norm
            if len(a) < 8:
                continue
            n = len(a.split())
            # 별칭과 같은 토큰 수의 창끼리만 비교한다(오타 수준만 허용, 부분 포함은 불허)
            for i in range(0, max(len(toks) - n + 1, 0)):
                w = " ".join(toks[i:i + n])
                if abs(len(w) - len(a)) > 2:
                    continue
                r = fuzz.ratio(a, w)
                if r >= 95 and (best is None or (r, len(a)) > best[0]):
                    best = ((r, len(a)), row)
        if best is None:
            return None
        row = best[1]
        tid = row.canonical_model_id
        return Resolution(brand, self.model_family.get(tid, tid.rsplit("/", 1)[0]), tid, "model", "fuzzy",
                          row.alias_norm, float(best[0][0]) / 10)


@lru_cache(maxsize=1)
def default_resolver() -> ModelResolver:
    return ModelResolver()
