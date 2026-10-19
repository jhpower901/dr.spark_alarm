"""적정가(fair price) 모델 — 재사용 가능한 클래스. 그림·파일 경로 없음.

구성
----
H  계층 경험적 베이즈 축소 + 속성 보정
   카테고리 → 하위 카테고리 → 브랜드×하위 카테고리 → 패밀리 → 모델 (→ 모델×시즌, id_grade A 행만).
   각 수준은 부모 예측값 대비 log 호가 편차의 그룹 평균 m_g 를 n/(n+k) 로 줄여 더한다.
   k = σ²_within / τ²_between 을 일원 랜덤효과 적률추정(ANOVA)으로 수준마다 구하고 k_mult 배를 쓴다.
   속성 보정(상태 등급, 경과 시즌×카테고리군, 사용 정도, 바인딩, 새상품 표기, 하자, 주니어, 사이즈, 관측 월)은
   능형 회귀로 추정하고, 계층과 번갈아 맞춘다(backfitting).
G  LightGBM 분위수 회귀(α = 0.1/0.5/0.9). 속성 + H 계층 인코딩 + log 정가.
   학습 행의 H 인코딩은 학습 데이터 안에서 판매자 그룹 K겹으로 만든 폴드 밖(OOF) 값이라 표적 누수가 없다.
블렌드  log P50 = w·H + (1−w)·G50.
구간  [P50 − (G50−G10), P50 + (G90−G50)] 에 id_grade별(Mondrian) 분할 컨포멀 보정을 아래·위 꼬리 따로
   적용한다(CQR). 보정 점수는 반드시 폴드 밖 예측으로 계산해 calibrate() 에 넘긴다.

입력 프레임 열(listings_clean 기준, 없으면 결측으로 본다)
   category, subcategory, brand, family_id, model_id, season_start, id_grade, grade, grade_ord, age_seasons,
   usage_bucket, usage_uses, with_binding, is_new_claim, like_new_claim, defect_level, gender, ski_length_cm,
   radius_m, boot_flex, boot_mondo_mm, apparel_size, observed_month, msrp_krw, msrp_basis, nego_open,
   nego_refused, urgent_sale, ship_parcel, is_apparel_set, price_krw(학습 시 표적)

사용
----
    m = FairPriceModel().fit(train_df, groups=train_df.seller_key)
    raw = m.predict_raw(cal_df)                       # 폴드 밖 행이어야 한다
    m.calibrate(cal_df, raw, np.log(cal_df.price_krw))
    out = m.predict(new_df)                          # fair_p10/p50/p90, basis_level, explain_ko …
    m.save(path); m2 = FairPriceModel.load(path)
"""
from __future__ import annotations

import math
import pickle
from dataclasses import dataclass, field, asdict

import numpy as np
import pandas as pd

VERSION = "fairprice-1.0"

LEVELS = ["category", "subcategory", "brand_subcat", "family", "model", "model_season"]
LEVEL_KO = {"model_season": "동일 모델·시즌", "model": "동일 모델", "family": "같은 패밀리",
            "brand_subcat": "같은 브랜드·종류", "subcategory": "같은 종류", "category": "같은 카테고리"}
CAT_GROUPS = ("ski", "boots", "apparel", "etc")
GRADE_LEVELS = ["sealed", "S", "A", "B", "C", "unk"]
GRADE_KO = {"sealed": "미개봉", "S": "S급", "A": "A급", "B": "B급", "C": "C급", "unk": "상태 미상"}
AGE_BUCKETS = ["0", "1", "2", "3", "4-5", "6+", "unk"]
USAGE_LEVELS = ["unknown", "new_unused", "le5", "le20", "one_season", "multi_season"]
USAGE_KO = {"new_unused": "미사용", "le5": "사용 5회 이하", "le20": "사용 20회 이하", "one_season": "한 시즌 사용",
            "multi_season": "여러 시즌 사용", "unknown": "사용 정도 미상"}
DEFECT_LEVELS = ["none", "minor", "repair", "major"]
DEFECT_KO = {"minor": "경미한 하자", "repair": "수리 이력", "major": "큰 하자", "none": "하자 없음"}
APPAREL_SIZE_ORD = {"XXS": 0, "XS": 1, "S": 2, "M": 3, "L": 4, "XL": 5, "2XL": 6, "3XL": 7, "FREE": np.nan}
MSRP_KIND = {"krw_official": "official", "krw_retailer": "retailer", "krw_other": "other"}

_DEFAULTS = {
    "category": "other", "subcategory": "other", "brand": np.nan, "family_id": np.nan, "model_id": np.nan,
    "season_start": np.nan, "id_grade": "C", "grade": np.nan, "grade_ord": np.nan, "age_seasons": np.nan,
    "usage_bucket": "unknown", "usage_uses": np.nan, "with_binding": False, "is_new_claim": False,
    "like_new_claim": False, "defect_level": "none", "gender": "U", "ski_length_cm": np.nan, "radius_m": np.nan,
    "boot_flex": np.nan, "boot_mondo_mm": np.nan, "apparel_size": np.nan, "observed_month": np.nan,
    "msrp_krw": np.nan, "msrp_basis": np.nan, "nego_open": False, "nego_refused": False, "urgent_sale": False,
    "ship_parcel": False, "is_apparel_set": False,
}
_NUMERIC = {"season_start", "grade_ord", "age_seasons", "usage_uses", "ski_length_cm", "radius_m", "boot_flex",
            "boot_mondo_mm", "msrp_krw"}


# --------------------------------------------------------------------------------------------- 전처리
def _prep(df: pd.DataFrame) -> pd.DataFrame:
    """필요한 열을 갖추고 nullable 형식을 numpy 형식으로 바꾼다."""
    d = pd.DataFrame(index=df.index)
    for c, default in _DEFAULTS.items():
        kind = "bool" if isinstance(default, bool) else ("num" if c in _NUMERIC else "str")
        if c in df.columns:
            s = df[c]
            if kind == "bool":
                s = s.astype("boolean").fillna(default).astype(bool)
            elif kind == "num":
                s = pd.to_numeric(s, errors="coerce").astype(float)
            else:
                s = s.astype(object)
                s = s.where(s.notna(), default if isinstance(default, str) else np.nan)
            d[c] = s
        else:
            d[c] = default
    d["grade"] = d["grade"].where(d["grade"].isin(GRADE_LEVELS[:-1]), "unk")
    d["usage_bucket"] = d["usage_bucket"].where(d["usage_bucket"].isin(USAGE_LEVELS), "unknown")
    d["defect_level"] = d["defect_level"].where(d["defect_level"].isin(DEFECT_LEVELS), "none")
    d["cat_group"] = d["category"].where(d["category"].isin(CAT_GROUPS[:-1]), "etc")
    a = d["age_seasons"]
    d["age_bucket"] = np.select([a.isna(), a <= 0, a <= 1, a <= 2, a <= 3, a <= 5],
                                ["unk", "0", "1", "2", "3", "4-5"], "6+")
    d["junior"] = (d["gender"].eq("J") | d["subcategory"].astype(str).str.endswith("junior"))
    return d


def level_keys(d: pd.DataFrame) -> pd.DataFrame:
    """수준별 그룹 키(해당 없음은 NaN)."""
    def join(*cols):
        out = cols[0].astype(str)
        ok = cols[0].notna()
        for c in cols[1:]:
            out = out + "|" + c.astype(str)
            ok &= c.notna()
        return out.where(ok, np.nan)
    season = d["season_start"].map(lambda v: f"{int(v)}" if pd.notna(v) else np.nan)
    ms = join(d["model_id"], season).where(d["id_grade"].eq("A"), np.nan)
    return pd.DataFrame({
        "category": d["category"].astype(str),
        "subcategory": join(d["category"], d["subcategory"]),
        "brand_subcat": join(d["brand"], d["subcategory"]),
        "family": d["family_id"].where(d["family_id"].notna(), np.nan),
        "model": d["model_id"].where(d["model_id"].notna(), np.nan),
        "model_season": ms,
    }, index=d.index)


def group_folds(groups, n_splits: int = 5, seed: int = 7) -> np.ndarray:
    """판매자 그룹 단위 폴드 번호(0..n_splits−1). 그룹을 섞은 뒤 큰 그룹부터 가장 작은 폴드에 넣는다."""
    g = pd.Series(np.asarray(groups)).astype(str)
    sizes = g.value_counts()
    rng = np.random.default_rng(seed)
    order = sizes.sample(frac=1.0, random_state=int(rng.integers(1 << 30))).sort_values(ascending=False, kind="stable")
    load = np.zeros(n_splits)
    assign = {}
    for key, n in order.items():
        f = int(np.argmin(load))
        assign[key] = f
        load[f] += n
    return g.map(assign).to_numpy()


# --------------------------------------------------------------------------------------------- H: 계층 축소
class _Hier:
    def __init__(self, k_mult: float = 1.0, k_bounds=(0.5, 200.0), clip_sd: float = 2.5, k_fixed: dict | None = None):
        self.k_mult, self.k_bounds, self.clip_sd, self.k_fixed = k_mult, k_bounds, clip_sd, k_fixed or {}

    @staticmethod
    def _robust_clip(e: np.ndarray, c: float) -> np.ndarray:
        med = np.median(e)
        s = 1.4826 * np.median(np.abs(e - med))
        if not np.isfinite(s) or s <= 0:
            s = np.std(e) or 1.0
        return np.clip(e, med - c * s, med + c * s)

    def _estimate_k(self, n: np.ndarray, m: np.ndarray, ssw: float, prev_msw: float | None) -> tuple[float, float]:
        N, G = n.sum(), len(n)
        msw = ssw / (N - G) if N > G else (prev_msw if prev_msw else 0.25)
        if G < 2:
            return self.k_bounds[1], msw
        gm = (n * m).sum() / N
        msb = (n * (m - gm) ** 2).sum() / (G - 1)
        n0 = (N - (n ** 2).sum() / N) / (G - 1)
        tau2 = max((msb - msw) / max(n0, 1e-9), 1e-3)
        k = float(np.clip(msw / tau2, *self.k_bounds))
        return k, msw

    def fit(self, keys: pd.DataFrame, r: np.ndarray, y: np.ndarray):
        r = np.asarray(r, float)
        self.mu0 = float(np.mean(self._robust_clip(r, self.clip_sd)))
        pred = np.full(len(r), self.mu0)
        self.tables, self.k = {}, {}
        prev_msw = None
        for lev in LEVELS:
            key = keys[lev].to_numpy(dtype=object)
            mask = pd.notna(key)
            if mask.sum() == 0:
                self.tables[lev] = pd.DataFrame(columns=["n", "raw_mean", "dev", "med_y"])
                self.k[lev] = np.nan
                continue
            e = self._robust_clip(r[mask] - pred[mask], self.clip_sd)
            g = pd.DataFrame({"key": key[mask], "e": e, "y": y[mask]})
            agg = g.groupby("key").agg(n=("e", "size"), raw_mean=("e", "mean"), med_y=("y", "median"))
            g["m"] = g["key"].map(agg["raw_mean"])
            ssw = float(((g["e"] - g["m"]) ** 2).sum())
            k, prev_msw = self._estimate_k(agg["n"].to_numpy(float), agg["raw_mean"].to_numpy(), ssw, prev_msw)
            k = self.k_fixed.get(lev, k * self.k_mult)
            agg["dev"] = agg["n"] / (agg["n"] + k) * agg["raw_mean"]
            self.tables[lev], self.k[lev] = agg, k
            pred[mask] += pd.Series(key[mask]).map(agg["dev"]).to_numpy()
        return self

    def predict(self, keys: pd.DataFrame) -> pd.DataFrame:
        loc = np.full(len(keys), self.mu0)
        out = {}
        for lev in LEVELS:
            t = self.tables[lev]
            k = keys[lev]
            dev = k.map(t["dev"]).fillna(0.0).to_numpy() if len(t) else np.zeros(len(keys))
            loc = loc + dev
            out[f"loc_{lev}"] = loc.copy()
            out[f"n_{lev}"] = (k.map(t["n"]).fillna(0).to_numpy() if len(t) else np.zeros(len(keys))).astype(int)
            out[f"medy_{lev}"] = k.map(t["med_y"]).to_numpy(dtype=float) if len(t) else np.full(len(keys), np.nan)
        res = pd.DataFrame(out, index=keys.index)
        res["loc"] = loc
        return res


# --------------------------------------------------------------------------------------------- H: 속성 보정
class _Adjust:
    """능형 회귀 속성 보정. 열 이름 → 요인(factor) 대응을 함께 기록해 설명에 쓴다."""

    def __init__(self, alpha: float = 3.0):
        self.alpha = alpha

    def _design(self, d: pd.DataFrame, fitting: bool = False) -> pd.DataFrame:
        X = {}
        fac = {}

        def add(name, vals, factor):
            X[name] = np.asarray(vals, float)
            fac[name] = factor
        for gl in ["sealed", "S", "B", "C", "unk"]:
            add(f"grade_{gl}", d["grade"].eq(gl), "grade")
        for cg in CAT_GROUPS:
            ing = d["cat_group"].eq(cg)
            for ab in AGE_BUCKETS:
                if ab == "1":
                    continue
                add(f"age_{cg}_{ab}", ing & d["age_bucket"].eq(ab), "age")
            add(f"junior_{cg}", ing & d["junior"], "junior")
        for ub in USAGE_LEVELS[1:]:
            add(f"usage_{ub}", d["usage_bucket"].eq(ub), "usage")
        add("binding", d["cat_group"].eq("ski") & d["with_binding"], "binding")
        add("new_claim", d["is_new_claim"], "new_claim")
        add("like_new", d["like_new_claim"], "like_new")
        for dl in DEFECT_LEVELS[1:]:
            add(f"defect_{dl}", d["defect_level"].eq(dl), "defect")
        # 사이즈: 스키 길이(같은 패밀리·종류의 중앙 길이 대비 10cm 단위), 아동용 짧은 스키, 작은 부츠
        is_ski = d["cat_group"].eq("ski") & d["ski_length_cm"].notna()
        if fitting:
            sk = d[is_ski]
            self.len_fam = sk.groupby("family_id")["ski_length_cm"].median().to_dict()
            self.len_sub = sk.groupby("subcategory")["ski_length_cm"].median().to_dict()
            self.len_all = float(sk["ski_length_cm"].median()) if len(sk) else 165.0
        ref = d["family_id"].map(self.len_fam)
        ref = ref.fillna(d["subcategory"].map(self.len_sub)).fillna(self.len_all)
        dev = ((d["ski_length_cm"] - ref) / 10.0).clip(-3, 3).where(is_ski, 0.0).fillna(0.0)
        add("len_dev", dev, "size")
        add("len_short", is_ski & (d["ski_length_cm"] < 140), "size")
        add("boot_small", d["cat_group"].eq("boots") & (d["boot_mondo_mm"] <= 225), "size")
        # 관측 월(첫 월 기준). 학습에 없던 월은 학습 마지막 월로 본다.
        month = d["observed_month"].astype(object)
        if fitting:
            self.months = sorted(m for m in month.dropna().unique())
        if self.months:
            month = month.where(month.isin(self.months), self.months[-1])
            for mo in self.months[1:]:
                add(f"month_{mo}", month.eq(mo), "month")
        self.factor_of = fac
        return pd.DataFrame(X, index=d.index)

    def fit(self, d: pd.DataFrame, target: np.ndarray, monotone_grade: bool = True):
        from sklearn.linear_model import Ridge
        X = self._design(d, fitting=True)
        self.cols = list(X.columns)
        self.model = Ridge(alpha=self.alpha, fit_intercept=True).fit(X.to_numpy(), target)
        if monotone_grade:
            self._monotone_grade(d)
        self.coef = pd.Series(self.model.coef_, index=self.cols)
        self.intercept = float(self.model.intercept_)
        # 설명용 중심값: 카테고리군 안 학습 행의 요인별 평균 기여
        C = self.contrib(d, center=False)
        C["cat_group"] = d["cat_group"].to_numpy()
        self.center = C.groupby("cat_group").mean()
        return self

    def _monotone_grade(self, d: pd.DataFrame):
        """상태 효과를 미개봉 ≥ S ≥ A(0) ≥ B ≥ C 로 맞춘다(가중 PAVA)."""
        order = ["sealed", "S", "A", "B", "C"]
        coef = dict(zip(self.cols, self.model.coef_))
        vals = np.array([coef.get(f"grade_{g}", 0.0) if g != "A" else 0.0 for g in order])
        w = np.array([max(int(d["grade"].eq(g).sum()), 1) for g in order], float)
        blocks = [[v, wt, 1] for v, wt in zip(-vals, w)]  # −값이 비감소가 되도록
        i = 0
        while i < len(blocks) - 1:
            if blocks[i][0] > blocks[i + 1][0] + 1e-12:
                v = (blocks[i][0] * blocks[i][1] + blocks[i + 1][0] * blocks[i + 1][1]) / (blocks[i][1] + blocks[i + 1][1])
                blocks[i] = [v, blocks[i][1] + blocks[i + 1][1], blocks[i][2] + blocks[i + 1][2]]
                del blocks[i + 1]
                i = max(i - 1, 0)
            else:
                i += 1
        fitted = np.concatenate([[-b[0]] * b[2] for b in blocks])
        fitted = fitted - fitted[order.index("A")]  # A급 = 0 기준 유지
        for g, v in zip(order, fitted):
            if g != "A" and f"grade_{g}" in coef:
                self.model.coef_[self.cols.index(f"grade_{g}")] = v

    def predict(self, d: pd.DataFrame) -> np.ndarray:
        X = self._design(d)[self.cols]
        return X.to_numpy() @ self.coef.to_numpy() + self.intercept

    def contrib(self, d: pd.DataFrame, center: bool = True) -> pd.DataFrame:
        X = self._design(d)[self.cols]
        coef = self.coef if hasattr(self, "coef") else pd.Series(self.model.coef_, index=self.cols)
        P = X * coef
        C = P.T.groupby(pd.Series(self.factor_of)[self.cols].to_numpy()).sum().T
        if center:
            cen = self.center.reindex(d["cat_group"].to_numpy()).fillna(0.0)
            cen.index = C.index
            C = C - cen[C.columns]
        return C


# --------------------------------------------------------------------------------------------- 파라미터
@dataclass
class FairPriceParams:
    k_mult: float = 1.0                 # 적률추정 k 의 배수(CV로 조정)
    ridge_alpha: float = 3.0            # 속성 보정 능형 벌점
    backfit_iter: int = 4
    monotone_grade: bool = True
    use_gbm: bool = True
    blend_w: float = 0.5                # log P50 = w·H + (1−w)·G50
    inner_splits: int = 5               # G 학습용 OOF 인코딩 폴드 수
    min_n_basis: int = 3                # basis_level 판정 최소 표본
    alpha: float = 0.2                  # 목표 구간 = 1 − alpha (P10–P90)
    min_cal_n: int = 30                 # 이보다 적은 id_grade 는 전체 보정값 사용
    gbm_params: dict = field(default_factory=lambda: dict(
        n_estimators=500, learning_rate=0.03, num_leaves=15, min_child_samples=15, subsample=0.8,
        subsample_freq=1, colsample_bytree=0.8, reg_lambda=1.0, verbose=-1, n_jobs=1))
    seed: int = 7


# --------------------------------------------------------------------------------------------- 본체
class FairPriceModel:
    """적정가 모델. fit → (OOF 예측으로) calibrate → predict."""

    GBM_CAT = ["category", "subcategory", "gender", "id_grade", "usage_bucket", "defect_level", "msrp_kind"]

    def __init__(self, params: FairPriceParams | None = None, **kw):
        self.params = params or FairPriceParams(**kw)
        self.cal = None
        self.version = VERSION

    # ---- H
    def _fit_h(self, d: pd.DataFrame, y: np.ndarray):
        p = self.params
        keys = level_keys(d)
        hier = _Hier(k_mult=p.k_mult)
        adj = _Adjust(alpha=p.ridge_alpha)
        a = np.zeros(len(d))
        for _ in range(p.backfit_iter):
            hier.fit(keys, y - a, y)
            loc = hier.predict(keys)["loc"].to_numpy()
            adj.fit(d, y - loc, monotone_grade=p.monotone_grade)
            a = adj.predict(d)
        hier.fit(keys, y - a, y)
        return hier, adj

    @staticmethod
    def _h_frame(hier: _Hier, adj: _Adjust, d: pd.DataFrame) -> pd.DataFrame:
        hp = hier.predict(level_keys(d))
        hp["adj"] = adj.predict(d)
        hp["h"] = hp["loc"] + hp["adj"]
        return hp

    # ---- G
    def _gbm_X(self, d: pd.DataFrame, hp: pd.DataFrame) -> pd.DataFrame:
        X = pd.DataFrame(index=d.index)
        for c in ["grade_ord", "age_seasons", "season_start", "usage_uses", "ski_length_cm", "radius_m",
                  "boot_flex", "boot_mondo_mm"]:
            X[c] = d[c].astype(float)
        for c in ["with_binding", "is_new_claim", "like_new_claim", "nego_open", "nego_refused", "urgent_sale",
                  "ship_parcel", "is_apparel_set", "junior"]:
            X[c] = d[c].astype(float)
        X["apparel_size_ord"] = d["apparel_size"].map(APPAREL_SIZE_ORD).astype(float)
        mon = pd.to_datetime(d["observed_month"].astype(str) + "-01", errors="coerce")
        X["month_idx"] = (mon.dt.year - 2026) * 12 + mon.dt.month - 3
        X["log_msrp"] = np.log(d["msrp_krw"].where(d["msrp_krw"] > 0))
        kind = d["msrp_basis"].astype(object).map(lambda s: MSRP_KIND.get(s, "fx" if str(s).startswith("fx")
                                                                        else ("listing" if str(s).startswith("listing") else "none")))
        X["msrp_kind"] = kind
        for c in self.GBM_CAT:
            src = X["msrp_kind"] if c == "msrp_kind" else d[c]
            X[c] = pd.Categorical(src.astype(str), categories=self._gbm_levels[c])
        for c in ["h", "loc", "adj", "loc_subcategory", "loc_brand_subcat", "loc_family", "loc_model",
                  "loc_model_season"]:
            X[f"h_{c}" if not c.startswith("h") else c] = hp[c].to_numpy()
        for lev in ["brand_subcat", "family", "model", "model_season"]:
            X[f"n_{lev}"] = np.log1p(hp[f"n_{lev}"].to_numpy())
        return X

    def _oof_h(self, d: pd.DataFrame, y: np.ndarray, groups) -> pd.DataFrame:
        p = self.params
        folds = group_folds(groups, p.inner_splits, seed=p.seed + 1)
        parts = []
        for f in range(p.inner_splits):
            tr, te = folds != f, folds == f
            if te.sum() == 0 or tr.sum() == 0:
                continue
            h, a = self._fit_h(d[tr], y[tr])
            parts.append(self._h_frame(h, a, d[te]))
        return pd.concat(parts).loc[d.index]

    def fit(self, df: pd.DataFrame, y: np.ndarray | None = None, groups=None):
        d = _prep(df)
        y = np.log(df["price_krw"].astype(float).to_numpy()) if y is None else np.asarray(y, float)
        groups = np.arange(len(d)) if groups is None else np.asarray(groups)
        self.hier, self.adj = self._fit_h(d, y)
        self.gbm = None
        if self.params.use_gbm:
            import lightgbm as lgb
            self._gbm_levels = {c: sorted(set(d[c].astype(str))) if c != "msrp_kind" else
                                ["official", "retailer", "other", "fx", "listing", "none"] for c in self.GBM_CAT}
            hp = self._oof_h(d, y, groups)
            X = self._gbm_X(d, hp)
            self.gbm_features = list(X.columns)
            self.gbm = {}
            for q in (0.1, 0.5, 0.9):
                m = lgb.LGBMRegressor(objective="quantile", alpha=q, random_state=self.params.seed,
                                      **self.params.gbm_params)
                m.fit(X, y)
                self.gbm[q] = m
        self.n_train = len(d)
        return self

    # ---- 예측
    def predict_raw(self, df: pd.DataFrame) -> pd.DataFrame:
        """보정 전 log 예측(h, g10/g50/g90, p50, lo0, hi0)과 H 내부값."""
        d = _prep(df)
        hp = self._h_frame(self.hier, self.adj, d)
        if self.gbm is not None:
            X = self._gbm_X(d, hp)[self.gbm_features]
            g = np.sort(np.column_stack([self.gbm[q].predict(X) for q in (0.1, 0.5, 0.9)]), axis=1)
            hp["g10"], hp["g50"], hp["g90"] = g[:, 0], g[:, 1], g[:, 2]
        else:
            hp["g10"] = hp["g50"] = hp["g90"] = np.nan
        hp["id_grade"] = d["id_grade"].to_numpy()
        return reblend(hp, self.params.blend_w if self.gbm is not None else 1.0)

    @staticmethod
    def _cq(s: np.ndarray, level: float) -> float:
        s = np.sort(s[np.isfinite(s)])
        n = len(s)
        if n == 0:
            return 0.0
        idx = min(int(math.ceil((n + 1) * level)) - 1, n - 1)
        return float(s[max(idx, 0)])

    def calibrate(self, df: pd.DataFrame, raw: pd.DataFrame, y) -> "FairPriceModel":
        """폴드 밖 예측 raw 와 실제 log 호가 y 로 id_grade별 아래·위 보정값을 정한다."""
        y = np.asarray(y, float)
        lvl = 1 - self.params.alpha / 2
        s_lo = raw["lo0"].to_numpy() - y
        s_hi = y - raw["hi0"].to_numpy()
        grade = raw["id_grade"].to_numpy()
        cal = {"_all": (self._cq(s_lo, lvl), self._cq(s_hi, lvl), int(len(y)))}
        for g in pd.unique(grade):
            m = grade == g
            if m.sum() >= self.params.min_cal_n:
                cal[g] = (self._cq(s_lo[m], lvl), self._cq(s_hi[m], lvl), int(m.sum()))
        self.cal = cal
        return self

    def intervals(self, raw: pd.DataFrame) -> pd.DataFrame:
        if self.cal is None:
            raise RuntimeError("calibrate() 를 먼저 호출해야 한다(폴드 밖 예측 필요).")
        q = raw["id_grade"].map(lambda g: self.cal.get(g, self.cal["_all"]))
        qlo = np.array([v[0] for v in q])
        qhi = np.array([v[1] for v in q])
        lo = np.minimum(raw["lo0"].to_numpy() - qlo, raw["p50"].to_numpy())
        hi = np.maximum(raw["hi0"].to_numpy() + qhi, raw["p50"].to_numpy())
        return pd.DataFrame({"log_p10": lo, "log_p50": raw["p50"].to_numpy(), "log_p90": hi}, index=raw.index)

    def predict(self, df: pd.DataFrame, explain: bool = True) -> pd.DataFrame:
        """적정가 P10/P50/P90(원), 근거 수준, 표본 수, 요인별 기여(%), 한 줄 설명."""
        raw = self.predict_raw(df)
        iv = self.intervals(raw)
        out = pd.DataFrame(index=df.index)
        for c in ("p10", "p50", "p90"):
            out[f"fair_{c}"] = np.exp(iv[f"log_{c}"])
        out["basis_level"] = self.basis_level(raw)
        for lev in ["model_season", "model", "family", "brand_subcat", "subcategory", "category"]:
            out[f"n_{lev}"] = raw[f"n_{lev}"].to_numpy()
        out["h_krw"] = np.exp(raw["h"])
        out["g50_krw"] = np.exp(raw["g50"])
        if explain:
            ex = self.explain(df, raw)
            out = out.join(ex)
        return out

    def basis_level(self, raw: pd.DataFrame) -> pd.Series:
        lev = pd.Series("category", index=raw.index, dtype=object)
        for L in ["subcategory", "brand_subcat", "family", "model", "model_season"]:
            lev = lev.where(raw[f"n_{L}"] < self.params.min_n_basis, L)
        return lev

    # ---- 설명
    def explain(self, df: pd.DataFrame, raw: pd.DataFrame | None = None, top: int = 3) -> pd.DataFrame:
        d = _prep(df)
        raw = self.predict_raw(df) if raw is None else raw
        C = self.adj.contrib(d, center=True)
        base = raw["loc"].to_numpy() + self.adj.intercept + self.adj.center.reindex(d["cat_group"]).fillna(0).sum(axis=1).to_numpy()
        basis = self.basis_level(raw)
        gbm = raw["p50"].to_numpy() - raw["h"].to_numpy()
        res = pd.DataFrame(index=df.index)
        res["base_krw"] = np.exp(base)
        for f in C.columns:
            res[f"eff_{f}"] = np.expm1(C[f].to_numpy())
        res["eff_gbm"] = np.expm1(gbm)
        lines = []
        for i, idx in enumerate(df.index):
            lev = basis.iloc[i]
            n = int(raw[f"n_{lev}"].iloc[i])
            medy = raw[f"medy_{lev}"].iloc[i]
            head = f"{LEVEL_KO[lev]} {n}건 중앙값 {_won(np.exp(medy))}" if np.isfinite(medy) else f"{LEVEL_KO[lev]} 기준"
            parts = [head, f"기준 {_won(np.exp(base[i]))}"]
            effs = [(f, C[f].iloc[i]) for f in C.columns]
            effs = [(f, v) for f, v in effs if abs(np.expm1(v)) >= 0.02]
            effs.sort(key=lambda t: -abs(t[1]))
            for f, v in effs[:top]:
                parts.append(f"{_factor_label(f, d.loc[idx], getattr(self.adj, 'months', None))} {_pct(np.expm1(v))}")
            if abs(np.expm1(gbm[i])) >= 0.03:
                parts.append(f"GBM 보정 {_pct(np.expm1(gbm[i]))}")
            lines.append(" · ".join(parts) + f" → {_won(np.exp(raw['p50'].iloc[i]))}")
        res["explain_ko"] = lines
        return res

    # ---- 중요도
    def feature_importance(self) -> pd.DataFrame:
        if self.gbm is None:
            return pd.DataFrame(columns=["feature", "gain"])
        b = self.gbm[0.5].booster_
        return (pd.DataFrame({"feature": b.feature_name(), "gain": b.feature_importance("gain")})
                .sort_values("gain", ascending=False).reset_index(drop=True))

    def summary(self) -> dict:
        return {"version": self.version, "params": asdict(self.params), "n_train": self.n_train,
                "k": {k: (None if v is None or (isinstance(v, float) and np.isnan(v)) else float(v)) for k, v in self.hier.k.items()},
                "grade_coef": {g: float(self.adj.coef.get(f"grade_{g}", 0.0)) for g in ["sealed", "S", "B", "C", "unk"]},
                "calibration": self.cal}

    # ---- 저장
    def save(self, path) -> None:
        with open(path, "wb") as fh:
            pickle.dump({"version": self.version, "model": self}, fh)

    @staticmethod
    def load(path) -> "FairPriceModel":
        with open(path, "rb") as fh:
            obj = pickle.load(fh)
        if obj.get("version") != VERSION:
            raise ValueError(f"모델 버전 불일치: {obj.get('version')} != {VERSION}")
        return obj["model"]


# --------------------------------------------------------------------------------------------- 블렌드·교차 적합
def reblend(raw: pd.DataFrame, w: float) -> pd.DataFrame:
    """raw(h, g10/g50/g90)에서 가중치 w 로 p50 과 보정 전 구간 lo0/hi0 를 다시 만든다(G 없으면 H만)."""
    r = raw.copy()
    if r["g50"].notna().all():
        r["p50"] = w * r["h"] + (1 - w) * r["g50"]
        r["lo0"] = r["p50"] - np.maximum(r["g50"] - r["g10"], 0.02)
        r["hi0"] = r["p50"] + np.maximum(r["g90"] - r["g50"], 0.02)
    else:
        r["p50"] = r["h"]
        r["lo0"] = r["hi0"] = r["p50"]
    return r


def cross_fit(df: pd.DataFrame, groups, folds: np.ndarray, params: FairPriceParams | None = None,
              y: np.ndarray | None = None) -> tuple[pd.DataFrame, list[FairPriceModel]]:
    """폴드별 적합 → 폴드 밖 raw 예측 → 교차 컨포멀(다른 폴드의 OOF 점수로 보정) 구간.

    반환: (oof: raw 열 + log_p10/p50/p90 + fold, models: 폴드별 보정된 모델)
    """
    params = params or FairPriceParams()
    y = np.log(df["price_krw"].astype(float).to_numpy()) if y is None else np.asarray(y, float)
    groups = np.asarray(groups)
    folds = np.asarray(folds)
    raws, models = [], []
    for f in sorted(np.unique(folds)):
        tr, te = folds != f, folds == f
        m = FairPriceModel(params).fit(df[tr], y[tr], groups[tr])
        r = m.predict_raw(df[te])
        r["fold"] = f
        raws.append(r)
        models.append(m)
    raw = pd.concat(raws).loc[df.index]
    ivs = []
    for f, m in zip(sorted(np.unique(folds)), models):
        other = folds != f
        m.calibrate(df[other], raw[other], y[other])
        ivs.append(m.intervals(raw[folds == f]))
    iv = pd.concat(ivs).loc[df.index]
    return raw.join(iv.drop(columns=["log_p50"])).assign(log_p50=raw["p50"]), models


# --------------------------------------------------------------------------------------------- 표기 도우미
def _won(x: float) -> str:
    if x is None or not np.isfinite(x):
        return "-"
    v = x / 10000
    if v >= 10:
        return f"{v:,.0f}만원"
    if v >= 1:
        s = f"{v:.1f}".rstrip("0").rstrip(".")
        return f"{s}만원"
    return f"{x:,.0f}원"


def _pct(v: float) -> str:
    s = f"{abs(v) * 100:.0f}%"
    return ("+" if v >= 0 else "−") + s


def _factor_label(f: str, row: pd.Series, months=None) -> str:
    if f == "grade":
        return GRADE_KO.get(row["grade"], "상태")
    if f == "age":
        a = row["age_seasons"]
        if pd.isna(a):
            return "연식 미상"
        return "당해 시즌" if a <= 0 else f"{int(a)}시즌 경과"
    if f == "usage":
        return USAGE_KO.get(row["usage_bucket"], "사용 정도")
    if f == "binding":
        return "바인딩 포함" if row["with_binding"] else "바인딩 없음"
    if f == "new_claim":
        return "새상품 표기" if row["is_new_claim"] else "새상품 표기 없음"
    if f == "like_new":
        return "새것 같음 표기" if row["like_new_claim"] else "새것 같음 표기 없음"
    if f == "defect":
        return DEFECT_KO.get(row["defect_level"], "하자")
    if f == "junior":
        return "주니어" if row["junior"] else "성인용"
    if f == "size":
        if pd.notna(row["ski_length_cm"]) and row["cat_group"] == "ski":
            return f"길이 {int(row['ski_length_cm'])}cm"
        if pd.notna(row["boot_mondo_mm"]) and row["cat_group"] == "boots":
            return f"몬도 {int(row['boot_mondo_mm'])}"
        return "사이즈"
    if f == "month":
        m = str(row["observed_month"])
        if not (len(m) >= 7 and m[-2:].isdigit()):
            return "게시 월"
        if months and m not in months:
            # 학습에 없던 달은 _Adjust가 마지막 학습 월의 효과로 계산한다: 실제 달 이름을 붙이지 않는다
            return f"게시 월({int(str(months[-1])[-2:])}월 기준)"
        return f"{int(m[-2:])}월 게시"
    return f


__all__ = ["FairPriceModel", "FairPriceParams", "cross_fit", "reblend", "group_folds", "level_keys", "LEVELS", "LEVEL_KO",
           "VERSION"]
