#!/usr/bin/env python3
"""과거 백업 DB로 감시기 파이프라인을 다시 돌려 보는 검증 도구(원본은 읽기만 하고, Discord로 보내지 않는다).

백업 DB의 마지막 N건을 관측 시각 순으로, source.run_once가 상세 파싱 뒤에 하는 순서 그대로
save_item → enrich_item → should_alert → build_payload로 처리하고 집계만 출력한다.

- 원본 DB는 읽기 전용(URI mode=ro)으로 연다. 처리는 /tmp 아래 임시 폴더의 새 DB(source.init_db로 만든 것)에서 하고,
  끝나면 임시 폴더를 지운다(백업에는 작성자·연락처가 있으므로 --keep을 주지 않으면 남기지 않는다).
- 기본으로 재생 구간 시작 전 180일(재게시 조회 기간) 안의 글을 임시 DB의 seen에 먼저 넣는다
  (운영 DB에 이미 쌓여 있었을 글. 재게시 탐지가 구간 밖의 이전 글도 찾을 수 있게). --no-history면 구간의 글만 쓴다.
- 백업 seen에 없는 값(썸네일, 거래지역, 상태 목록의 네고·거래방법)은 비워 둔다. 임베드 크기는 그만큼 작게 나온다.
- 출력은 집계 값뿐이다(제목·작성자·연락처·글번호를 내지 않는다). requests.post는 막아 둔다.
- 분석 저장소의 매물 묶음(listings_clean.parquet의 item_id·item_seq)이 있으면 재게시 탐지의 재현율·정밀도를 함께 낸다.

실행(분석 컨테이너 안에서. 아래 bin/dx는 분석 저장소 루트에서 부른다):
  cd <분석 저장소 루트>          # analysis/drspark (bin/dx가 있는 폴더)
  bin/dx sh -c 'cd /work/alarm && python tools/replay_backup.py -n 800'
  bin/dx sh -c 'cd /work/alarm && python tools/replay_backup.py -n 200 --json /tmp/replay_200.json'
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import shutil
import sqlite3
import sys
import tempfile
import time
import urllib.parse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# 컨테이너에는 분석 저장소의 drspark(/work/src)도 경로에 있다. 동기화된 사본(alarm/drspark)을 먼저 보게 한다.
while str(ROOT) in sys.path:
    sys.path.remove(str(ROOT))
sys.path.insert(0, str(ROOT))

DEFAULT_DB = "/scratch/raw/drspark_seen.db"
DEFAULT_LISTINGS = "/work/data/curated/listings_clean.parquet"
DEFAULT_OOF = "/work/data/curated/fair_price_estimates.csv"
KST = timezone(timedelta(hours=9))
SEEN_COLUMNS = ["post_id", "observed_at", "title", "price", "author", "status", "body_content", "release_year",
                "model_serial", "spec", "purchased_at", "usage_count", "features", "phone", "email"]
# 알림 필터 시나리오: 이름 → (ALERT_MODE, ALERT_CATEGORIES, ALERT_SKIP_WANTED)
SCENARIOS = {
    "all": ("all", set(), False),
    "all_skip_wanted": ("all", set(), True),
    "deals": ("deals", set(), False),
    "deals_ski_boots": ("deals", {"ski", "boots"}, False),
}
DISCORD_LIMITS = {"title": 256, "field_name": 256, "field_value": 1024, "fields": 25, "embed_total": 6000,
                  "content": 2000}


# ------------------------------------------------------------------------------------------------ 보조
def mem_mb() -> dict:
    """현재 프로세스의 RSS(VmRSS)와 최대 RSS(VmHWM), MB. /proc이 없으면 빈 dict."""
    out = {}
    try:
        with open("/proc/self/status", encoding="ascii") as f:
            for line in f:
                if line.startswith(("VmRSS:", "VmHWM:")):
                    k, v = line.split(":", 1)
                    out[k] = round(int(v.split()[0]) / 1024, 1)
    except OSError:
        pass
    return out


def pct(values, q: float):
    """최근접 순위 백분위(q: 0~100). 빈 목록이면 None."""
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, math.ceil(q / 100 * len(s)) - 1))
    return s[k]


def stats(values, nd: int = 1) -> dict:
    if not values:
        return {"n": 0}
    r = lambda v: round(v, nd)  # noqa: E731
    return {"n": len(values), "median": r(pct(values, 50)), "p95": r(pct(values, 95)), "max": r(max(values)),
            "mean": r(sum(values) / len(values))}


def share(n: int, d: int) -> float | None:
    return round(n / d, 4) if d else None


class CountingHandler(logging.Handler):
    """WARNING 이상 로그를 (로거, 레벨)별로 센다(메시지는 저장하지 않는다)."""

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.counts = Counter()

    def emit(self, record):
        self.counts[f"{record.name}:{record.levelname}"] += 1


def read_backup(path: str) -> list[tuple]:
    uri = "file:" + urllib.parse.quote(str(Path(path).resolve())) + "?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    try:
        cols = ", ".join(SEEN_COLUMNS)
        return con.execute(f"SELECT {cols} FROM seen WHERE observed_at IS NOT NULL "
                           f"ORDER BY observed_at, CAST(post_id AS INTEGER), post_id").fetchall()
    finally:
        con.close()


def row_to_item(source, row: tuple) -> dict:
    """seen 행 → run_once가 normalize_item_from_et_vars 뒤에 넘기는 모양의 매물 dict."""
    r = dict(zip(SEEN_COLUMNS, row))
    pid = str(r["post_id"])
    status = r["status"] or ""
    return {
        "id": pid, "url": f"{source.BASE}/ski_sell2/{pid}", "title": r["title"] or "", "raw_price": r["price"],
        "author": r["author"] or "", "observed_at": int(r["observed_at"]), "status": status,
        "status_list": [status] if status else [], "body_content": r["body_content"],
        "release_year": r["release_year"], "model_serial": r["model_serial"], "spec": r["spec"],
        "purchased_at": r["purchased_at"], "usage_count": r["usage_count"], "features": r["features"],
        "phone": r["phone"], "email": r["email"], "region": None, "thumb": None, "et_vars": {},
    }


def embed_total(embed: dict) -> int:
    """Discord가 6000자 상한에 세는 글자 수(제목 + 필드 이름·값). source._fit_embed와 같은 계산."""
    return len(embed.get("title") or "") + sum(len(f["name"]) + len(f["value"]) for f in embed.get("fields", []))


# ------------------------------------------------------------------------------------------------ 본체
def replay(args) -> dict:
    t_begin = time.perf_counter()
    mem_start = mem_mb()

    rows = read_backup(args.db)
    if not rows:
        raise SystemExit(f"{args.db}: seen 테이블이 비어 있다")
    n = min(args.n, len(rows))
    window = rows[-n:]
    w_first, w_last = int(window[0][1]), int(window[-1][1])

    tmp_root = args.tmp if args.tmp and os.path.isdir(args.tmp) else tempfile.gettempdir()
    tmp = Path(tempfile.mkdtemp(prefix="drspark-replay-", dir=tmp_root))
    db_path = tmp / "drspark_seen.db"
    handlers = []
    try:
        # source는 import할 때 DRSPARK_DB·DISCORD_WEBHOOK_URL을 읽고 현재 폴더에 drspark.log를 만든다
        os.environ["DRSPARK_DB"] = str(db_path)
        for k in ("DISCORD_WEBHOOK_URL", "ALERT_MODE", "ALERT_CATEGORIES", "ALERT_SKIP_WANTED", "DEAL_MENTION"):
            os.environ.pop(k, None)
        if args.pricing_dir:
            os.environ["DRSPARK_PRICING_DIR"] = args.pricing_dir
        if args.no_pricing:
            os.environ["DRSPARK_PRICING"] = "0"
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            import requests

            import source
        finally:
            os.chdir(cwd)
        mem_after_import = mem_mb()

        # 로그: 화면·syslog 출력은 끄고(글 단위 INFO가 많다) 임시 폴더의 파일 로그만 남긴다. 경고 이상은 센다
        lg = logging.getLogger("drspark")
        for h in list(lg.handlers):
            if not isinstance(h, logging.FileHandler):
                lg.removeHandler(h)
        counter = CountingHandler()
        lg.addHandler(counter)
        handlers = [h for h in lg.handlers if isinstance(h, logging.FileHandler)]

        # 전송 차단: 웹후크 없음 + requests.post 자체를 막는다(build_payload만 부른다)
        def _blocked(*a, **kw):
            raise RuntimeError("replay_backup: 네트워크 전송은 막혀 있다")

        requests.post = _blocked
        requests.sessions.Session.request = _blocked
        source.DISCORD_WEBHOOK = None
        source.DEAL_MENTION = ""

        # _fit_embed 앞에서 줄이기 전 크기를 기록한다(동작은 그대로)
        fit_pre = []
        _orig_fit = source._fit_embed

        def _fit_spy(embed):
            fit_pre.append(embed_total(embed))
            return _orig_fit(embed)

        source._fit_embed = _fit_spy

        source.init_db()
        history = []
        if args.history:
            lookback = args.lookback_days * 86400
            history = [r for r in rows[:-n] if int(r[1]) >= w_first - lookback] if n < len(rows) else []
            with sqlite3.connect(db_path) as con:
                con.executemany(f"INSERT INTO seen({', '.join(SEEN_COLUMNS)}) VALUES "
                                f"({', '.join('?' for _ in SEEN_COLUMNS)})", history)
                con.commit()

        # 적정가 보강 읽기(운영의 __main__과 같은 Enricher.from_env). 무거운 의존성 import 시간까지 포함
        mem_before_load = mem_mb()
        t0 = time.perf_counter()
        source.ENRICHER = source.Enricher.from_env() if source.Enricher is not None else None
        load_s = time.perf_counter() - t0
        mem_after_load = mem_mb()
        if source.ENRICHER is None and not args.no_pricing:
            raise SystemExit("Enricher를 읽지 못했다(pricing_data/ 확인). 보강 없이 돌리려면 --no-pricing")
        drspark_mod = sys.modules.get("drspark")
        drspark_path = getattr(drspark_mod, "__file__", None)

        from pricing import discord_fields as DF

        # ---------------------------------------------------------------- 재생
        errors = Counter()
        error_types = Counter()
        lat_internal, lat_wall = [], []
        clock_skew = 0
        dist = defaultdict(Counter)
        alerts = {k: 0 for k in SCENARIOS}
        alerts_by_day = {k: Counter() for k in SCENARIOS}
        deals_reason = Counter()
        payload_total, payload_bytes = [], []
        max_field_value = max_field_name = max_fields = max_title = max_content = 0
        violations = Counter()
        field_presence = Counter()
        n_model = n_family = n_fair = n_deal = n_cut = n_repost = n_repost_change = 0
        privacy_checked = privacy_leaks = 0
        detected = {}  # post_id → prev_post_id(재게시로 찾은 글)
        replayed_ids = []
        records = []  # (item, enrichment) — 표본 밖 적정가 민감도 계산용(임시, 출력하지 않는다)

        t_loop = time.perf_counter()
        for row in window:
            item = row_to_item(source, row)
            pid = item["id"]
            replayed_ids.append(pid)
            try:
                source.save_item(item)
            except Exception as ex:
                errors["save_item"] += 1
                error_types[f"save_item:{type(ex).__name__}"] += 1
                continue

            t1 = time.perf_counter()
            e = source.enrich_item(item)
            lat_wall.append((time.perf_counter() - t1) * 1000)
            if source.ENRICHER is not None:
                if e is None:
                    errors["enrich_none"] += 1
                else:
                    lat_internal.append(e.elapsed_ms)
                    # Enrichment.elapsed_ms는 time.time()(벽시계) 차이라 시계 보정이 끼면 실제보다 크게 나온다
                    clock_skew += e.elapsed_ms > lat_wall[-1] + 5
                    if e.error:
                        errors["enrich_error"] += 1
                        error_types[f"enrich:{e.error.split(':', 1)[0]}"] += 1
                    if e.extra.get("repost_error"):
                        errors["repost_error"] += 1
                        error_types[f"repost:{e.extra['repost_error'].split(':', 1)[0]}"] += 1

            if e is not None and not e.error:
                dist["listing_type"][e.listing_type] += 1
                dist["category"][e.category or "none"] += 1
                dist["id_grade"][e.id_grade] += 1
                dist["deal_flag"][e.deal_flag or "none"] += 1
                dist["deal_confidence"][e.deal_confidence or "none"] += 1
                dist["skipped_reason"][e.skipped_reason or "none"] += 1
                dist["basis_level"][e.basis_level or "none"] += 1
                dist["price_kind"][e.price_kind or "none"] += 1
                n_model += e.model_id is not None
                n_family += e.family_id is not None
                n_fair += e.fair_p50 is not None
                n_deal += DF.is_deal(e)
                n_cut += DF.is_price_cut(e)
                if e.repost is not None:
                    n_repost += 1
                    n_repost_change += e.repost.change_pct is not None
                    detected[pid] = e.repost.prev_post_id
                if e.deal_flag and e.deal_confidence:
                    dist["flag_x_conf"][f"{e.deal_flag}/{e.deal_confidence}"] += 1
                records.append((item, e))

            # 보강 결과(enrichment 행)에 연락처가 새지 않았는지
            if source.ENRICHER is not None and (item.get("phone") or item.get("email")):
                with sqlite3.connect(db_path) as con:
                    erow = con.execute("SELECT * FROM enrichment WHERE post_id = ?", (pid,)).fetchone()
                if erow is not None:
                    privacy_checked += 1
                    text = " ".join(str(v) for v in erow[1:] if isinstance(v, str))
                    secrets_ = [s for s in (item.get("phone"), item.get("email")) if s and len(s.strip()) >= 6]
                    privacy_leaks += any(s.strip() in text for s in secrets_)

            # 알림 필터(시나리오별로 모듈 설정을 바꿔 같은 should_alert를 부른다)
            day = datetime.fromtimestamp(item["observed_at"], tz=KST).strftime("%Y-%m-%d")
            saved = (source.ALERT_MODE, source.ALERT_CATEGORIES, source.ALERT_SKIP_WANTED)
            try:
                for name, (mode, cats, skip) in SCENARIOS.items():
                    source.ALERT_MODE, source.ALERT_CATEGORIES, source.ALERT_SKIP_WANTED = mode, set(cats), skip
                    ok = source.should_alert(item, e)
                    alerts[name] += bool(ok)
                    if ok:
                        alerts_by_day[name][day] += 1
                    if ok and name == "deals":
                        if e is None or e.error:
                            deals_reason["no_enrichment"] += 1
                        elif DF.is_deal(e):
                            deals_reason["undervalued"] += 1
                        else:
                            deals_reason["price_cut"] += 1
            except Exception as ex:
                errors["should_alert"] += 1
                error_types[f"should_alert:{type(ex).__name__}"] += 1
            finally:
                source.ALERT_MODE, source.ALERT_CATEGORIES, source.ALERT_SKIP_WANTED = saved

            # 페이로드(모든 글에 대해 만든다. all 모드에서는 모든 글이 알림이다)
            try:
                payload = source.build_payload(item, e)
                b = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
            except Exception as ex:
                errors["build_payload"] += 1
                error_types[f"build_payload:{type(ex).__name__}"] += 1
                continue
            emb = payload["embeds"][0]
            tot = embed_total(emb)
            payload_total.append(tot)
            payload_bytes.append(b)
            flds = emb.get("fields", [])
            max_fields = max(max_fields, len(flds))
            max_title = max(max_title, len(emb.get("title") or ""))
            max_content = max(max_content, len(payload.get("content") or ""))
            for f in flds:
                max_field_value = max(max_field_value, len(f["value"]))
                max_field_name = max(max_field_name, len(f["name"]))
                field_presence[f["name"]] += 1
                violations["field_value>1024"] += len(f["value"]) > DISCORD_LIMITS["field_value"]
                violations["field_name>256"] += len(f["name"]) > DISCORD_LIMITS["field_name"]
                violations["empty_field_value"] += not f["value"].strip()
            violations["title>256"] += len(emb.get("title") or "") > DISCORD_LIMITS["title"]
            violations["fields>25"] += len(flds) > DISCORD_LIMITS["fields"]
            violations["embed_total>6000"] += tot > DISCORD_LIMITS["embed_total"]
            violations["content>2000"] += len(payload.get("content") or "") > DISCORD_LIMITS["content"]
        loop_s = time.perf_counter() - t_loop
        mem_end = mem_mb()

        n_ok = sum(dist["id_grade"].values())
        span_days = max((w_last - w_first) / 86400, 1e-9)
        # 날짜별 값은 알림이 0건인 날도 넣는다(구간의 첫날·마지막 날은 하루 전체가 아닐 수 있다)
        dates = sorted({datetime.fromtimestamp(int(r[1]), tz=KST).strftime("%Y-%m-%d") for r in window})

        def per_day(name):
            c = alerts_by_day[name]
            daily = [c.get(d, 0) for d in dates]
            return {"per_day_mean": round(alerts[name] / span_days, 2),
                    "per_date_median": pct(daily, 50), "per_date_max": max(daily), "per_date_min": min(daily)}

        result = {
            "config": {
                "n_requested": args.n, "n_replayed": len(window), "history_rows": len(history),
                "history": bool(args.history), "lookback_days": args.lookback_days,
                "window_start_kst": datetime.fromtimestamp(w_first, tz=KST).strftime("%Y-%m-%d %H:%M"),
                "window_end_kst": datetime.fromtimestamp(w_last, tz=KST).strftime("%Y-%m-%d %H:%M"),
                "window_days": round(span_days, 2), "window_dates": len(dates), "backup_rows": len(rows),
                "posts_per_day": round(len(window) / span_days, 1),
                "pricing": source.ENRICHER.version if source.ENRICHER else "off",
                "drspark_module": os.path.relpath(drspark_path, ROOT) if drspark_path else None,
            },
            "performance": {
                "enricher_load_s": round(load_s, 2),
                "rss_mb": {"start": mem_start.get("VmRSS"), "after_import_source": mem_after_import.get("VmRSS"),
                           "before_enricher": mem_before_load.get("VmRSS"),
                           "after_enricher": mem_after_load.get("VmRSS"), "end": mem_end.get("VmRSS"),
                           "peak": mem_end.get("VmHWM")},
                "enrich_internal_ms": stats(lat_internal),
                "internal_exceeds_wall": clock_skew,
                "enrich_item_wall_ms": stats(lat_wall),
                "first_enrich_wall_ms": round(lat_wall[0], 1) if lat_wall else None,
                "replay_loop_s": round(loop_s, 1),
                "total_s": round(time.perf_counter() - t_begin, 1),
            },
            "errors": {"by_stage": dict(errors), "by_type": dict(error_types),
                       "log_records_warning_or_above": dict(counter.counts)},
            "enrichment": {
                "n_enriched_ok": n_ok,
                "model_resolved": n_model, "model_resolved_share": share(n_model, n_ok),
                "family_resolved": n_family, "family_resolved_share": share(n_family, n_ok),
                "fair_price": n_fair, "fair_price_share": share(n_fair, n_ok),
                "is_deal": n_deal, "is_price_cut": n_cut,
                "repost_detected": n_repost, "repost_with_price_change": n_repost_change,
                "distributions": {k: dict(v.most_common()) for k, v in dist.items()},
                "privacy_checked_rows": privacy_checked, "privacy_leaks": privacy_leaks,
            },
            "alerts": {name: {"count": alerts[name], "share": share(alerts[name], len(window)), **per_day(name)}
                       for name in SCENARIOS},
            "deals_alert_reason": dict(deals_reason),
            "payload": {
                "n": len(payload_total),
                "embed_total_chars": stats(payload_total, 0),
                "json_bytes": stats(payload_bytes, 0),
                "max_field_value_chars": max_field_value, "max_field_name_chars": max_field_name,
                "max_field_count": max_fields, "max_title_chars": max_title, "max_content_chars": max_content,
                "pre_fit_embed_total_max": max(fit_pre) if fit_pre else None,
                "fit_truncated": sum(1 for v in fit_pre if v > DISCORD_LIMITS["embed_total"]),
                "limit_violations": {k: v for k, v in violations.items()},
                "field_presence": dict(field_presence.most_common()),
            },
        }

        if args.listings and Path(args.listings).exists():
            result["repost_vs_analysis"] = evaluate_reposts(args.listings, window, rows, replayed_ids, detected,
                                                            db_path, args.lookback_days)
            result["in_sample"] = in_sample_share(args.listings, replayed_ids)
            result["labels_vs_analysis"] = label_agreement(args.listings, records) if records else None
        else:
            result["repost_vs_analysis"] = None
        if source.ENRICHER is not None and args.oof and Path(args.oof).exists():
            result["oof_sensitivity"] = oof_sensitivity(source, DF, records, args.oof, span_days)
        return result
    finally:
        for h in handlers:
            try:
                h.close()
                logging.getLogger("drspark").removeHandler(h)
            except Exception:
                pass
        if args.keep:
            print(f"[replay] 임시 폴더를 남겼다(개인정보 포함, 확인 후 지울 것): {tmp}", file=sys.stderr)
        else:
            shutil.rmtree(tmp, ignore_errors=True)


def oof_sensitivity(source, DF, records, path: str, span_days: float) -> dict:
    """표본 안(in-sample) 판정을 표본 밖 적정가로 바꿔 보면 저평가·deals 알림이 얼마나 되는가.

    적정가 모델은 재생한 글 대부분을 학습에 썼다. 분석 저장소의 fair_price_estimates.csv에는 학습 행마다
    판매자 5겹 CV의 폴드 밖 모델(그 판매자를 보지 않은 모델)로 낸 P10/P50/P90이 있다(oof=True).
    그런 글은 Enricher의 모델 출력만 그 값으로 바꿔(Enricher.enrich를 그대로 다시 불러 판정·신뢰 규칙은 같게)
    deals 필터를 다시 센다. 재게시 정보는 원래 보강 결과의 것을 쓴다. 폴드 밖 값이 없는 글은 원래 판정 그대로.
    한계: 폴드 밖 값은 분석의 최종 라벨(LLM 검수 포함)로 만든 것이라 감시기의 규칙 식별과 다를 수 있다.
    """
    import pandas as pd

    cols = ["post_id", "oof", "fair_p10", "fair_p50", "fair_p90", "basis_level", "n_model_season", "n_model",
            "n_family", "n_brand_subcat"]
    est = pd.read_csv(path, usecols=cols, dtype={"post_id": str})
    est = est[est["oof"].astype(bool)].drop_duplicates("post_id").set_index("post_id")
    enr = source.ENRICHER
    model = enr.model
    current = {}

    def predict(frame, **kw):
        r = current["row"]
        b = r["basis_level"]
        nb = r.get(f"n_{b}")  # subcategory·category 수준은 CSV에 표본 수 열이 없다(None → 신뢰 '참고용')
        row = {"fair_p10": float(r["fair_p10"]), "fair_p50": float(r["fair_p50"]), "fair_p90": float(r["fair_p90"]),
               "basis_level": b, f"n_{b}": nb, "explain_ko": None}
        return pd.DataFrame([row], index=frame.index)

    flags_in, flags_oof = Counter(), Counter()
    deal_in = deal_oof = n_sub = 0
    p50_diff = []
    alerts = 0
    reason = Counter()
    saved = (source.ALERT_MODE, source.ALERT_CATEGORIES, source.ALERT_SKIP_WANTED)
    orig = vars(model).get("predict")  # 인스턴스에 덮어쓴 predict가 이미 있으면 되돌릴 값
    try:
        source.ALERT_MODE, source.ALERT_CATEGORIES, source.ALERT_SKIP_WANTED = "deals", set(), False
        model.predict = predict
        for item, e in records:
            use = e
            if e.fair_p50 and item["id"] in est.index:
                current["row"] = est.loc[item["id"]]
                e2 = enr.enrich(item, None)
                if not e2.error and e2.fair_p50:
                    e2.repost = e.repost
                    use = e2
                    n_sub += 1
                    flags_in[e.deal_flag or "none"] += 1
                    flags_oof[e2.deal_flag or "none"] += 1
                    deal_in += DF.is_deal(e)
                    deal_oof += DF.is_deal(e2)
                    p50_diff.append(abs(math.log(e2.fair_p50 / e.fair_p50)) * 100)
            if source.should_alert(item, use):
                alerts += 1
                reason["undervalued" if DF.is_deal(use) else "price_cut"] += 1
    finally:
        if orig is None:
            del model.predict
        else:
            model.predict = orig
        source.ALERT_MODE, source.ALERT_CATEGORIES, source.ALERT_SKIP_WANTED = saved
    return {
        "n_records": len(records), "n_replaced_with_oof": n_sub,
        "p50_abs_log_diff_pct": stats(p50_diff),
        "flags_in_sample": dict(flags_in.most_common()), "flags_oof": dict(flags_oof.most_common()),
        "is_deal_in_sample": deal_in, "is_deal_oof": deal_oof,
        "deals_alerts": alerts, "deals_per_day": round(alerts / span_days, 2), "deals_reason": dict(reason),
    }


def label_agreement(listings: str, records) -> dict:
    """감시기(규칙·별칭만)의 식별을 분석 저장소의 최종 라벨(LLM 검수 포함)과 비교한다(집계만)."""
    import pandas as pd

    df = pd.read_parquet(listings, columns=["post_id", "listing_type", "category", "family_id", "model_id"])
    df["post_id"] = df.post_id.astype(str)
    ref = df.set_index("post_id").to_dict("index")
    c = Counter()
    for item, e in records:
        r = ref.get(item["id"])
        if r is None:
            continue
        c["n"] += 1
        c["listing_type_same"] += e.listing_type == r["listing_type"]
        c["category_same"] += e.category == r["category"]
        a_m, r_m = e.model_id, r["model_id"] if isinstance(r["model_id"], str) else None
        c["ref_model"] += r_m is not None
        c["alarm_model"] += a_m is not None
        c["both_model"] += a_m is not None and r_m is not None
        c["both_model_same"] += a_m is not None and a_m == r_m
        c["alarm_model_ref_none"] += a_m is not None and r_m is None
        a_f, r_f = e.family_id, r["family_id"] if isinstance(r["family_id"], str) else None
        c["both_family"] += a_f is not None and r_f is not None
        c["both_family_same"] += a_f is not None and a_f == r_f
    n = c["n"]
    return {**dict(c), "listing_type_agree": share(c["listing_type_same"], n),
            "category_agree": share(c["category_same"], n),
            "model_recall_vs_ref": share(c["both_model"], c["ref_model"]),
            "model_agree_when_both": share(c["both_model_same"], c["both_model"]),
            "family_agree_when_both": share(c["both_family_same"], c["both_family"])}


def in_sample_share(listings: str, replayed_ids: list[str]) -> dict:
    import pandas as pd

    df = pd.read_parquet(listings, columns=["post_id", "in_price_model"])
    d = df[df.post_id.astype(str).isin(set(replayed_ids))]
    n_in = int(d.in_price_model.fillna(False).astype(bool).sum())
    return {"n_matched": int(len(d)), "in_price_model": n_in, "in_price_model_share": share(n_in, len(d))}


def evaluate_reposts(listings: str, window, rows, replayed_ids, detected: dict, db_path, lookback_days: int) -> dict:
    """분석 저장소의 매물 묶음(item_id, item_seq)과 find_previous 결과를 비교한다.

    재현율: 재생한 글 중 item_seq > 1(분석이 재게시로 본 글)에서 find_previous가 이전 글을 찾은 비율.
    정밀도: find_previous가 찾은 글 중 직전 글(prev_post_id)이 같은 item_id인 비율.
    놓친 글은 같은 매물의 바로 앞 글과 posts_match를 다시 대어 사유 코드를 센다(집계만).
    """
    import pandas as pd

    from drspark import listing_units as L
    from drspark import parse as P
    from pricing.repost import _post_key

    df = pd.read_parquet(listings, columns=["post_id", "item_id", "item_seq"])
    df["post_id"] = df.post_id.astype(str)
    item_of = dict(zip(df.post_id, df.item_id))
    seq_of = dict(zip(df.post_id, df.item_seq.astype(int)))
    row_of = {str(r[0]): r for r in rows}
    by_item = defaultdict(list)
    for r in rows:
        pid = str(r[0])
        if pid in item_of:
            by_item[item_of[pid]].append((int(r[1]), pid))
    with sqlite3.connect(db_path) as con:
        in_db = {str(r[0]) for r in con.execute("SELECT post_id FROM seen")}

    replayed = [p for p in replayed_ids if p in item_of]
    truth = [p for p in replayed if seq_of[p] > 1]
    tp_recall = [p for p in truth if p in detected]
    det = [p for p in replayed if p in detected]
    same_item = [p for p in det if item_of.get(detected[p]) == item_of[p]]
    det_on_truth = [p for p in det if seq_of[p] > 1]

    miss_reason = Counter()
    for p in truth:
        if p in detected:
            continue
        t = int(row_of[p][1])
        earlier = sorted(x for x in by_item[item_of[p]] if x[0] < t or (x[0] == t and x[1] != p and _lt(x[1], p)))
        if not earlier:
            miss_reason["no_earlier_post_in_backup"] += 1
            continue
        _, prev = earlier[-1]
        pr, cr = row_of[prev], row_of[p]
        if prev not in in_db:
            miss_reason["prev_outside_lookback"] += 1
        elif t - int(pr[1]) > lookback_days * 86400:
            miss_reason["prev_older_than_lookback"] += 1
        elif not _lt(prev, p):
            miss_reason["prev_has_larger_post_id"] += 1
        elif (pr[4] or "").strip() != (cr[4] or "").strip():
            miss_reason["author_differs"] += 1
        else:
            k_new, _ = _post_key(L, P, p, cr[4], cr[1], cr[2], cr[8], cr[9], cr[7], cr[6], cr[3])
            k_old, _ = _post_key(L, P, prev, pr[4], pr[1], pr[2], pr[8], pr[9], pr[7], pr[6], pr[3])
            ok, _r, why = L.posts_match(k_new, k_old)
            miss_reason["pair_matches_but_not_found" if ok else f"posts_match:{why}"] += 1

    fp_kind = Counter()
    for p in det:
        if item_of.get(detected[p]) == item_of[p]:
            continue
        if detected[p] not in item_of:
            fp_kind["prev_not_in_listings"] += 1
        elif seq_of[p] == 1:
            fp_kind["analysis_says_first_post"] += 1
        else:
            fp_kind["analysis_links_other_item"] += 1

    return {
        "n_replayed_in_listings": len(replayed),
        "analysis_reposts": len(truth),
        "detected": len(det),
        "recall": share(len(tp_recall), len(truth)),
        "recall_hits": len(tp_recall),
        "precision_same_item": share(len(same_item), len(det)),
        "precision_hits": len(same_item),
        "detected_on_analysis_reposts": len(det_on_truth),
        "precision_any_repost": share(len(det_on_truth), len(det)),
        "miss_reasons": dict(miss_reason.most_common()),
        "false_positive_kinds": dict(fp_kind.most_common()),
    }


def _lt(a: str, b: str) -> bool:
    """글번호 a < b(숫자면 숫자로)."""
    if a.isdigit() and b.isdigit():
        return int(a) < int(b)
    return a < b


# ------------------------------------------------------------------------------------------------ 출력
def print_summary(r: dict) -> None:
    c, p, en, al, pl = r["config"], r["performance"], r["enrichment"], r["alerts"], r["payload"]
    print(f"== 재생 {c['n_replayed']}건 ({c['window_start_kst']} ~ {c['window_end_kst']} KST, {c['window_days']}일) "
          f"| 사전 적재 {c['history_rows']}건 | 보강 {c['pricing']} | drspark={c['drspark_module']}")
    print(f"Enricher 로드 {p['enricher_load_s']}s | RSS(MB) {p['rss_mb']} ")
    print(f"보강 지연(ms, Enricher 내부) {p['enrich_internal_ms']} | enrich_item 전체 {p['enrich_item_wall_ms']} "
          f"| 첫 호출 {p['first_enrich_wall_ms']}ms | 루프 {p['replay_loop_s']}s "
          f"| 내부값>전체값(시계 보정 의심) {p['internal_exceeds_wall']}건")
    print(f"오류 {r['errors']}")
    print(f"모델 식별 {en['model_resolved']}/{en['n_enriched_ok']} ({en['model_resolved_share']}) | "
          f"모델군 {en['family_resolved_share']} | 적정가 산출 {en['fair_price_share']} | "
          f"저평가(알림 강조) {en['is_deal']} | 가격인하 {en['is_price_cut']} | 재게시 {en['repost_detected']} "
          f"| 연락처 유출 {en['privacy_leaks']}/{en['privacy_checked_rows']}")
    for k, v in en["distributions"].items():
        print(f"  {k}: {v}")
    for k, v in al.items():
        print(f"알림[{k}] {v}")
    print(f"deals 사유 {r['deals_alert_reason']}")
    print(f"페이로드 임베드 글자 {pl['embed_total_chars']} | JSON 바이트 {pl['json_bytes']} | 필드 값 최대 "
          f"{pl['max_field_value_chars']} | 필드 수 최대 {pl['max_field_count']} | 제목 최대 {pl['max_title_chars']} "
          f"| 줄이기 전 최대 {pl['pre_fit_embed_total_max']} (줄인 글 {pl['fit_truncated']}) | 상한 위반 "
          f"{ {k: v for k, v in pl['limit_violations'].items() if v} or 0}")
    if r.get("repost_vs_analysis"):
        rv = r["repost_vs_analysis"]
        print(f"재게시 vs 분석 묶음: 재현율 {rv['recall']} ({rv['recall_hits']}/{rv['analysis_reposts']}) | "
              f"정밀도(같은 item) {rv['precision_same_item']} ({rv['precision_hits']}/{rv['detected']}) | "
              f"정밀도(분석도 재게시) {rv['precision_any_repost']}")
        print(f"  놓친 사유 {rv['miss_reasons']} | 오탐 유형 {rv['false_positive_kinds']}")
    if r.get("in_sample"):
        print(f"학습 포함(in_price_model) {r['in_sample']}")
    if r.get("labels_vs_analysis"):
        print(f"식별 vs 분석 라벨 {r['labels_vs_analysis']}")
    if r.get("oof_sensitivity"):
        o = r["oof_sensitivity"]
        print(f"표본 밖 적정가로 바꾼 글 {o['n_replaced_with_oof']}/{o['n_records']} | P50 차이(|log|%) "
              f"{o['p50_abs_log_diff_pct']} | 판정 표본 안 {o['flags_in_sample']} → 표본 밖 {o['flags_oof']} | "
              f"저평가 알림 {o['is_deal_in_sample']} → {o['is_deal_oof']} | deals 알림 {o['deals_alerts']}건 "
              f"({o['deals_per_day']}/일, {o['deals_reason']})")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="백업 DB로 감시기 파이프라인 재생(읽기 전용, 전송 없음)")
    ap.add_argument("--db", default=DEFAULT_DB, help=f"원본 백업 DB(읽기 전용으로 연다, 기본 {DEFAULT_DB})")
    ap.add_argument("-n", type=int, default=800, help="재생할 마지막 글 수(기본 800)")
    ap.add_argument("--no-history", dest="history", action="store_false",
                    help="구간 앞의 글을 임시 DB에 미리 넣지 않는다(재게시 탐지는 구간 안에서만)")
    ap.add_argument("--lookback-days", type=int, default=None, help="사전 적재 기간(기본 pricing.repost.LOOKBACK_DAYS)")
    ap.add_argument("--oof", default=DEFAULT_OOF,
                    help="분석 저장소 fair_price_estimates.csv(판매자 5겹 CV의 폴드 밖 적정가, 없으면 건너뜀)")
    ap.add_argument("--listings", default=DEFAULT_LISTINGS,
                    help="분석 저장소 listings_clean.parquet(재게시 비교, 없으면 건너뜀)")
    ap.add_argument("--pricing-dir", default=None, help="pricing_data 폴더(기본 저장소의 pricing_data/)")
    ap.add_argument("--no-pricing", action="store_true", help="보강을 끈 채로 돌린다(DRSPARK_PRICING=0, 기존 동작 확인)")
    ap.add_argument("--tmp", default="/tmp", help="임시 DB를 만들 폴더(기본 /tmp)")
    ap.add_argument("--keep", action="store_true", help="임시 폴더를 지우지 않는다(개인정보가 든 DB가 남는다)")
    ap.add_argument("--json", default=None, help="집계 결과 JSON을 쓸 경로(저장소 밖 권장)")
    args = ap.parse_args(argv)
    if args.n <= 0:
        ap.error("-n은 1 이상")
    if args.lookback_days is None:
        from pricing.repost import LOOKBACK_DAYS
        args.lookback_days = LOOKBACK_DAYS

    result = replay(args)
    print_summary(result)
    if args.json:
        Path(args.json).write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"[replay] JSON: {args.json}")
    errs = result["errors"]["by_stage"]
    return 1 if any(errs.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
