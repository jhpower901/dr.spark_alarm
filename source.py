# watcher.py
# pip install requests beautifulsoup4 apscheduler

import os, re, time, datetime, sqlite3, sys, logging, json
from contextlib import closing
from logging.handlers import RotatingFileHandler, SysLogHandler, NTEventLogHandler

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from bs4 import BeautifulSoup
from urllib.parse import urljoin
from apscheduler.schedulers.blocking import BlockingScheduler

# 적정가 보강(모델 식별·적정가·재게시). 무거운 의존성(pandas, lightgbm)은 Enricher를 만들 때만 읽는다.
# pricing/ 없이 source.py만 배포해도 기존 알림은 그대로 돈다(보강 없음, 시작할 때 경고 한 줄).
try:
    from pricing import discord_fields as DF
    from pricing.enrich import CATEGORY_KO, ENRICHMENT_COLUMNS, ENRICHMENT_DDL, Enricher, save_enrichment
    PRICING_IMPORT_ERROR = None
except ImportError as _ex:
    PRICING_IMPORT_ERROR = _ex
    CATEGORY_KO, ENRICHMENT_COLUMNS = {}, {}
    ENRICHMENT_DDL = Enricher = save_enrichment = None

    class DF:  # 보강 결과가 없을 때(None) pricing.discord_fields가 내는 값과 같다
        fields = staticmethod(lambda e: [])
        color = staticmethod(lambda e: 0x3498DB)
        title_prefix = staticmethod(lambda e: "")
        is_deal = staticmethod(lambda e: False)
        is_price_cut = staticmethod(lambda e: False)

_SETTING_WARNINGS = []  # 설정값 문제(시작 로그에 경고로 남긴다)


def _env_or(name: str, default):
    """환경변수 값. 없거나 빈 값(VAR=)이면 기본값(.env에서 값을 비워 둔 줄은 '설정 안 함'으로 본다)."""
    v = (os.getenv(name) or "").strip()
    return v if v else default


def _env_float(name: str, default: float) -> float:
    v = _env_or(name, None)
    if v is None:
        return default
    try:
        f = float(v)
        if f >= 0:
            return f
    except ValueError:
        pass
    _SETTING_WARNINGS.append(f"{name}={v!r}는 0 이상의 숫자가 아니라 기본값 {default:g}를 쓴다")
    return default


# ===== 기본 설정 =====
BASE = "https://www.drspark.net"
LIST_URL = "https://www.drspark.net/ski_sell2"
DB = _env_or("DRSPARK_DB", "drspark_seen.db")
HEADERS = {"User-Agent": "Mozilla/5.0 (+alerts)"}
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")

# ===== 알림 필터(기본값은 기존과 같이 모든 새 글 알림) =====
# ALERT_MODE=all   : 모든 새 글(기본)
# ALERT_MODE=deals : 저평가(신뢰 높음·보통) 또는 적정가 이하로 내린 재게시만
ALERT_MODE = os.getenv("ALERT_MODE", "all").strip().lower()
# ALERT_CATEGORIES=ski,boots : 이 카테고리만(대소문자 무관. ski, boots, binding, apparel, gloves, helmet, goggle, poles, bag, ...)
ALERT_CATEGORIES = {c.strip().lower() for c in os.getenv("ALERT_CATEGORIES", "").split(",") if c.strip()}
# ALERT_SKIP_WANTED=1 : 구매 희망 글은 알리지 않음
ALERT_SKIP_WANTED = os.getenv("ALERT_SKIP_WANTED", "0").strip().lower() in ("1", "true", "yes", "on")
# DEAL_MENTION="<@&역할ID>" 또는 "@here" : 저평가 알림에 멘션을 붙임
DEAL_MENTION = os.getenv("DEAL_MENTION", "").strip()
# deals 모드에서 적정가가 없는 글(묶음 등)의 가격 인하는 이만큼(%) 넘게 내린 것만 알린다
DEALS_NO_FAIR_CUT_PCT = 10.0
# Discord 전송에 실패한 알림을 다시 보내는 기간(시간). 이보다 오래된 미전송 알림은 버린다(expired)
ALERT_RETRY_HOURS = _env_float("ALERT_RETRY_HOURS", 6.0)

ENRICHER = None  # __main__에서 Enricher.from_env()로 채운다(없으면 기존 알림만)

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

logger = logging.getLogger("drspark")


def setup_logging():
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    fh = RotatingFileHandler("drspark.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    fh.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    ch.setFormatter(fmt)
    fh.setFormatter(fmt)

    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(ch)
    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        logger.addHandler(fh)

    try:
        if os.name == "nt":
            eh = NTEventLogHandler(appname="DrSparkWatcher")
            eh.setLevel(logging.WARNING)
            eh.setFormatter(fmt)
            logger.addHandler(eh)
        elif os.path.exists("/dev/log"):
            # /dev/log가 없는 컨테이너(Docker, 일부 LXC)에서 붙이면 경고마다 'Logging error'가 찍힌다
            sh = SysLogHandler(address="/dev/log")
            sh.setLevel(logging.WARNING)
            sh.setFormatter(logging.Formatter("DrSparkWatcher: %(levelname)s %(message)s"))
            logger.addHandler(sh)
    except Exception:
        logger.debug("System log handler not attached", exc_info=True)


setup_logging()

# ===== requests 세션 + 재시도 설정 =====
_session = None


def get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.7,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            raise_on_status=False
        )
        _session.mount("https://", HTTPAdapter(max_retries=retry))
        _session.mount("http://", HTTPAdapter(max_retries=retry))
    return _session


POST_ID_RE = re.compile(r"/(\d{5,})$")


# ===== DB =====
# 알림 전송 상태. status: pending(보낼 차례·재시도 대기) / sent / filtered(필터로 거름) /
# no_webhook(웹후크 없음) / failed(Discord가 페이로드를 거부, 400·413) / expired(재시도 기간 초과)
# payload(임베드 JSON, 연락처 포함)는 pending일 때만 두고 보내거나 끝나면 지운다.
ALERT_OUTBOX_DDL = """
CREATE TABLE IF NOT EXISTS alert_outbox
(
    post_id    TEXT PRIMARY KEY,
    status     TEXT NOT NULL,
    payload    TEXT,
    attempts   INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at INTEGER,
    updated_at INTEGER
)
"""


# 1. DB 초기화 (WAL 모드 추가로 안전성 강화)
def init_db():
    with sqlite3.connect(DB) as con:
        cur = con.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")

        cur.execute("""
                    CREATE TABLE IF NOT EXISTS seen
                    (
                        post_id TEXT PRIMARY KEY,
                        observed_at INTEGER,
                        title TEXT,
                        price INTEGER,
                        author TEXT,
                        status TEXT,
                        body_content TEXT,
                        release_year TEXT,
                        model_serial TEXT,
                        spec TEXT,
                        purchased_at TEXT,
                        usage_count TEXT,
                        features TEXT,
                        phone TEXT,
                        email TEXT
                    )
                    """)
        # 적정가 보강 결과(파생값만, seen 스키마는 그대로 둔다). 새 버전에서 늘어난 열은 기존 테이블에 붙인다
        if ENRICHMENT_DDL:
            cur.execute(ENRICHMENT_DDL)
            have = {r[1] for r in cur.execute("PRAGMA table_info(enrichment)")}
            for col, typ in ENRICHMENT_COLUMNS.items():
                if col not in have:
                    cur.execute(f"ALTER TABLE enrichment ADD COLUMN {col} {typ}")
                    logger.info(f"enrichment table: added column {col} {typ}")
        # 알림 전송 상태(전송 실패 시 다음 실행에서 다시 보낸다)
        cur.execute(ALERT_OUTBOX_DDL)

        con.commit()


# 2. 중복 확인 (조회 전용)
def is_known(post_id: str) -> bool:
    with sqlite3.connect(DB) as con:
        cur = con.cursor()
        cur.execute("SELECT 1 FROM seen WHERE post_id=?", (post_id,))
        return cur.fetchone() is not None


# 3. 데이터 저장 (안전한 트랜잭션 처리)
def save_item(item: dict):
    with sqlite3.connect(DB) as con:
        cur = con.cursor()
        cur.execute("""
                    INSERT INTO seen(post_id, observed_at, title, price, author, status, body_content,
                                     release_year, model_serial, spec, purchased_at, usage_count, features, phone,
                                     email)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        item.get("id"),
                        item.get("observed_at"),
                        item.get("title"),
                        item.get("raw_price"),
                        item.get("author"),
                        item.get("status"),
                        item.get("body_content"),
                        item.get("release_year"),
                        item.get("model_serial"),
                        item.get("spec"),
                        item.get("purchased_at"),
                        item.get("usage_count"),
                        item.get("features"),
                        item.get("phone"),
                        item.get("email"),
                    ))
        con.commit()


# ===== HTTP Fetch =====
def fetch_html(url: str) -> str:
    ses = get_session()
    logger.info(f"Fetching: {url}")
    try:
        r = ses.get(url, headers=HEADERS, timeout=15)
        if r.status_code >= 400:
            snippet = (r.text or "")[:500].replace("\n", " ")
            logger.warning(f"HTTP {r.status_code} for {url} | body[:500]={snippet}")
            r.raise_for_status()
        return r.text
    except requests.exceptions.RequestException as e:
        logger.exception(f"Fetch failed: {url} | {e}")
        raise


def _norm_img(src: str | None):
    if not src:
        return None
    s = src.strip()
    if s.startswith("//"):
        return "https:" + s
    return urljoin(BASE, s)


# ===== 목록 파싱 =====
def parse_list(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    items = []

    for a in soup.select(".simple-board__webzine .item a.item__container"):
        href = a.get("href", "").strip()
        m = POST_ID_RE.search(href)
        if not m:
            continue

        pid = m.group(1)
        card = a

        subj_el = card.select_one(".item__inner.item__subject .subject")
        title = subj_el.get_text(strip=True) if subj_el else ""

        # 목록 썸네일(디코 전송용)
        thumb_el = card.select_one(".item__thumbnail img")
        thumb = _norm_img(thumb_el.get("src") if thumb_el else None)

        # 목록 가격(상세 판매가격으로 덮어쓸 수 있음)
        price_el = card.select_one(".item__inner.item__etc-wrp span[style*='font-size']")
        if price_el:
            price_text = price_el.get_text(strip=True)
            digits = re.sub(r"[^\d]", "", price_text)
            raw_price = int(digits) if digits else None
        else:
            raw_price = None

        # 목록에서의 status_icon은 이제 참고용(최종 status_list는 상세에서 재구성)
        status_icon_list = [s.get_text(strip=True) for s in card.select(".status_icon")]

        author_el = card.select_one(".item__author span")
        author = author_el.get_text(strip=True) if author_el else ""

        items.append({
            "id": pid,
            "url": urljoin(BASE, href),
            "title": title,
            "raw_price": raw_price,
            "author": author,
            "thumb": thumb,

            # 디버그/보조용
            "status_icon_list": status_icon_list,
        })

    logger.info(f"Parsed {len(items)} items")
    return items


# ===== 상세 파싱 =====
def _extract_extravars_comment_region(detail_html: str) -> str | None:
    """
    <div class="simple-board__read__extravars"> 시작 ~ <div class="et_vars"> 시작 직전까지 슬라이스
    (이 구간에 조각 주석들이 들어 있음)
    """
    start_marker = '<div class="simple-board__read__extravars"'
    start = detail_html.find(start_marker)
    if start == -1:
        return None

    tail = detail_html[start:]
    et_vars_marker = '<div class="et_vars"'
    et_pos = tail.find(et_vars_marker)
    if et_pos == -1:
        return tail
    return tail[:et_pos]


def _parse_extravars_from_comments(detail_html: str) -> dict:
    """
    simple-board__read__extravars 영역의 '조각 주석'을 전부 uncomment한 뒤
    item__label/item__value를 dict로 만듦.
    """
    region = _extract_extravars_comment_region(detail_html)
    if not region:
        return {}

    uncommented = region.replace("<!--", "").replace("-->", "")
    frag = BeautifulSoup(uncommented, "html.parser")

    out = {}
    for it in frag.select(".simple-board__read__extravars .item"):
        lab = it.select_one(".item__label")
        val = it.select_one(".item__value")
        label = lab.get_text(" ", strip=True) if lab else ""
        value = val.get_text(" ", strip=True) if val else ""
        if label:
            out[label] = value
    return out


def parse_detail(detail_html: str, it: dict) -> dict:
    soup = BeautifulSoup(detail_html, "html.parser")

    # 1. 실제 본문 영역(div) 파싱 시도
    content_div = soup.select_one('div.rhymix_content.xe_content')
    if content_div:
        # 텍스트만 추출 (separator="\n"으로 줄바꿈 유지)
        body_content = content_div.get_text(separator="\n", strip=True)
    else:
        body_content = None

    # 2. 만약 본문 태그가 없거나 내용이 비어있다면 meta 태그에서 시도 (백업용)
    if not body_content:
        m1 = soup.select_one('meta[name="description"]')
        if m1 and m1.get("content"):
            body_content = m1.get("content", "").strip()

    # 3. 사진만 있는 글처럼 아예 내용이 없다면 제목을 본문에 넣어 검색 가능하게 함
    if not body_content or body_content == "":
        body_content = f"[본문 요약 없음] {it['title']}".strip()

    et_vars = _parse_extravars_from_comments(detail_html)
    return {"body_content": body_content, "et_vars": et_vars}


# ===== et_vars -> item 필드로 정규화 =====
def _digits_to_int(s: str | None) -> int | None:
    if not s:
        return None
    digits = re.sub(r"[^\d]", "", s)
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def normalize_item_from_et_vars(item: dict):
    """
    et_vars dict를 item의 정규화 필드로 풀어놓습니다.
    - DB 저장 대상과 Discord 표시 대상 분리
    - raw_price는 et_vars['판매가격'] 또는 '희망가'(2026-10 사이트 라벨 변경)가 있으면 우선
    - status_list는 et_vars 기반(장비상태/네고/거래방법)으로 재구성
    """
    et = item.get("et_vars") or {}

    # DB 저장 대상(문자열)
    item["release_year"] = (et.get("출시연도") or "").strip() or None
    item["model_serial"] = (et.get("Model/Serial No.") or "").strip() or None
    item["spec"] = (et.get("제품스펙") or "").strip() or None
    item["purchased_at"] = (et.get("구입처/시기") or "").strip() or None
    item["usage_count"] = (et.get("사용횟수") or "").strip() or None
    item["features"] = (et.get("특장점") or "").strip() or None
    item["email"] = (et.get("E-mail") or "").strip() or None
    phone_str = (et.get("전화번호") or "").strip() or None
    item["phone"] = phone_str

    # Discord에만 표시
    item["region"] = (et.get("거래지역") or "").strip() or None

    # 상세 '거래' 칸(판매중 / 거래완료 …). 2026-10 기준 사이트에 새로 생긴 칸이라 없을 수 있다
    item["trade_status"] = (et.get("거래") or "").strip() or None

    # raw_price: 상세 가격 우선(없으면 목록 raw_price 유지).
    # 사이트가 라벨을 '판매가격'에서 '희망가'로 바꿨다(2026-10 확인). 둘 다 읽는다.
    sell_price_str = (et.get("판매가격") or et.get("희망가") or "").strip()
    sell_price_int = _digits_to_int(sell_price_str)
    if sell_price_int is not None:
        item["raw_price"] = sell_price_int

    # status_list 재구성: 장비상태, 네고, 거래방법만
    status_list = []
    for k in ["장비상태", "네고", "거래방법"]:
        v = (et.get(k) or "").strip()
        if v:
            status_list.append(v)

    item["status_list"] = status_list
    item["status"] = status_list[0] if status_list else ""  # DB 저장용(첫 요소)


def fetch_and_parse_detail(it: dict) -> dict:
    html = fetch_html(it["url"])
    return parse_detail(html, it)


# ===== 적정가 보강 =====
def enrich_item(it: dict):
    """모델 식별·적정가·재게시를 계산해 enrichment 테이블에 기록. 꺼져 있거나 계산 단계가 실패하면 None
    (알림은 그대로 보낸다). 저장만 실패하면 로그를 남기고 계산한 결과는 그대로 돌려준다(필터·알림에 쓴다)."""
    if ENRICHER is None:
        return None
    try:
        with closing(sqlite3.connect(DB)) as con:
            e = ENRICHER.enrich(it, con)
            try:
                save_enrichment(con, it["id"], e)
                con.commit()
            except sqlite3.Error:
                logger.exception(f"Enrichment save failed (alert keeps the computed result): {it.get('id')}")
        logger.info(
            f"Enriched {it['id']} | grade={e.id_grade} model={e.model_id or e.family_id or e.brand} "
            f"p50={e.fair_p50} flag={e.deal_flag} conf={e.deal_confidence} "
            f"repost={e.repost.n_previous if e.repost else 0} {e.elapsed_ms:.0f}ms"
            + (f" | skipped={e.skipped_reason}" if e.skipped_reason else "")
            + (f" | price_flags={','.join(e.extra['price_flags'])}" if e.extra.get("price_flags") else "")
            + (f" | error={e.error}" if e.error else "")
            + (f" | repost_error={e.extra['repost_error']}" if e.extra.get("repost_error") else "")
        )
        return e
    except Exception:
        logger.exception(f"Enrichment step failed: {it.get('id')}")
        return None


def should_alert(it: dict, e) -> bool:
    """알림 필터. 보강 결과가 없으면(꺼짐·실패) 놓치지 않도록 항상 알린다."""
    if e is None or e.error:
        return True
    if ALERT_SKIP_WANTED and e.listing_type == "wanted":
        return False
    if ALERT_CATEGORIES and e.category not in ALERT_CATEGORIES:
        return False
    if ALERT_MODE == "deals":
        if (it.get("trade_status") or e.trade_status) == "거래완료":
            return False  # 이미 거래가 끝난 글은 살 수 없다
        if DF.is_deal(e):
            return True
        if not DF.is_price_cut(e):
            return False
        if e.fair_p50:
            # 값을 내렸어도 여전히 적정가(P50)보다 비싸거나 고평가·가격 확인 필요면 '싸게 나온 글'이 아니다
            return e.deal_flag in ("undervalued", "fair") and e.price_krw is not None and e.price_krw <= e.fair_p50
        # 적정가가 없는 글(묶음 등)은 큰 폭으로 내린 것만
        return e.repost.change_pct <= -DEALS_NO_FAIR_CUT_PCT
    return True


def _validate_alert_settings():
    """시작할 때 알림 설정을 점검한다. 모르는 값은 경고를 남기고 무시한다(조용히 알림이 끊기지 않게)."""
    global ALERT_MODE, ALERT_CATEGORIES
    for msg in _SETTING_WARNINGS:
        logger.warning(msg)
    if ALERT_MODE not in ("all", "deals"):
        logger.warning(f"ALERT_MODE={ALERT_MODE!r}는 알 수 없는 값이라 all로 동작한다(all 또는 deals)")
        ALERT_MODE = "all"
    known = set(CATEGORY_KO)
    unknown = ALERT_CATEGORIES - known if known else set()
    if unknown:
        logger.warning(f"ALERT_CATEGORIES의 알 수 없는 카테고리 {sorted(unknown)}는 무시한다. "
                       f"가능한 값: {', '.join(CATEGORY_KO)}")
        ALERT_CATEGORIES = ALERT_CATEGORIES & known
        if not ALERT_CATEGORIES:
            logger.warning("유효한 ALERT_CATEGORIES가 없어 카테고리 필터를 끈다(모든 카테고리 알림)")


# ===== 알림 대기열(alert_outbox) =====
# Discord가 이 상태 코드로 거부하면 페이로드 자체의 문제라 다시 보내도 소용없다(다음 알림은 계속 보낸다)
PERMANENT_HTTP_STATUS = {400, 413}


def _outbox_put(post_id: str, status: str, payload: dict | None = None, created_at: int | None = None) -> bool:
    """새 글의 알림 상태를 기록한다. 기록에 실패하면 False(알림은 그래도 보낸다)."""
    now = int(time.time())
    try:
        with closing(sqlite3.connect(DB)) as con:
            con.execute(
                "INSERT OR REPLACE INTO alert_outbox(post_id, status, payload, attempts, last_error, created_at, "
                "updated_at) VALUES (?, ?, ?, 0, NULL, ?, ?)",
                (post_id, status, None if payload is None else json.dumps(payload, ensure_ascii=False),
                 int(created_at or now), now))
            con.commit()
        return True
    except sqlite3.Error:
        logger.exception(f"alert_outbox write failed: {post_id} ({status})")
        return False


def _outbox_update(post_id: str, status: str, *, error: str | None = None, attempted: bool = False,
                   keep_payload: bool = False) -> None:
    try:
        with closing(sqlite3.connect(DB)) as con:
            con.execute(
                "UPDATE alert_outbox SET status = ?, attempts = attempts + ?, last_error = COALESCE(?, last_error), "
                "payload = CASE WHEN ? THEN payload END, updated_at = ? WHERE post_id = ?",
                (status, 1 if attempted else 0, error, 1 if keep_payload else 0, int(time.time()), post_id))
            con.commit()
    except sqlite3.Error:
        logger.exception(f"alert_outbox update failed: {post_id} -> {status}")


def _deliver(post_id: str, payload: dict) -> bool:
    """대기 중인 알림 1건을 보낸다. Discord가 받았거나 페이로드만의 문제(failed)면 True,
    Discord 쪽 장애(연결 실패·5xx·429 반복 등)라 나중에 다시 보내야 하면 False(pending 유지)."""
    try:
        if not discord_send(None, payload=payload):
            _outbox_update(post_id, "no_webhook")
            return True
    except requests.exceptions.RequestException as ex:
        code = getattr(getattr(ex, "response", None), "status_code", None)
        permanent = code in PERMANENT_HTTP_STATUS
        _outbox_update(post_id, "failed" if permanent else "pending", error=f"{type(ex).__name__}: {ex}"[:300],
                       attempted=True, keep_payload=not permanent)
        if permanent:
            logger.error(f"[Discord] payload rejected (HTTP {code}), giving up: {post_id}")
        else:
            logger.warning(f"[Discord] send failed, will retry next run (up to {ALERT_RETRY_HOURS:g}h): {post_id}")
        return permanent
    _outbox_update(post_id, "sent", attempted=True)
    return True


def flush_pending() -> bool:
    """앞선 실행에서 보내지 못한 알림(pending)을 오래된 것부터 다시 보낸다.
    재시도 기간(ALERT_RETRY_HOURS)이 지난 것은 expired로 바꾸고 보내지 않는다. Discord가 아직 안 되면 False."""
    if not DISCORD_WEBHOOK:
        return True
    now = int(time.time())
    cutoff = now - int(ALERT_RETRY_HOURS * 3600)
    with closing(sqlite3.connect(DB)) as con:
        expired = con.execute(
            "UPDATE alert_outbox SET status = 'expired', payload = NULL, updated_at = ? "
            "WHERE status = 'pending' AND created_at < ?", (now, cutoff)).rowcount
        con.commit()
        rows = con.execute("SELECT post_id, payload FROM alert_outbox WHERE status = 'pending' "
                           "ORDER BY created_at, rowid").fetchall()
    if expired:
        logger.warning(f"[Discord] {expired} unsent alert(s) older than {ALERT_RETRY_HOURS:g}h expired")
    if rows:
        logger.info(f"[Discord] retrying {len(rows)} pending alert(s)")
    for post_id, payload in rows:
        if payload is None:
            _outbox_update(post_id, "failed", error="payload missing")
            continue
        if not _deliver(post_id, json.loads(payload)):
            return False
    return True


def _alert(it: dict, discord_ok: bool) -> bool:
    """새 글 1건: 보강 → 필터 → 대기열 기록 → 전송. 돌려주는 값은 이번 실행에서 다음 글을 바로 보내도 되는지
    (False면 Discord 장애로 보고 남은 글은 pending으로만 기록해 다음 실행에서 보낸다)."""
    pid = it["id"]
    created_at = it.get("observed_at")
    enrichment = enrich_item(it)
    if not should_alert(it, enrichment):
        logger.info(f"Alert filtered: {pid} (mode={ALERT_MODE}, flag={enrichment.deal_flag}, "
                    f"category={enrichment.category}, type={enrichment.listing_type})")
        _outbox_put(pid, "filtered", created_at=created_at)
        return discord_ok
    if not DISCORD_WEBHOOK:
        discord_send(it, enrichment)  # 기존처럼 경고 로그만 남긴다
        _outbox_put(pid, "no_webhook", created_at=created_at)
        return discord_ok
    payload = build_payload(it, enrichment)
    if not _outbox_put(pid, "pending", payload, created_at=created_at):
        discord_send(None, payload=payload)  # 상태를 기록하지 못하면 기존처럼 바로 보내기만 한다
        return True
    if not discord_ok:
        logger.warning(f"[Discord] earlier send failed in this run; queued for next run: {pid}")
        return False
    return _deliver(pid, payload)


# ===== 실행 =====
def run_once():
    # 앞선 실행에서 보내지 못한 알림부터(목록을 못 가져와도 재전송은 한다)
    discord_ok = True
    try:
        discord_ok = flush_pending()
    except Exception:
        logger.exception("Pending alert retry failed")

    try:
        list_html = fetch_html(LIST_URL)
        for it in parse_list(list_html):

            # (필요 시) 구매중 필터: 이제 최종 status_list는 상세에서 오므로,
            # 목록 status_icon_list에 '구매중'이 있으면 선필터로 유지
            # if "구매중" in (it.get("status_icon_list") or []):
            #     logger.debug(f"Filtered(list 구매중): {it['id']}")
            #     continue

            if is_known(it["id"]):
                logger.debug(f"Seen: {it['id']}")
                continue

            it["observed_at"] = int(time.time())

            # 상세 수집
            try:
                detail = fetch_and_parse_detail(it)
                it.update(detail)
            except Exception:
                logger.exception(f"Detail fetch/parse failed: {it['url']}")
                it.setdefault("body_content", None)
                it.setdefault("et_vars", {})

            # et_vars -> 정규화 필드로 풀기(필수)
            normalize_item_from_et_vars(it)

            logger.info(
                f"Detail parsed for {it['id']} | "
                f"et_vars={len(it.get('et_vars') or {})} | "
                f"status_list={it.get('status_list')} | "
                f"body_len={len((it.get('body_content') or ''))}"
            )

            # DB 저장(먼저 기록 -> 보강 -> 디코)
            save_item(it)

            logger.info(f"NEW: {it['title']} -> {it['url']}")
            # 글 하나의 알림 단계가 실패해도 이번 실행의 나머지 새 글은 계속 처리한다
            try:
                discord_ok = _alert(it, discord_ok)
            except Exception:
                logger.exception(f"Alert step failed: {it['id']}")

    except Exception as e:
        logger.exception(f"run_once failed: {e}")


# ===== Discord 전송 =====
EMBED_TOTAL_LIMIT = 6000  # Discord 임베드 전체 글자 수 상한


def _fit_embed(embed: dict) -> dict:
    """임베드 전체 글자 수가 상한을 넘으면 본문·판매 정보 필드부터 줄인다."""
    def total(e):
        return len(e.get("title") or "") + sum(len(f["name"]) + len(f["value"]) for f in e.get("fields", []))
    for name in ("본문내용", "판매 정보", "적정가 근거"):
        over = total(embed) - EMBED_TOTAL_LIMIT
        if over <= 0:
            break
        for f in embed.get("fields", []):
            if f["name"] == name:
                f["value"] = f["value"][: max(len(f["value"]) - over - 1, 20)] + "…"
    return embed


def build_payload(item: dict, enrichment=None) -> dict:
    """Discord 웹후크 페이로드(전송 없이 만들기만 한다. 테스트·미리보기용)."""
    title = (item.get("title") or "").strip()
    url = item.get("url") or ""

    # 가격은 raw_price(판매가격 우선 반영됨)
    price = f"{item.get('raw_price'):,}원" if isinstance(item.get("raw_price"), int) else "—"

    # 디코에는 status_list (장비상태/네고/거래방법)
    status_full = " / ".join(item.get("status_list") or []) or "—"
    # 판매중이 아닌 거래 상태(거래완료 등)는 상태 칸 앞에 붙인다(판매중이거나 칸이 없으면 기존 표기 그대로)
    if item.get("trade_status") and item["trade_status"] != "판매중":
        status_full = f"{item['trade_status']} / {status_full}"

    ts = item.get("observed_at")
    time_text = f"<t:{ts}:f> (<t:{ts}:R>)"

    author = item.get("author") or "—"

    fields = [
        {"name": "가격", "value": price, "inline": True},
        {"name": "상태", "value": status_full, "inline": True},
        {"name": "작성자", "value": author, "inline": False},
        {"name": "작성시간", "value": time_text, "inline": False},
    ]

    # 적정가 보강(모델·적정가·근거·재게시). 보강이 없거나 실패했으면 빈 목록
    fields.extend(DF.fields(enrichment))

    # DB에 저장되는 필드들도 디코에 함께 보여주기(원하신 방향: item에 저장해 전달)
    # 너무 길어지지 않게 key를 추려서 표시
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

    # 거래지역은 디코에만
    if item.get("region"):
        fields.append({"name": "거래지역", "value": item["region"][:200], "inline": False})

    if item.get("body_content"):
        fields.append({"name": "본문내용", "value": item["body_content"][:500], "inline": False})

    embed = {"title": (DF.title_prefix(enrichment) + title)[:256], "url": url, "color": DF.color(enrichment),
             "fields": fields}

    # 썸네일 추가
    if item.get("thumb"):
        embed["thumbnail"] = {"url": item["thumb"]}

    content = DEAL_MENTION if (DEAL_MENTION and DF.is_deal(enrichment)) else None
    return {"content": content, "embeds": [_fit_embed(embed)]}


def discord_send(item: dict | None, enrichment=None, payload: dict | None = None) -> bool:
    """웹후크로 보낸다(payload가 있으면 그대로, 없으면 item·enrichment로 만든다).
    보냈으면 True, 웹후크가 없으면 False. 3번 모두 실패하면(429 반복 포함) RequestException을 던진다."""
    if not DISCORD_WEBHOOK:
        logger.warning("DISCORD_WEBHOOK_URL이 없습니다. 환경변수에 넣어주세요.")
        return False

    if payload is None:
        payload = build_payload(item, enrichment)
    title = payload["embeds"][0]["title"]

    max_attempts = 3
    status = None
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"[Discord] send attempt {attempt}/{max_attempts} | '{title}'")
            r = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
            status = r.status_code

            if r.status_code == 204 or (200 <= r.status_code < 300):
                logger.info(f"[Discord] sent OK ({r.status_code}): '{title}'")
                return True

            if r.status_code == 429:
                try:
                    data = r.json()
                except ValueError:
                    data = {}
                retry_after = float(data.get("retry_after", 1.0))
                logger.warning(f"[Discord] rate limited 429: retry_after={retry_after}s (attempt {attempt})")
                time.sleep(retry_after + 0.25)
                continue

            snippet = (r.text or "")[:300].replace("\n", " ")
            logger.error(f"[Discord] HTTP {r.status_code} for '{title}': body[:300]={snippet}")
            r.raise_for_status()

        except requests.exceptions.RequestException as e:
            logger.exception(f"[Discord] POST failed (attempt {attempt}/{max_attempts}) for '{title}': {e}")
            if attempt < max_attempts:
                time.sleep(1.5 * attempt)
                continue
            raise

    # 429를 계속 받아 시도를 다 쓴 경우: 조용히 끝내지 않고 실패로 알린다(호출한 쪽이 pending으로 남긴다)
    logger.error(f"[Discord] giving up after {max_attempts} attempts (last HTTP {status}) for '{title}'")
    raise requests.exceptions.RetryError(f"Discord send failed after {max_attempts} attempts (last HTTP {status})")


if __name__ == "__main__":
    init_db()
    _validate_alert_settings()
    if PRICING_IMPORT_ERROR is not None:
        logger.warning(f"pricing package not importable ({PRICING_IMPORT_ERROR}); "
                       f"alerts will be sent without fair prices")
    # pricing_data/가 없거나 DRSPARK_PRICING=0이면 None(기존 알림만)
    ENRICHER = Enricher.from_env() if Enricher is not None else None
    logger.info(f"Pricing enrichment: {'on (' + ENRICHER.version + ')' if ENRICHER else 'off'} | "
                f"alert mode={ALERT_MODE} categories={sorted(ALERT_CATEGORIES) or 'all'} "
                f"skip_wanted={ALERT_SKIP_WANTED} mention={'on' if DEAL_MENTION else 'off'}")
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_once,
        "interval",
        minutes=1,
        jitter=20,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60
    )
    logger.info("Watcher started: every 1 min")
    scheduler.start()
