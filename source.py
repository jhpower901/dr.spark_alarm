# watcher.py
# pip install requests beautifulsoup4 apscheduler

import os, re, time, datetime, sqlite3, sys, logging
from logging.handlers import RotatingFileHandler, SysLogHandler, NTEventLogHandler

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from bs4 import BeautifulSoup
from urllib.parse import urljoin
from apscheduler.schedulers.blocking import BlockingScheduler

# ===== 기본 설정 =====
BASE = "https://www.drspark.net"
LIST_URL = "https://www.drspark.net/ski_sell2"
DB = "drspark_seen.db"
HEADERS = {"User-Agent": "Mozilla/5.0 (+alerts)"}
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")

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
        else:
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
def init_db():
    """
    et_vars JSON -> 정규화 컬럼 저장 (요청 반영)

    - phone_int: 전화번호를 숫자만 남겨 int로 저장 (없으면 NULL)
    - region(거래지역)은 DB 저장 X (디코에만)
    - decision_order(결정순서)은 저장 X
    """
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seen(
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
    con.commit()
    con.close()


def is_known(post_id: str) -> bool:
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM seen WHERE post_id=?", (post_id,))
    known = cur.fetchone() is not None
    con.close()
    return known


def save_item(item: dict):
    con = sqlite3.connect(DB)
    cur = con.cursor()

    cur.execute("""
        INSERT INTO seen(
            post_id, observed_at, title, price, author, status, body_content,
            release_year, model_serial, spec, purchased_at, usage_count, features, phone, email
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
    con.close()


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
    - raw_price는 et_vars['판매가격']가 있으면 우선
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

    # raw_price: 판매가격 우선(없으면 목록 raw_price 유지)
    sell_price_str = (et.get("판매가격") or "").strip()
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


# ===== 실행 =====
def run_once():
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

            # DB 저장(먼저 기록 -> 디코)
            save_item(it)

            logger.info(f"NEW: {it['title']} -> {it['url']}")
            discord_send(it)

    except Exception as e:
        logger.exception(f"run_once failed: {e}")


# ===== Discord 전송 =====
def discord_send(item: dict):
    if not DISCORD_WEBHOOK:
        logger.warning("DISCORD_WEBHOOK_URL이 없습니다. 환경변수에 넣어주세요.")
        return

    title = (item.get("title") or "").strip()
    url = item.get("url") or ""

    # 가격은 raw_price(판매가격 우선 반영됨)
    price = f"{item.get('raw_price'):,}원" if isinstance(item.get("raw_price"), int) else "—"

    # 디코에는 status_list (장비상태/네고/거래방법)
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

    embed = {"title": title, "url": url, "color": 0x3498DB, "fields": fields}

    # 썸네일 추가
    if item.get("thumb"):
        embed["thumbnail"] = {"url": item["thumb"]}

    payload = {"content": None, "embeds": [embed]}

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"[Discord] send attempt {attempt}/{max_attempts} | '{title}'")
            r = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)

            if r.status_code == 204 or (200 <= r.status_code < 300):
                logger.info(f"[Discord] sent OK ({r.status_code}): '{title}'")
                return

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


if __name__ == "__main__":
    init_db()
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
