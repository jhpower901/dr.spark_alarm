# logging, 요청 재시도, 에러 로그까지 포함한 예시
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


try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

logger = logging.getLogger("drspark")

def setup_logging():
    logger.setLevel(logging.DEBUG)

    # 콘솔
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # 회전 파일 (약 5MB, 보관 5개)
    fh = RotatingFileHandler("drspark.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    fh.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    ch.setFormatter(fmt); fh.setFormatter(fmt)
    logger.addHandler(ch); logger.addHandler(fh)

    # (선택) 시스템 로그로도 내보내기
    try:
        if os.name == "nt":
            # Windows 이벤트 로그
            eh = NTEventLogHandler(appname="DrSparkWatcher")
            eh.setLevel(logging.WARNING)  # 시스템 로그는 경고 이상만
            eh.setFormatter(fmt)
            logger.addHandler(eh)
        else:
            # Linux syslog (journald는 자동 수집 가능)
            sh = SysLogHandler(address="/dev/log")
            sh.setLevel(logging.WARNING)
            sh.setFormatter(logging.Formatter("DrSparkWatcher: %(levelname)s %(message)s"))
            logger.addHandler(sh)
    except Exception:
        # 시스템 로그가 없거나 권한 부족해도 메인 로깅은 계속
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
            backoff_factor=0.7,                # 0.7s, 1.4s, 2.1s...
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            raise_on_status=False
        )
        _session.mount("https://", HTTPAdapter(max_retries=retry))
        _session.mount("http://", HTTPAdapter(max_retries=retry))
    return _session

POST_ID_RE = re.compile(r"/(\d{5,})$")

def init_db():
    con = sqlite3.connect(DB); cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seen(
            post_id TEXT PRIMARY KEY,
            first_seen_ts INTEGER,
            product_name TEXT,
            price INTEGER
        )
    """)
    con.commit()
    con.close()

def is_new(item: dict) -> bool:
    con = sqlite3.connect(DB); cur = con.cursor()
    cur.execute("SELECT 1 FROM seen WHERE post_id=?", (item['id'],))
    known = cur.fetchone() is not None
    if not known:
        item['date'] = int(time.time())
        cur.execute("INSERT INTO seen(post_id, first_seen_ts, product_name, price)VALUES(?,?,?,?)"
                    , (item['id'], item['date'], item['title'], item['raw_price'] ))
        con.commit()
    con.close()
    return not known

# === 여기: 요청 + 상태체크 + 로그 ===
def fetch_html(url=LIST_URL) -> str:
    ses = get_session()
    logger.info(f"Fetching: {url}")
    try:
        r = ses.get(url, headers=HEADERS, timeout=15)
        # 실패코드면 본문 일부를 로그로 남기고 예외
        if r.status_code >= 400:
            snippet = (r.text or "")[:500].replace("\n", " ")
            logger.warning(f"HTTP {r.status_code} for {url} | body[:500]={snippet}")
            r.raise_for_status()  # -> requests.HTTPError
        return r.text
    except requests.exceptions.RequestException as e:
        logger.exception(f"Fetch failed: {url} | {e}")
        # 필요하면 여기서 None 리턴 대신 예외 재전파
        raise

def _norm_img(src: str | None):
    if not src:
        return None
    s = src.strip()
    if s.startswith("//"):
        return "https:" + s
    return urljoin(BASE, s)

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

        # 제목
        subj_el = card.select_one(".item__inner.item__subject .subject")
        title = subj_el.get_text(strip=True) if subj_el else ""

        # 썸네일
        thumb_el = card.select_one(".item__thumbnail img")
        thumb = _norm_img(thumb_el.get("src") if thumb_el else None)

        # 가격(있을 때만)
        price_el = card.select_one(".item__inner.item__etc-wrp span[style*='font-size']")
        if price_el:
            price = price_el.get_text(strip=True)  # "11,000원"
            digits = re.sub(r"[^\d]", "", price)  # 숫자만 남김 → "11000"
            raw_price = int(digits) if digits else None
        else:
            raw_price = None
            price = None

        # 상태 아이콘(거래완료/구매중/S급/네고X/직거래 등)
        status = [s.get_text(strip=True) for s in card.select(".status_icon")]

        # 작성자/상대시간
        author_el = card.select_one(".item__author span")
        date_el   = card.select_one(".item__date span")
        author = author_el.get_text(strip=True) if author_el else ""
        age    = date_el.get_text(strip=True) if date_el else ""

        # 조회/댓글 (soupsieve :contains() 안 씀)
        views = comments = ""
        for dv in card.select(".item__inner.item__etc-wrp > div"):
            txt = (dv.get_text(" ", strip=True) or "")
            if txt.startswith("조회:"):
                sp = dv.select_one("span")
                views = sp.get_text(strip=True) if sp else ""
            elif txt.startswith("댓글:"):
                sp = dv.select_one("span")
                comments = sp.get_text(strip=True) if sp else ""


        items.append({
            "id": pid,
            "url": urljoin(BASE, href),
            "title": title,
            "raw_price": raw_price,
            "price": price,
            "status": status,
            "author": author,
            "age": age,
            "thumb": thumb,
            "views": views,
            "comments": comments,
        })

    logger.info(f"Parsed {len(items)} items")
    return items

def run_once():
    try:
        html = fetch_html()
        for it in parse_list(html):
            if "구매중" in it["status"]:
                logger.debug(f"Filtered: {it['id']}")
                continue
            if is_new(it):
                logger.info(f"NEW: {it['title']} -> {it['url']}")
                discord_send(it)
            else:
                logger.debug(f"Seen: {it['id']}")
    except Exception as e:
        logger.exception(f"run_once failed: {e}")

# ===== Discord 전송 =====
def discord_send(item: dict):
    """
    Discord 웹훅으로 임베드 전송 + 상세 로깅.
    logger는 전역 로거(예: logging.getLogger("drspark"))가 이미 구성되어 있다고 가정.
    """
    if not DISCORD_WEBHOOK:
        logger.warning("DISCORD_WEBHOOK_URL이 없습니다. 환경변수에 넣어주세요.")
        return

    title = (item.get("title") or "").strip()
    url   = item.get("url") or ""
    price = item.get("price") or "—"
    status = " / ".join(item.get("status") or []) or "—"
    date = datetime.datetime.fromtimestamp(item.get("date")).strftime("%Y-%m-%d %H:%M:%S")
    author_age = " · ".join([x for x in [item.get("author"), date] if x]) or "—"

    embed = {
        "title": title,
        "url": url,
        "color": 0xFF8C8C,
        "fields": [
            {"name": "가격", "value": price, "inline": True},
            {"name": "상태", "value": status, "inline": True},
            {"name": "작성자/시간", "value": author_age, "inline": False},
        ],
    }
    if item.get("thumb"):
        embed["thumbnail"] = {"url": item["thumb"]}

    payload = {"content": None, "embeds": [embed]}

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"[Discord] send attempt {attempt}/{max_attempts} | '{title}'")
            r = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)

            # 성공(2xx 또는 204 No Content)
            if r.status_code == 204 or (200 <= r.status_code < 300):
                logger.info(f"[Discord] sent OK ({r.status_code}): '{title}'")
                return

            # 레이트리밋(429) → retry_after 만큼 대기 후 재시도
            if r.status_code == 429:
                try:
                    data = r.json()
                except ValueError:
                    data = {}
                retry_after = float(data.get("retry_after", 1.0))  # Discord는 초 단위 반환
                logger.warning(f"[Discord] rate limited 429: retry_after={retry_after}s (attempt {attempt})")
                time.sleep(retry_after + 0.25)
                continue

            # 기타 비정상 코드: 본문 일부 로그 후 예외
            snippet = (r.text or "")[:300].replace("\n", " ")
            logger.error(f"[Discord] HTTP {r.status_code} for '{title}': body[:300]={snippet}")
            r.raise_for_status()

        except requests.exceptions.RequestException as e:
            # 네트워크 계열 예외(타임아웃 등)
            logger.exception(f"[Discord] POST failed (attempt {attempt}/{max_attempts}) for '{title}': {e}")
            if attempt < max_attempts:
                time.sleep(1.5 * attempt)  # 점증 백오프
                continue
            # 마지막 시도에서도 실패면 종료(상위에서 잡도록)
            raise



if __name__ == "__main__":
    init_db()
    # run_once()
    scheduler = BlockingScheduler()
    scheduler.add_job(run_once, "interval", minutes=1, jitter=20, max_instances=1, coalesce=True,
                      misfire_grace_time=60)
    logger.info("Watcher started: every 1 min")
    scheduler.start()
