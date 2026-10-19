"""적정가 보강·알림 테스트 공용 픽스처.

모든 매물은 합성 데이터다(실제 작성자·전화번호·이메일을 쓰지 않는다). 연락처가 필요한 곳은
010-0000-0000, seller@example.com 같은 자리표시값만 쓴다.

- 네트워크 차단: requests.post / Session.request를 막아 Discord·사이트에 실제로 나가지 않게 한다.
- source.py는 import할 때 현재 폴더에 drspark.log를 만든다. 저장소를 더럽히지 않도록 임시 폴더에서 import한다.
- Enricher(사전·적정가 모델 읽기, 약 4초)는 세션당 한 번만 만든다.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest
import requests

ROOT = Path(__file__).resolve().parent.parent
# 컨테이너에는 분석 저장소의 drspark(/work/src)도 경로에 있다. 동기화된 사본(alarm/drspark)을 먼저 보게 한다.
while str(ROOT) in sys.path:
    sys.path.remove(str(ROOT))
sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "pricing_data"
HAS_PRICING_DATA = (DATA_DIR / "fairprice.pkl").exists()

KST = dt.timezone(dt.timedelta(hours=9))
DAY = 86400
# 기준 관측 시각: 2026-10-10 12:00 KST (25/26 시즌 직전)
TS = int(dt.datetime(2026, 10, 10, 12, 0, tzinfo=KST).timestamp())

# commit fc71c35("DB WAL모드 사용")의 init_db가 만들던 seen 테이블(공백까지 그대로)
ORIGINAL_SEEN_DDL = """
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
                    """
ORIGINAL_SEEN_COLUMNS = ["post_id", "observed_at", "title", "price", "author", "status", "body_content",
                         "release_year", "model_serial", "spec", "purchased_at", "usage_count", "features",
                         "phone", "email"]

# 합성 연락처(실제 번호·주소 아님). 보강 결과에 새어 나가지 않는지 확인하는 데 쓴다.
FAKE_PHONE = "010-0000-0000"
FAKE_EMAIL = "seller@example.com"
FAKE_AUTHOR = "테스트판매자"

# ---------------------------------------------------------------------------------------- source import
_LOG_DIR = tempfile.mkdtemp(prefix="drspark-alarm-tests-")
_cwd = os.getcwd()
os.chdir(_LOG_DIR)
try:
    import source  # noqa: E402  (setup_logging()이 drspark.log를 임시 폴더에 만든다)
finally:
    os.chdir(_cwd)


def pytest_unconfigure(config):
    lg = logging.getLogger("drspark")
    for h in list(lg.handlers):
        if isinstance(h, logging.FileHandler):
            h.close()
            lg.removeHandler(h)
    shutil.rmtree(_LOG_DIR, ignore_errors=True)


# ---------------------------------------------------------------------------------------- 공용 픽스처
@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """네트워크 차단 + source 전역값을 기본값(기존 동작)으로 고정."""
    def _blocked(*a, **kw):
        raise AssertionError("테스트에서 실제 네트워크 요청이 시도됐습니다")

    monkeypatch.setattr(requests, "post", _blocked)
    monkeypatch.setattr(requests.sessions.Session, "request", _blocked)
    monkeypatch.setattr(source, "DISCORD_WEBHOOK", None)
    monkeypatch.setattr(source, "ENRICHER", None)
    monkeypatch.setattr(source, "ALERT_MODE", "all")
    monkeypatch.setattr(source, "ALERT_CATEGORIES", set())
    monkeypatch.setattr(source, "ALERT_SKIP_WANTED", False)
    monkeypatch.setattr(source, "DEAL_MENTION", "")
    monkeypatch.setattr(source, "_session", None)
    monkeypatch.setattr(source.time, "sleep", lambda s: None)
    yield


@pytest.fixture
def src():
    return source


@pytest.fixture
def db_path(tmp_path, monkeypatch):
    """새 임시 DB를 source.DB로 지정하고 init_db()까지 마친다."""
    p = tmp_path / "seen.db"
    monkeypatch.setattr(source, "DB", str(p))
    source.init_db()
    return p


@pytest.fixture(scope="session")
def enricher():
    if not HAS_PRICING_DATA:
        pytest.skip("pricing_data/가 없어 적정가 모델 테스트를 건너뜁니다")
    from pricing.enrich import Enricher
    return Enricher(DATA_DIR)


@pytest.fixture
def fake_predict(enricher, monkeypatch):
    """적정가 모델 출력을 고정한다. set(p10, p50, p90, basis_level='model', n=12) → 호출 기록 리스트."""
    calls = []

    def set_(p10, p50, p90, basis_level="model", n=12, explain="동일 모델 12건 중앙값 100만원 → 100만원"):
        def predict(frame, **kw):
            calls.append(frame.copy())
            row = {"fair_p10": float(p10), "fair_p50": float(p50), "fair_p90": float(p90),
                   "basis_level": basis_level, f"n_{basis_level}": n, "explain_ko": explain}
            return pd.DataFrame([row], index=frame.index)
        monkeypatch.setattr(enricher.model, "predict", predict)
        return calls
    return set_


@pytest.fixture
def seen_con():
    """원래 스키마(fc71c35) 그대로의 seen 테이블을 가진 메모리 DB."""
    con = sqlite3.connect(":memory:")
    con.execute(ORIGINAL_SEEN_DDL)
    yield con
    con.close()


# ---------------------------------------------------------------------------------------- 합성 매물
def make_item(title, raw_price=None, **kw) -> dict:
    """source.run_once가 normalize_item_from_et_vars 뒤에 넘기는 모양의 합성 매물 dict."""
    it = {
        "id": "9900001", "url": "https://www.drspark.net/ski_sell2/9900001", "title": title,
        "raw_price": raw_price, "author": FAKE_AUTHOR, "observed_at": TS, "status": "A급",
        "status_list": ["A급", "네고 가능", "직거래"], "body_content": title, "release_year": None,
        "model_serial": None, "spec": None, "purchased_at": None, "usage_count": None, "features": None,
        "phone": None, "email": None, "region": None, "thumb": None,
    }
    it.update(kw)
    return it


def add_seen(con, post_id, observed_at, title, price, author="판매자A", **kw):
    """seen에 합성 글 한 건을 넣는다(source.save_item과 같은 열)."""
    con.execute(
        "INSERT INTO seen(post_id, observed_at, title, price, author, status, body_content, release_year, "
        "model_serial, spec, purchased_at, usage_count, features, phone, email) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (str(post_id), int(observed_at), title, price, author, kw.get("status", "A급"),
         kw.get("body_content", title), kw.get("release_year"), kw.get("model_serial"), kw.get("spec"),
         None, None, None, kw.get("phone"), kw.get("email")))
