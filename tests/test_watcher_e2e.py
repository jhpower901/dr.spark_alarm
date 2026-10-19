"""run_once() 끝단 테스트: 합성 목록·상세 HTML(fixture) → seen/enrichment 저장 → Discord 페이로드.

- fixture는 사이트 마크업 구조만 본뜬 가짜 글이다(tests/fixtures/make_fixtures.py가 만든다. 실제 글·연락처 없음).

- 네트워크는 쓰지 않는다: fetch_html은 URL별 fixture를 돌려주고(없는 상세는 404 예외 → 원래 코드의 실패 처리 경로),
  get_session은 부르면 실패하며, requests.post는 페이로드만 기록한다(웹후크 주소도 .invalid).
- DB는 임시 파일, 시각은 source.time을 가짜 시계로 바꿔 고정한다(재게시 판정에 '며칠 뒤'가 필요하다).
- 적정가 모델 값(P50 등)은 번들을 다시 만들면 바뀔 수 있으므로, 기대값은 대부분 enrichment 표에서 끌어내
  알림·필터와 일관한지를 본다. 규칙 기반이라 안정적인 것(카테고리·브랜드·재게시·구매 글)만 값으로 고정한다.
"""
from __future__ import annotations

import copy
import importlib.util
import json
import sqlite3
import subprocess
import sys
import types
from contextlib import closing
from pathlib import Path

import pytest
import requests
from bs4 import BeautifulSoup

ALARM = Path(__file__).resolve().parents[1]
# 동기화된 사본(alarm/drspark)을 먼저 보게 한다(컨테이너에는 분석 저장소의 drspark도 있다)
while str(ALARM) in sys.path:
    sys.path.remove(str(ALARM))
sys.path.insert(0, str(ALARM))

import source  # noqa: E402
from pricing import discord_fields as DF  # noqa: E402

FIX = ALARM / "tests" / "fixtures"
LIST_HTML = (FIX / "list_ski_sell2.html").read_text(encoding="utf-8")
DETAIL_IDS = ("9901017", "9901019", "9901006")  # 로시뇰 스키, 랑게 부츠, 거래완료 부츠백
LIST_IDS = [it["id"] for it in source.parse_list(LIST_HTML)]
BOOTS, SKI, BAG, CAMCORDER, JUNIOR_SKI = "9901019", "9901017", "9901006", "9901015", "9901020"

FAKE_WEBHOOK = "https://discord.invalid/api/webhooks/0/test"
T0 = 1_790_823_600  # 2026-10-01 12:00 KST: 임의로 정한 테스트 시각(fixture 작성일 26.10.01과 맞춤)
DAY = 86_400
ORIGINAL_COMMIT = "fc71c35"  # 적정가 보강 이전 원본("DB WAL모드 사용")

BASE_FIELDS = ["가격", "상태", "작성자", "작성시간"]
OPTIONAL_BASE_FIELDS = {"판매 정보", "거래지역", "본문내용"}
ENRICH_FIELDS = {"모델", "적정가", "적정가 근거", "재게시"}


def post_url(pid: str) -> str:
    return f"{source.BASE}/ski_sell2/{pid}"


def detail_html(pid: str) -> str:
    return (FIX / f"detail_{pid}.html").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------------------------- 가짜 외부 세계
class FakeSite:
    """URL → HTML. 모르는 URL은 실제 사이트의 404처럼 예외를 던진다."""

    def __init__(self):
        self.pages = {source.LIST_URL: LIST_HTML}
        self.pages.update({post_url(pid): detail_html(pid) for pid in DETAIL_IDS})
        self.requested: list[str] = []

    def __call__(self, url: str) -> str:
        self.requested.append(url)
        if url not in self.pages:
            raise requests.HTTPError(f"404 Client Error: Not Found for url: {url}")
        return self.pages[url]


class Clock:
    """source.time 대신 쓰는 시계(time()·sleep()만 쓴다)."""

    def __init__(self, t: float):
        self.t = float(t)

    def time(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.t += s


class Webhook:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def post(self, url, json=None, timeout=None, **kw):  # noqa: A002 (requests.post와 같은 이름)
        self.calls.append((url, copy.deepcopy(json)))
        return types.SimpleNamespace(status_code=204, text="", json=lambda: {})

    @property
    def payloads(self) -> list[dict]:
        return [p for _, p in self.calls]


def _no_network(*a, **kw):
    raise AssertionError("테스트에서 실제 HTTP 세션을 쓰려고 했다")


def quiet_logs(monkeypatch):
    """source.setup_logging()이 붙인 핸들러(작업 폴더의 drspark.log, /dev/log syslog)를 테스트 동안 뗀다.
    로그는 루트로 올라가 pytest가 잡는다(실패 시 출력). /dev/log가 없는 컨테이너에서 경고마다 찍히는
    'Logging error' 소음도 막는다."""
    monkeypatch.setattr(source.logger, "handlers", [])


def patch_module(monkeypatch, mod, *, db: Path, site: FakeSite, clock: Clock):
    monkeypatch.setattr(mod, "DB", str(db))
    monkeypatch.setattr(mod, "DISCORD_WEBHOOK", FAKE_WEBHOOK)
    monkeypatch.setattr(mod, "fetch_html", site)
    monkeypatch.setattr(mod, "get_session", _no_network)
    monkeypatch.setattr(mod, "time", clock)


class Watcher:
    def __init__(self, monkeypatch, tmp_path):
        self.mp = monkeypatch
        self.db = tmp_path / "seen.db"
        self.site = FakeSite()
        self.clock = Clock(T0)
        self.hook = Webhook()
        quiet_logs(monkeypatch)
        patch_module(monkeypatch, source, db=self.db, site=self.site, clock=self.clock)
        monkeypatch.setattr(requests, "post", self.hook.post)
        self.configure(enricher=None)
        source.init_db()

    def configure(self, *, enricher=..., mode="all", categories=(), skip_wanted=False, mention=""):
        if enricher is not ...:
            self.mp.setattr(source, "ENRICHER", enricher)
        self.mp.setattr(source, "ALERT_MODE", mode)
        self.mp.setattr(source, "ALERT_CATEGORIES", set(categories))
        self.mp.setattr(source, "ALERT_SKIP_WANTED", skip_wanted)
        self.mp.setattr(source, "DEAL_MENTION", mention)

    def run(self) -> list[dict]:
        """run_once() 한 번. 이번 실행에서 보낸 페이로드만 돌려준다."""
        n = len(self.hook.calls)
        source.run_once()
        return self.hook.payloads[n:]

    def rows(self, table: str) -> dict[str, dict]:
        with closing(sqlite3.connect(self.db)) as con:
            con.row_factory = sqlite3.Row
            return {r["post_id"]: dict(r) for r in con.execute(f"SELECT * FROM {table}")}


@pytest.fixture
def watcher(monkeypatch, tmp_path):
    return Watcher(monkeypatch, tmp_path)


@pytest.fixture(scope="module")
def real_enricher():
    from pricing.enrich import Enricher

    d = ALARM / "pricing_data"
    if not (d / "fairprice.pkl").exists():
        pytest.skip("pricing_data/가 없어 적정가 보강 끝단 테스트를 건너뜀")
    return Enricher(d)


# ---------------------------------------------------------------------------------------------- 도우미
def pid_of(payload: dict) -> str:
    url = payload["embeds"][0]["url"]
    m = source.POST_ID_RE.search(url)
    assert m, url
    return m.group(1)


def field_map(payload: dict) -> dict[str, str]:
    return {f["name"]: f["value"] for f in payload["embeds"][0]["fields"]}


def row_is_deal(r: dict) -> bool:
    return r["deal_flag"] == "undervalued" and r["deal_confidence"] in ("high", "medium") and not r["error"]


def row_is_cut(r: dict) -> bool:
    return r["price_change_pct"] is not None and r["price_change_pct"] <= -DF.PRICE_CUT_MIN_PCT and not r["error"]


def row_passes_deals(r: dict) -> bool:
    """source.should_alert의 deals 규칙을 enrichment 행으로 다시 쓴 것."""
    if row_is_deal(r):
        return True
    if not row_is_cut(r):
        return False
    if r["fair_p50"]:
        return r["deal_flag"] in ("undervalued", "fair") and r["price_krw"] is not None and r["price_krw"] <= r["fair_p50"]
    return r["price_change_pct"] <= -source.DEALS_NO_FAIR_CUT_PCT


def make_card(src_id: str, new_id: str, *, price: int | None = None, title: str | None = None,
              author: str | None = None) -> str:
    """목록 fixture의 카드 하나를 복제해 새 글처럼 만든다(재게시·구매 글 시나리오용 합성 카드)."""
    soup = BeautifulSoup(LIST_HTML, "html.parser")
    a = soup.select_one(f'a.item__container[href="/ski_sell2/{src_id}"]')
    card = copy.copy(a.find_parent("div", class_="item"))
    card.select_one("a.item__container")["href"] = f"/ski_sell2/{new_id}"
    if price is not None:
        card.select_one(".item__inner.item__etc-wrp span[style*='font-size']").string = f"{price:,}원"
    if title is not None:
        card.select_one(".item__inner.item__subject .subject").string = title
    if author is not None:
        card.select_one(".item__author span").string = author
    return str(card)


def list_with(*cards: str) -> str:
    marker = '<div class="simple-board__webzine">'
    assert marker in LIST_HTML
    return LIST_HTML.replace(marker, marker + "".join(cards), 1)  # 새 글은 목록 맨 위


def assert_discord_limits(payload: dict) -> None:
    json.dumps(payload, ensure_ascii=False)  # numpy 값 등 직렬화 안 되는 값이 없어야 한다
    emb = payload["embeds"][0]
    assert len(emb["title"]) <= 256
    assert len(emb["fields"]) <= 25
    for f in emb["fields"]:
        assert f["value"] and len(f["value"]) <= 1024, f["name"]
    total = len(emb["title"]) + sum(len(f["name"]) + len(f["value"]) for f in emb["fields"])
    assert total <= source.EMBED_TOTAL_LIMIT


# ---------------------------------------------------------------------------------------------- 보강 켬
def test_uses_vendored_drspark_package(real_enricher):
    """번들로 동기화된 alarm/drspark를 써야 한다(분석 저장소 쪽 drspark가 먼저 잡히면 버전이 어긋난다)."""
    import drspark

    assert Path(drspark.__file__).resolve().parent == (ALARM / "drspark").resolve()


def test_first_run_saves_enriches_and_alerts_each_new_post(watcher, real_enricher):
    watcher.configure(enricher=real_enricher)
    payloads = watcher.run()

    # 목록 → 새 글마다 상세 요청(목록 순서)
    assert watcher.site.requested == [source.LIST_URL] + [post_url(pid) for pid in LIST_IDS]

    seen = watcher.rows("seen")
    enr = watcher.rows("enrichment")
    assert set(seen) == set(LIST_IDS)
    assert set(enr) == set(LIST_IDS)
    assert all(r["error"] is None for r in enr.values()), {k: r["error"] for k, r in enr.items() if r["error"]}
    assert {r["pricing_version"] for r in enr.values()} == {real_enricher.version}
    assert all(r["observed_at"] == T0 for r in seen.values())

    # 새 글 하나에 페이로드 하나, 목록 순서대로, 가짜 웹후크로만
    assert [pid_of(p) for p in payloads] == LIST_IDS
    assert {url for url, _ in watcher.hook.calls} == {FAKE_WEBHOOK}

    for p in payloads:
        pid = pid_of(p)
        r = enr[pid]
        emb = p["embeds"][0]
        names = [f["name"] for f in emb["fields"]]
        fm = field_map(p)
        assert_discord_limits(p)
        assert names[:4] == BASE_FIELDS
        assert names[4] == "모델"
        assert set(names) <= set(BASE_FIELDS) | OPTIONAL_BASE_FIELDS | ENRICH_FIELDS
        assert p["content"] is None  # DEAL_MENTION 없음
        assert emb["color"] == (DF.COLOR.get(r["deal_flag"], DF.DEFAULT_COLOR) if r["deal_flag"] else DF.DEFAULT_COLOR)
        assert emb["title"].startswith("[저평가] ") == row_is_deal(r)
        assert ("[가격인하]" in emb["title"]) == row_is_cut(r)
        assert emb["thumbnail"]["url"].startswith("https://img.example.invalid/")
        if r["fair_p50"]:
            assert r["fair_p10"] <= r["fair_p50"] <= r["fair_p90"]
            assert "80% 범위" in fm["적정가"]
            assert DF.FLAG_LABEL[r["deal_flag"]] in fm["적정가"]
        elif r["skipped_reason"]:
            assert fm["적정가"] == f"산출 안 함({r['skipped_reason']})"
        assert ("재게시" in fm) == (r["repost_of"] is not None)
        if pid in DETAIL_IDS:
            assert "판매 정보" in fm and "본문내용" in fm
        else:  # 상세 404 → 목록 정보만으로 저장·보강·알림
            assert seen[pid]["body_content"] is None
            assert "본문내용" not in fm

    # 상세까지 있는 글: seen에 et_vars 정규화 값, enrichment에 규칙 기반 식별 결과
    s = seen[BOOTS]
    assert (s["price"], s["author"], s["status"]) == (150_000, "판매자B", "A급")
    assert (s["release_year"], s["model_serial"], s["spec"]) == ("2324", "Lange LX 90", "Flex90 255mm")
    assert (s["phone"], s["email"]) == ("010-0000-0000", "user@example.com")
    assert s["body_content"].startswith("23/24 시즌 구매")
    e = enr[BOOTS]
    assert (e["listing_type"], e["category"], e["brand"]) == ("sale", "boots", "lange")
    assert (e["model_id"] or e["family_id"] or "").startswith("lange/")
    assert e["season_start"] == 2023 and e["price_krw"] == 150_000
    fm = field_map(next(p for p in payloads if pid_of(p) == BOOTS))
    assert "출시연도: 2324" in fm["판매 정보"] and "Model/Serial: Lange LX 90" in fm["판매 정보"]
    assert "전화번호: 010-0000-0000" in fm["판매 정보"]
    assert fm["거래지역"] == "경기 성남시"
    assert fm["상태"] == "A급 / 네고X / 직거래, 택배"
    assert fm["가격"] == "150,000원"

    e = enr[SKI]
    assert (e["category"], e["brand"], e["model_id"], e["id_grade"], e["season_start"]) == \
        ("ski", "rossignol", "rossignol/hero/elite-st-ti", "A", 2024)
    # 본문의 '~하시면 60만원에 드립니다'는 같은 물건의 조건부 금액이지 두 번째 물건 가격이 아니다
    # (묶음 판정 규칙 2026-10-19 개정 전에는 이런 글을 '여러 물건 묶음'으로 보고 적정가를 내지 않았다)
    assert e["skipped_reason"] is None and e["fair_p10"] <= e["fair_p50"] <= e["fair_p90"]
    assert enr[BAG]["category"] == "bag"
    assert enr[CAMCORDER]["skipped_reason"] == "장비 외 품목"

    # 한 번에 들어온 같은 판매자 글: 먼저 저장된 '더 새 글'을 이전 글로 잡지 않는다(9901012→9901013 같은 역방향 연결 금지)
    for pid, r in enr.items():
        assert r["repost_of"] is None or int(r["repost_of"]) < int(pid), (pid, r["repost_of"])

    # 알림 대기열: 모두 보냈고 페이로드는 지웠다
    box = watcher.rows("alert_outbox")
    assert {k: (v["status"], v["attempts"], v["payload"]) for k, v in box.items()} == \
        {pid: ("sent", 1, None) for pid in LIST_IDS}


def test_second_run_sends_nothing(watcher, real_enricher):
    watcher.configure(enricher=real_enricher)
    first = watcher.run()
    assert len(first) == len(LIST_IDS)
    watcher.site.requested.clear()
    watcher.clock.t += 60

    assert watcher.run() == []
    assert watcher.site.requested == [source.LIST_URL]  # 이미 본 글은 상세도 다시 받지 않는다
    assert len(watcher.rows("seen")) == len(LIST_IDS)
    assert len(watcher.rows("enrichment")) == len(LIST_IDS)


def test_deals_mode_sends_only_deals_and_price_cuts(watcher, real_enricher):
    watcher.configure(enricher=real_enricher, mode="deals")
    sent = [pid_of(p) for p in watcher.run()]
    enr = watcher.rows("enrichment")
    expected = [pid for pid in LIST_IDS if row_passes_deals(enr[pid])]
    assert sent == expected
    assert len(sent) < len(LIST_IDS)
    # 걸러진 글도 기록은 남는다(다음 실행에서 다시 알리지 않도록)
    assert set(watcher.rows("seen")) == set(LIST_IDS) and set(enr) == set(LIST_IDS)
    assert {k: v["status"] for k, v in watcher.rows("alert_outbox").items()} == \
        {pid: ("sent" if pid in sent else "filtered") for pid in LIST_IDS}

    # 이틀 뒤: 판매자B가 같은 부츠를 15만→14만원으로 다시 올림, 판매자A는 같은 값으로 다시 올림
    watcher.clock.t += 2 * DAY
    watcher.site.pages[source.LIST_URL] = list_with(
        make_card(BOOTS, "9909001", price=140_000),
        make_card(JUNIOR_SKI, "9909002"),
    )
    # 상세 희망가도 같이 내린다(source는 상세 희망가를 목록 가격보다 우선한다)
    watcher.site.pages[post_url("9909001")] = detail_html(BOOTS).replace("150,000원", "140,000원")
    second = watcher.run()
    enr = watcher.rows("enrichment")

    cut = enr["9909001"]
    assert (cut["repost_of"], cut["repost_n"], cut["prev_price"], cut["price_change_pct"]) == \
        (BOOTS, 1, 150_000, -6.7)
    same = enr["9909002"]
    assert same["repost_of"] == JUNIOR_SKI and same["price_change_pct"] == 0.0
    assert not row_is_deal(same)  # 브랜드만 식별(C) → 신뢰 '참고용'이라 저평가 알림 대상이 아님

    # 6.7% 내린 부츠는 내린 값(14만원)이 적정가(P50) 이하라 deals 모드에서도 알린다
    assert row_is_cut(cut) and cut["deal_flag"] in ("undervalued", "fair") and cut["price_krw"] <= cut["fair_p50"]
    assert [pid_of(p) for p in second] == ["9909001"]
    emb = second[0]["embeds"][0]
    assert "[가격인하]" in emb["title"]
    assert emb["title"].endswith("2324 랑게 LX 90 부츠 255mm")
    repost = field_map(second[0])["재게시"]
    assert repost.startswith("같은 판매자의 이전 글 1건")
    assert "직전 호가 15만원 → 이번 14만원 (-6.7%)" in repost


def test_category_and_wanted_filters(watcher, real_enricher):
    wanted = make_card(BOOTS, "9909003", price=100_000, title="[구매] 랑게 부츠 255mm 구합니다", author="판매자K")
    watcher.site.pages[source.LIST_URL] = list_with(wanted)
    watcher.configure(enricher=real_enricher, categories={"boots"}, skip_wanted=True)
    sent = [pid_of(p) for p in watcher.run()]
    enr = watcher.rows("enrichment")

    w = enr["9909003"]
    assert (w["listing_type"], w["category"], w["skipped_reason"]) == ("wanted", "boots", "구매 희망 글")
    expected = [pid for pid in ["9909003"] + LIST_IDS
                if enr[pid]["category"] == "boots" and enr[pid]["listing_type"] != "wanted"]
    assert sent == expected
    assert BOOTS in sent and "9909003" not in sent
    assert len(watcher.rows("seen")) == len(LIST_IDS) + 1


def test_deal_mention_only_on_deals(watcher, real_enricher):
    watcher.configure(enricher=real_enricher, mention="<@&123456789>")
    payloads = watcher.run()
    enr = watcher.rows("enrichment")
    for p in payloads:
        assert p["content"] == ("<@&123456789>" if row_is_deal(enr[pid_of(p)]) else None)


def test_enricher_crash_still_sends_original_alert(watcher):
    class Broken:
        version = "broken"

        def enrich(self, item, con=None):
            raise RuntimeError("모델 파일 손상")

    watcher.configure(enricher=Broken(), mode="deals")  # 보강 실패면 필터와 무관하게 알린다
    payloads = watcher.run()
    assert [pid_of(p) for p in payloads] == LIST_IDS
    assert watcher.rows("enrichment") == {}
    for p in payloads:
        assert p["embeds"][0]["color"] == 0x3498DB
        assert not ENRICH_FIELDS & set(field_map(p))


# ---------------------------------------------------------------------------------------------- 보강 끔
def load_original_source(tmp_path: Path):
    """적정가 보강 이전 원본 source.py(fc71c35)를 별도 모듈로 읽는다. git이 없으면 건너뜀."""
    try:
        out = subprocess.run(["git", "-C", str(ALARM), "show", f"{ORIGINAL_COMMIT}:source.py"],
                             capture_output=True, check=True, timeout=60).stdout
    except (OSError, subprocess.SubprocessError) as ex:
        pytest.skip(f"원본 커밋 {ORIGINAL_COMMIT}을 읽을 수 없음: {ex}")
    path = tmp_path / f"source_{ORIGINAL_COMMIT}.py"
    path.write_bytes(out)
    spec = importlib.util.spec_from_file_location(f"source_{ORIGINAL_COMMIT}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_enrichment_off_payload_matches_original_format(watcher, monkeypatch, tmp_path):
    from pricing.enrich import Enricher

    monkeypatch.setenv("DRSPARK_PRICING", "0")
    enricher = Enricher.from_env()
    assert enricher is None
    watcher.configure(enricher=enricher)
    payloads = watcher.run()

    assert [pid_of(p) for p in payloads] == LIST_IDS
    assert watcher.rows("enrichment") == {}
    titles = {it["id"]: it["title"] for it in source.parse_list(LIST_HTML)}
    for p in payloads:
        emb = p["embeds"][0]
        names = [f["name"] for f in emb["fields"]]
        assert p["content"] is None
        assert emb["color"] == 0x3498DB
        assert emb["title"] == titles[pid_of(p)]
        assert names[:4] == BASE_FIELDS and set(names[4:]) <= OPTIONAL_BASE_FIELDS
        assert set(emb) == {"title", "url", "color", "fields", "thumbnail"}

    # 원본 코드(fc71c35)를 같은 fixture·시각·가짜 웹후크로 돌린 결과와 페이로드·seen 행이 똑같아야 한다
    orig = load_original_source(tmp_path)
    quiet_logs(monkeypatch)  # 원본 모듈도 import 때 같은 'drspark' 로거에 핸들러를 붙인다
    orig_site, orig_hook = FakeSite(), Webhook()
    orig_db = tmp_path / "seen_original.db"
    patch_module(monkeypatch, orig, db=orig_db, site=orig_site, clock=Clock(T0))
    monkeypatch.setattr(requests, "post", orig_hook.post)
    orig.init_db()
    orig.run_once()

    # 의도한 차이는 두 가지뿐이다. ① 사이트가 바꾼 라벨 '희망가'를 상세 가격으로 읽는다(원본은 '판매가격'만 읽어
    # 목록 가격이 그대로 남거나 비었다). ② 판매중이 아닌 '거래' 상태(거래완료 등)를 상태 칸 앞에 붙인다.
    # 그 두 가지를 원본 결과에 적용하면 나머지는 원본과 완전히 같아야 한다.
    detail = {}
    for pid in LIST_IDS:
        page = orig_site.pages.get(post_url(pid))
        if page:
            et = source.parse_detail(page, {"title": ""})["et_vars"]
            detail[pid] = (source._digits_to_int(et.get("판매가격") or et.get("희망가")), (et.get("거래") or "").strip())
    expected = copy.deepcopy(orig_hook.payloads)
    for p in expected:
        hope, trade = detail.get(pid_of(p), (None, ""))
        for f in p["embeds"][0]["fields"]:
            if f["name"] == "가격" and hope is not None:
                f["value"] = f"{hope:,}원"
            if f["name"] == "상태" and trade and trade != "판매중":
                f["value"] = f"{trade} / {f['value']}"
    assert payloads == expected
    assert any(h is not None for h, _ in detail.values())  # fixture에 희망가가 실제로 있어 차이를 검사했다
    assert orig_site.requested == watcher.site.requested
    with closing(sqlite3.connect(orig_db)) as con:
        orig_rows = {r[0]: list(r) for r in con.execute("SELECT * FROM seen")}
    with closing(sqlite3.connect(watcher.db)) as con:
        new_rows = {r[0]: list(r) for r in con.execute("SELECT * FROM seen")}
    for pid, (hope, _trade) in detail.items():
        if hope is not None:
            orig_rows[pid][3] = hope  # seen.price
    assert new_rows == orig_rows
