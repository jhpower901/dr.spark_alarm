"""알림 대기열(alert_outbox): Discord 전송이 실패해도 알림을 잃지 않고 다음 실행에서 한 번만 보낸다(OPS-1).

합성 글 3건(가짜 목록·상세 HTML)과 가짜 웹후크(requests.post)만 쓴다. 실제 사이트·Discord에는 아무것도 보내지 않는다.
"""
from __future__ import annotations

import copy
import sqlite3
import types
from contextlib import closing

import pytest
import requests

from pricing.enrich import Enrichment
from tests.conftest import FAKE_AUTHOR, FAKE_EMAIL, FAKE_PHONE, TS

FAKE_WEBHOOK = "https://discord.invalid/api/webhooks/0/outbox-test"
# 목록은 최신 글이 먼저
POSTS = [("9900103", "2526 아토믹 레드스터 S9 FIS 165 급처", "900,000"),
         ("9900102", "2425 로시뇰 히어로 월드컵 ZA 26.5", "700,000"),
         ("9900101", "카브 하드쉘 세트 L", "300,000")]
IDS = [p[0] for p in POSTS]


def _list_html(posts) -> str:
    cards = "".join(f"""
      <div class="item"><a class="item__container" href="/ski_sell2/{pid}">
        <div class="item__inner item__subject"><span class="subject">{title}</span></div>
        <div class="item__inner item__etc-wrp"><span style="font-size:13px">{price}원</span></div>
        <div class="item__author"><span>{FAKE_AUTHOR}</span></div>
      </a></div>""" for pid, title, price in posts)
    return f'<html><body><div class="simple-board__webzine">{cards}</div></body></html>'


def _detail_html(price: str) -> str:
    et = {"판매가격": price, "장비상태": "A급", "네고": "네고 가능", "거래방법": "직거래", "전화번호": FAKE_PHONE,
          "E-mail": FAKE_EMAIL}
    items = "".join(f'<!--<div class="item"><div class="item__label">{k}</div>'
                    f'<div class="item__value">{v}</div></div>-->' for k, v in et.items())
    return f"""<html><body><div class="rhymix_content xe_content"><p>상태 좋습니다.</p></div>
      <div class="simple-board__read__extravars">{items}</div><div class="et_vars"></div></body></html>"""


class Clock:
    def __init__(self, t):
        self.t = float(t)

    def time(self):
        return self.t

    def sleep(self, s):
        self.t += s


class Hook:
    """가짜 웹후크. mode: 'ok'(204) / 'conn'(연결 실패) / 정수(그 HTTP 상태). reject: 이 제목이 들어간 글만 400."""

    def __init__(self):
        self.mode = "ok"
        self.reject = None
        self.calls = 0
        self.delivered: list[dict] = []

    def post(self, url, json=None, timeout=None, **kw):  # noqa: A002
        assert url == FAKE_WEBHOOK
        self.calls += 1
        status = self.mode
        if self.reject and self.reject in json["embeds"][0]["title"]:
            status = 400
        if status == "conn":
            raise requests.exceptions.ConnectionError("discord down")
        if status == "ok":
            self.delivered.append(copy.deepcopy(json))
            status = 204
        resp = types.SimpleNamespace(status_code=status, text="", json=lambda: {"retry_after": 0})

        def raise_for_status():
            if status >= 400:
                raise requests.HTTPError(f"{status} Error", response=resp)
        resp.raise_for_status = raise_for_status
        return resp

    @property
    def delivered_ids(self):
        return [p["embeds"][0]["url"].rsplit("/", 1)[1] for p in self.delivered]


class World:
    def __init__(self, src, monkeypatch, tmp_path):
        self.src, self.mp = src, monkeypatch
        self.db = tmp_path / "seen.db"
        self.clock = Clock(TS)
        self.hook = Hook()
        self.pages = {src.LIST_URL: _list_html(POSTS)}
        self.pages.update({f"{src.BASE}/ski_sell2/{pid}": _detail_html(price) for pid, _, price in POSTS})
        monkeypatch.setattr(src.logger, "handlers", [])  # 로그는 pytest가 잡는다
        monkeypatch.setattr(src, "DB", str(self.db))
        monkeypatch.setattr(src, "DISCORD_WEBHOOK", FAKE_WEBHOOK)
        monkeypatch.setattr(src, "fetch_html", self.fetch)
        monkeypatch.setattr(src, "time", self.clock)
        monkeypatch.setattr(requests, "post", self.hook.post)
        self.list_down = False
        src.init_db()

    def fetch(self, url):
        if url == self.src.LIST_URL and self.list_down:
            raise requests.exceptions.ConnectionError("site down")
        return self.pages[url]

    def run(self, minutes=1):
        self.clock.t += 60 * minutes
        self.src.run_once()

    def outbox(self) -> dict[str, dict]:
        with closing(sqlite3.connect(self.db)) as con:
            con.row_factory = sqlite3.Row
            return {r["post_id"]: dict(r) for r in con.execute("SELECT * FROM alert_outbox")}

    def statuses(self) -> dict[str, str]:
        return {k: v["status"] for k, v in self.outbox().items()}

    def seen_ids(self) -> set[str]:
        with closing(sqlite3.connect(self.db)) as con:
            return {r[0] for r in con.execute("SELECT post_id FROM seen")}


@pytest.fixture
def world(src, monkeypatch, tmp_path):
    return World(src, monkeypatch, tmp_path)


# ------------------------------------------------------------------------------------ 정상
def test_healthy_run_sends_once_and_records_sent(world, src):
    world.run()
    assert world.hook.delivered_ids == IDS  # 목록 순서 그대로(기존 동작)
    assert world.hook.calls == 3
    box = world.outbox()
    assert {k: (v["status"], v["attempts"], v["payload"]) for k, v in box.items()} == \
        {pid: ("sent", 1, None) for pid in IDS}  # 보낸 뒤에는 페이로드(연락처 포함)를 지운다
    # 보낸 페이로드는 보강 없는 기존 형식 그대로
    emb = world.hook.delivered[0]["embeds"][0]
    assert emb["title"] == POSTS[0][1] and emb["color"] == 0x3498DB
    assert [f["name"] for f in emb["fields"]] == ["가격", "상태", "작성자", "작성시간", "판매 정보", "본문내용"]
    assert world.hook.delivered[0]["content"] is None
    world.run()
    assert world.hook.calls == 3  # 이미 본 글·보낸 알림은 다시 보내지 않는다


# ------------------------------------------------------------------------------------ 장애 → 복구
def test_outage_then_recovery_delivers_each_alert_once(world):
    world.hook.mode = "conn"
    world.run()
    # 첫 글에서 3번 시도해 실패 → 남은 글은 시도하지 않고 대기열에만(1분 주기를 넘기지 않게)
    assert world.hook.calls == 3
    assert world.seen_ids() == set(IDS)
    box = world.outbox()
    assert {k: v["status"] for k, v in box.items()} == {pid: "pending" for pid in IDS}
    assert [box[pid]["attempts"] for pid in IDS] == [1, 0, 0]
    assert box[IDS[0]]["last_error"].startswith("ConnectionError")
    assert all(v["payload"] for v in box.values())

    world.run()  # 아직 장애: 대기열 맨 앞 1건만 시도하고 멈춘다
    assert world.hook.calls == 6
    assert world.statuses() == {pid: "pending" for pid in IDS}

    world.hook.mode = "ok"
    world.run()
    assert world.hook.delivered_ids == IDS  # 잃은 알림 없이, 처음 순서대로, 한 번씩
    assert {k: (v["status"], v["payload"]) for k, v in world.outbox().items()} == {pid: ("sent", None) for pid in IDS}
    assert world.outbox()[IDS[0]]["attempts"] == 3

    world.run()
    assert world.hook.delivered_ids == IDS


def test_rate_limited_round_keeps_alerts_pending(world):
    world.hook.mode = 429  # 429만 3번: 예전에는 예외 없이 끝나 이번 회차의 새 글 알림을 모두 잃었다
    world.run()
    assert world.hook.calls == 3
    assert world.statuses() == {pid: "pending" for pid in IDS}
    assert world.outbox()[IDS[0]]["last_error"].startswith("RetryError")
    world.hook.mode = "ok"
    world.run()
    world.run()
    assert world.hook.delivered_ids == IDS


def test_server_error_then_recovery(world):
    world.hook.mode = 503
    world.run()
    assert world.statuses() == {pid: "pending" for pid in IDS}
    world.hook.mode = "ok"
    world.run()
    assert world.hook.delivered_ids == IDS


def test_new_posts_during_outage_are_queued_after_older_ones(world, src):
    world.pages[src.LIST_URL] = _list_html(POSTS[1:])
    world.hook.mode = "conn"
    world.run()
    world.pages[src.LIST_URL] = _list_html(POSTS)  # 장애 중에 새 글이 올라옴
    world.run()
    world.hook.mode = "ok"
    world.run()
    assert world.hook.delivered_ids == IDS[1:] + IDS[:1]  # 먼저 들어온 알림부터


def test_list_fetch_failure_still_retries_pending(world):
    world.hook.mode = "conn"
    world.run()
    world.hook.mode = "ok"
    world.list_down = True
    world.run()
    assert world.hook.delivered_ids == IDS


# ------------------------------------------------------------------------------------ 버리는 경우
def test_pending_alerts_expire_after_retry_window(world, src, monkeypatch):
    monkeypatch.setattr(src, "ALERT_RETRY_HOURS", 6.0)
    world.hook.mode = "conn"
    world.run()
    world.hook.mode = "ok"
    world.run(minutes=6 * 60 + 2)  # 6시간 넘게 장애가 이어진 뒤
    assert world.hook.delivered == []  # 오래된 알림을 한꺼번에 쏟아내지 않는다
    assert {k: (v["status"], v["payload"]) for k, v in world.outbox().items()} == \
        {pid: ("expired", None) for pid in IDS}


def test_rejected_payload_is_failed_and_others_continue(world):
    world.hook.reject = POSTS[1][1]  # 이 글의 페이로드만 Discord가 400으로 거부
    world.run()
    assert world.hook.delivered_ids == [IDS[0], IDS[2]]
    st = world.statuses()
    assert st == {IDS[0]: "sent", IDS[1]: "failed", IDS[2]: "sent"}
    box = world.outbox()
    assert box[IDS[1]]["payload"] is None and box[IDS[1]]["last_error"].startswith("HTTPError")
    calls = world.hook.calls
    world.run()
    assert world.hook.calls == calls  # 거부된 글은 다시 보내지 않는다(다른 알림을 막지도 않는다)


def test_no_webhook_is_recorded_and_not_retried(world, src, monkeypatch):
    monkeypatch.setattr(src, "DISCORD_WEBHOOK", None)
    world.run()
    assert world.hook.calls == 0
    assert world.statuses() == {pid: "no_webhook" for pid in IDS}
    assert all(v["payload"] is None for v in world.outbox().values())
    monkeypatch.setattr(src, "DISCORD_WEBHOOK", FAKE_WEBHOOK)
    world.run()
    assert world.hook.calls == 0


# ------------------------------------------------------------------------------------ 필터·글별 실패
class FakeEnricher:
    """제목에 '급처'가 있으면 저평가(신뢰 높음), 아니면 적정 범위."""
    version = "fake"

    def enrich(self, item, con=None):
        deal = "급처" in (item.get("title") or "")
        return Enrichment(listing_type="sale", category="ski", id_grade="A", price_krw=item.get("raw_price"),
                          fair_p10=800_000, fair_p50=1_000_000, fair_p90=1_200_000, basis_level="model", n_basis=12,
                          deal_flag="undervalued" if deal else "fair", deal_ratio=0.5,
                          deal_confidence="high", pricing_version="fake")


def test_filtered_posts_are_recorded_and_never_sent(world, src, monkeypatch):
    monkeypatch.setattr(src, "ENRICHER", FakeEnricher())
    monkeypatch.setattr(src, "ALERT_MODE", "deals")
    world.run()
    assert world.hook.delivered_ids == [IDS[0]]
    assert world.statuses() == {IDS[0]: "sent", IDS[1]: "filtered", IDS[2]: "filtered"}
    world.run()
    assert world.hook.delivered_ids == [IDS[0]]


def test_one_post_failing_does_not_stop_the_round(world, src, monkeypatch):
    real = src.build_payload

    def flaky(item, enrichment=None):
        if item["id"] == IDS[1]:
            raise ValueError("bad item")
        return real(item, enrichment)
    monkeypatch.setattr(src, "build_payload", flaky)
    world.run()
    assert world.hook.delivered_ids == [IDS[0], IDS[2]]  # 예전에는 여기서 회차가 끝나 3번째 글이 늦어졌다
    assert world.seen_ids() == set(IDS)


def test_outbox_write_failure_still_sends(world, src, monkeypatch):
    monkeypatch.setattr(src, "_outbox_put", lambda *a, **kw: False)
    world.run()
    assert world.hook.delivered_ids == IDS
