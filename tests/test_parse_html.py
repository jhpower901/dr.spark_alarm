"""합성 fixture(tests/fixtures/*.html)로 목록·상세 파서 회귀 테스트.

fixture는 닥터스파크 장터 페이지의 마크업 구조(클래스·extravars 주석 조각·et_vars·본문/댓글 div·공지 줄 등)를
그대로 본뜨되 글번호·제목·본문·속성·이미지·닉네임은 모두 지어낸 값이다(tests/fixtures/make_fixtures.py가 만든다).
사이트 마크업이 바뀌면 make_fixtures.py의 템플릿을 새 구조에 맞춰 고치고, 다시 만든 뒤 기대값을 맞춘다.
실제 페이지를 fixture로 넣지 않는다(맨 아래 위생 검사가 막는다).
"""
from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

ALARM = Path(__file__).resolve().parents[1]
# 동기화된 사본(alarm/drspark)을 먼저 보게 한다(컨테이너에는 분석 저장소의 drspark도 있다)
while str(ALARM) in sys.path:
    sys.path.remove(str(ALARM))
sys.path.insert(0, str(ALARM))

import source  # noqa: E402

FIX = ALARM / "tests" / "fixtures"


def _read(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


LIST_HTML = _read("list_ski_sell2.html")

# 목록 1쪽(위에서부터, 끌어올린 글 때문에 글번호 순서가 군데군데 뒤바뀐다).
# 공지 4건(9900901~9900904)은 item__container가 아니라 빠진다
LIST_IDS = [
    "9901020", "9901019", "9901018", "9901017", "9901016", "9901014", "9901015", "9901013", "9901012", "9901011",
    "9901010", "9901009", "9901008", "9901007", "9901005", "9901006", "9901004", "9901003", "9901002", "9901001",
]
NOTICE_IDS = {"9900901", "9900902", "9900903", "9900904"}

# 사이트 상세 페이지의 extravars 라벨(주석 조각, fixture도 같은 라벨). '판매가격'이 아니라 '희망가'다
FULL_LABELS = ["거래", "출시연도", "Model/Serial No.", "제품스펙", "구입처/시기", "사용횟수", "특장점", "장비상태",
               "희망가", "네고", "결정순서", "거래방법", "거래지역", "전화번호", "E-mail"]


@pytest.fixture(autouse=True)
def quiet_logs(monkeypatch):
    """source.setup_logging()의 파일·syslog 핸들러를 테스트 동안 뗀다(로그는 pytest가 잡는다)."""
    monkeypatch.setattr(source.logger, "handlers", [])


@pytest.fixture(scope="module")
def list_items():
    return source.parse_list(LIST_HTML)


def _item(list_items, pid: str) -> dict:
    return dict(next(it for it in list_items if it["id"] == pid))


def _detail_item(list_items, pid: str) -> dict:
    """run_once()와 같은 순서: 목록 항목 + 상세 파싱 → et_vars 정규화."""
    it = _item(list_items, pid)
    it.update(source.parse_detail(_read(f"detail_{pid}.html"), it))
    source.normalize_item_from_et_vars(it)
    return it


# ---------------------------------------------------------------------------------------------- 목록
def test_parse_list_ids_titles_urls(list_items):
    assert [it["id"] for it in list_items] == LIST_IDS
    assert not NOTICE_IDS & {it["id"] for it in list_items}
    for it in list_items:
        assert it["url"] == f"https://www.drspark.net/ski_sell2/{it['id']}"
        assert it["title"].strip(), it["id"]
        assert re.fullmatch(r"판매자[A-Z]+", it["author"]), it["author"]


def test_parse_list_thumbnails_are_absolute(list_items):
    for it in list_items:
        # 사이트처럼 src가 스킴 없이 //호스트/... 로 온다 → https: 붙임
        assert it["thumb"].startswith("https://img.example.invalid/files/thumbnails/"), it["thumb"]
        assert it["thumb"].split("?")[0].endswith("150x100.crop.jpg")


def test_parse_list_prices_and_status_icons(list_items):
    prices = {it["id"]: it["raw_price"] for it in list_items}
    assert prices["9901020"] == 500_000
    assert prices["9901019"] == 150_000
    assert prices["9901017"] == 650_000
    assert prices["9901010"] == 990_000  # 카드 안에 광고 블록이 끼어 있는 항목
    assert prices["9901003"] == 40_000
    # 거래완료 카드는 목록에 가격 줄이 없다
    assert prices["9901006"] is None
    assert all(isinstance(v, int) and v > 0 for k, v in prices.items() if k != "9901006")

    icons = {it["id"]: it["status_icon_list"] for it in list_items}
    assert icons["9901017"] == ["S급", "네고X", "직거래"]
    assert icons["9901014"] == ["S급", "네고○", "직거래, 택배"]
    assert icons["9901004"] == ["미개봉", "네고X", "직거래, 택배"]
    assert icons["9901006"] == ["거래완료"]


def test_parse_list_titles_exact(list_items):
    titles = {it["id"]: it["title"] for it in list_items}
    assert titles["9901017"] == "[판매] 로시뇰 HERO ELITE ST Ti 24/25 162cm 스키"
    assert titles["9901019"] == "2324 랑게 LX 90 부츠 255mm"
    assert titles["9901006"] == "부츠백 팝니다 (택배 가능)"
    # 같은 판매자의 비슷한 글 두 건(재게시 판정에 쓰이는 관계가 가명으로도 남아 있어야 한다)
    a, b = _item(list_items, "9901013"), _item(list_items, "9901012")
    assert a["author"] == b["author"]


def test_parse_list_empty_page():
    assert source.parse_list("<html><body><div class='simple-board__webzine'></div></body></html>") == []


# ---------------------------------------------------------------------------------------------- 상세
DETAIL_EXPECT = {
    "9901017": {
        "labels": FULL_LABELS,
        "release_year": "24/25",
        "model_serial": "ROSSIGNOL HERO ELITE ST Ti",
        "spec": "162cm / Radius 13m",
        "purchased_at": "24년 가을 매장 구매",
        "usage_count": "10회 정도",
        "region": "서울 송파구",
        "status_list": ["S급", "네고X", "직거래"],
        "희망가": "650,000원",
        "거래": "판매중",
        "body_first_line": "로시뇰 HERO ELITE ST Ti 24/25 스키 판매합니다 — 65만 원",
    },
    "9901019": {
        "labels": FULL_LABELS,
        "release_year": "2324",
        "model_serial": "Lange LX 90",
        "spec": "Flex90 255mm",
        "purchased_at": "중고 구매",
        "usage_count": "10회",
        "region": "경기 성남시",
        "status_list": ["A급", "네고X", "직거래, 택배"],
        "희망가": "150,000원",
        "거래": "판매중",
        "body_first_line": "23/24 시즌 구매, 10일 정도 사용했습니다.",
    },
    "9901006": {
        # 제품스펙·거래지역을 비워 둔 글: 라벨 자체가 빠진다
        "labels": [x for x in FULL_LABELS if x not in ("제품스펙", "거래지역")],
        "release_year": "25/26",
        "model_serial": "Skitzo 부츠백",
        "spec": None,
        "purchased_at": "온라인몰 구매",
        "usage_count": "1시즌",
        "region": None,
        "status_list": ["S급", "네고○", "택배"],
        "희망가": "20,000원",
        "거래": "거래완료",
        "body_first_line": "부츠백 팝니다. 택배 가능합니다.",
    },
}


@pytest.mark.parametrize("pid", sorted(DETAIL_EXPECT))
def test_parse_detail_extravars_from_comments(list_items, pid):
    exp = DETAIL_EXPECT[pid]
    it = _detail_item(list_items, pid)
    et = it["et_vars"]
    assert list(et) == exp["labels"]
    assert et["희망가"] == exp["희망가"]
    assert et["거래"] == exp["거래"]
    for k in ("release_year", "model_serial", "spec", "purchased_at", "usage_count", "region", "status_list"):
        assert it[k] == exp[k], k
    assert it["status"] == exp["status_list"][0]
    assert it["features"]
    # fixture는 연락처를 자리표시자로 바꿔 두었다(주석 안의 원본 값까지)
    assert it["phone"] == "010-0000-0000"
    assert it["email"] == "user@example.com"


@pytest.mark.parametrize("pid", sorted(DETAIL_EXPECT))
def test_parse_detail_body(list_items, pid):
    it = _detail_item(list_items, pid)
    body = it["body_content"]
    assert body.splitlines()[0].startswith(DETAIL_EXPECT[pid]["body_first_line"])
    assert "[본문 요약 없음]" not in body
    # 본문 아래 '이 상품 적정가?' 광고 상자·스크립트 문구가 섞이지 않아야 한다
    assert "jQuery" not in body and "적정가 보기" not in body


def test_parse_detail_ignores_comment_body(list_items):
    """댓글도 div.rhymix_content.xe_content라서, 문서 본문(먼저 나옴)만 골라야 한다."""
    html = _read("detail_9901006.html")
    assert html.count("rhymix_content xe_content") == 2
    it = _detail_item(list_items, "9901006")
    assert it["body_content"] == "부츠백 팝니다. 택배 가능합니다.\n문자 주세요"
    assert "연락드릴게요" not in it["body_content"]


def test_parse_detail_full_body_keeps_line_breaks(list_items):
    it = _detail_item(list_items, "9901017")
    lines = it["body_content"].splitlines()
    assert "판매가격:" in lines and "65만 원" in lines
    assert lines[-1].startswith("좋은 시즌 보내세요")


def test_price_comes_from_list_when_detail_has_only_hope_price(list_items):
    """사이트 상세에는 '판매가격' 라벨이 없고 '희망가'만 있다. 지금 정규화는 '판매가격'만 보므로 목록 가격이 남는다."""
    for pid, price in (("9901017", 650_000), ("9901019", 150_000)):
        it = _detail_item(list_items, pid)
        assert "판매가격" not in it["et_vars"]
        assert it["raw_price"] == price


def test_hope_price_fills_missing_list_price(list_items):
    """사이트 라벨이 '판매가격'에서 '희망가'로 바뀌었다: 목록에 가격이 없는 거래완료 글도 상세 희망가로 채운다."""
    it = _detail_item(list_items, "9901006")
    assert it["raw_price"] == 20_000


def test_trade_status_parsed(list_items):
    """상세 '거래' 칸(판매중 / 거래완료)을 trade_status로 읽는다."""
    assert _detail_item(list_items, "9901017")["trade_status"] == "판매중"
    assert _detail_item(list_items, "9901006")["trade_status"] == "거래완료"


def test_parse_detail_meta_description_fallback(list_items):
    """본문 div가 없으면 meta description을 본문으로 쓴다."""
    html = _read("detail_9901017.html")
    html = re.sub(r'<div class="document_\d+_\d+ rhymix_content xe_content">.*?</div><!--AfterDocument',
                  "<!--AfterDocument", html, flags=re.S)
    assert "rhymix_content" not in html
    it = _item(list_items, "9901017")
    out = source.parse_detail(html, it)
    assert out["body_content"].startswith("[닥터스파크 스키중고장터] [판매] 로시뇰 HERO ELITE ST Ti 24/25")
    assert len(out["et_vars"]) == len(FULL_LABELS)


def test_parse_detail_without_body_or_meta_uses_title(list_items):
    html = _read("detail_9901019.html")
    html = re.sub(r'<div class="document_\d+_\d+ rhymix_content xe_content">.*?</div><!--AfterDocument',
                  "<!--AfterDocument", html, flags=re.S)
    html = re.sub(r'<meta content="[^"]*" name="description"/>', "", html)
    it = _item(list_items, "9901019")
    out = source.parse_detail(html, it)
    assert out["body_content"] == "[본문 요약 없음] 2324 랑게 LX 90 부츠 255mm"


def test_parse_detail_without_extravars():
    out = source.parse_detail(LIST_HTML, {"title": "목록 페이지"})
    assert out["et_vars"] == {}
    it = {"et_vars": out["et_vars"], "raw_price": 1234}
    source.normalize_item_from_et_vars(it)
    assert it["raw_price"] == 1234 and it["status_list"] == [] and it["status"] == ""
    assert it["phone"] is None and it["email"] is None and it["region"] is None


# ---------------------------------------------------------------------------------------------- fixture 위생
PHONE_LIKE = re.compile(r"(?<!\d)(?:\+82[-. ]?)?0\d{1,2}[-. )]{0,2}\d{3,4}[-. ]{0,2}\d{4}(?!\d)")
EMAIL_LIKE = re.compile(r"[\w.%+-]+@[\w-]+(?:\.[\w-]+)+")
# 글·댓글 번호가 들어가는 자리: href, document_/comment_ 클래스·id, Before/After 주석
POST_NO_SLOT = re.compile(r'href="([^"]*)"|(?:document|comment)_(\d+)|(?:Before|After)(?:Document|Comment)\((\d+)')
UPLOAD_NAME = re.compile(r"(?<!\d)\d{10,13}\.(?:jpe?g|png|gif|webp)", re.I)  # 휴대폰 사진의 업로드 시각 파일명
DONG_LIKE = re.compile(r"[가-힣]+[시군구]\s*,?\s*[가-힣]{1,5}[동읍면리](?![가-힣])")  # '○○구 ○○동' 같은 동네 단위 주소
DATE_LIKE = re.compile(r"(?<![\d.])\d{2}\.\d{2}\.\d{2}(?![\d.])")


def _load_fixture_builder():
    spec = importlib.util.spec_from_file_location("make_fixtures", FIX / "make_fixtures.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("path", sorted(FIX.glob("*.html")), ids=lambda p: p.name)
def test_fixtures_contain_only_placeholder_contacts(path):
    """fixture에 실제 글·연락처·닉네임이 들어오지 않았는지(실제 페이지를 그대로 넣는 실수 방지).
    fixture는 make_fixtures.py가 만든 합성 페이지 그대로여야 한다."""
    text = path.read_text(encoding="utf-8")
    builder = _load_fixture_builder()
    assert text == builder.render_all().get(path.name), \
        f"{path.name}: make_fixtures.py로 만든 합성 fixture가 아니다(python -I tests/fixtures/make_fixtures.py로 다시 만든다)"
    assert set(PHONE_LIKE.findall(text)) <= {"010-0000-0000"}
    assert set(EMAIL_LIKE.findall(text)) <= {"user@example.com"}
    assert "open.kakao.com/o/" not in text or "open.kakao.com/o/XXXXXXXX" in text
    assert "csrf-token" not in text and "<script" not in text
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(text, "html.parser")
    for el in soup.select(".item__author span, .document-meta a.author"):
        assert re.fullmatch(r"판매자[A-Z]+", el.get_text(strip=True)), el
    for el in soup.select(".comment-list a[class^=member_]"):
        assert re.fullmatch(r"(판매자|댓글러)[A-Z]+", el.get_text(strip=True)), el
    for srl in re.findall(r"member_(\d+)", text):
        assert int(srl) >= 1_000_001  # 가명 회원번호
    assert re.findall(r"가입:\s*[\d.]+", text) in ([], ["가입: 20.01.01"])
    assert path.stat().st_size < 300 * 1024

    # 실제 글의 흔적: 글·댓글 번호(가짜는 99로 시작), 사이트 이미지 서버·첨부 파일명·업로드 시각, 동네 단위 지역, 작성일
    for href, doc_no, marker_no in POST_NO_SLOT.findall(text):
        for n in re.findall(r"\d{5,}", href) + [x for x in (doc_no, marker_no) if x]:
            assert n.startswith("99"), (path.name, n)
    assert "img.drspark.net" not in text
    for el in soup.find_all(src=True):
        assert el["src"].startswith(builder.IMG + "/"), el["src"]
    assert not UPLOAD_NAME.search(text)
    assert not re.search(r"\?t=\d{2,}", text)  # 썸네일 업로드 시각 꼬리표
    assert not DONG_LIKE.search(text), DONG_LIKE.search(text).group(0)
    region = source.parse_detail(text, {"title": ""})["et_vars"].get("거래지역")
    assert region is None or re.fullmatch(r"[가-힣]+ [가-힣]+[시군구]", region), region
    assert set(DATE_LIKE.findall(text)) <= {builder.JOIN_DATE, builder.POST_DATE}
