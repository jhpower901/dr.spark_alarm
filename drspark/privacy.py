"""개인정보 처리: 작성자 HMAC 해시, 본문 속 연락처·URL 마스킹.

phone/email 컬럼은 적재 단계에서 버린다. author는 재게시(같은 판매자의 같은 매물) 묶음에만 쓰도록
HMAC-SHA256으로 바꾼다. 키는 named volume(/scratch/secret)에만 있고 산출물 폴더에는 없다.
"""
from __future__ import annotations

import hashlib
import hmac
import re
import secrets

from . import config

_PHONE = re.compile(
    r"(?<!\d)(?:\+?82[\s\-.]?)?0?1[016789][\s\-.)]*\d{3,4}[\s\-.]*\d{4}(?!\d)"  # 휴대전화(공백·하이픈·점 변형)
    r"|(?<!\d)0\d{1,2}[\s\-.)]+\d{3,4}[\s\-.]+\d{4}(?!\d)"  # 지역번호
)
# 한글 숫자를 섞어 쓴 번호(010-12삼4-5678, 공일공 등)
_PHONE_OBF = re.compile(
    r"(?<!\d)(?:0|공|영)\s*(?:1|일)\s*(?:[016789]|공|영|일|육|륙|칠|팔|구)[\s\-.)]*"
    r"(?:[\d공영일이삼사오육륙칠팔구][\s\-.]*){7,8}"
)
# 영문 O/I/l·한글 숫자·띄어쓰기로 바꿔 쓴 번호(O1O-..., "O  I  O  9 ...")
_PHONE_FUZZY = re.compile(
    r"(?<![0-9A-Za-z])[0Oo공영ㅇ][\s\-.]{0,10}[1Il|일]"
    r"(?:[\s\-.]{0,10}[0-9OoIl|공영일이삼사오육륙칠팔구ㅇ]){8,9}(?![0-9A-Za-z])"
)
# 글자마다 띄어 쓴 숫자열(O로 시작, 공백 구분, 숫자 같은 글자 10~11개, 실제 숫자 6개 이상).
# 점·하이픈 구분은 날짜(24.12.20)와 겹치므로 받지 않는다
_SPACED_DIGITS = re.compile(
    r"(?<![0-9A-Za-z.])[0Oo공영ㅇ](?:\s{1,10}[0-9OoIl|공영일이삼사오육륙칠팔구ㅇ]){9,10}(?![0-9A-Za-z])"
)


def _mask_fuzzy(m: re.Match) -> str:
    # 숫자만 있는 경우는 _PHONE이 맡는다(날짜 '01.15 25.10.20' 같은 오탐 방지). 문자 치환이 있을 때만 가린다.
    return "[PHONE]" if re.search(r"[OoIl|ㅇ공영일이삼사오육륙칠팔구]", m.group(0)) else m.group(0)


def _mask_spaced(m: re.Match) -> str:
    return "[PHONE]" if len(re.findall(r"[0-9공영일이삼사오육륙칠팔구]", m.group(0))) >= 6 else m.group(0)
# 앞 경계가 없으면 긴 단어 덩어리에서 시작 위치마다 다시 훑어 O(n²)가 된다(긴 본문 한 건이 알림을 멈춤)
_EMAIL = re.compile(r"(?<![\w.+\-])[\w.+\-]+@[\w\-]+(?:\.[\w\-]+)+")
_URL = re.compile(r"(?:https?://|www\.)\S+", re.I)
# 카톡/오픈채팅/아이디 안내 뒤의 식별자
# 'id'는 IDONE·Sidewall 같은 단어 안에서 걸리지 않도록 앞뒤 경계와 콜론을 요구한다
_MESSENGER = re.compile(
    r"(카톡|카카오톡|오픈채팅|오픈톡|아이디)\s*[:：]?\s*[A-Za-z0-9_.\-]{4,}"
    r"|(?<![A-Za-z])(kakao(?:talk)?|id)\s*[:：]\s*[A-Za-z0-9_.\-]{3,}",
    re.I,
)


def _key() -> bytes:
    config.SECRET_KEY.parent.mkdir(parents=True, exist_ok=True)
    if not config.SECRET_KEY.exists():
        config.SECRET_KEY.write_bytes(secrets.token_bytes(32))
    return config.SECRET_KEY.read_bytes()


def hash_author(author: str | None, key: bytes | None = None) -> str | None:
    if author is None or author == "":
        return None
    k = key or _key()
    return hmac.new(k, author.encode("utf-8"), hashlib.sha256).hexdigest()[:12]


def scrub_text(text: str | None) -> str | None:
    if text is None:
        return None
    t = _URL.sub("[URL]", text)
    t = _EMAIL.sub("[EMAIL]", t)
    t = _MESSENGER.sub(lambda m: (m.group(1) or m.group(2)) + " [ID]", t)
    t = _PHONE.sub("[PHONE]", t)
    t = _PHONE_OBF.sub("[PHONE]", t)
    t = _PHONE_FUZZY.sub(_mask_fuzzy, t)
    t = _SPACED_DIGITS.sub(_mask_spaced, t)
    return t
