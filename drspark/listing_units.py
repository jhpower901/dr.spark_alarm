"""매물 단위 규칙: 글 유형, 묶음·상태·거래 조건 플래그, 재게시 매물(item) 묶기.

- classify_listing(): sale / wanted / non_sale (+ 세부 사유)
- bundle_flags(): 여러 물건 일괄·각각 가격·바인딩 포함·상하의 세트 등
- condition_flags(): 새상품 주장, 시승·렌탈 매물, 데모 언급, 하자, 네고, 배송
- group_items(): 같은 판매자(author_hash)의 재게시 글을 하나의 매물(item_id)로 묶는다
- item_history(): 매물별 가격 궤적 요약

모든 판정은 정규식 규칙이다. 규칙이 놓치거나 잘못 거는 경우는 phase1/sections/04_listing_units.md에
검수 결과와 함께 적는다. 이 모듈은 본문 원문을 저장하지 않고 불리언·짧은 키워드만 돌려준다.
"""
from __future__ import annotations

import math
import re
import statistics
import unicodedata
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# 공통 정규화
# ---------------------------------------------------------------------------
_ZW = re.compile("[\u200b-\u200f\u2060\ufeff]")
_WS = re.compile(r"\s+")


def norm(s: object) -> str:
    """NFKC, 소문자, 제로폭 문자 제거, 공백 정리."""
    if s is None:
        return ""
    if isinstance(s, float) and math.isnan(s):
        return ""
    t = unicodedata.normalize("NFKC", str(s))
    t = _ZW.sub("", t).replace("\xa0", " ")
    return _WS.sub(" ", t.lower()).strip()


_JUNK_VALUES = {
    "none", "null", "nan", "협의", "협", "무관", "모름", "없음", "상관없음", "상관 없음", "x", "o",
    "-", ".", "..", "...", "?", "ㅇ", "ㄴ", "ㄴㄴ", "ㅇㅇ", "무", "미상", "기타", "사진참조", "사진 참조",
    "아래기재", "아래 기재", "확인 중", "확인중", "잘 모름", "모르겠음", "1", "0", "00", "11", "111",
}


def is_junk(s: object) -> bool:
    """판매자 입력 칸의 의미 없는 값('.', 'ㅇ', '협의', '1' 등)."""
    t = norm(s)
    if not t or t in _JUNK_VALUES:
        return True
    if re.fullmatch(r"[\W_ㄱ-ㅎㅏ-ㅣ]+", t):
        return True
    # 'L', 'M', 'XO', '95', '255' 같은 사이즈 값은 짧아도 의미가 있다
    if _SIZE_VALUE.fullmatch(t):
        return False
    return len(t) <= 2 and not re.search(r"[a-z가-힣]{2}", t)


_SIZE_VALUE = re.compile(r"(?:xs|s|m|l|xl|xxl|2xl|3xl|o|xo|\d{2,3}(?:\.\d)?)")


def _any(pat: re.Pattern, *texts: str) -> bool:
    return any(pat.search(t) for t in texts if t)


# ---------------------------------------------------------------------------
# 1. 글 유형
# ---------------------------------------------------------------------------
# 구매 희망 표현. '구매하실 분', '구매 희망하시면', '구매원하시는'처럼 판매자가 구매자에게 하는 말은 제외한다.
_WANTED_CORE = (
    r"구\s?합니다|구해요|구합니당|구함(?![께])|구해\s?봅니다|구해\s?봐요|구해봄|구해\s?보려|구해\s?보고|구해\s?봅니당"
    r"|구하고\s?(?:있|싶)|구하는\s?중"
    r"|구매\s?(?:합니다|해요|합니당|해\s?봅니다|해\s?봐요|하고\s?싶|할게요|하겠습니다|하려고\s?합니다|맙니다|중(?:입니다|이에요)?(?![가-힣])|희망\s?합니다|희망해요|희망(?![하])|원\s?합니다|원해요|원합니당|원함|원(?![하가-힣]))"
    r"|구입\s?(?:합니다|해요|원\s?합니다|원해요|원(?![하가-힣])|희망|하고\s?싶|해\s?봅니다)"
    r"|삽니다|삽니당|(?<![가-힣])사요(?![가-힣])|사봅니다|사\s?봅니다|사고\s?싶|살게요"
    r"|(?<!판)매입(?![가-힣]?가)|파실\s?분|파시는\s?분|판매하실\s?분|팔\s?분\s?(?:계|구|찾)"
)
_WANTED = re.compile(_WANTED_CORE)
# 본문만으로 구매 글로 볼 때는 1인칭 단정형만 인정('구매를 희망하시면', '구매 희망 시 연락 주세요'는 판매자 문구)
_WANTED_BODY = re.compile(
    r"구\s?합니다|구해요|구해\s?봅니다|구해\s?봐요|구하고\s?있습니다|구매\s?(?:합니다|해요|해\s?봅니다|하고\s?싶습니다|하려고\s?합니다)"
    r"|구매\s?희망\s?합니다|구매\s?원\s?합니다|구매\s?원해요|구입\s?(?:합니다|원\s?합니다|하고\s?싶습니다)|삽니다|사\s?봅니다|매입\s?합니다"
)
_WANTED_TITLE_EXTRA = re.compile(
    r"구매\s*[.!~)\]]*\s*$|^\s*[\[(]+\s*(?:구매|삽니다|구함|구해요)\s*[\])]+|\(\s*구매\s*\)|찾습니다|찾아요|찾고\s?있"
)
# 판매 문맥에서 구매 표현이 나오는 경우(오탐 방지)
_WANTED_FALSE = re.compile(
    r"주인\s?(?:분\s?)?찾|임자\s?찾|못\s?구합니다|구하기\s?(?:쉽지|힘들|어려)|구하기가|구하실|구매하실|구매\s?희망\s?하시|구매\s?원하시|구입\s?원하시"
)
_SALE_TITLE = re.compile(
    r"판매|팝니다|팝니당|팔아요|팔아봅니다|팔아\s?봅니다|팔께요|팔게요|처분|정리합니다|정리해요|급처|급매|양도|넘깁니다|보냅니다|내놓|떨이"
)
_SALE_BODY = re.compile(r"판매|팝니다|팔아요|팔아\s?봅니다|처분|정리합니다|급처|급매|양도합니다|넘깁니다|보냅니다")

_BLOCKED = re.compile(r"블럭\s?처리|블록\s?처리|임시로\s?블|게시물은\s?신고")
_SCAM = re.compile(
    r"사기\s?(?:꾼|조심|주의|피해|신고|연락처|번호|계정|글|당|치|친|범|행각|의심|예방)|사기를|거래\s?사기|사칭|먹튀|잠수\s?탔"
)
_COMPLAINT = re.compile(r"신고\s?(?:하신|했|합니다|철회|하지)|신고하신")
_NOTICE = re.compile(r"공지|운영자|운영진|글쓰기\s?테스트|장터\s?테스트|테스트\s?(?:글|게시)|게시판\s?(?:이용|규칙)|이용\s?규칙")
_EXCHANGE = re.compile(r"교환(?!\s?렌즈|용\s?렌즈)|맞교환|바꾸실|바꿔요|바꿀\s?분|트레이드|(?<![a-z])trade(?![a-z])")
_EXCH_OPTION = re.compile(r"교환\s?(?:도\s?)?(?:가능|ok|o(?![a-z]))|교환\s?(?:or|또는|/)|(?:or|또는|/)\s?교환")
_SALE_IN_BODY_FOR_EXCH = re.compile(r"판매\s?(?:도|합니다|해요|가능|or|또는|하거나)|팝니다|판매\s?혹은")
_EXCHANGE_BODY = re.compile(r"교환\s?(?:원|희망|하실|해요|합니다|하고\s?싶|원합니다)|맞교환")
_GIVEAWAY = re.compile(r"나눔|무료\s?(?:로\s?)?(?:드립|드려|증정|양도|가져)|공짜|무상\s?(?:으로\s?)?(?:드립|양도)")
_QUESTION = re.compile(
    r"질문|궁금합니다|궁금해요|문의\s?드립니다|여쭤|알려\s?주세요|추천\s?(?:부탁|해\s?주|좀)|어디서|어떤가요|어떨까요|어떻게\s?(?:하나요|해야)|인가요|되나요|맞나요|할까요"
)
# 시즌권·리프트권·회원권 등 장비가 아닌 거래(판매 글이지만 장비 적정가 대상 아님)
_PASS_TICKET = re.compile(
    r"시즌\s?권|리프트\s?권|회원\s?권|이용\s?권|숙박\s?권|상품\s?권|바우처|voucher|락커|라커|프렌즈\s?(?:추가|권)|리조트\s?회원|객실\s?(?:투숙|이용)"
)


@dataclass
class ListingType:
    listing_type: str  # sale / wanted / non_sale
    reason: str  # 판정 근거 코드


def classify_listing(title: object, body: object = None, price: object = None) -> ListingType:
    """글 유형 판정.

    순서: 블럭 처리 → 사기 신고/항의 → 공지·테스트 → 구매 희망 → 교환 전용 → 나눔 → 질문 → 판매.
    구매 희망은 제목의 표현을 우선으로 보고, 제목이 중립적일 때만 본문 앞부분을 본다.
    """
    t = norm(title)
    b = norm(body)
    head = b[:220]
    sale_t = bool(_SALE_TITLE.search(t))

    if _BLOCKED.search(t):
        return ListingType("non_sale", "blocked")
    if _SCAM.search(t) or (_COMPLAINT.search(t) and not sale_t):
        return ListingType("non_sale", "scam_report")
    if not sale_t and b and _SCAM.search(b) and not _SALE_BODY.search(b) and not _WANTED.search(t) \
            and not price_is_valid(price):
        # 제목은 중립적이고 본문이 사기 고발이며 가격이 자리채움 값인 글
        # (정상 가격의 판매 글이 '사기꾼 연락 사절'을 적은 경우는 판매로 둔다)
        return ListingType("non_sale", "scam_report")
    if _NOTICE.search(t) and not sale_t:
        return ListingType("non_sale", "notice")

    w_title = bool(_WANTED.search(t) or _WANTED_TITLE_EXTRA.search(t)) and not _WANTED_FALSE.search(t)
    if w_title and not sale_t:
        return ListingType("wanted", "wanted_title")
    if w_title and sale_t:
        # 같은 제목에 두 표현: 앞에 나온 쪽을 따른다('A 팝니다 / B 구합니다'는 드묾)
        mw = (_WANTED.search(t) or _WANTED_TITLE_EXTRA.search(t)).start()
        ms = _SALE_TITLE.search(t).start()
        if mw < ms:
            return ListingType("wanted", "wanted_title_mixed")

    if _EXCHANGE.search(t) and not sale_t and not _EXCH_OPTION.search(t) and not _SALE_IN_BODY_FOR_EXCH.search(b):
        return ListingType("non_sale", "exchange_only")
    if _GIVEAWAY.search(t) and not sale_t and not re.search(r"무료\s?배송|무료\s?택배|택배비\s?무료", t):
        return ListingType("non_sale", "giveaway")

    if not sale_t and not w_title:
        # 제목이 중립적인 글: 본문 앞부분에 구매 희망 표현이 있고 판매 표현이 없으면 구매 글
        if head and _WANTED_BODY.search(head) and not _WANTED_FALSE.search(head) and not _SALE_BODY.search(b):
            return ListingType("wanted", "wanted_body")
        if b and _EXCHANGE_BODY.search(b) and not _SALE_BODY.search(b):
            return ListingType("non_sale", "exchange_only")
        if _QUESTION.search(t) and not _SALE_BODY.search(b):
            return ListingType("non_sale", "question")
    return ListingType("sale", "sale")


def is_pass_ticket(title: object, model_serial: object = None) -> bool:
    """시즌권·리프트권·회원권·숙박권 등 장비가 아닌 거래."""
    return _any(_PASS_TICKET, norm(title), norm(model_serial))


# ---------------------------------------------------------------------------
# 2. 품목 카테고리 단서(묶음 판정용, 최종 카테고리는 taxonomy/LLM 라벨이 맡는다)
# ---------------------------------------------------------------------------
# 순서가 중요하다: 합성어(스키복, 부츠백, 부츠 건조기)를 먼저 지운 뒤 일반어(스키, 부츠)를 본다.
_CAT_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("other", re.compile(r"(?:부츠|장갑|신발)\s?(?:건조기|드라이어)|건조기|드라이어|발열\s?(?:깔창|양말)|깔창|배터리|c-?pack|세나|sena|카메라|캐리어\s?가로바|가로바|루프\s?박스")),
    # '모글 바지', 'GS 레이싱 슈트', '아톰 SL 후디'의 모글·GS·SL은 스키 단서가 아니다(의류 한 벌)
    ("apparel", re.compile(
        r"모글\s?(?:바지|팬츠|복|쉘|자켓|재킷|웨어|슈트|수트|pants)|(?<![a-z])(?:gs|sl|sg|dh)\s?(?:레이싱\s?|race\s?)?(?:슈트|수트|suit)|"
        r"아톰\s?(?:sl|lt|ar)|(?<![a-z])atom\s?(?:sl|lt|ar)(?![a-z])|"
        r"스키\s?복|보드\s?복|팀\s?복|데모\s?(?:복|웨어|wear)|코치\s?(?:복|바지|자켓|베스트)|레이싱\s?(?:슈트|수트)|경기복|"
        r"스키\s?(?:바지|자켓|재킷|팬츠|양말|웨어)|자켓|재킷|쟈켓|jacket|jkt|아노락|anorak|팬츠|pants?(?![a-z])|바지|빕\s?팬츠|bibs?(?![a-z])|"
        r"상의|하의|상하의?|미들러|미드\s?레이어|미들\s?웨어|이너\s?웨어|베이스\s?레이어|플리스|fleece|조끼|(?<![가-힣])베스트(?![가-힣])|vest|"
        r"구스\s?다운|덕\s?다운|패딩|셔츠|후드|비니|넥\s?워머|바라클라바|슈트(?![가-힣])|suit(?![a-z])|멜빵|서스펜더|레깅스|양말"
    )),
    ("bag", re.compile(r"(?:스키|부츠|휠|보드|헬멧)\s?(?:가방|백)(?![a-z가-힣])|(?<![a-z])(?:boots?|ski)\s?bag|가방|백팩|휠\s?백|bag(?![a-z])|backpack|보조\s?가방|스포튜브|sportube")),
    # '바인딩 부츠사이즈 275'는 바인딩을 맞춘 부츠 크기(부츠 단서 아님). '바운딩'은 바인딩 오타
    ("binding", re.compile(
        r"(?:바인딩|바운딩|binding)\s?(?:세팅\s?|셋팅\s?)?(?:부츠|boots?)\s?(?:사이즈|size|솔\s?길이)?\s?\d{3}(?:\s?mm)?|"
        r"바인딩|바운딩|binding|레이스\s?플레이트|race\s?plate|인터페이스\s?플레이트|"
        r"(?<![a-z])(?:spx|xcell|x-cell|comp\s?1[0-9](?!\d)|icon\s?(?:rs\s?)?1[0-9](?!\d)|griffon|그리폰|pivot|피벗|fm\s?585|sr\s?585|"
        r"attack|어택|prd\s?1[0-9](?!\d)|wrt\s?1[0-9](?!\d)|rs\s?1[0-9](?![\d.]|\s?(?:cm|m))|xpress|nx\s?1[0-9](?!\d)|z\s?1[0-9](?!\d)|"
        r"[xi]\s?1[0-9]\s?gw|ff\s?1[0-9](?!\d)|wc\s?piston|piston)(?![a-z])|피스톤"
    )),
    # '아웃쉘 315'는 스키 바인딩 세팅값이므로 부츠 단서로 쓰지 않는다. '모글 부츠'는 부츠 한 켤레
    ("boots", re.compile(r"(?:스키\s?|모글\s?)?부츠(?!\s?(?:백|가방|스트랩|밴드|커버|히터))|(?<![a-z])boots?(?![a-z])")),
    # 고글 일체형·바이저 헬멧은 헬멧 한 개('스키 바이저 헬멧 모델명(변색 고글)'의 고글은 바이저).
    # 사이의 모델명에는 나열 기호·'고글'이 없어야 한다('바이저 헬멧, 스미스 고글(변색 렌즈)'는 두 물건)
    ("helmet", re.compile(
        r"(?:(?:스키|레이싱|보드)\s?)*바이저\s?(?:헬멧|핼멧|helmet)(?:(?!고글|goggle)[^()+,/&]){0,20}\(\s?(?:변색\s?)?(?:고글|렌즈)\s?\)|"
        r"(?:고글\s?)?일체형\s?(?:헬멧|핼멧)|(?:스키\s?|레이싱\s?|보드\s?|스키\s?보드\s?)?(?:바이저\s?)?(?:헬멧|핼멧|헐멧|헬맷|핼맷|helmet)|visor"
    )),
    ("goggle", re.compile(r"(?:스키\s?)?(?:고글|goggle)|포디\s?맥|4d\s?mag|아이오\s?맥|i/?o\s?mag|스쿼드\s?맥|squad\s?mag")),
    ("gloves", re.compile(r"(?:스키\s?)?(?:장갑|글러브|glove|미튼|mitt)")),
    ("protector", re.compile(r"(?:스키\s?)?(?:보호대|보호\s?테이프|신\s?가드|씬\s?가드|암\s?가드|친\s?가드|프로텍터|protector)|폴\s?가드")),
    ("poles", re.compile(r"모글\s?(?:폴|pole)|폴\s?대|스키\s?폴|길이\s?조절\s?(?:카본\s?)?폴|카본\s?폴|고정식\s?(?:카본\s?)?폴|레이싱\s?폴|(?:대)?회전\s?폴|(?<![가-힣a-z])폴(?![가-힣])|(?<![a-z])poles?(?![a-z])")),
    ("snowboard", re.compile(r"스노우\s?보드|스노\s?보드|snowboard|보드\s?(?:데크|덱)|(?<![가-힣])데크|(?<![a-z])deck(?![a-z])")),
    ("tuning", re.compile(r"왁스|왁싱\s?(?:다리미|브러쉬|세트|솔|도구)|다리미|스크래퍼|튜닝\s?(?:툴|용품|도구)|에지\s?(?:튜너|튜닝)|엣지\s?(?:튜너|튜닝)|바이스|브러쉬|edger|tuner")),
    # '유리 플레이트'·'리프터 플레이트'는 부츠 밑창 부품, '파이어버드'는 아래 _FIREBIRD에서 따로 본다
    ("ski", re.compile(
        r"스키\s?판|스키\s?플레이트|(?<!레이스\s)(?<!race\s)(?<!유리)(?<!유리\s)(?<!리프터)(?<!리프터\s)플레이트|"
        r"(?<![가-힣])(?:대)?회전(?![가-힣]?반경)|모글|"
        r"(?<![a-z])(?:sl|gs|slr|e-?sl|i-?sl|e\.sl|s9i|s8i|x9|g9|sg|race\s?tiger|racetiger|"
        r"supershape|wrc|tc-?s[ab]?|qst|mantra)(?![a-z])|만트라|레이스\s?타이거|레이스타이거|슈퍼\s?셰이프|슈퍼\s?쉐이프"
    )),
]
# 파이어버드: 블리자드는 스키, 테크니카는 레이스 부츠 라인. 부츠 단서·테크니카가 있으면 부츠로 본다
_FIREBIRD = re.compile(r"(?<![a-z])firebird(?![a-z])|파이어버드")
_TECNICA = re.compile(r"테크니카|티크니카|tecnica")
# 다른 품목과 쉼표·'+'로 나열한 일반어 '스키'('헤드 스키, 부츠, 폴', '랑게 부츠, 스키')는 스키 한 품목.
# '스키 부츠'(스키 부츠 한 켤레)와 '스키/보드 헬멧', '스키, 보드 겸용'은 여기에 걸리지 않는다
_SKI_LISTED = re.compile(
    r"스키(?:\s?판)?\s?(?:\d{2,3}\s?(?:cm|센치)?\s?)?(?:[,+&]|및)(?!\s?(?:스노우?\s?)?보드|\s?(?:등산|아웃도어|일상|여행|캠핑|골프|겸용))"
    r"|(?:[,+&]|및)\s?스키(?:\s?판)?(?=\s?(?:$|[,+&(]|판매|팝니다|팔아요|일괄|세트|처분))"
)
# 다른 품목 단서를 지운 뒤 남은 '브랜드+스키'('로시뇰스키 랑게부츠', '헤드스키 테크니카부츠')도 스키 한 품목
# ('로시뇰 스키 부츠', '아토믹 스키 헬멧'의 '스키'는 부츠·헬멧 패턴이 먼저 지운다)
_BRAND_SKI = re.compile(
    r"(?:아토믹|로시뇰|로시놀|피셔|헤드|노르디카|뵐클|볼클|살로몬|블리자드|오가사카|다이나스타|엘란|스토클리|케슬레|캐슬|어그먼트|카브)"
    r"\s?스키(?![가-힣])"
)
# '스키', '보드' 같은 일반어는 다른 주요 품목 단서가 없을 때만 스키·보드로 본다('스키 보드 헬멧', '스키 팀복')
_GENERIC_SKI = re.compile(
    r"스키(?!\s?(?:장|샵|세트|용품|장비|어|강사|타|를|정비|튜닝|왁싱|보호|캐리어|건조|케이스|커버|스트랩|테이프|락커|라커|강습|레슨|회원|시즌|리프트|모자|와펜|휠))"
    r"|(?<![a-z])ski(?![a-z])"
)
_GENERIC_BOARD = re.compile(r"(?<![가-힣])보드(?!\s?복)")
_SKI_CONTEXT = re.compile(r"월드컵|반드컵|(?<![a-z])(?:fis|wc|worldcup)(?![a-z])|데모|올라운드|카빙|레이싱")
_SKI_LEN = re.compile(r"(?<![\d.])1[2-9]\d(?![\d.])(?!\s?(?:mm|플렉스|flex))")
_MAJOR = {"ski", "boots", "helmet", "goggle", "apparel", "snowboard"}
_BINDING_PAT = dict(_CAT_PATTERNS)["binding"]
_BINDING_NEG = re.compile(r"^\s?(?:은|는)?\s?(?:미포함|없|제외|별도|x(?![a-z])|불포함|빼고)")
_BINDING_EXCL = re.compile(r"바인딩\s?(?:은|는)?\s?(?:x(?![a-z])|없|미포함|제외|별도|불포함|빼고)|플레이트만|판만|스키만|no\s?binding|flat")


def category_hints(text: object) -> list[str]:
    """제목·모델명에서 보이는 품목 단서(중복 제거, 등장 순서)."""
    s = s0 = norm(text)
    tecnica = bool(_TECNICA.search(s))
    found: list[str] = []
    for cat, pat in _CAT_PATTERNS:
        if pat.search(s):
            if cat not in found:
                found.append(cat)
            s = pat.sub(" ", s)
    if re.search(r"(?<![가-힣])헬고(?![가-힣])", s0):  # '헬고' = 헬멧 + 고글
        found += [c for c in ("helmet", "goggle") if c not in found]
    if _FIREBIRD.search(s):
        fb = "boots" if ("boots" in found or tecnica) else "ski"
        if fb not in found:
            found.append(fb)
        s = _FIREBIRD.sub(" ", s)
    if found and "ski" not in found and (_SKI_LISTED.search(s0) or _BRAND_SKI.search(s)):
        found.append("ski")
    if not any(c in _MAJOR for c in found) and _SKI_CONTEXT.search(s) and _SKI_LEN.search(s):
        # '반디어 월드컵 165 + 룩18바인딩'처럼 스키라는 말 없이 월드컵·FIS·데모 + 길이만 적은 스키
        found.append("ski")
    if not found:
        # 다른 품목 단서가 전혀 없을 때만 '스키'·'보드'를 스키·보드로 본다('스키 폴', 'Ski Glove'는 폴·장갑)
        if _GENERIC_SKI.search(s):
            found.append("ski")
        elif _GENERIC_BOARD.search(s):
            found.append("snowboard")
    return found


# ---------------------------------------------------------------------------
# 3. 묶음(여러 물건) 판정
# ---------------------------------------------------------------------------
_ILGWAL = re.compile(r"일괄|통째|한꺼번에|몽땅|전부\s?(?:해서|같이|한번에)")
# '다른 제품 일괄 구매 시 네고', '같이 판매 중인 헬멧과 일괄 구매 추천'은 이 글의 묶음이 아니다(판매자의 다른 글)
_ILGWAL_OTHER = re.compile(
    r"다른\s?(?:제품|물건|상품|장비|판매\s?글|글)|제\s?(?:다른|판매)|(?:같이|함께|따로|별도로?)\s?(?:판매\s?(?:중|하는|하고)|올린|올려)"
    r"|판매\s?중인|올린\s?(?:다른\s?)?(?:제품|물건|상품|장비|글)"
)
# 본문의 '일괄 구매하시면 네고해 드려요', '일괄로도 거래합니다'는 금액 없는 제안이다. 단품 글에도 흔해(판매자의 다른 글과 묶음)
# 이것만으로는 묶음으로 보지 않는다. 진짜 여러 물건이면 품목·수량·가격 나열이 함께 잡힌다
_ILGWAL_OFFER_AFTER = re.compile(
    r"^\s?(?:로|으로)?\s?(?:구매|구입|거래|판매|가져가)?\s?(?:"
    r"(?:시|하시면|하면|하실\s?경우|할\s?경우)\s?(?:추가\s?)?(?:할인|네고|에누리|dc|디씨|가격\s?(?:조정|협의)|협의)"
    r"|도\s?(?:가능|합니다|해요|ok|환영)|(?:추천|문의|환영))"
    # '자켓, 장갑도 일괄로도 거래 가능합니다'
    r"|^\s?(?:로|으로)?\s?도\s?(?:구매|구입|거래|판매)\s?(?:가능|합니다|해요|됩니다|ok|환영|할게요|할께요|해\s?드)"
)
_ILGWAL_PRICE_AFTER = re.compile(r"^[^\d]{0,12}\d")  # '일괄: 58만원'처럼 금액이 바로 붙은 '일괄'
_MULTI_QTY = re.compile(
    r"(?<![\d.,/])(?:[2-9]|두|세|네|다섯|여러)\s?(?:대|벌|세트|셋트|set|쌍|켤레|종|자루|족)(?![가-힣])"
    r"|(?<![\d.,/])(?:[2-9]|두|세|네)\s?개(?!월|\s?(?:렌즈|월))"
)
_MULTI_QTY_FALSE = re.compile(r"(?:렌즈|배터리|디스크|스트랩|바스켓)\s?$")
_MULTI_QTY_FALSE_AFTER = re.compile(r"^\s?(?:까지\s?)?(?:수납|들어|넣|보관\s?가능)")  # '스키백(2대 보관 가능)'
_BOTH = re.compile(r"(?<![가-힣])둘\s?다(?![가-힣])")  # 제목 '(둘 다 XXL)'
# 본문은 오탐이 많아 '총 두 벌', '총 3대'처럼 '총'을 붙인 수량만 본다('렌즈 주야간 총 2개', '총 두 세트 보유 중 한 세트'는 아님)
_MULTI_QTY_BODY = re.compile(r"총\s?(?:[2-9]|두|세|네|다섯)\s?(?:벌|대|켤레|세트|셋트|개|종|자루)(?![가-힣])(?!\s?(?:보유|가지고|중\s?(?:한|1)))")
_MULTI_QTY_BODY_FALSE = re.compile(r"(?:렌즈|배터리|디스크|스트랩|바스켓)[^\d]{0,6}$")
# 의류 제목의 사이즈 나열('S , L', 'S사이즈 XL사이즈', 'XL1개 L1개')은 여러 벌
_SIZE_TOK = r"(?:xxl|xl|xs|2xl|3xl|xo|s|m|l|o)"
_MULTI_SIZE = re.compile(
    rf"(?<![a-z0-9]){_SIZE_TOK}\s?,\s?{_SIZE_TOK}(?![a-z0-9])"
    rf"|(?<![a-z0-9]){_SIZE_TOK}\s?(?:사이즈|1\s?(?:개|벌))\s?,?\s?{_SIZE_TOK}\s?(?:사이즈|1\s?(?:개|벌))"
)
# 회전(SL)과 대회전(GS)을 함께 적은 스키 제목('헤드 회전 / 대회전'은 두 대). '올라운드, 대회전'은 스키 한 대의 성격 설명이 많아 쓰지 않는다
_DISC_SL = re.compile(r"(?<![가-힣])회전(?!\s?반경)|(?<![a-z])(?:sl|slr|e-?sl|i-?sl|e\.sl)(?![a-z])")
_DISC_GS = re.compile(r"대회전|(?<![a-z])(?:gs|e-?gs|i-?gs)(?![a-z])")
_DISC_ONE = re.compile(r"겸용|공용|사이|중간|모두\s?(?:가능|좋)|다\s?(?:되는|가능)")
# '각 10만', '각 60', '각 1벌씩'은 개당 가격·수량('엣지·왁싱 각 1회씩', '왁싱 횟수 각 3,5회', '엣지각 87도'는 아님).
# '고글 단품 / 장갑 단품 각각 올려 두었습니다'은 판매자의 다른 글 안내라 개당 가격이 아니다
_PER_UNIT = re.compile(
    r"각각(?!\s?의|\s?(?:다른|장단|특징|매력|차이|성격|느낌)"
    r"|\s?(?:따로\s?)?(?:판매\s?(?:중|하고\s?있)|올려|올렸|올린|올림|글|게시|등록))|개당|세트당|세트\s?당|대당|장당|켤레당|벌당|한\s?(?:세트|대|개)\s?당|"
    r"(?<![가-힣])각\s?\d+(?:[.,]\d+)*(?![\d.,])(?!\s?(?:회|번|도|시즌|일|개월|년|mm|cm|m(?![a-z가-힣])|kg|g(?![a-z])|%|/))"
    r"|(?<![가-힣])각\s?(?:1|한)\s?(?:개|벌|대|켤레)"
)
_SEPARATE = re.compile(
    r"(?:개별|따로|낱개|분리)\s?(?:로\s?)?(?:구매|판매|구입)\s?(?:가능|도\s?가능|시|하실|원하|문의|하셔도|하시면)"
    r"|(?:개별|따로|낱개|분리)\s?(?:로\s?)?(?:판매|구매)도|(?:개별|따로)\s?(?:가능|판매합니다)"
)
_PARTIAL = re.compile(r"한\s?짝|한\s?쪽|한쪽만|짝\s?(?:만|하나)|1\s?짝|한\s?개만\s?(?:판매|팝)")
# 덤으로 주는 소품: 소품 이름 뒤에 '드림/증정/같이 드려요'가 붙은 경우만('사진 드려요'·'왁싱 해드립니다'는 제외)
_EXTRAS = re.compile(
    r"(?:가방|백|폴대|폴|장갑|헬멧|고글|렌즈|미들러|케이스|스트랩|커버|왁스|스키백|부츠백|휠백|바인딩)\S{0,3}\s?(?:도\s?)?"
    r"(?:드림|드려요|드립니다|드릴게요|드릴께요|증정|같이\s?(?:드|나갑|가져)|서비스|덤|포함)|사은품|(?<![가-힣])덤으로"
)
# 폴·튜닝·소품 위주 브랜드는 '스키 + 폴 드림'처럼 덤으로 붙는 경우가 많아 다중 브랜드 묶음 판정에서 뺀다
_ACCESSORY_BRANDS = {"kizaki", "leki", "komperdell", "shinano", "gabel", "oneway", "swix", "toko", "thermic", "hestra", "chums"}
_COMPAT_WORDS = re.compile(r"호환|겸용|사용\s?가능|(?<!안)(?<!안\s)맞는|용\s?(?:으로|입니다)|(?:멜빵|서스펜더)\s?\(|등\s?(?:구매|구합)|콜라보|collab|협업")
# 멜빵만 파는 제목의 브랜드 나열('A사 B사 스키 멜빵')은 맞는 바지 브랜드 목록이다
_SUSPENDER = re.compile(r"멜빵|서스펜더|(?<![a-z])suspenders?(?![a-z])")
# 제목·모델에서 물건을 나열하는 기호·표현('스키 + 부츠', '헬멧, 고글', 'A / B', '일괄', '세트')
_ITEM_SEP = re.compile(r"[+,&]|\s/|/\s|(?<![가-힣])및(?![가-힣])|일괄|세트|셋트|(?<![a-z])set(?![a-z])")
_APPAREL_SET = re.compile(r"상하의?|세트|셋트|set(?![a-z])|위\s?아래|투피스|자켓\s?\+|\+\s?(?:팬츠|바지|빕)|(?:자켓|jacket).{0,20}(?:팬츠|바지|빕|pants|bib)")
# 제목은 '스키복'뿐이고 본문에 '상하의 일괄 판매', '상의 하의 세트'라고 적은 한 벌
_APPAREL_SET_BODY = re.compile(
    r"상\s?하의?\s?(?:세트|일괄|같이|함께|한\s?벌|모두)|상의\s?(?:와|랑|,|/|및|\+)?\s?하의\s?(?:세트|일괄|같이|함께|모두)"
    r"|위\s?아래\s?(?:세트|일괄|한\s?벌)"
)

# 본문 가격 추출: 숫자 + 단위(만/만원/원/천원)
_AMOUNT = re.compile(
    r"(?<![\d.,a-z])(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s?(만\s?(?:\d\s?천\s?)?(?:원)?|천\s?원|원)(?!\s?(?:mm|m(?![a-z가-힣])|g(?![a-z])|km|명|회|번|키로|대|시간|일|년|개월))"
)
# 기준가(정가·구매가·매장가·이전 호가 등)·배송비·묶음 총액 문맥
_AMT_REF_BEFORE = re.compile(
    r"(?:정가|소비자\s?가|신품\s?가|새\s?(?:상품|제품)\s?(?:가|가격)|출고\s?가|정상\s?가|원가|구매\s?가|구입\s?가|원래|공홈|매장|신형|양판형|시세|"
    r"예치금|보증금|구매\s?당시|당시|인터넷|최저가|할인\s?가|판매처|국내|직구|발매가|리테일|retail|msrp|"
    r"스토어|쇼핑몰|백화점|아울렛|공식\s?(?:몰|홈)|시중|해외|새로\s?(?:사|구매|구입)|신품으로|새\s?(?:거|것)\s?(?:가격|가|는)|"
    r"기존|이전|처음|종전|(?:인하|변경|수정|조정)\s?전|올렸던|올린\s?가격|"
    r"수선비|기름값|선입금|노쇼|추가금|추금|최대|이하|이내|미만|부터|가격대는?|예산|생각\s?(?:중|합니다)|"
    r"튜닝비|피팅비|열성형|부스터|유리\s?플레이트|공임|정비비)[^\d]{0,12}$"
)
_AMT_SHIP_BEFORE = re.compile(r"(?:택배비|배송비|운임|택배|배송|반값|편의점|착불|퀵|추가|\+)[^\d]{0,6}$")
_AMT_VARIANT_BEFORE = re.compile(
    r"(?:직거래|쿨거래|쿨|네고|현금|급처|택배\s?거래|택거래|직거래시|택배시|택포|무료\s?배송)[^\d]{0,6}$"
)
_AMT_VARIANT_AFTER = re.compile(
    r"^\s?(?:원)?\s?\(?\s?(?:택포|택배비?\s?포함|배송비?\s?포함|무료\s?배송|직거래\s?(?:시|가)|택배\s?(?:거래\s?)?시|네고\s?(?:시|하면))"
)
_AMT_TOTAL_BEFORE = re.compile(
    r"(?:일괄|세트로?|한번에|한꺼번에|모두|전부|둘\s?다|두\s?개|두\s?대|[2-9]\s?(?:개|대|벌|켤레|세트)\s?(?:다|모두)|같이|동시에|총|합계|합쳐서|묶어서|전체|통째)"
    r"[^\d]{0,10}$"
)
# 조건부 금액(같은 물건의 다른 조건): '(시즌에) 타게 되면 55만', '직접 오시면 3만', '한 대 가져가시면 45만', '~할 경우'
_AMT_COND_BEFORE = re.compile(
    r"(?:(?<=[가-힣])(?:면|시)(?:에|엔|는)?|경우(?:에|엔|는)?|때(?:는|엔|에)?|조건(?:으로|이면|에)?)\s?[,:~\-=>→)]*\s?$"
)
# 조건부라도 '대회전만 구매 시 70만', '폴만 따로 하시면 2만'은 물건별 가격
_AMT_ONLY_BEFORE = re.compile(r"(?:만|따로|개별|단품|낱개)(?:으로|로)?\s?(?:구매|구입|가져가|사|거래|판매|하)[^\d]{0,8}$")
_AMT_REF_AFTER = re.compile(
    r"^[^\d]{0,4}(?:에\s?(?:구매|구입|샀|주고|결제|사서|산|주문|거래된|팔리|판매되|올라|나와)|주고|짜리|정도에\s?(?:구|샀|사)|"
    r"정도\s?(?:주고|하는|합니다|해요|줬|주었|들었|했던)|정도에\s?판매|에\s?(?:판매\s?(?:되|중|하고)|올렸|올린)|할인|추가|이상\s?(?:나|주|지불|들)|지불|"
    r"으로\s?(?:올랐|인상|구매|구입)|에서\s?\d|하는\s?제품|입니다\s?\(정가|선에|이내|이하|까지|대\s?(?:로|에서)|"
    r"줬|주었|들었|들여|들인|(?:이)?었(?:는데|던|습니다만)|였(?:는데|던)|이던|했었|올렸다가|에서\s?(?:인하|내림|내렸|다운|할인)|"
    r"(?:더\s?)?(?:비싼|비쌉|비싸|저렴|차이)|수준)"
)
# 이미 팔린(예약된) 물건의 값: '헬멧 10만원 (판매완료)', '헬멧: 10만원 헬멧 판매완료'
_AMT_SOLD_AFTER = re.compile(r"^[^\d]{0,4}\(?\s?(?:판매\s?완료|거래\s?완료|판매됨|팔림|예약\s?중|sold)")
_ARROW_AFTER = re.compile(r"^\s?(?:원)?\s?(?:->|→|=>|>|에서)\s?\d")
# '250만원 230만원으로 인하' → 앞 금액은 이전 호가
_PRICE_CUT_AFTER = re.compile(
    r"^\s?(?:원)?\s?(?:으?로|에)\s?(?:다시\s?)?(?:인하|내림|내립|내려|내렸|낮춥|낮췄|낮춰|낮춤|다운|조정|변경|수정|할인)"
)
# 기준가 바로 뒤에 이어 적은 구성품별 기준가('정가 300만원 자켓 180만 팬츠 120만')
_CHAIN_GAP = re.compile(r"^[\s,/:()\-]*(?:[a-z가-힣]{1,6}[\s,/:()\-]*)?$")
_CHAIN_BREAK = re.compile(r"판매|팝니|팔아|가격|현재|지금|최종|급처|네고|할인|드려|드립|합니다|입니다")
_CHAIN_SALE_AFTER = re.compile(r"^[^\d]{0,3}(?:에\s?(?:판매|팝|드|넘|내놓|올립)|으로\s?(?:판매|드|팝))")


def _amount_value(num: str, unit: str) -> int | None:
    n = num.replace(",", "")
    try:
        v = float(n)
    except ValueError:
        return None
    u = unit.replace(" ", "")
    if u.startswith("만"):
        extra = re.search(r"(\d)천", u)
        return int(v * 10000 + (int(extra.group(1)) * 1000 if extra else 0))
    if u.startswith("천"):
        return int(v * 1000)
    return int(v)


@dataclass
class BodyPrices:
    item: list[int] = field(default_factory=list)  # 물건별 판매가로 보이는 금액
    total: list[int] = field(default_factory=list)  # 일괄·세트 총액
    variant: list[int] = field(default_factory=list)  # 직거래/택배/네고/택포 조건별 금액(같은 물건)
    cond: list[int] = field(default_factory=list)  # '~하면/~시/~경우' 조건부 금액(같은 물건)
    sold: list[int] = field(default_factory=list)  # 이미 팔린 물건의 값('10만원(판매완료)')
    arrow_drop: bool = False  # '50만 → 44만', '250만 230만으로 인하' 같은 가격 인하 표기


def _amount_kind(b: str, m: re.Match) -> str:
    """금액 하나의 문맥: arrow / sold / ref / ship / total / variant / item / cond."""
    before = b[max(0, m.start() - 16):m.start()]
    after = b[m.end():m.end() + 14]
    if _ARROW_AFTER.search(after):
        return "arrow"
    if _AMT_SOLD_AFTER.search(after):
        return "sold"
    if _AMT_REF_BEFORE.search(before) or _AMT_REF_AFTER.search(after):
        return "ref"
    if _AMT_SHIP_BEFORE.search(before):
        return "ship"
    if _AMT_TOTAL_BEFORE.search(before):
        # '다른 제품과 일괄 구매 시 100만'은 판매자의 다른 글과 묶은 값
        return "ref" if _ILGWAL_OTHER.search(b[max(0, m.start() - 30):m.start()]) else "total"
    if _AMT_VARIANT_BEFORE.search(before) or _AMT_VARIANT_AFTER.search(after):
        return "variant"
    if _AMT_ONLY_BEFORE.search(before):
        return "item"
    if _AMT_COND_BEFORE.search(before):
        return "cond"
    return "item"


def body_prices(body: object) -> BodyPrices:
    """본문에서 금액을 뽑아 문맥별로 나눈다.

    정가·신품가·매장가·구매가·이전 호가·택배비는 버리고, 판매 완료·예약된 물건 값은 `sold`에, 조건부 금액('타게 되면 60만',
    '가지러 오시면 5만')과 거래 조건별 금액(직거래·택배·네고·택포)은 같은 물건의 다른 값으로 따로 둔다.
    """
    b = norm(body)
    out = BodyPrices()
    toks: list[list] = []  # [start, end, value, kind]
    for m in _AMOUNT.finditer(b):
        v = _amount_value(m.group(1), m.group(2))
        if v is None or v < 5000 or v > 50_000_000:
            continue
        kind = _amount_kind(b, m)
        prev = toks[-1] if toks else None
        gap = b[prev[1]:m.start()] if prev else ""
        if prev and prev[3] in ("item", "cond", "variant") and len(gap) <= 12 and kind in ("item", "cond", "variant") \
                and _PRICE_CUT_AFTER.search(b[m.end():m.end() + 14]):
            # '250만원 230만원으로 인하해 봅니다': 앞 금액은 이전 호가
            prev[3] = "old"
            out.arrow_drop = True
        elif prev and prev[3] == "ref" and kind == "item" and len(gap) <= 10 and _CHAIN_GAP.search(gap) \
                and not _CHAIN_BREAK.search(gap) and not _CHAIN_SALE_AFTER.search(b[m.end():m.end() + 12]):
            kind = "ref"
        toks.append([m.start(), m.end(), v, kind])
    for _s, _e, v, kind in toks:
        if kind == "arrow":
            out.arrow_drop = True
        elif kind == "total":
            out.total.append(v)
        elif kind == "variant":
            out.variant.append(v)
        elif kind == "cond":
            out.cond.append(v)
        elif kind == "sold":
            out.sold.append(v)
        elif kind == "item":
            out.item.append(v)
    return out


@dataclass
class BundleFlags:
    is_bundle: bool
    bundle_reasons: str  # 세미콜론으로 이은 사유 코드
    multi_price: bool
    n_body_item_prices: int
    is_apparel_set: bool
    with_binding: bool
    has_extras: bool
    is_partial: bool
    categories: str  # 제목·모델 단서(콤마)


def _listed_price(price: object) -> int | None:
    return int(float(price)) if price_is_valid(price) else None


def bundle_flags(title: object, model_serial: object = None, spec: object = None, body: object = None,
                 price: object = None) -> BundleFlags:
    """여러 물건 묶음 여부와 관련 플래그.

    - 스키+바인딩은 정상 구성 → with_binding, 묶음 아님
    - 상하의 세트(의류만) → is_apparel_set, 묶음 아님
    - 스키/부츠 + 폴·가방 '드림/포함' → has_extras, 묶음 아님
    - 주요 품목 2종 이상(스키+부츠, 헬멧+고글 등), '일괄', '2대/2벌', 본문 물건별 가격 2개 이상, '각/개당' → 묶음
    - 본문의 조건부 금액('타게 되면 60만', '네고 시', '택포')·참고 금액(정가·매장가·구매가·이전 호가)은
      물건별 가격으로 세지 않는다(body_prices)
    """
    t = norm(title)
    m = "" if is_junk(model_serial) else norm(model_serial)
    if m and m == t:
        m = ""
    sp = "" if is_junk(spec) else norm(spec)
    b = norm(body)
    tm = f"{t} {m}".strip()

    cats = category_hints(tm)
    if m and len({c for c in cats if c in _MAJOR}) >= 2:
        t_major = {c for c in category_hints(t) if c in _MAJOR}
        m_major = {c for c in category_hints(m) if c in _MAJOR}
        if len(t_major) == 1 and m_major and not (t_major & m_major) and not _ITEM_SEP.search(t) and not _ITEM_SEP.search(m):
            # 제목은 한 품목인데 모델 칸은 다른 품목만 가리킨다('A사 부츠 flex 130' + 모델 칸 'A사 회전 스키 165'):
            # 나열 기호가 없으면 다른 글의 모델명을 옮겨 적은 것으로 보고 모델 칸을 버린다
            m, tm = "", t
            cats = category_hints(tm)
    majors = [c for c in cats if c in _MAJOR]
    reasons: list[str] = []

    if len(set(majors)) >= 2:
        reasons.append("multi_category")
    if set(majors) == {"ski"} and not _DISC_ONE.search(tm) and _DISC_GS.search(tm) and _DISC_SL.search(tm):
        # '아토믹 회전 / 대회전': 종목이 다른 스키 두 대
        reasons.append("multi_discipline")
    brands = {tok for tok in normalize_title(t).split() if tok in _BRAND_TOKENS} - _ACCESSORY_BRANDS
    ski_binding_pair = "binding" in cats and set(majors) == {"ski"} and len(brands) == 2  # '블랙크로우 스키 + 살로몬 바인딩'
    suspender_only = bool(_SUSPENDER.search(t)) and set(majors) <= {"apparel"} and not _ITEM_SEP.search(t)
    if len(brands) >= 2 and (majors or (len(brands) >= 3 and not cats)) and not _COMPAT_WORDS.search(t) \
            and not ski_binding_pair and not suspender_only:
        # '뵐클 GS, 피셔 CURV'처럼 브랜드가 다른 물건 둘 이상(바인딩 브랜드 제외). 품목 단서가 하나도 없어도
        # 장비 브랜드가 셋 이상이면('헤드170 피셔175 로시뇰168') 여러 물건('살로몬/아토믹/아르마다 바인딩 브레이크'는 호환 목록)
        reasons.append("multi_brand")
    ilgwal_body = [mi for mi in _ILGWAL.finditer(b) if not _ILGWAL_OTHER.search(b[max(0, mi.start() - 40):mi.start()])
                   and not _ILGWAL_OFFER_AFTER.search(b[mi.end():mi.end() + 24])]
    if _ILGWAL.search(t) or ilgwal_body:
        reasons.append("ilgwal")
    for mm in _MULTI_QTY.finditer(t):
        if not _MULTI_QTY_FALSE.search(t[max(0, mm.start() - 6):mm.start()]) \
                and not _MULTI_QTY_FALSE_AFTER.search(t[mm.end():mm.end() + 8]):
            reasons.append("multi_qty")
            break
    only_apparel = bool(cats) and set(majors) == {"apparel"}
    set_title = only_apparel and bool(_APPAREL_SET.search(tm) or _APPAREL_SET.search(sp))
    if "multi_qty" not in reasons and _BOTH.search(t):
        reasons.append("multi_qty")
    if "multi_qty" not in reasons:
        mq = _MULTI_QTY_BODY.search(b)
        if (mq and not _MULTI_QTY_BODY_FALSE.search(b[max(0, mq.start() - 10):mq.start()])) or \
                (not set(cats) - {"apparel"} and not set_title and _MULTI_SIZE.search(t)):
            # 본문 '총 두 벌', 의류 제목의 사이즈 나열('M , XL')
            reasons.append("multi_qty")
    if _PER_UNIT.search(t) or _PER_UNIT.search(b):
        reasons.append("per_unit_price")
    if _SEPARATE.search(b) and set(cats) - {"apparel"}:
        reasons.append("separate_ok")

    bp = body_prices(b)
    lp = _listed_price(price)

    def in_range(xs: list[int]) -> list[int]:
        # 호가의 0.1~1.5배 밖 금액은 대개 정가·구매가·부품값이라 물건별 가격에서 뺀다
        return [v for v in xs if 0.1 * lp <= v <= 1.5 * lp] if lp else list(xs)

    distinct_items = sorted(set(in_range(bp.item)))
    # 조건부·거래 조건별 금액은 같은 물건의 다른 값이라 물건 수로 세지 않는다. 다만 그보다 큰 일괄가가 있으면
    # ('한 대 가져가시면 45만, 두 대 모두 85만') 여러 물건이다
    priced = distinct_items or sorted(set(in_range(bp.variant + bp.cond)))
    # 다른 물건이 팔리고 한 물건만 남은 글('스키 50만 / 헬멧 10만 헬멧 판매완료 / 일괄 58만')의 일괄가와
    # 그 일괄가에 붙은 '일괄'은 지난 값이다
    sold_rest = bool(bp.sold) and len(distinct_items) <= 1
    multi_price = len(distinct_items) >= 2 or \
        (not sold_rest and bool(priced) and bool(bp.total) and max(bp.total) > max(priced))
    if multi_price:
        reasons.append("multi_price")
    if sold_rest and "ilgwal" in reasons and not _ILGWAL.search(t) \
            and all(_ILGWAL_PRICE_AFTER.search(b[mi.end():mi.end() + 14]) for mi in ilgwal_body):
        reasons.remove("ilgwal")

    set_body = only_apparel and not set_title and bool(_APPAREL_SET_BODY.search(b))
    is_apparel_set = set_title or set_body
    if set_title:
        # 상하의 세트의 '상의 X원 / 하의 Y원'은 한 벌 안의 구성 → 묶음 사유에서 뺀다
        reasons = [r for r in reasons if r not in ("multi_price", "separate_ok", "per_unit_price", "ilgwal")]
    elif set_body:
        # 본문에만 '상하의 일괄 판매'라고 적은 한 벌: 일괄·가격 나열만 뺀다('1세트당 16만, L·M 하나씩'은 여러 벌로 남긴다)
        reasons = [r for r in reasons if r not in ("multi_price", "ilgwal")]

    with_binding = "binding" in cats and ("ski" in cats or bool(re.search(r"\+|포함|장착|달린|세트", tm)))
    if with_binding and _BINDING_EXCL.search(tm):
        # '바인딩 x', '플레이트만'
        with_binding = False
    elif not with_binding and "ski" in cats and "binding" not in cats and not _BINDING_EXCL.search(tm):
        # 제목에는 없고 본문에 '바인딩 포함/장착'만 적은 스키
        bm = _BINDING_PAT.search(b)
        if bm and not _BINDING_NEG.search(b[bm.end():bm.end() + 10]):
            with_binding = True
    has_extras = bool(majors) and any(c in cats for c in ("poles", "bag", "gloves", "protector")) and \
        len(set(majors)) == 1 and not only_apparel
    if not has_extras and majors and _EXTRAS.search(t):
        has_extras = True
    is_partial = bool(_PARTIAL.search(t) or _PARTIAL.search(m))

    return BundleFlags(
        is_bundle=bool(reasons),
        bundle_reasons=";".join(dict.fromkeys(reasons)),
        multi_price=multi_price,
        n_body_item_prices=len(distinct_items),
        is_apparel_set=is_apparel_set,
        with_binding=with_binding,
        has_extras=has_extras,
        is_partial=is_partial,
        categories=",".join(cats),
    )


# ---------------------------------------------------------------------------
# 4. 상태·거래 조건 플래그
# ---------------------------------------------------------------------------
_NEW = re.compile(
    r"새\s?상품|새\s?제품|새\s?물건|신\s?품|신\s?제품|미\s?사용|미\s?개봉|미\s?착용|새\s?거|새\s?것|"
    r"비닐\s?(?:그대로|안\s?뜯|미개봉|도\s?안)|택\s?(?:있|붙|달린|달려|그대로|안\s?(?:뗀|뜯|땐|떼|띤)|도\s?안|미제거|보유|포함|부착)|"
    r"택\s?(?:o|ㅇ)(?![a-z])|(?<![a-z])brand\s?new|(?<![a-z])unused"
)
_NEW_LIKE_AFTER = re.compile(
    r"^\s?(?:급|수준|같|처럼|컨디션|이나\s?다름|과\s?다름|와\s?다름|느낌|에\s?가까|이라고|라고|이라\s?(?:해도|봐도|보셔도)|라\s?(?:해도|봐도)|"
    r"로\s?보셔|으로\s?보셔|나\s?다름|상태|에\s?준하는|와\s?진배)"
)
_NEW_LIKE_BEFORE = re.compile(r"(?:거의|준|사실상)\s?$")
# '새제품으로 지난해 구입했고', '미사용 신품으로 구매 후' → 과거 구매 이야기(지금 새것이라는 뜻 아님)
_NEW_PAST_AFTER = re.compile(r"^[^.\n/!]{0,8}?(?:구매|구입|샀|사서|주문|출고|받아|받은|들인|개시|출시|에\s?사|으로\s?사|로\s?사)")
_NEW_PRICE_AFTER = re.compile(r"^\s?(?:가(?![능])|가격|대비|기준|시세|정가)")
_NEW_NEG_AFTER = re.compile(r"^\s?(?:은|는|이)?\s?(?:아닙|아님|아닌|x(?![a-z]))")
_NEW_PART_BEFORE = re.compile(r"(?:렌즈|브러쉬|브러시|디스크|이너|라이너|깔창|배터리|스트랩|바인딩|인솔|커버|파츠|부품|케이스|폴|가방)\s?(?:은|는|도|만)?\s?$")


def _new_hits(text: str) -> tuple[bool, bool]:
    """(새상품 주장, 새상품급 주장)."""
    strict = like = False
    for m in _NEW.finditer(text):
        after = text[m.end():m.end() + 12]
        before = text[max(0, m.start() - 10):m.start()]
        if _NEW_LIKE_AFTER.search(after) or _NEW_LIKE_BEFORE.search(before):
            like = True
            continue
        if _NEW_PAST_AFTER.search(after) or _NEW_PRICE_AFTER.search(after) or _NEW_NEG_AFTER.search(after):
            continue
        if _NEW_PART_BEFORE.search(before):
            continue
        if re.search(r"새\s?(?:거|것)$", m.group(0)) and not re.search(r"^\s?(?:입니다|이에요|예요|임|그대로|에요)", after):
            # '새거'는 '새거나 다름없는' 같은 비유가 많아 단정 표현만 인정
            continue
        strict = True
    return strict, like


_TEST_UNIT = re.compile(
    r"시승\s?(?:용|스키|제품|품|장비|했던|하던|으로\s?(?:사용|쓰|나온)|회\s?(?:용|에서)|출신)|시타\s?(?:용|품)|"
    r"전시\s?(?:품|상품|제품|용)|샘플\s?(?:품|제품|용|상품)|(?<![a-z])sample\s?(?:unit|ski|product)|테스트\s?(?:용|스키|제품|품)|demo\s?(?:unit|용)"
)
_RENTAL_UNIT = re.compile(
    r"(?:렌탈|렌털|대여|rental)\s?(?:용|스키|장비|제품|품|했던|하던|출신|샵\s?(?:스키|장비|출신|제품|용|에서\s?(?:사용|쓰던|나온|운영|빼))|"
    r"으로\s?(?:사용|쓰던|운영|돌리)|에\s?쓰던)"
)
_USED_COUNT = re.compile(r"(?<![\d.])[1-9]\d*\s?(?:회|번|일|시즌|시간|달|개월|주|년)|(?:한|두|세|네|반)\s?(?:시즌|번|달|해)|시즌|주말|자주|여러\s?번")
# 비교 표현('시승용 스키 정도', '렌탈 스키와는 급이 달라요', '렌탈 서너 번 값')은 해당 매물이 아니다
_UNIT_COMPARE_AFTER = re.compile(r"^[^.\n/]{0,10}?(?:수준|정도|급이|처럼|같|보다|랑|이랑|대신|비용|값|에서\s?벗어)")
_DEMO = re.compile(r"데모|demo")
_DEMO_GRADE = re.compile(r"데모\s?(?:급|수준|정도)|demo\s?급")

_DEF_MAJOR = re.compile(
    r"크랙|crack|금\s?(?:이\s?)?(?:갔|감|간)|파손|깨짐|깨진|깨졌|(?<!일)부러|코어\s?샷|코어\s?노출|core\s?shot|박리|들뜸|들떴|떴습|떠\s?있|"
    r"찢어|찢김|찢긴|구멍|(?<!중)고장|휘어|휨|변형|(?:엣지|에지|edge)\s?(?:손상|크랙|깨|떨어|빠짐|빠져|이탈|들뜸)|"
    r"베이스\s?(?:손상|패임|파임|데미지|구멍|깊은)|누수|방수\s?(?:안|불량|기능\s?저하|저하)|작동\s?(?:안|불량)|헛돌"
)
_DEF_REPAIR = re.compile(r"(?<!통\s)(?<!통)(?<!기장\s)(?<!기장)수선(?!비)|수리|보수|에폭시|피텍스|p-?tex|땜질|용접")
_DEF_MINOR = re.compile(
    r"하자|기스|스크래치|scratch|긁힘|긁힌|긁혔|찍힘|찍힌|찍혔|찍혀|까짐|까진|까졌|흠집|상처|데미지|damage|얼룩|오염|변색|"
    r"(?:엣지|에지)\s?빵|늘어짐|보풀|이염|탈색|눌림|패임|파임|스크레치|생활\s?흔적|마모"
)
# 부정: 용어 뒤 16자 안에 '없'이 '있'보다 먼저 나오고 그 사이에 문장 경계가 없으면 부정으로 본다
# ('기스나 찍힘 변색 등 없이' → 셋 다 부정, '하자 없고 기스가 몇 개 있는' → 기스는 살아 있음)
_DEF_NEG_X = re.compile(r"^[^.\n/|!]{0,6}?(?:x(?![a-z])|無|zero|제로|0\s?(?:개|군데)?(?:입니다|$))")
_DEF_NEG_BEFORE = re.compile(r"(?:무|노|no|non|하자\s?없는|없는)\s?$")


def _negated_after(after: str) -> bool:
    if _DEF_NEG_X.search(after):
        return True
    cut = re.search(r"[.\n/|!]", after)
    seg = after[:cut.start()] if cut else after
    i = seg.find("없")
    if i < 0:
        i2 = seg.find("아님")
        i = i2 if i2 >= 0 else seg.find("아닙")
    if i < 0:
        return False
    j = seg.find("있")
    return j < 0 or j > i


def _defect_hits(text: str) -> tuple[str, list[str], bool]:
    """(등급 none/minor/repair/major, 부정되지 않은 용어, 하자 부정 문구 존재)."""
    level = 0
    terms: list[str] = []
    denied = False
    for lv, pat in ((3, _DEF_MAJOR), (2, _DEF_REPAIR), (1, _DEF_MINOR)):
        for m in pat.finditer(text):
            after = text[m.end():m.end() + 16]
            before = text[max(0, m.start() - 6):m.start()]
            if _negated_after(after) or _DEF_NEG_BEFORE.search(before):
                denied = True
                continue
            level = max(level, lv)
            term = m.group(0).replace(" ", "")
            if term not in terms:
                terms.append(term)
    return ("none", "minor", "repair", "major")[level], terms[:6], denied


_NEGO_OPEN = re.compile(
    r"네고\s?(?:가능|ㅇ|o(?![a-z])|ok|환영|해\s?드|됩니다|되요|돼요|있|문의|조금|약간|살짝|가능합니다)|쿨\s?(?:거래\s?)?(?:시\s?)?(?:네고|에누리|할인)|"
    r"가격\s?(?:협의|조정|조율|제안)|협의\s?가능|에누리|절충|흥정|네고\s?(?:\d|해\s?드)"
)
_NEGO_REFUSED = re.compile(
    r"네고\s?(?:는\s?)?(?:불가|사절|x(?![a-z])|안\s?(?:됩|해|합|받)|사양|no(?![a-z])|없|노(?![가-힣])|절대|힘들|어렵|불|ㄴ)|"
    r"노\s?네고|no\s?nego|정찰|가격\s?(?:고정|변동\s?없)|무리한\s?네고|네고\s?문의\s?(?:사절|사양)"
)
_PRICE_DOWN = re.compile(
    r"가격\s?(?:다운|인하|내림|내렸|다윤|수정|변경|재조정)|금액\s?(?:다운|인하|내림|조정|수정)|(?:다운|인하)\s?\)|"
    r"\d\s?(?:만|만원|원)?\s?(?:->|→|=>)\s?\d|최종\s?가|마지막\s?가격|파격\s?(?:가|가격|인하)|가격\s?낮|가격\s?내립|재\s?가격|"
    r"할인\s?\)|가격\s?조정"
)
_URGENT = re.compile(r"급처|급매|급전|급하게|빠른\s?(?:판매|처분|거래)|오늘만|떨이|빨리\s?(?:판매|처분|정리)|이번\s?주\s?(?:까지|만)|자정\s?까지")
_PARCEL = re.compile(r"택배|반값\s?택배|편의점\s?택배|화물|퀵|우체국|착불|택포|배송|발송")
_PARCEL_NO = re.compile(
    r"택배\s?(?:거래\s?)?(?:는\s?)?(?:불가|안\s?(?:됩|해|합|받)|x(?![a-z])|사절|no(?![a-z])|사양|어렵|힘들)|직거래\s?만|직거래만|직거래\s?only|only\s?직거래|"
    r"(?:택배|배송)\s?(?:거래\s?)?(?:는\s?)?(?:하지\s?않|안\s?합)"
)
_DIRECT = re.compile(r"직거래|직접\s?거래|만나서|방문\s?(?:거래|수령)|직\s?거래")


@dataclass
class ConditionFlags:
    is_new_claim: bool
    is_like_new_claim: bool
    is_test_unit: bool  # 시승·전시·샘플
    is_rental_unit: bool  # 렌탈·대여용 장비
    demo_mention: str | None  # None / model_line / grade
    defect_level: str  # none / minor / repair / major
    defect_terms: str
    defect_denied: bool
    nego_open: bool
    nego_refused: bool
    price_down_note: bool
    urgent_sale: bool
    ship_parcel: bool
    ship_direct: bool
    direct_only: bool


def condition_flags(title: object, body: object = None, features: object = None, usage_count: object = None,
                    status: object = None, model_serial: object = None, spec: object = None) -> ConditionFlags:
    t = norm(title)
    b = norm(body)
    f = "" if is_junk(features) else norm(features)
    u = "" if is_junk(usage_count) else norm(usage_count)
    m = "" if is_junk(model_serial) else norm(model_serial)
    sp = "" if is_junk(spec) else norm(spec)
    st = norm(status)
    alltxt = " | ".join(x for x in (t, m, sp, f, u, b) if x)

    new_strict = False
    new_like = False
    for txt in (t, f, u, b):
        s, lk = _new_hits(txt)
        new_strict |= s
        new_like |= lk
    used = bool(_USED_COUNT.search(u)) and not re.fullmatch(r"0\s?(?:회|번|일)?", u)
    if new_strict and used and st != "미개봉":
        # 사용 횟수 칸에 실제 사용(1회 이상, 한 시즌 등)이 적혀 있으면 새상품이 아니라 새것 같은 상태
        new_strict, new_like = False, True
    if st == "미개봉":
        new_strict = True
    if re.fullmatch(r"0\s?(?:회|번|일)?|미사용|사용\s?(?:안\s?함|x|무)|없음\s?\(?미사용\)?", u):
        new_strict = True

    is_test = any(not _UNIT_COMPARE_AFTER.search(alltxt[mt.end():mt.end() + 14]) for mt in _TEST_UNIT.finditer(alltxt))
    is_rental = any(not _UNIT_COMPARE_AFTER.search(alltxt[mt.end():mt.end() + 14]) for mt in _RENTAL_UNIT.finditer(alltxt))
    demo = None
    if _DEMO.search(alltxt):
        demo = "grade" if _DEMO_GRADE.search(alltxt) and not re.search(r"데모\s?(?:복|웨어|라인|팀|스키|wear|line)", alltxt) else "model_line"

    dlevel, dterms, ddenied = _defect_hits(" | ".join(x for x in (t, f, b) if x))

    return ConditionFlags(
        is_new_claim=new_strict,
        is_like_new_claim=new_like and not new_strict,
        is_test_unit=is_test,
        is_rental_unit=is_rental,
        demo_mention=demo,
        defect_level=dlevel,
        defect_terms=",".join(dterms),
        defect_denied=ddenied,
        nego_open=bool(_NEGO_OPEN.search(alltxt)),
        nego_refused=bool(_NEGO_REFUSED.search(alltxt)),
        price_down_note=bool(_PRICE_DOWN.search(t) or _PRICE_DOWN.search(b)),
        urgent_sale=bool(_URGENT.search(t) or _URGENT.search(b)),
        ship_parcel=bool(_PARCEL.search(alltxt)) and not bool(_PARCEL_NO.search(alltxt)),
        ship_direct=bool(_DIRECT.search(alltxt)),
        direct_only=bool(_PARCEL_NO.search(alltxt)),
    )


# ---------------------------------------------------------------------------
# 5. 가격 유효성
# ---------------------------------------------------------------------------
_PLACEHOLDER = {12345, 123456, 1234567, 12345678, 123456789, 54321, 98765}


def price_is_valid(price: object) -> bool:
    """판매 호가로 쓸 수 있는 가격인가(1원·999999·111111 같은 자리채움 값 제외).

    5천 원 미만·3천만 원 초과, 같은 숫자 반복(11111, 999999), 999로 시작하는 값, 12345류를 뺀다.
    """
    if price is None:
        return False
    try:
        p = float(price)
    except (TypeError, ValueError):
        return False
    if math.isnan(p):
        return False
    p = int(p)
    if p < 5000 or p > 30_000_000:
        return False
    s = str(p)
    if re.fullmatch(r"(\d)\1{3,}", s) or s.startswith("999") or p in _PLACEHOLDER:
        return False
    if re.fullmatch(r"(\d)\1{3,}[0-9]", s):  # 1111110 같은 값
        return False
    return True


# ---------------------------------------------------------------------------
# 6. 재게시 매물 묶기
# ---------------------------------------------------------------------------
# 한글 브랜드·일반어 → 영문 토큰(재게시 때 표기를 바꾸는 경우를 맞추기 위함)
_ALIASES: list[tuple[str, str]] = [
    (r"아토믹", "atomic"), (r"로시뇰|로시놀|로시니올", "rossignol"), (r"피셔", "fischer"), (r"헤드(?!폰)", "head"),
    (r"노르디카", "nordica"), (r"뵐클|볼클|뵐끌|뷀클|뷜클|폴클|völkl|voelkl", "volkl"), (r"살로몬|살로먼", "salomon"),
    (r"블리자드", "blizzard"), (r"오가사카", "ogasaka"), (r"다이나스타", "dynastar"), (r"엘란", "elan"),
    (r"스토클리|스톡클리|스퇴클리|stöckli|stoeckli", "stockli"), (r"케슬레|캐슬|kästle", "kastle"), (r"어그먼트", "augment"),
    (r"랑게", "lange"), (r"테크니카", "tecnica"), (r"달벨로", "dalbello"), (r"마커", "marker"), (r"티롤리아", "tyrolia"),
    (r"데상트|데쌍트|데센트", "descente"), (r"피닉스", "phenix"), (r"골드윈", "goldwin"), (r"온요네", "onyone"),
    (r"(?<![가-힣])카브(?![가-힣])|(?<![가-힣])카브(?=스키|하드|쉘|주니어|여성|아동|데모)", "karv"), (r"스파이더", "spyder"), (r"큐스|츄스", "kjus"), (r"퓨잡|퓨살프", "fusalp"),
    (r"아크테릭스", "arcteryx"), (r"콜마", "colmar"), (r"미즈노", "mizuno"), (r"에너지아\s?퓨라|에너지아\s?푸라", "energiapura"),
    (r"로이쉬", "reusch"), (r"레키|렉키", "leki"), (r"콤퍼델|컴퍼델", "komperdell"), (r"시나노", "shinano"),
    (r"스미스", "smith"), (r"지로", "giro"), (r"우벡스", "uvex"), (r"오클리", "oakley"), (r"스윗\s?프로텍션", "sweetprotection"),
    (r"브리코", "briko"), (r"다이네즈", "dainese"), (r"블랙\s?브라이어", "blackbriar"), (r"키자키", "kizaki"),
    (r"아이디원", "idone"), (r"스윅스", "swix"), (r"토코", "toko"), (r"파타고니아", "patagonia"), (r"노스페이스", "northface"),
    (r"말로야", "maloja"), (r"썰믹", "thermic"), (r"therm-?ic", "thermic"), (r"가벨", "gabel"), (r"반디어", "vandeer"),
    (r"레이스\s?타이거", "racetiger"), (r"도베르만", "dobermann"), (r"히어로", "hero"), (r"레드스터", "redster"),
    (r"혹스", "hawx"), (r"파이어버드", "firebird"), (r"슈퍼\s?[셰쉐]이프", "supershape"), (r"월드컵", "worldcup"),
    (r"(?<![가-힣])하트(?=\s|모글|스키|$)", "hart"), (r"블루\s?모리스", "bluemoris"), (r"쉐펠|쇠펠", "schoffel"),
    (r"헤스트라", "hestra"), (r"원웨이", "oneway"), (r"오메\s?글라스|오메그라스", "omeglass"), (r"마스터즈?", "master"),
    (r"렉삼", "rexam"), (r"욘요네", "onyone"), (r"포뮬라", "formula"), (r"울트라", "ultra"), (r"프라임", "prime"),
    (r"마하", "mach"), (r"(?<![가-힣])보아(?![가-힣])|(?<![가-힣])보아(?=부츠|스키)", "boa"), (r"모글", "mogul"), (r"데모\s?(?:복|웨어)", "demo skiwear"),
    (r"팀\s?복", "team skiwear"), (r"코치\s?복", "coach skiwear"), (r"데모", "demo"), (r"(?:부츠\s?)?(?:가방|백)(?=\s|$)", "bag"),
    (r"닷아웃", "dotout"), (r"밀레(?![가-힣])", "millet"), (r"블랙\s?크로우즈?|black\s?crows", "blackcrows"),
    (r"주이카", "juika"), (r"보그너", "bogner"), (r"케슬러|kessler", "kessler"), (r"글렌\s?스키|글렌(?=\s|$)", "glenski"),
    (r"마운틴\s?스튜디오|the\s?mountain\s?studio", "mountainstudio"), (r"아르마다", "armada"), (r"첨스", "chums"),
    (r"(?<![a-z])poc(?![a-z])|피오씨", "poc"), (r"버튼(?=\s|$)", "burton"),
    (r"스키\s?부츠", "boots"), (r"고글", "goggle"), (r"헬멧|핼멧|헬맷|핼맷", "helmet"), (r"스키\s?복", "skiwear"), (r"스키(?![가-힣])", "ski"),
    (r"부츠", "boots"), (r"자켓|재킷|쟈켓", "jacket"), (r"바지|팬츠", "pants"), (r"대회전", "gs"), (r"(?<![가-힣])회전", "sl"),
]
_ALIASES_C = [(re.compile(p), r) for p, r in _ALIASES]

# 재게시 때 붙였다 떼는 말(가격 조정·판매 문구·상태 수식어)
_TITLE_NOISE = re.compile(
    r"가격\s?(?:다운|인하|내림|조정|수정|변경|다윤)|금액\s?(?:다운|인하|조정|수정)|최종\s?(?:가격|가)?|마지막\s?(?:가격|가)?|"
    r"재\s?업로드|재업|다시\s?올림|재등록|끌올|급매|급처|떨이|네고\s?(?:가능|ㅇ|ok|불가|x)?|쿨\s?거래|쿨거|"
    r"판매\s?(?:합니다|해요|중|완료|합니당)?|팝니다|팝니당|팔아요|팔아\s?봅니다|처분\s?(?:합니다)?|정리\s?(?:합니다)?|"
    r"(?:새\s?(?:상품|제품)|신품|미사용|미개봉)\s?급?|상태\s?(?:최상|좋음|굿|좋아요|s급?|a급?)|(?<![a-z0-9])[sabc]\+?\s?급|"
    r"택포|무료\s?배송|직거래|택배\s?(?:가능)?|입니다|해요|합니다|급|오늘만|할인|세일|파격|특가|연휴|시즌\s?오프|"
    r"거래\s?파기|저렴하게|싸게|상태\s?(?:깨끗|양호)|깨끗|거의\s?새거|새거|새것"
)
# 퍼지 비교에서 뺄 토큰: 시즌·의류 사이즈는 spec_signature로 따로 대조하므로 표기 유무가 유사도를 깎지 않게 한다
# 숫자·단위 토큰(길이 165, 165cm, 255mm, 23m)도 spec_signature가 따로 대조하므로 퍼지 비교에서 뺀다
_FUZZY_DROP = re.compile(r"^(?:\d+(?:\.\d+)?(?:cm|mm|m|l|kg)?|xs|s|m|l|xl|xxl|2xl|3xl|o|xo|시즌|사이즈|size|센치|센티)$")
_NONWORD = re.compile(r"[^0-9a-z가-힣]+")
_SCRIPT_SPLIT = re.compile(r"(?<=[가-힣])(?=[a-z0-9])|(?<=[a-z0-9])(?=[가-힣])")


def normalize_title(title: object) -> str:
    """재게시 비교용 제목: 가격 조정·판매 문구·기호를 지우고 브랜드 표기를 맞춘다."""
    t = norm(title)
    t = _SCRIPT_SPLIT.sub(" ", t)  # '피셔sc' → '피셔 sc', '2526온요네' → '2526 온요네'
    t = _TITLE_NOISE.sub(" ", t)
    for pat, rep in _ALIASES_C:
        t = pat.sub(f" {rep} ", t)
    t = _NONWORD.sub(" ", t)
    return _WS.sub(" ", t).strip()


def match_text(title: object, model_serial: object) -> str:
    """제목 + 모델명(의미 있는 경우만) 퍼지 비교 문자열(시즌·사이즈 토큰 제외)."""
    nt = normalize_title(title)
    if not (is_junk(model_serial) or norm(model_serial) == norm(title)):
        nt = f"{nt} {normalize_title(model_serial)}"
    return " ".join(tok for tok in nt.split() if not _FUZZY_DROP.match(tok))


_SEASON = re.compile(r"(?<!\d)(?:20)?([1-2]\d)\s?[/\-~.]?\s?(?:20)?([1-2]\d)(?!\d)")
_LEN_CM = re.compile(r"(?<![\d.])(\d{2,3})\s?(?:cm|센치|센티|㎝)")
_BOOT = re.compile(r"(?<![\d.])(2[2-9][05]|3[01][05])(?:\s?mm)?(?![\d.])|(?<![\d.])(2[2-9]\.5)(?!\d)")
_RADIUS = re.compile(r"(?:r\s?[=:]?\s?)?(?<![\d.])(\d{1,2}(?:\.\d)?)\s?m(?![a-z가-힣])")
_APP_SIZE = re.compile(r"(?<![a-z0-9])(xxxl|xxl|xl|xs|s|m|l|2xl|3xl|4xl)(?![a-z0-9])")
_CODE = re.compile(r"(?<![a-z0-9])[a-z0-9-]{5,}(?![a-z0-9])")
_CODE_UNIT = re.compile(r"[\d\-~/.]+(?:cm|mm|m|kg|g)?|(?:19|20)\d\d[-/]?\d*|\d+(?:cm|mm)\d*")
_NUM3 = re.compile(r"(?<![\d.])(1[2-9]\d)(?![\d.])")
# 성별·연령: 남성/여성/주니어가 서로 다르면 다른 매물('공용'은 비교하지 않음)
_GENDER = [
    ("M", re.compile(r"남성|남자|(?<![a-z])(?:men|mens|men's|male)(?![a-z])")),
    ("W", re.compile(r"여성|여자|와이프|(?<![a-z])(?:women|womens|women's|lady|ladies|wmns)(?![a-z])")),
    ("J", re.compile(r"주니어|쥬니어|아동|키즈|어린이|초등|유아|(?<![a-z])(?:junior|jr|kids?|youth)(?![a-z])")),
]
# 브랜드 충돌 비교용 토큰(바인딩 전문 브랜드 marker·look·tyrolia는 스키와 함께 쓰이므로 뺀다)
_BRAND_TOKENS = {
    "atomic", "rossignol", "fischer", "head", "nordica", "volkl", "salomon", "blizzard", "ogasaka", "dynastar", "elan",
    "stockli", "kastle", "augment", "hart", "lange", "tecnica", "dalbello", "descente", "phenix", "goldwin", "onyone",
    "karv", "spyder", "kjus", "fusalp", "arcteryx", "colmar", "mizuno", "energiapura", "reusch", "leki", "komperdell",
    "shinano", "smith", "giro", "uvex", "oakley", "sweetprotection", "briko", "dainese", "blackbriar", "kizaki", "idone",
    "swix", "toko", "patagonia", "northface", "maloja", "thermic", "gabel", "vandeer", "bluemoris", "schoffel", "hestra",
    "oneway", "dotout", "millet", "blackcrows", "juika", "bogner", "kessler", "glenski", "mountainstudio", "armada",
    "chums", "poc", "burton",
}
# 종목(회전/대회전): normalize_title 뒤 토큰 기준. 회전=sl, 대회전=gs
_DISC = {"sl": "sl", "slr": "sl", "esl": "sl", "isl": "sl", "gs": "gs", "egs": "gs", "sg": "sg", "dh": "dh"}
# 짧은 모델 코드(x9, g9, s9i, rc4, hp1 …). 바인딩 수치(x12, z17, rs16)·단위는 뺀다
_MCODE = re.compile(r"[a-z]{1,3}\d{1,2}[a-z]{0,3}")
_MCODE_BINDING = re.compile(r"(?:x|z|i|rs|nx|ff|prd|wrt|spx|comp|xcell|icon|r|ld)\d{2}[a-z]{0,3}|[a-z]{1,3}\d{1,2}(?:cm|mm|m|l|kg)")


def season_from_year_field(release_year: object) -> set[int]:
    """판매자 입력 연식 칸에서 '24/25', '2425', '24-25'처럼 시즌이 명시된 경우만 뽑는다(단일 연도는 무시)."""
    if is_junk(release_year):
        return set()
    out = set()
    for a, b2 in _SEASON.findall(norm(release_year)):
        ia, ib = int(a), int(b2)
        if ib == ia + 1 and 10 <= ia <= 29:
            out.add(ia)
    return out if len(out) == 1 else set()


def spec_signature(*texts: object) -> dict[str, set]:
    """제목·모델·스펙에서 비교 가능한 속성 집합(시즌, 길이, 부츠 사이즈, 회전반경, 의류 사이즈, 모델 코드)."""
    s = " ".join(norm(x) for x in texts if not is_junk(x))
    sig: dict[str, set] = {"season": set(), "len": set(), "boot": set(), "radius": set(), "size": set(), "code": set(),
                           "num3": set(), "disc": set(), "mcode": set()}
    for g, pat in _GENDER:
        if pat.search(s):
            sig.setdefault("gender", set()).add(g)
    sn = normalize_title(s)
    sig["brand"] = {tok for tok in sn.split() if tok in _BRAND_TOKENS}
    for tok in sn.split():
        if tok in _DISC:
            sig["disc"].add(_DISC[tok])
        elif _MCODE.fullmatch(tok) and not _MCODE_BINDING.fullmatch(tok) and not _CODE_UNIT.fullmatch(tok):
            sig["mcode"].add(tok)
    for a, b2 in _SEASON.findall(s):
        ia, ib = int(a), int(b2)
        if ib == ia + 1 and 10 <= ia <= 29:
            sig["season"].add(ia)
    for x in _LEN_CM.findall(s):
        v = int(x)
        if 80 <= v <= 215:
            sig["len"].add(v)
    for g1, g2 in _BOOT.findall(s):
        if g1:
            sig["boot"].add(int(g1))
        elif g2:
            sig["boot"].add(int(round(float(g2) * 10)))
    for x in _RADIUS.findall(s):
        v = float(x)
        if 8 <= v <= 40:
            sig["radius"].add(v)
    for x in _APP_SIZE.findall(s):
        sig["size"].add({"xxl": "2xl", "xxxl": "3xl"}.get(x, x))
    for x in _CODE.findall(s):
        if _CODE_UNIT.fullmatch(x):
            continue
        if len(re.findall(r"[a-z]", x)) >= 2 and len(re.findall(r"\d", x)) >= 2:
            sig["code"].add(x.replace("-", ""))
    for x in _NUM3.findall(s):
        sig["num3"].add(int(x))
    return sig


def spec_compatible(a: dict[str, set], b: dict[str, set]) -> bool:
    """두 글의 속성이 충돌하지 않는가(한쪽에만 있는 속성은 통과, 양쪽에 있으면 교집합 필요)."""
    for k in ("season", "len", "boot", "radius", "code", "disc", "mcode", "gender", "brand"):
        if a.get(k) and b.get(k) and not (a[k] & b[k]):
            return False
    # 의류 사이즈는 's'·'m'·'l'이 단어로도 흔해 두 쪽 모두 1개일 때만 비교
    if len(a["size"]) == 1 and len(b["size"]) == 1 and a["size"] != b["size"]:
        return False
    # 숫자만 쓴 길이·플렉스(예: '피셔 165' vs '피셔 170')
    if a["num3"] and b["num3"] and not (a["num3"] & b["num3"]) and not (a["len"] & b["len"]):
        return False
    return True


def categories_compatible(ca, cb) -> bool:
    """주요 품목 단서가 양쪽에 있으면 집합이 같아야 같은 매물(고글 단품 vs 헬멧+고글 세트는 다른 매물)."""
    ma = {c for c in ca if c in _MAJOR}
    mb = {c for c in cb if c in _MAJOR}
    return not (ma and mb and ma != mb)


@dataclass
class PostKey:
    post_id: str
    author: str
    t: int  # epoch 초
    listing_type: str
    title_norm: str
    mtext: str
    sig: dict
    cats: list
    tsig: dict | None = None  # 제목만으로 만든 속성(모델 칸 복사·붙여넣기 오류 대비)
    is_bundle: bool = False
    price: int | None = None  # 유효 호가만(자리채움 값은 None)


def _tokens(s: str) -> list[str]:
    return [x for x in s.split() if x]


def posts_match(a: PostKey, b: PostKey, threshold: float = 90.0) -> tuple[bool, float, str]:
    """같은 매물인가: (정규화 제목 동일 or token_set_ratio ≥ 90) and 속성 호환 and 품목 호환.

    token_set_ratio는 한쪽 토큰이 다른 쪽의 부분집합이면 100이 되므로, 짧은 쪽 토큰이 3개 미만일 때는
    token_sort_ratio ≥ 80도 함께 요구한다.
    반환: (일치 여부, token_set_ratio, 사유 코드)
    """
    from rapidfuzz import fuzz

    if a.listing_type != b.listing_type:
        return False, 0.0, "type"
    same_title = bool(a.title_norm) and a.title_norm == b.title_norm
    r = 100.0 if same_title else (float(fuzz.token_set_ratio(a.mtext, b.mtext)) if a.mtext and b.mtext else 0.0)
    if not spec_compatible(a.sig, b.sig) or (a.tsig and b.tsig and not spec_compatible(a.tsig, b.tsig)):
        return False, r, "spec"
    if a.is_bundle != b.is_bundle:
        # 일괄 글과 낱개 글은 가격 단위가 다르므로 잇지 않는다
        return False, r, "bundle"
    if not categories_compatible(a.cats, b.cats):
        return False, r, "category"
    if a.price and b.price and max(a.price, b.price) > MAX_PRICE_RATIO * min(a.price, b.price):
        # 재게시 사이 호가가 2배 넘게 벌어지면(여러 개 묶음 글, 다른 물건) 잇지 않는다
        return False, r, "price_gap"
    early, late = (a, b) if a.t <= b.t else (b, a)
    if early.price and late.price and early.price != late.price and late.t - early.t < CONCURRENT_SEC:
        # 1시간 안에 가격이 다른 글 여러 개 = 같은 판매자가 비슷한 물건 여러 개를 따로 올린 경우
        return False, r, "concurrent"
    if early.price and late.price and late.price > MAX_PRICE_RISE * early.price:
        # 나중 글 호가가 1.5배 넘게 오르는 재게시는 드물다(다른 모델일 가능성이 큼)
        return False, r, "price_jump"
    if same_title:
        return True, r, "title_equal"
    if r < threshold:
        return False, r, "ratio"
    ua, ub = _uniq(a.mtext), _uniq(b.mtext)
    n_short = min(len(ua), len(ub))
    if n_short <= 2 and _shared_evidence(a.sig, b.sig):
        # 토큰은 적어도 시즌·길이·사이즈·모델코드 같은 속성 값이 겹치면 같은 매물로 본다
        return True, r, "ratio"
    if n_short <= 1:
        # 토큰 1개짜리(예: 브랜드명만)는 제목이 완전히 같을 때만 잇는다
        return False, r, "short_subset"
    if n_short == 2 and fuzz.token_sort_ratio(" ".join(ua), " ".join(ub)) < 85:
        return False, r, "short_subset"
    return True, r, "ratio"


_EVIDENCE_KEYS = ("season", "len", "boot", "radius", "code", "mcode", "num3")


def _shared_evidence(a: dict, b: dict) -> bool:
    return any(a.get(k) and b.get(k) and (a[k] & b[k]) for k in _EVIDENCE_KEYS)


def _uniq(s: str) -> list[str]:
    return sorted(set(_tokens(s)))


def _copy_sig(sig: dict) -> dict:
    return {k: set(v) for k, v in sig.items()}


def _merge_sig(dst: dict, src: dict) -> None:
    for k, v in src.items():
        dst.setdefault(k, set()).update(v)


class _DSU:
    def __init__(self, n: int):
        self.p = list(range(n))

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[max(ra, rb)] = min(ra, rb)


MAX_GAP_DAYS = 180
MAX_PRICE_RATIO = 2.0
MAX_PRICE_RISE = 1.5
CONCURRENT_SEC = 3600


NEAR_MIN_RATIO = 80.0
NEAR_PRICE_TOL = 0.10
NEAR_MIN_GAP_SEC = 3600


def group_items(keys: list[PostKey], max_gap_days: int = MAX_GAP_DAYS, threshold: float = 90.0,
                loose: bool = False) -> tuple[dict[str, str], list[tuple[str, str, float, bool, str]]]:
    """작성자별로 글 쌍을 비교해 매물 id를 붙인다.

    반환: (post_id → item_id, 비교 기록[(post_a, post_b, ratio, matched, 사유)] — 비율 80 이상 쌍만)
    item_id = 'it_' + 매물의 첫 글 post_id. 두 글의 관측 간격이 max_gap_days 이내일 때만 잇고,
    그 이상 벌어진 글은 중간 글을 거쳐야만 같은 매물이 된다(연쇄).

    loose=True(민감도 분석용): 기본 규칙에 더해, 비율 80~90이고 속성·품목·묶음 조건을 통과하며
    두 유효 호가 차이가 10% 이내이고 1시간 이상 떨어진 쌍도 잇는다(사유 'near_price').
    """
    from collections import defaultdict

    by_author: dict[str, list[int]] = defaultdict(list)
    for i, k in enumerate(keys):
        by_author[k.author or f"__none_{k.post_id}"].append(i)
    dsu = _DSU(len(keys))
    # 묶음(클러스터)별 누적 속성: 속성이 빈 글을 거쳐 M과 L이 이어지는 연쇄 오류를 막는다
    agg_sig = {i: _copy_sig(k.sig) for i, k in enumerate(keys)}
    agg_tsig = {i: _copy_sig(k.tsig) if k.tsig else None for i, k in enumerate(keys)}
    agg_cat = {i: {c for c in k.cats if c in _MAJOR} for i, k in enumerate(keys)}
    log: list[tuple[str, str, float, bool, str]] = []
    gap = max_gap_days * 86400
    for idx in by_author.values():
        idx.sort(key=lambda i: (keys[i].t, keys[i].post_id))
        cands = []
        for x in range(len(idx)):
            a = keys[idx[x]]
            for y in range(x + 1, len(idx)):
                b = keys[idx[y]]
                if b.t - a.t > gap:
                    break
                ok, r, why = posts_match(a, b, threshold)
                if ok:
                    cands.append((0 if why == "title_equal" else 1, -r, b.t - a.t, idx[x], idx[y], r, why))
                elif loose and why == "ratio" and r >= NEAR_MIN_RATIO and a.price and b.price                         and abs(a.price - b.price) <= NEAR_PRICE_TOL * max(a.price, b.price)                         and b.t - a.t >= NEAR_MIN_GAP_SEC:
                    cands.append((2, -r, b.t - a.t, idx[x], idx[y], r, "near_price"))
                elif r >= 80:
                    log.append((a.post_id, b.post_id, float(r), False, why))
        # 강한 연결(제목 동일 → 비율 높은 순 → 시간 가까운 순)부터 잇는다
        for _, _, _, i, j, r, why in sorted(cands):
            ri, rj = dsu.find(i), dsu.find(j)
            if ri == rj:
                log.append((keys[i].post_id, keys[j].post_id, float(r), True, why))
                continue
            ok = spec_compatible(agg_sig[ri], agg_sig[rj]) and                 (agg_tsig[ri] is None or agg_tsig[rj] is None or spec_compatible(agg_tsig[ri], agg_tsig[rj])) and                 not (agg_cat[ri] and agg_cat[rj] and agg_cat[ri] != agg_cat[rj])
            if not ok:
                log.append((keys[i].post_id, keys[j].post_id, float(r), False, "cluster"))
                continue
            dsu.union(ri, rj)
            root = dsu.find(ri)
            other = rj if root == ri else ri
            _merge_sig(agg_sig[root], agg_sig[other])
            if agg_tsig[root] is None:
                agg_tsig[root] = agg_tsig[other]
            elif agg_tsig[other] is not None:
                _merge_sig(agg_tsig[root], agg_tsig[other])
            agg_cat[root] |= agg_cat[other]
            log.append((keys[i].post_id, keys[j].post_id, float(r), True, why))
    root_first: dict[int, str] = {}
    order = sorted(range(len(keys)), key=lambda i: (keys[i].t, keys[i].post_id))
    for i in order:
        r = dsu.find(i)
        root_first.setdefault(r, keys[i].post_id)
    return {keys[i].post_id: f"it_{root_first[dsu.find(i)]}" for i in range(len(keys))}, log


def item_history(rows: list[dict]) -> dict:
    """한 매물의 글 목록(rows: observed_at, post_id, price, price_valid)으로 가격 궤적 요약.

    가격 변화는 유효 가격만 시간 순으로 비교한다. 인하율은 (나중 - 처음) / 처음.
    """
    rows = sorted(rows, key=lambda r: (r["observed_at"], r["post_id"]))
    ts = [r["observed_at"] for r in rows]
    gaps = [(ts[i + 1] - ts[i]) / 86400 for i in range(len(ts) - 1)]
    vp = [(r["observed_at"], int(r["price"])) for r in rows if r["price_valid"]]
    prices = [p for _, p in vp]
    n_changes = n_cuts = n_raises = 0
    first_cut_t = None
    first_cut_pct = None
    for i in range(1, len(vp)):
        if prices[i] != prices[i - 1]:
            n_changes += 1
            if prices[i] < prices[i - 1]:
                n_cuts += 1
                if first_cut_t is None:
                    first_cut_t = vp[i][0]
                    first_cut_pct = prices[i] / prices[i - 1] - 1
            else:
                n_raises += 1
    out = {
        "n_posts": len(rows),
        "first_post_id": rows[0]["post_id"],
        "last_post_id": rows[-1]["post_id"],
        "first_observed_at": ts[0],
        "last_observed_at": ts[-1],
        "span_days": (ts[-1] - ts[0]) / 86400,
        "n_valid_prices": len(prices),
        "first_price": prices[0] if prices else None,
        "last_price": prices[-1] if prices else None,
        "min_price": min(prices) if prices else None,
        "max_price": max(prices) if prices else None,
        "n_price_changes": n_changes,
        "n_cuts": n_cuts,
        "n_raises": n_raises,
        "total_pct_change": (prices[-1] / prices[0] - 1) if len(prices) >= 2 else None,
        "max_drawdown_pct": (min(prices) / prices[0] - 1) if len(prices) >= 2 else None,
        "first_cut_pct": first_cut_pct,
        "days_to_first_cut": (first_cut_t - vp[0][0]) / 86400 if first_cut_t is not None else None,
        "mean_repost_gap_days": sum(gaps) / len(gaps) if gaps else None,
        "median_repost_gap_days": statistics.median(gaps) if gaps else None,
        "max_repost_gap_days": max(gaps) if gaps else None,
        "price_path": prices,
        "post_ids": [r["post_id"] for r in rows],
    }
    # 적정가 모델용 대표 글: 유효 가격이 있는 가장 최근 글
    valid_rows = [r for r in rows if r["price_valid"]]
    out["model_post_id"] = valid_rows[-1]["post_id"] if valid_rows else None
    return out
