"""필드 파서(Phase 1-b): 가격·시즌·길이/사이즈·사용 횟수·구입 시기·상태 등급.

모두 순수 함수(I/O 없음)다. 각 파서는 값과 함께 `kind`(어떤 규칙으로 얻었나)와
`flags`(신뢰도 판단용 표시)를 돌려준다. 규칙의 근거·적용 건수·모호 사례는
phase1/sections/02_parsing.md 에 있다.

반환값에는 원문 문자열(본문 구절, 판매처 이름 등)을 넣지 않는다. 숫자·정규화된 코드·불리언만 돌려준다.
"""
from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import asdict, dataclass

__all__ = [
    "SINGLE_YEAR_OFFSET", "REF_YEAR_DEFAULT", "MIN_SEASON", "MAX_SEASON",
    "norm_text", "rough_category", "listing_hints",
    "PriceMention", "PriceResult", "extract_price_mentions", "parse_price",
    "SeasonResult", "parse_season", "single_year_value", "explicit_two_year_starts",
    "single_year_to_start",
    "LengthResult", "parse_lengths",
    "UsageResult", "parse_usage",
    "PurchaseResult", "parse_purchased_at",
    "GradeResult", "parse_grade",
    "asdict",
]

# ---------------------------------------------------------------------------
# 공통
# ---------------------------------------------------------------------------


def _missing(x) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and math.isnan(x):
        return True
    return type(x).__name__ in ("NAType", "NaTType")


def norm_text(x) -> str:
    """NFKC 정규화 + 폭 0 문자 제거 + 숫자 사이 'ㅡ'·긴 대시를 '-'로."""
    if _missing(x):
        return ""
    s = str(x)
    s = re.sub(r"(?<=\d)\s*[ㅡᅳ—–―]\s*(?=\d)", "-", s)
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("​", "").replace("﻿", "").replace("‌", "")
    s = s.replace("～", "~").replace("〜", "~")
    return s


def _uniq(seq):
    out = []
    for v in seq:
        if v not in out:
            out.append(v)
    return out


def _mask(s: str, m: re.Match) -> str:
    return s[: m.start()] + " " * (m.end() - m.start()) + s[m.end():]


# 매물 유형 힌트(제목 기준). 최종 판정은 LLM 라벨이 맡고, 여기서는 가격 해석에만 쓴다.
_WANTED_RE = re.compile(
    r"구합니다|구해요|구해\s*봅|구함|구매\s*합니다|구매해\s*봅|구매\s*원|구매\s*희망|구매하고|구매\s*중|"
    r"구입\s*합니다|구입해요|구입\s*원|구입해\s*봅|삽니다|사요(?![가-힣])|매입|찾습니다|찾고\s*있|찾아\s*봅|"
    r"구해\s*봐|구매\s*요청|살게요"
)
_EXCHANGE_RE = re.compile(r"교환|트레이드")
_SELL_RE = re.compile(r"판매|팝니다|팔아요|팔아봅|팝니당|처분|급처|양도|정리")
_NONSALE_RE = re.compile(r"사기|테스트|공지|조심|신고|주의\s*하|무료\s*나눔|나눔")
_NONGEAR_RE = re.compile(r"회원권|시즌권|리프트권|이용권|강습권|숙박|콘도|리프트\s*티켓|발렌시아가|무전기")


def listing_hints(title) -> dict:
    """제목에서 구매글·교환글·비매물·비장비 힌트를 뽑는다(불리언만)."""
    t = norm_text(title)
    wanted = bool(_WANTED_RE.search(t))
    exch = bool(_EXCHANGE_RE.search(t))
    sell = bool(_SELL_RE.search(t))
    return {
        "wanted": wanted and not sell,
        "exchange": exch and not sell,
        "non_sale": bool(_NONSALE_RE.search(t)),
        "non_gear": bool(_NONGEAR_RE.search(t)),
    }


# 거친 카테고리(파서의 category_hint·커버리지 집계용). 최종 카테고리는 drspark.taxonomy/LLM 라벨.
_CAT_RULES: list[tuple[str, re.Pattern]] = [(c, re.compile(p, re.I)) for c, p in [
    ("other", r"회원권|시즌권|리프트권|이용권|강습권|숙박|콘도|무전기|발렌시아가"),
    ("bag", r"가방|백팩|휠\s*백|부츠\s*백|스키\s*백|캐리어|(?<![a-z])bag"),
    ("tuning", r"튜너|튜닝|왁스|다리미|바이스|스크래퍼|(?<![a-z])wax|스톤|에지\s*그라인더"),
    # 스키가 들어간 묶음 판매(스키+부츠+폴 등)는 스키로
    ("ski", r"(?=.*(?:\+|&|및|일괄|풀\s*세트|스키\s*세트|세트로|,))"
            r"(?=.*(?:스키(?!\s*(?:복|장|바지|백|가방|장갑|양말|헬멧|핼멧|고글|폴|강습|부츠|밴드|하네스))|\d{3}\s*cm))"),
    ("helmet", r"헬멧|핼멧|헐멧|helmet|친\s*가드"),
    ("goggle", r"고글|goggle|(?<![가-힣])렌즈|(?<![a-z])lens"),
    ("gloves", r"장갑|글러브|글로브|glove|벙어리|(?<![가-힣])미트(?![가-힣])"),
    ("protector", r"보호대|프로텍터|protector|신\s*가드|씬\s*가드|암\s*가드|척추|엉덩이\s*패드"),
    ("poles", r"(?<![가-힣])폴(?![가-힣])|폴대|스키\s*폴|레이싱\s*폴|카본\s*폴|조절\s*폴|(?<![a-z])poles?(?![a-z])|스틱"),
    ("apparel", r"스키복|스캐복|자켓|재킷|jacket|jkt|바지|팬츠|pants|상하의|상의|하의|팀복|데모복|데모\s*웨어|"
                r"하드\s*쉘|(?<![가-힣])쉘|미들러|베이스\s*레이어|슈트|(?<![a-z])suit|코치복|조끼|베스트|(?<![a-z])vest|"
                r"패딩|덕다운|플리스|멜빵|서스펜더|(?<![가-힣])빕|모글복|넥\s*워머|바라클라바|양말|아노락|후드|레깅스|"
                # 의류 전문 브랜드(이 브랜드의 '데모'는 데모복)
                r"(?<![가-힣])카브(?!링)|(?<![a-z])karv|온요네|욘요네|onyone|데상트|데쌍트|descente|에너지아\s*푸라|energiapura|"
                r"골드윈|goldwin|스파이더|spyder|콜마|colmar|퓨잡|fusalp|주이카|글렌\s*스키|큐스|kjus|말로야|maloja|아크테릭스|arc'?teryx|arcteryx|노스페이스"),
    ("snowboard", r"스노우?\s*보드|snowboard|(?<![가-힣])보드(?![가-힣])"),
    # 부츠 액세서리·발열·건조기 등(포함/장착 문맥은 제외)
    ("other", r"(?:스트랩|스트렙|strap|하네스|harness|스키\s*밴드|힙색|파우치|부스터|booster|열선|발열|썰믹|therm-?ic|"
              r"건조기|드라이어|dryer|액션\s*캠|인솔|깔창|insole)(?!\s*(?:포함|장착|교체|드림|같이|증정|서비스))"),
    ("boots", r"부츠|(?<![a-z])boots?(?![a-z])|몬도|mondo|플렉스|플랙스|(?<![a-z])flex|이너\s*부츠|혹스|hawx|"
              r"마하\s*1|mach\s*1|랩터|raptor|스피드\s*머신|speed\s*machine|s/?pro|s/?max|스키화|(?<![가-힣])보아(?![가-힣])|(?<![a-z])boa(?![a-z])|"
              r"podium|포디움|(?:rc4|도베르만|dobermann|파이어버드|firebird)\s*(?:[a-z]+\s*)?(?:1[0-4]0|[5-9]0)(?![\d])|"
              # 몬도 범위(250-255, 260/65) 또는 'NNNmm'
              r"(?<![\d.])2[2-9][05]\s*(?:mm|[-~/]\s*(?:2[2-9][05]|[0-9][05])(?![\d]))"),
    ("ski", r"스키(?!복|장|바지|부츠|백|가방|장갑|양말|헬멧|고글|폴|강습)|(?<![a-z])skis?(?![a-z])|플레이트|회전|대회전|"
            r"올라운드|올마운틴|모글|카빙|월드컵|반드컵|데모|(?<![a-z])(?:sl|gs|fis)(?![a-z])|레이스\s*타이거|"
            r"racetiger|redster|레드스터|(?<![a-z])[sxg]9|rc4|e[-.]?sl|e[-.]?gs|tc-?s|파이어버드|firebird|"
            r"도베르만|dobermann|히어로|(?<![a-z])hero|\d{3}\s*cm|만트라|mantra"),
    ("binding", r"바인딩|binding|(?<![a-z])fks|피벗|pivot|x-?comp"),
]]


# 품목어가 없을 때 쓰는 보조 규칙(브랜드·모델명·사이즈 표기)
_CAT_FALLBACK: list[tuple[str, re.Pattern]] = [(c, re.compile(p, re.I)) for c, p in [
    ("helmet", r"mips|밉스"),
    ("goggle", r"아시안\s*핏|asian\s*fit|스쿼드|squad|4d\s*mag|포디\s*맥|컨투어|contour|메소드|(?<![a-z])method"),
    ("boots", r"(?<!\d)2[2-9][05]\s*(?:mm|사이즈)|(?<!\d)2[2-9][05]\s*[-~/]\s*(?:2[2-9][05]|[0-9][05])(?!\d)|hawks|hwax|magna|"
              r"포뮬러|formula|(?<![a-z])drs|인튜이션|intuition|테크니카|tecnica|달벨로|dalbello|랑게|(?<![a-z])lange|엣지\s*lyt|edge\s*lyt|"
              r"x\s*-?\s*pro|x\s*max|스피드\s*1[0-4]0|speed\s*1[0-4]0"),
    ("ski", r"오가사카|ogasaka|마스터|숏턴|(?<![a-z])qst|(?<![a-z])3v|(?<![a-z])rtx|curv|xsr|(?<![a-z])s[78]i?(?![a-z0-9])|"
            r"슈퍼\s*쉐이프|supershape|트라이언|아이디원\s*mr|(?<![a-z])mr[-\s]?[a-z]{1,2}(?![a-z])"),
    ("apparel", r"카브|karv|온요네|욘요네|onyone|데상트|데쌍트|descente|에너지아\s*푸라|energiapura|아크테릭스|arc'?teryx|arcteryx|"
                r"피닉스|phenix|골드윈|goldwin|스파이더|spyder|콜마|colmar|퓨잡|주이카|글렌|노스페이스|큐스|kjus|다이네즈|말로야|"
                r"미즈노"),
    ("ski", r"(?<![\d.])1[4-9]\d(?![\d])"),
    ("apparel", r"(?<![a-z0-9])(?:xs|s|m|l|xl|xxl|2xl|3xl|xo)(?![a-z0-9])|사이즈"),
]]


def rough_category(title, model_serial=None, spec=None) -> str:
    """키워드 기반 거친 상위 카테고리(schema.CATEGORIES 키 중 하나). 제목 우선, 없으면 모델·스펙, 그다음 보조 규칙."""
    texts = [norm_text(title).lower(), (norm_text(model_serial) + " " + norm_text(spec)).lower()]
    for rules in (_CAT_RULES, _CAT_FALLBACK):
        for t in texts:
            if not t.strip():
                continue
            for i, (cat, pat) in enumerate(rules):
                if rules is _CAT_RULES and i == _ACC_RULE_IDX and re.search(r"[&+]|및|포함|세트", t):
                    continue   # '부츠 & 열선'처럼 본품과 액세서리를 같이 파는 글은 본품 규칙으로
                if pat.search(t):
                    return cat
    return "other"


_ACC_RULE_IDX = next(i for i, (c, p) in enumerate(_CAT_RULES) if c == "other" and "스트랩" in p.pattern)


HARDGOODS = {"ski", "boots", "binding", "snowboard"}

# ---------------------------------------------------------------------------
# 가격
# ---------------------------------------------------------------------------

PRICE_MAN_MAX = 999            # 2..999 는 만원 단위 입력으로 본다(본문이 천원 단위를 가리키면 천원)
PRICE_HIGH_KEEP = 3_500_000    # 장비 매물에서 이보다 크면 '0 하나 더' 입력 의심
PRICE_HIGH_CHECK = 3_000_000   # 이보다 크면 확인 표시만
PRICE_ABSURD = 100_000_000     # 이상은 가격으로 보지 않는다
PRICE_LOW_HARDGOODS = 50_000   # 스키·부츠·바인딩·보드가 이보다 싸면 표시
RETAIL_GAP_MULT = 12           # 본문의 정가·구매가가 칸 값의 이 배수 이상이면 '0 하나 빠짐' 입력 의심(retail_gap)


@dataclass(frozen=True)
class PriceMention:
    value: int
    role: str   # plain | bundle | per_unit | budget | retail | shipping | discount | old
    start: int
    end: int


@dataclass(frozen=True)
class PriceResult:
    value: int | None
    kind: str             # field | field_man | field_cheon | field_fixed | body | placeholder | missing | implausible
    flags: tuple[str, ...]
    raw: int | None
    body_values: tuple[int, ...]   # 제목·본문에서 읽은 '판매가로 볼 만한' 금액들(정가·택배비 제외)


_PRICE_RE = re.compile(
    r"(?P<mc_m>(?<![\d.,])\d+(?:\.\d+)?)\s*만\s*(?P<mc_c>\d)\s*천\s*원?"
    r"|(?P<man>(?<![\d.,])\d+(?:\.\d+)?)\s*만(?![트족큼])\s*원?"
    r"|(?P<cheon>(?<![\d.,])\d+)\s*천\s*원"
    r"|(?P<grp>(?<![\d.,])\d{1,3}(?:[,.]\d{3})+)(?![\d])\s*(?P<grp_won>원(?!웨이|사이즈|피스|정|래|단|에잇))?"
    r"|(?P<plain>(?<![\d.,])\d{4,8})\s*(?:원(?!웨이|사이즈|피스|정|래|단|에잇)|씩)"
    r"|(?P<manwon>(?<![\d.십백천몇수])만\s*원(?![가-힣]{0,1}대))"
    r"|(?:가격|금액|판매가)\s*(?:은|는)?\s*[:：]?\s*(?P<bare>(?<![\d.,])\d{1,3})"
    r"(?![\d,.]|\s*(?:만|천|원|cm|mm|kg|키로|회|명|개|%|년|월|일|시|사이즈))"
)
_RETAIL_KW = re.compile(
    r"정가|신품\s*(?:판매\s*)?가|신품\s*가격|새\s*상품\s*가|새\s*제품\s*가|소비자\s*가|출고가|구매가|구입가|정상가|매장\s*가|"
    r"발매가|공홈|원가|리테일|최저가|시판|구매\s*당시|구입\s*당시|구매\s*금액|구입\s*금액|인터넷|홈페이지"
)
# '…정도에 구매'는 구매 글의 예산일 수 있어 정가로 보지 않는다. '…정도에 직구'는 판매자의 구입가다
_RETAIL_AFTER = re.compile(r"^\s*(?:에|으로|을|를)?\s*(?:구매|구입|샀|삼|주고|업어|결제)|^\s*(?:정도\s*)?(?:에|으로)?\s*직구|"
                           r"^\s*짜리|^\s*대\s*(?:상품|제품|모델)|^\s*(?:정도\s*)?하는|"
                           r"^\s*으로\s*(?:올|인상|상승|오름)")
_OLD_AFTER = re.compile(r"^\s*에서\s*\d")
_SHIP_KW = re.compile(r"택배|배송|배달|운송|착불|반값|편의점|\+\s*$")
_DISC_KW = re.compile(r"(?:네고|할인|에누리|깎|빼)\s*(?:은|는)?\s*(?:최대)?\s*$")
_DISC_AFTER = re.compile(r"^\s*(?:정도\s*)?(?:할인|빼|깎|에누리)")
_BUNDLE_KW = re.compile(r"일괄|전체|모두|세트|통으로|한\s*번에|같이|합쳐|묶음|함께|동시|합\s*$|총")
_UNIT_KW = re.compile(r"개당|장당|각각|(?<![가-힣])각\s|세트당|짝당|하나당|개별|각개")
_BUDGET_AFTER = re.compile(r"^\s*(?:대|선|정도|내외|이하|이내|까지|안쪽)?\s*(?:정도|로|으로|에)?\s*(?:생각|예산|희망|구매|구입|구합|구해|원합|찾)")


def _classify_mention(text: str, s: int, e: int, value: int) -> str:
    before = text[max(0, s - 14):s]
    after = text[e:e + 12]
    if _OLD_AFTER.search(after):
        return "old"
    if value <= 10_000 and _SHIP_KW.search(before[-8:]):
        return "shipping"
    if _RETAIL_KW.search(before) or _RETAIL_AFTER.search(after):
        return "retail"
    if _DISC_KW.search(before) or _DISC_AFTER.search(after):
        return "discount"
    if _UNIT_KW.search(before):
        return "per_unit"
    if _BUNDLE_KW.search(before):
        return "bundle"
    if _BUDGET_AFTER.search(after):
        return "budget"
    return "plain"


def extract_price_mentions(text) -> list[PriceMention]:
    """제목·본문 속 금액 표현('35만', '2만5천원', '350,000원', '가격: 60' 등)을 KRW로 읽는다."""
    t = norm_text(text)
    out: list[PriceMention] = []
    for m in _PRICE_RE.finditer(t):
        g = m.groupdict()
        try:
            if g["mc_m"]:
                v = round(float(g["mc_m"]) * 10_000) + int(g["mc_c"]) * 1_000
            elif g["man"]:
                v = round(float(g["man"]) * 10_000)
            elif g["cheon"]:
                v = int(g["cheon"]) * 1_000
            elif g["grp"]:
                v = int(re.sub(r"[,.]", "", g["grp"]))
                nxt = t[m.end():m.end() + 1]
                # '원' 없는 자리수 구분 숫자는 1000 단위로 끝날 때만 금액으로 본다('140,150' 사이즈, '20,000mm' 방수)
                if not g["grp_won"] and (v < 10_000 or v % 1000 or re.match(r"[a-zA-Z]", nxt)):
                    continue
            elif g["plain"]:
                v = int(g["plain"])
            elif g["manwon"]:
                v = 10_000
            elif g["bare"]:
                v = int(g["bare"]) * 10_000
            else:
                continue
        except ValueError:
            continue
        if v <= 0 or v >= PRICE_ABSURD:
            continue
        out.append(PriceMention(v, _classify_mention(t, m.start(), m.end(), v), m.start(), m.end()))
    return out


def _is_placeholder_number(v: int) -> bool:
    if v in (0, 1, 1000):
        return True
    s = str(v)
    if len(s) >= 3 and len(set(s)) == 1:          # 111, 1111, 9999, 999999, 7777777 ...
        return True
    if len(s) >= 4 and s in "1234567890":          # 1234, 12345, 123456
        return True
    if len(s) >= 5 and re.fullmatch(r"10+1", s):   # 1000001
        return True
    return False


_ASKING_ROLES = ("plain", "bundle", "per_unit", "budget")


def parse_price(price, title=None, body=None, *, category_hint: str | None = None) -> PriceResult:
    """가격 필드(KRW)를 해석한다. 제목·본문 금액 표현으로 단위·자리수를 검증하고, 빈 값·자리표시값을 보완한다."""
    t, b = norm_text(title), norm_text(body)
    hints = listing_hints(t)
    flags: list[str] = []
    if hints["wanted"]:
        flags.append("wanted_hint")
    if hints["exchange"]:
        flags.append("exchange_hint")
    if hints["non_sale"]:
        flags.append("non_sale_hint")
    if hints["non_gear"]:
        flags.append("non_gear_hint")

    mentions = extract_price_mentions(t + "\n" + b)
    asking = [m for m in mentions if m.role in _ASKING_ROLES]
    ask_vals = tuple(_uniq(m.value for m in asking))
    all_vals = {m.value for m in mentions}
    retail_vals = {m.value for m in mentions if m.role == "retail"}

    def from_body(fallback_kind: str):
        plain = _uniq(m.value for m in asking if m.role in ("plain", "budget"))
        bundle = _uniq(m.value for m in asking if m.role == "bundle")
        unit = _uniq(m.value for m in asking if m.role == "per_unit")
        if bundle:
            f = ["body_bundle"] + (["multi_price"] if plain or unit or len(bundle) > 1 else [])
            return bundle[-1], "body", f
        if len(plain) == 1:
            return plain[0], "body", (["body_per_unit_also"] if unit else [])
        if len(plain) > 1:
            return None, fallback_kind, ["multi_price"]
        if unit:
            return unit[0], "body", ["body_per_unit"]
        return None, fallback_kind, []

    raw = None if _missing(price) else int(round(float(price)))
    if raw is None:
        v, kind, f = from_body("missing")
        flags += f
    elif _is_placeholder_number(raw):
        v, kind, f = from_body("placeholder")
        flags += ["placeholder_raw"] + f
    elif raw <= PRICE_MAN_MAX:
        body_exchange = (bool(re.search(r"교환\s*(?:을\s*)?(?:원|희망|하실|해\s*주실|합니다|하고\s*싶|해요|부탁)", b))
                         and not _SELL_RE.search(t + b))
        if hints["exchange"] or hints["non_sale"] or body_exchange:
            v, kind, f = from_body("placeholder")
            flags += ["placeholder_raw"] + f + (["exchange_in_body"] if body_exchange else [])
        else:
            man, cheon = raw * 10_000, raw * 1_000
            if man in all_vals:
                v, kind = man, "field_man"
                flags.append("unit_body_confirmed")
            elif cheon in all_vals:
                v, kind = cheon, "field_cheon"
                flags.append("unit_body_confirmed")
            elif man > PRICE_HIGH_KEEP:         # 908 → 908만원 같은 값은 단위로 설명되지 않는다
                v, kind = None, "implausible"
                flags.append("man_unit_too_high")
            else:
                v, kind = man, "field_man"
                flags.append("unit_assumed")
    elif raw >= PRICE_ABSURD:
        v, kind, f = from_body("implausible")
        flags += ["absurd_raw"] + f
    else:
        v, kind = raw, "field"
        r = raw % 10_000
        if raw >= 50_000 and 0 < r < 100:          # 240007, 380050 같은 끝자리 잡음
            v, kind = raw - r, "field_fixed"
            flags.append("trailing_noise_fixed")
        if v > PRICE_HIGH_KEEP and not hints["non_gear"]:
            if v in all_vals:
                flags.append("high_body_confirmed")
            else:
                bv, _, bf = from_body("x")
                if bv is not None and bv <= PRICE_HIGH_KEEP:
                    v, kind = bv, "body"
                    flags += ["high_body_override"] + bf
                elif v % 10 == 0:
                    v, kind = v // 10, "field_fixed"
                    flags.append("extra_zero_fixed")
                else:
                    v, kind = None, "implausible"
        elif v > PRICE_HIGH_CHECK:
            flags.append("high_check")

    if v is not None:
        if v < 10_000:
            flags.append("very_low")
        if category_hint in HARDGOODS and v < PRICE_LOW_HARDGOODS and not hints["wanted"]:
            flags.append("suspect_low")
        if v in retail_vals and v not in ask_vals:
            flags.append("equals_retail")
        # 칸 값이 본문의 정가·구매가보다 한 자리 이상 작다: 0 하나 빠진 입력 의심(고치지 않고 학습에서만 뺀다)
        if kind.startswith("field") and retail_vals and max(retail_vals) >= RETAIL_GAP_MULT * v:
            flags.append("retail_gap")
        if kind.startswith("field"):
            if ask_vals:
                if v in ask_vals:
                    flags.append("body_agrees")
                elif len(ask_vals) == 1:
                    flags.append("body_differs")
            if len(ask_vals) > 1:
                flags.append("body_multi_price")
    return PriceResult(v, kind, tuple(_uniq(flags)), raw, ask_vals)


# ---------------------------------------------------------------------------
# 시즌
# ---------------------------------------------------------------------------

# 단일 연도 Y의 기본 해석: 시즌 시작 = Y + SINGLE_YEAR_OFFSET.
# 2026-03..10 관측분에서 단일 연도 release_year와 제목·모델·본문의 명시적 두 해 표기가 함께 있는 131행을
# 대조한 결과(02_parsing.md 표 S-1) Y≤2025는 'Y = 시작 연도'(오프셋 0)가 96:15로 우세했고,
# Y≥관측 연도(2026)는 'Y-1'이 17:1로 우세했다(2026 → 25/26). 그래서 기본 오프셋은 0,
# 관측 연도 이상인 Y만 Y-1로 내린다.
SINGLE_YEAR_OFFSET = 0
REF_YEAR_DEFAULT = 2026   # 관측 연도(데이터 기간 2026-03-05..2026-10-16)
MIN_SEASON, MAX_SEASON = 2005, 2026


def single_year_to_start(y: int, ref_year: int | None = None) -> int:
    ref = ref_year or REF_YEAR_DEFAULT
    if y >= ref:
        return y - 1
    return y + SINGLE_YEAR_OFFSET


def _y2(d: int) -> int:
    return 2000 + d if d <= 40 else 1900 + d


@dataclass(frozen=True)
class _STok:
    pos: int
    kind: str        # two_year | single | date
    value: int       # two_year: 시작 연도, single/date: 연도
    month: int | None
    flags: tuple[str, ...]


@dataclass(frozen=True)
class SeasonResult:
    start: int | None            # 시즌 시작 연도(24/25 → 2024)
    kind: str                    # two_year | single_year | model_code | none
    source: str                  # release_year | title | model_serial | spec | body | none
    flags: tuple[str, ...]
    candidates: tuple[int, ...]  # 모든 출처에서 읽은 시작 연도 후보(우선순위 순)

    @property
    def label(self) -> str | None:
        if self.start is None:
            return None
        return f"{self.start % 100:02d}/{(self.start + 1) % 100:02d}"


_RE_SIX = re.compile(r"(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)")
_RE_FULL = re.compile(r"(?<!\d)((?:19|20)\d{2})\s*([/\-~._,]|\s)\s*((?:19|20)\d{2}|\d{2})(?!\d)")
_RE_YM_KO = re.compile(r"(?<!\d)((?:19|20)?\d{2})\s*년\s*(\d{1,2})\s*월")
_RE_YM4 = re.compile(r"(?<!\d)((?:19|20)\d{2})\s*[./\-]\s*(\d{1,2})(?:\s*[./\-]\s*\d{1,2})?(?!\d)")
_RE_YM2 = re.compile(r"(?<![\d.])(\d{2})\s*\.\s*(\d{1,2})(?:\s*\.\s*\d{1,2})?(?![\d])")
_RE_PAIR = re.compile(r"(?<![\d.a-z])(\d{2})\s*([/\-~._,]|\s)?\s*(\d{2})(?![\d])")
_RE_Y4 = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")
_RE_Y2 = re.compile(r"(?<![\d.a-z])(\d{2})(?!\d)\s*(년도|년식|년형|연도|년|시즌|season|모델|fw|f/w|정도|쯤|추정|이후|이상|부터)?")
_RE_RELDUR = re.compile(r"^\s*년\s*(?:전|째|간|동안|이내|가까이)")
_RE_BOUND_MIN = re.compile(r"^\s*(?:년도?|시즌|모델)?\s*(?:이후|이상|부터|~\s*$|\+)")
_UNCERTAIN_RE = re.compile(r"\?|추정|정도|쯤|아마|인듯|인\s*것|같|대략|약\s")
_UNKNOWN_RE = re.compile(r"모름|모르|몰라|기억|미상|미정|불명|잘\s*모|무관|상관")
_RELATIVE_RE = re.compile(r"이번\s*시즌|올\s*시즌|올해|금년|신상|최신|작년|지난\s*시즌|저번\s*시즌|\d+\s*년\s*전")


def _season_tokens(text: str, *, cell: bool) -> list[_STok]:
    """문자열에서 시즌·연도 토큰을 위치 순으로 뽑는다. cell=True는 release_year 칸(오타 보정·단독 2자리 허용)."""
    s = text.lower()
    toks: list[_STok] = []

    for m in list(_RE_SIX.finditer(s)):           # 242526 → 24/25, 25/26
        a, b, c = (int(x) for x in m.groups())
        if b == a + 1 and c == b + 1:
            toks += [_STok(m.start(), "two_year", _y2(a), None, ("multi_compact",)),
                     _STok(m.start() + 2, "two_year", _y2(b), None, ("multi_compact",))]
            s = _mask(s, m)

    for m in list(_RE_FULL.finditer(s)):          # 2025/2026, 2024/25, 2021~2022
        a = int(m.group(1))
        g3 = m.group(3)
        b2 = int(g3[-2:])
        if b2 == (a + 1) % 100:
            toks.append(_STok(m.start(), "two_year", a, None, ()))
            s = _mask(s, m)
        elif len(g3) == 4 and int(g3) == a and cell:
            toks.append(_STok(m.start(), "single", a, None, ("same_year_typo",)))
            s = _mask(s, m)

    for m in list(_RE_YM_KO.finditer(s)):         # 25년 2월, 2025년 10월
        y, mo = m.group(1), int(m.group(2))
        if 1 <= mo <= 12:
            yy = int(y) if len(y) == 4 else _y2(int(y))
            toks.append(_STok(m.start(), "date", yy, mo, ()))
            s = _mask(s, m)

    for rx in (_RE_YM4, _RE_YM2):                 # 2025.7, 2024.08, 25.12, 2026/9
        for m in list(rx.finditer(s)):
            y, mo = m.group(1), int(m.group(2))
            yy = int(y) if len(y) == 4 else _y2(int(y))
            if not 1 <= mo <= 12:
                continue
            if mo == (yy + 1) % 100:              # 11.12 같은 연속 두 해는 시즌으로 둔다
                continue
            if rx is _RE_YM2 and not cell and not re.match(r"\s*월", s[m.end():m.end() + 2]):
                continue                           # 제목·본문의 '25.12'는 '월'이 붙을 때만
            toks.append(_STok(m.start(), "date", yy, mo, ()))
            s = _mask(s, m)

    for m in list(_RE_PAIR.finditer(s)):          # 25/26, 2526, 24-25, 24 25, 오타
        g1, sep, g3 = m.group(1), m.group(2), m.group(3)
        a, b = int(g1), int(g3)
        compact = sep is None
        if compact and g1 == "20" and b <= 30:    # 2021, 2025 같은 4자리 연도는 단일 연도 단계로
            continue
        if compact and g1 == "19" and b >= 80:
            continue
        if b == (a + 1) % 100 and (cell or 5 <= a <= 27):
            toks.append(_STok(m.start(), "two_year", _y2(a), None, ()))
            s = _mask(s, m)
            continue
        if not cell:
            continue
        if compact and (len(set(g1 + g3)) == 1 or (g1 + g3) in "0123456789"):    # 1111, 2222, 1234
            s = _mask(s, m)
            continue
        if a == b:
            toks.append(_STok(m.start(), "single", _y2(a), None, ("same_year_typo",)))
        elif sep in ("~", "-") and a + 1 < b <= a + 10:
            toks.append(_STok(m.start(), "two_year", _y2(a), None, ("range",)))
        elif 5 <= a <= 27:
            toks.append(_STok(m.start(), "two_year", _y2(a), None, ("typo",)))
        elif 6 <= b <= 28:
            toks.append(_STok(m.start(), "two_year", _y2(b - 1), None, ("typo",)))
        else:
            continue
        s = _mask(s, m)

    for m in list(_RE_Y4.finditer(s)):            # 2025, 2021
        y = int(m.group(1))
        if 1980 <= y <= 2030:
            fl = ("ambiguous_2021",) if y == 2021 else ()
            nxt = s[m.end():m.end() + 8]
            if re.match(r"\s*년?\s*(?:식|형|모델|년식)", nxt):
                fl += ("model_suffix",)
            elif re.match(r"\s*년?\s*(?:시즌|season)", nxt):
                fl += ("season_word",)
            toks.append(_STok(m.start(), "single", y, None, fl))
            s = _mask(s, m)

    short = cell and len(re.sub(r"[\s?.,~!\-()ㅋㅎ]", "", s)) <= 10 and not re.search(r"[a-z]", s)
    for m in list(_RE_Y2.finditer(s)):            # 25, 24년, 25시즌, 24정도
        d, suf = int(m.group(1)), m.group(2)
        if _RE_RELDUR.match(s[m.end(1):m.end(1) + 6]):
            continue
        if d == 0 or 41 <= d <= 79:
            continue
        if not cell and suf not in ("년식", "년형", "모델", "시즌"):
            continue
        if cell and not suf and not short:
            continue
        nxt = s[m.end():m.end() + 4]
        if re.match(r"\s*(?:cm|mm|m(?![a-z])|사이즈|size|kg|회|번|만|천|원|%|l(?![a-z])|인치|살|세|월|일|개|시(?!즌))", nxt):
            continue
        fl = ("season_word",) if suf in ("시즌", "season") else ("model_suffix",) if suf in ("년식", "년형", "모델") else ()
        toks.append(_STok(m.start(), "single", _y2(d), None, fl))
        s = _mask(s, m)

    out = []
    for tk in sorted(toks, key=lambda x: x.pos):
        after = text.lower()[tk.pos:tk.pos + 16]
        extra = ("bound_min",) if _RE_BOUND_MIN.search(re.sub(r"^[\d/\-~._, ]+", "", after)) else ()
        out.append(_STok(tk.pos, tk.kind, tk.value, tk.month, tk.flags + extra))
    return out


_KARV_CTX = re.compile(r"카브|karv|carv", re.I)
_KARV_CODE = re.compile(r"(?<![a-z])c(?:pm|hs|sm|dj|dt|hp|pt|pp|rm|sw|om|bp|sj|hj|dp)\s?-?(\d{2})(?:[a-z]{1,3})?(?![0-9])", re.I)


def _resolve(tok: _STok, ref: int) -> tuple[int, tuple[str, ...]]:
    if tok.kind == "two_year":
        return tok.value, tok.flags
    if tok.kind == "date":
        start = tok.value if (tok.month or 0) >= 7 else tok.value - 1
        return start, tok.flags + ("date_like",)
    start = single_year_to_start(tok.value, ref)
    fl = tok.flags + (("future_year_shift",) if tok.value >= ref else ())
    return start, fl


def single_year_value(release_year) -> int | None:
    """release_year 칸이 단일 연도만 담고 있으면 그 연도(Y)를, 아니면 None. 오프셋 검증용."""
    toks = _season_tokens(norm_text(release_year), cell=True)
    if len(toks) == 1 and toks[0].kind == "single" and "same_year_typo" not in toks[0].flags:
        return toks[0].value
    return None


def explicit_two_year_starts(text) -> set[int]:
    """제목·본문 등에서 명시적인 두 해 표기(25/26, 2526, 2024-25 …)의 시작 연도 집합."""
    return {t.value for t in _season_tokens(norm_text(text), cell=False) if t.kind == "two_year"}


def parse_season(release_year, title=None, model_serial=None, body=None, *,
                 spec=None, observed_year: int | None = None) -> SeasonResult:
    """시즌 시작 연도를 정한다(24/25 → 2024).

    우선순위: release_year의 두 해 표기 > 제목 > 모델 > 카브 모델코드 > release_year 단일 연도·날짜
    > 본문 두 해 표기 > 제목·모델의 단일 연도(접미어 '년식·모델·시즌' 또는 4자리) > 본문 '년식·모델'.
    """
    ref = observed_year or REF_YEAR_DEFAULT
    ry, ti, ms, bo, sp = (norm_text(x) for x in (release_year, title, model_serial, body, spec))
    ry_t = _season_tokens(ry, cell=True)
    ti_t = _season_tokens(ti, cell=False)
    ms_t = _season_tokens(ms, cell=False)
    bo_t = _season_tokens(bo, cell=False)

    def two(toks):
        return [t for t in toks if t.kind == "two_year"]

    def single(toks):
        return [t for t in toks if t.kind == "single"]

    def dates(toks):
        return [t for t in toks if t.kind == "date"]

    # 카브(KARV) 모델코드: CPM26A·CHS25 등 숫자 2자리 = 시즌 끝 연도(CPM26A → 25/26)
    code_vals: list[tuple[int, tuple[str, ...]]] = []
    code_src = "model_serial"
    if _KARV_CTX.search(" ".join((ti, ms, sp, ry, bo[:200]))):
        for src_name, txt in (("title", ti), ("model_serial", ms), ("release_year", ry), ("spec", sp)):
            for m in _KARV_CODE.finditer(txt):
                if not code_vals:
                    code_src = src_name
                code_vals.append((2000 + int(m.group(1)) - 1, ("karv_code",)))

    def res(items):
        return [_resolve(t, ref) for t in sorted(items, key=lambda x: x.pos)]

    _corrected = ("typo", "range")
    ry_clean = [t for t in two(ry_t) if not set(t.flags) & set(_corrected)]
    ry_fixed = [t for t in two(ry_t) if set(t.flags) & set(_corrected)]
    ladder: list[tuple[str, str, list]] = [
        ("two_year", "release_year", res(ry_clean)),
        ("two_year", "title", res(two(ti_t))),
        ("two_year", "model_serial", res(two(ms_t))),
        ("two_year", "release_year", res(ry_fixed)),     # 오타 보정한 release_year는 제목·모델의 명시 표기보다 뒤
        ("model_code", code_src, code_vals),
        ("single_year", "release_year", res(single(ry_t) + dates(ry_t))),
        ("two_year", "body", res(two(bo_t))),
        ("single_year", "title", res(single(ti_t))),
        ("single_year", "model_serial", res(single(ms_t))),
        ("single_year", "body", res([t for t in single(bo_t) if "model_suffix" in t.flags])),
    ]

    resolved_all: list[int] = []
    chosen = None
    for kind, src, vals in ladder:
        resolved_all += [v for v, _ in vals]
        if chosen is None and vals:
            chosen = (kind, src, vals)

    flags: list[str] = []
    if chosen is None:
        if _UNKNOWN_RE.search(ry):
            flags.append("unknown_stated")
        if _RELATIVE_RE.search(ry):
            flags.append("relative_expr")
        if ry and not re.search(r"\d", ry) and not flags:
            flags.append("no_digits")
        return SeasonResult(None, "none", "none", tuple(flags), ())

    kind, src, vals = chosen
    start, f0 = vals[0]
    flags += list(f0)
    distinct = _uniq(v for v, _ in vals)
    if len(distinct) > 1:
        flags.append("multi_season")
    if src != "release_year":
        ry_single = [v for v, _ in res(single(ry_t) + dates(ry_t))]
        if ry_single and ry_single[0] != start:
            flags.append("ry_single_overridden")
    # 다른 출처의 명시적 두 해 표기와 어긋나는지
    for src2, toks2 in (("release_year", ry_t), ("title", ti_t), ("model_serial", ms_t)):
        vs = {t.value for t in two(toks2)}
        if src2 != src and vs and start not in vs:
            flags.append(f"conflict_{src2}")
    if _UNCERTAIN_RE.search(ry) and src == "release_year":
        flags.append("uncertain")
    if start > MAX_SEASON or start < MIN_SEASON:
        flags.append("implausible")
    return SeasonResult(start, kind, src, tuple(_uniq(flags)), tuple(_uniq(resolved_all)))


# ---------------------------------------------------------------------------
# 길이·사이즈
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LengthResult:
    ski_length_cm: int | None = None
    ski_lengths_cm: tuple[int, ...] = ()
    radius_m: float | None = None
    waist_mm: int | None = None
    pole_length_cm: int | None = None
    pole_range_cm: tuple[int, int] | None = None
    boot_mondo_mm: int | None = None              # 범위는 아래값(260/265 → 260)
    boot_mondo_range_mm: tuple[int, int] | None = None
    boot_flex: int | None = None
    boot_last_mm: float | None = None
    boot_last_class: str | None = None            # LV/MV/HV 등 표기
    boot_bsl_mm: int | None = None                # 아웃솔 길이(몬도와 구별)
    apparel_size: str | None = None               # 국제 표기로 정규화(XS..4XL, FREE)
    apparel_size_raw: str | None = None           # JP O/XO 등 원 표기
    apparel_sizes: tuple[str, ...] = ()
    apparel_kr: int | None = None                 # KR 85..120
    apparel_eu: int | None = None                 # EU 44..58
    helmet_size: str | None = None
    helmet_cm: tuple[int, int] | None = None
    candidates: tuple[tuple[str, float], ...] = ()
    flags: tuple[str, ...] = ()


_LEN_NOISE = [
    re.compile(r"\[(?:phone|url|email|id)\]", re.I),
    re.compile(r"(?<![\d.,])\d+(?:\.\d+)?\s*만\s*(?:\d\s*천)?\s*원?"),
    re.compile(r"(?<![\d.,])\d+\s*천\s*원"),
    re.compile(r"(?<![\d.,])\d{1,3}(?:,\d{3})+\s*원?"),
    re.compile(r"(?<![\d.,])\d{4,8}\s*원"),
    re.compile(r"(?<!\d)(?:19|20)\d{2}\s*년?\s*\d{1,2}\s*월(?:\s*\d{1,2}\s*일)?"),
    re.compile(r"(?<!\d)\d{1,2}\s*월\s*\d{1,2}\s*일"),
]
_RE_SIDECUT = re.compile(r"(?<![\d.])(\d{2,3})\s*[-/.]\s*(\d{2,3})\s*[-/.]\s*(\d{2,3})(?![\d])\s*(?:mm)?")
_RE_LEN_RAD = re.compile(r"(?<![\d.])(1[0-9]\d|20\d)\s*(?:cm)?\s*/\s*(\d{1,2}(?:\.\d{1,2})?)\s*(?:m(?![a-z])|미터)?(?![\d])")
_RE_RAD_KW = re.compile(r"(?:(?<![a-z])r|반경|회전\s*반경|radius|라디우스|(?<![a-z])rad)\s*[:=]?\s*(\d{1,2}(?:\.\d{1,2})?)\s*(?:m(?![a-z])|미터)?(?![\d])")
_RE_RAD_M = re.compile(r"(?<![\d.])(\d{1,2}(?:\.\d{1,2})?)\s*(?:m|미터)(?![a-z])")
_RE_BSL = re.compile(r"(?:아웃\s*[쉘셀]|아웃솔|out\s*shell|outsole|bsl|솔\s*길이)\s*[:=]?\s*(\d{3})")
_RE_LAST = re.compile(r"(?:last|라스트|발볼|볼\s*넓이|볼폭|width)\s*[:=]?\s*(\d{2,3}(?:\.\d)?)(?![\d])\s*(?:mm)?")
_RE_LAST2 = re.compile(r"(?<![\d.])(\d{2,3}(?:\.\d)?)\s*(?:mm)?\s*(?:라스트|last)")
_HEIGHT_BEFORE = re.compile(r"(?<![가-힣])키|신장|height|기장|(?<![가-힣])팔|어깨|가슴|몸무게|체중")
_RE_MM2 = re.compile(r"(?<![\d.\-/])(\d{2,3}(?:\.\d)?)\s*mm(?![a-z])")
_RE_LAST_CLASS = re.compile(r"(?<![a-z])(lv|mv|hv|lw|hw|nw|ww|sw)(?![a-z])")
_RE_FLEX_KW = re.compile(r"(?:flex|플렉스|플랙스|플렉|플랙)\s*[:=]?\s*(\d{2,3})(?!\d)")
_RE_FLEX_KW2 = re.compile(r"(?<![\d.])(\d{2,3})\s*(?:flex|플렉스|플랙스|플렉|플랙)")
_BOOT_MODEL = (r"hawx|혹스|prime|ultra|magna|mach\s*1|마하\s*1|마하원|(?<![a-z])rs|(?<![a-z])zb|(?<![a-z])zj|(?<![a-z])zc|"
               r"(?<![a-z])drs|s/?pro|s/?max|s/?race|alpha|speed\s*machine|스피드\s*머신|(?<![a-z])hv|(?<![a-z])mv|"
               r"(?<![a-z])lv|raptor|랩터|formula|포뮬러|remedy|레메디|cochise|(?<![a-z])x|(?<![a-z])rc4|rc\s*one|"
               r"pursuit|allspeed|(?<![a-z])gt|(?<![a-z])sx|(?<![a-z])rx|(?<![a-z])lx|evo|(?<![a-z])wc|world\s*cup")
_RE_FLEX_MODEL = re.compile(rf"(?:{_BOOT_MODEL})\s*[-]?\s*(\d{{2,3}})(?![\d])")
_RE_MONDO_RANGE = re.compile(r"(?<![\d.])(\d{3})\s*(?:mm)?\s*([-~/.,]|\s)\s*(\d{2,3})(?![\d])\s*(mm|사이즈|size)?")
_RE_MONDO_UNIT = re.compile(r"(?<![\d.])(\d{3})\s*(mm|미리|사이즈|size|몬도)")
_RE_MONDO_PT = re.compile(r"(?<![\d.])(1[89]|2\d|3[01])\.([05])(?![\d])(?!\s*(?:m(?![a-z])|미터|cm))")
_RE_CM = re.compile(r"(?<![\d.])(\d{2,3})\s*(?:cm|센치|센티|씨엠)")
_RE_L_EQ = re.compile(r"(?<![a-z])l\s*[=:]\s*(\d{3})")
_RE_POLE_KW = re.compile(r"(?:폴|pole|스틱)[^\d]{0,8}(\d{2,3})(?:\s*(?:[-~]\s*(\d{2,3})))?|(\d{2,3})\s*(?:cm)?\s*(?:폴|pole)")
_RE_RANGE2 = re.compile(r"(?<![\d.])(\d{2,3})\s*(?:cm)?\s*[-~]\s*(\d{2,3})(?![\d])\s*(?:cm)?")
_RE_WAIST = re.compile(r"(?:허리|waist|웨이스트)\s*[:=]?\s*(\d{2,3})\s*(?:mm)?(?!\s*(?:인치|inch|in\b))")
_SIZE_TOKEN = (r"xxxxl|xxxl|xxl|[2-5]\s?xl|xl|xs|xxs|[2-5]x(?![a-z])|xxo|2xo|xo|o|s|m|l|"
               r"free\s*size|free|프리\s*사이즈|프리|원\s*사이즈|one\s*size")
_RE_SIZE = re.compile(rf"(?<![a-z0-9/])({_SIZE_TOKEN})(?![a-z0-9])")
_SIZE_PARTICLE = re.compile(r"사이즈|싸이즈|입니|이고|이며|이에|예요|임(?![가-힣])|으로|로(?![가-힣])|이나|정도|핏|>|->")
_RE_SIZE_COMBO = re.compile(r"(?<![a-z0-9])(xs|s|m|l|xl)\s*[-/~]\s*(s|m|l|xl|xxl|2xl)(?![a-z0-9])|(?<![a-z0-9])(sm|ml|lxl)(?![a-z0-9])")
_COMBO_PACKED = {"sm": ("s", "m"), "ml": ("m", "l"), "lxl": ("l", "xl")}
_RE_SIZE_KO = re.compile(r"(투|2|쓰리|3)?\s*(엑스\s*라지|엑스라지|엑라|엑스\s*스몰)|라지|미디움|미듐|미디엄|엠\s*사이즈|스몰")
_SIZE_NORM = {"xs": "XS", "xxs": "XXS", "s": "S", "m": "M", "l": "L", "xl": "XL", "xxl": "2XL", "2xl": "2XL",
              "xxxl": "3XL", "3xl": "3XL", "xxxxl": "4XL", "4xl": "4XL", "5xl": "5XL", "2x": "2XL", "3x": "3XL",
              "4x": "4XL", "o": "XL", "xo": "2XL", "xxo": "3XL", "2xo": "3XL"}
_RE_SIZE_PAREN = re.compile(r"(?<![a-z0-9])(xxxl|xxl|[2-4]xl|xl|xs|s|m|l|o|xo)\s*\(\s*(\d{2,3})\s*(?:cm)?\s*(?:[~\-]\s*(\d{2,3}))?\s*(?:cm)?\s*\)")
_RE_NUM_PAREN_SIZE = re.compile(r"(?<![\d.])(\d{2,3})\s*\(\s*(xxxl|xxl|[2-4]xl|xl|xs|s|m|l|o|xo)\b")
_RE_KR = re.compile(r"(?:kr|국내|한국|korea)\s*(\d{2,3})|(\d{2,3})\s*(?:호|사이즈)")
_RE_BARE = re.compile(r"(?<![a-z\d.,+])(\d{2,3}(?:\.\d)?)(?![a-z\d%,]|\.\d|\s*(?:만|천|원(?!웨이)|년|시즌|시간|분(?![가-힣])|켤레|kg|키로|인치|리터|km|%|"
                      r"(?:회|번|월|일|개|대|장|벌|명|살|세|위|등|차|층|평|도)(?![가-힣])))")


def _size_letter(tok: str) -> tuple[str, str] | None:
    t = re.sub(r"\s+", "", tok.lower())
    if t in ("free", "freesize", "프리", "프리사이즈", "원사이즈", "onesize"):
        return "FREE", "FREE"
    if t in _SIZE_NORM:
        return _SIZE_NORM[t], t.upper()
    return None


def parse_lengths(text, category_hint: str | None = None, *, bare_numbers: bool = True) -> LengthResult:
    """스키 길이·반경·허리폭, 폴 길이, 부츠 몬도·플렉스·라스트, 의류·헬멧 사이즈를 읽는다.

    단위·키워드가 붙은 값은 힌트 없이도 채운다. 단위 없는 숫자는 category_hint로만 해석하고,
    힌트가 없으면 candidates로만 돌려준다(165 → 스키 길이 후보, 265 → 몬도 후보).
    """
    s = norm_text(text).lower()
    for rx in _LEN_NOISE:
        s = rx.sub(lambda m: " " * len(m.group(0)), s)
    for tk in _season_tokens(s, cell=False):      # 2526, 24/25, 2025 같은 시즌·연도 숫자 제거
        if tk.kind == "two_year":
            m = re.compile(r"(?:(?:19|20)?\d{2}\s*[/\-~._,]?\s*(?:19|20)?\d{2})").match(s, tk.pos)
            if m:
                s = _mask(s, m)
    s = re.sub(r"(?<!\d)(?:19|20)\d{2}(?!\d)", lambda m: " " * len(m.group(0)), s)
    hint = category_hint
    o: dict = {}
    flags: list[str] = []
    cands: list[tuple[str, float]] = []
    ski_lens: list[int] = []
    sizes: list[tuple[str, str]] = []
    boot_ctx = hint == "boots" or bool(re.search(r"부츠|boot|몬도|mondo|플렉스|플랙스|flex", s))
    pole_ctx = hint == "poles" or bool(re.search(r"폴(?![가-힣])|폴대|pole|스틱", s))
    soft = hint in ("apparel", "helmet", "gloves", "protector", "goggle", "bag")

    # 1) 사이드컷(팁-허리-테일)
    for m in list(_RE_SIDECUT.finditer(s)):
        a, w, c = (int(x) for x in m.groups())
        if 95 <= a <= 200 and 55 <= w <= 130 and 80 <= c <= 190 and w < a and w < c:
            o.setdefault("waist_mm", w)
            s = _mask(s, m)
    # 2) 길이/반경 쌍(182/25, 179/19m)
    if not soft and not boot_ctx or hint in ("ski", "snowboard"):
        for m in list(_RE_LEN_RAD.finditer(s)):
            L, r = int(m.group(1)), float(m.group(2))
            if 9 <= r <= 40 and 100 <= L <= 215:
                ski_lens.append(L)
                o.setdefault("radius_m", r)
                s = _mask(s, m)
    # 3) 반경 키워드, 4) 'NNm' 단독
    if not soft:
        for rx in (_RE_RAD_KW, _RE_RAD_M):
            for m in list(rx.finditer(s)):
                r = float(m.group(1))
                if 8 <= r <= 45:
                    o.setdefault("radius_m", r)
                    s = _mask(s, m)
    # 5) 아웃솔 길이
    for m in list(_RE_BSL.finditer(s)):
        v = int(m.group(1))
        if 240 <= v <= 370:
            o.setdefault("boot_bsl_mm", v)
            s = _mask(s, m)
    # 6) 플렉스 키워드, 부츠 모델명 뒤 숫자('flex 90 last 102'에서 90을 라스트로 읽지 않도록 먼저)
    for rx in (_RE_FLEX_KW, _RE_FLEX_KW2):        # 'flex 90'을 '255 flex'보다 먼저
        for m in list(rx.finditer(s)):
            v = int(m.group(1))
            if 40 <= v <= 170:
                o.setdefault("boot_flex", v)
                s = _mask(s, m)
    if boot_ctx and "boot_flex" not in o:
        for m in list(_RE_FLEX_MODEL.finditer(s)):
            v = int(m.group(1))
            if 50 <= v <= 150 and v % 5 == 0:
                o["boot_flex"] = v
                s = s[:m.start(1)] + " " * (m.end(1) - m.start(1)) + s[m.end(1):]
                break
    # 7) 라스트
    if boot_ctx:
        for rx in (_RE_LAST, _RE_LAST2):          # 'last 102'를 '255 last'보다 먼저
            for m in list(rx.finditer(s)):
                v = float(m.group(1))
                if 90 <= v <= 110:
                    o.setdefault("boot_last_mm", int(v) if v.is_integer() else v)
                    s = _mask(s, m)
        m = _RE_LAST_CLASS.search(s)
        if m:
            o.setdefault("boot_last_class", m.group(1).upper())
    # 6-1) 'NNmm' 두 자리: 부츠면 라스트, 스키면 허리폭, 힌트 없으면 후보
    for m in list(_RE_MM2.finditer(s)):
        v = float(m.group(1))
        iv = int(v) if v.is_integer() else v
        if boot_ctx and 90 <= v <= 110:
            o.setdefault("boot_last_mm", iv)
        elif hint in ("ski", "snowboard") and 60 <= v <= 130:
            o.setdefault("waist_mm", int(v))
        elif hint is None and 60 <= v <= 130:
            cands.append(("waist_mm", v))
            if 90 <= v <= 110:
                cands.append(("boot_last_mm", v))
        else:
            continue
        s = _mask(s, m)
    # 9) 몬도 범위(260/265, 240~245, 260-65)
    if hint not in ("ski", "poles", "apparel", "helmet", "goggle"):
        for m in list(_RE_MONDO_RANGE.finditer(s)):
            a, sep, g3, unit = int(m.group(1)), m.group(2), m.group(3), m.group(4)
            b = int(g3) if len(g3) == 3 else (a // 100) * 100 + int(g3)
            if not (180 <= a <= 315 and a % 5 == 0 and b % 5 == 0 and 0 < b - a <= 20):
                continue
            if sep in (",", " ") and not (boot_ctx or unit):
                continue
            if hint == "boots" or boot_ctx or unit or (b - a == 5):
                if "boot_mondo_mm" not in o:
                    o["boot_mondo_mm"], o["boot_mondo_range_mm"] = a, (a, b)
                elif o["boot_mondo_mm"] != a:
                    flags.append("multi_mondo")
                s = _mask(s, m)
            else:
                cands.append(("boot_mondo_mm", float(a)))
    # 10) 단위 붙은 몬도(255mm, 265사이즈), 몬도 포인트(26.5)
    for m in list(_RE_MONDO_UNIT.finditer(s)):
        v, unit = int(m.group(1)), m.group(2)
        if 180 <= v <= 310 and v % 5 == 0 and (hint in ("boots", None, "snowboard", "other") or unit in ("mm", "미리", "몬도")):
            if unit in ("사이즈", "size") and hint in ("ski", "snowboard") and v <= 215:
                continue
            if hint in ("ski", "poles", "apparel", "helmet"):
                continue
            if "boot_mondo_mm" not in o:
                o["boot_mondo_mm"] = v
            elif o["boot_mondo_mm"] != v:
                flags.append("multi_mondo")
            s = _mask(s, m)
        elif 100 <= v <= 215 and unit in ("사이즈", "size") and hint in ("ski", "snowboard"):
            ski_lens.append(v)
            s = _mask(s, m)
    if boot_ctx or hint == "boots":
        for m in list(_RE_MONDO_PT.finditer(s)):
            v = int(m.group(1)) * 10 + int(m.group(2))
            if "boot_mondo_mm" not in o:
                o["boot_mondo_mm"] = v
            s = _mask(s, m)
    # 12) 폴 길이(키워드·조절 범위)
    if pole_ctx:
        for m in list(_RE_POLE_KW.finditer(s)):
            v = int(m.group(1) or m.group(3))
            v2 = int(m.group(2)) if m.group(2) else None
            if 70 <= v <= 145:
                if v2 and v < v2 <= 150:
                    o.setdefault("pole_range_cm", (v, v2))
                else:
                    o.setdefault("pole_length_cm", v)
                s = _mask(s, m)
        for m in list(_RE_RANGE2.finditer(s)):
            a, b = int(m.group(1)), int(m.group(2))
            if 70 <= a < b <= 150 and b - a >= 15:
                o.setdefault("pole_range_cm", (a, b))
                s = _mask(s, m)
    # 13) 헬멧 둘레 범위(55-59, 54~58cm)
    if hint in (None, "helmet", "goggle", "other"):
        for m in list(_RE_RANGE2.finditer(s)):
            a, b = int(m.group(1)), int(m.group(2))
            if 48 <= a < b <= 66 and b - a <= 8:
                o.setdefault("helmet_cm", (a, b))
                s = _mask(s, m)
    # 11) cm 길이
    for rx in (_RE_L_EQ, _RE_CM):
        for m in list(rx.finditer(s)):
            v = int(m.group(1))
            if _HEIGHT_BEFORE.search(s, max(0, m.start() - 6), m.start()):   # 원 문자열 기준(앞 글자까지 보는 lookbehind)
                s = _mask(s, m)
                continue
            if hint == "helmet" or (48 <= v <= 66 and hint in (None, "goggle")):
                if 48 <= v <= 66:
                    o.setdefault("helmet_cm", (v, v))
                s = _mask(s, m)
                continue
            if hint == "apparel":
                s = _mask(s, m)
                continue
            if pole_ctx and 70 <= v <= 145 and hint != "ski":
                o.setdefault("pole_length_cm", v)
            elif 60 <= v <= 215:
                ski_lens.append(v)
            s = _mask(s, m)
    # 14) 허리폭 키워드
    if hint in (None, "ski", "snowboard", "other"):
        for m in list(_RE_WAIST.finditer(s)):
            v = int(m.group(1))
            if 60 <= v <= 130:
                o.setdefault("waist_mm", v)
                s = _mask(s, m)
    # 15) 문자 사이즈 + 괄호 숫자(M(95~100), L(59~63), 50(L))
    if hint not in ("ski", "boots", "binding", "poles", "snowboard", "tuning"):
        for m in list(_RE_SIZE_PAREN.finditer(s)):
            letter, a = m.group(1), int(m.group(2))
            if 48 <= a <= 66 and hint in (None, "helmet", "goggle", "other"):
                b = int(m.group(3)) if m.group(3) else a
                o.setdefault("helmet_cm", (a, b))
            elif 85 <= a <= 125 and a % 5 == 0:
                o.setdefault("apparel_kr", a)
            elif 44 <= a <= 58 and a % 2 == 0:
                o.setdefault("apparel_eu", a)
            elif 120 <= a <= 170:
                flags.append("junior_size")
        for m in list(_RE_NUM_PAREN_SIZE.finditer(s)):
            a = int(m.group(1))
            if 44 <= a <= 58 and a % 2 == 0:
                o.setdefault("apparel_eu", a)
            elif 85 <= a <= 125 and a % 5 == 0:
                o.setdefault("apparel_kr", a)
            s = s[:m.start(1)] + " " * len(m.group(1)) + s[m.end(1):]
        for m in list(_RE_KR.finditer(s)):
            a = int(m.group(1) or m.group(2))
            if 85 <= a <= 125 and a % 5 == 0 and hint in (None, "apparel", "other", "protector"):
                o.setdefault("apparel_kr", a)
                s = _mask(s, m)
            elif 44 <= a <= 58 and a % 2 == 0 and hint in (None, "apparel", "other"):
                o.setdefault("apparel_eu", a)
                s = _mask(s, m)
            elif 120 <= a <= 170 and hint == "apparel":
                flags.append("junior_size")
                s = _mask(s, m)
        if hint == "helmet":
            m = _RE_SIZE_COMBO.search(s)
            if m:
                a, b = (m.group(1), m.group(2)) if m.group(1) else _COMBO_PACKED[m.group(3)]
                o["helmet_size"] = f"{_SIZE_NORM.get(a, a.upper())}/{_SIZE_NORM.get(b, b.upper())}"
        for m in _RE_SIZE.finditer(s):
            tok = m.group(1)
            nxt = s[m.end():m.end() + 5]
            prv = s[max(0, m.start() - 2):m.start()]
            if re.match(r"\s*[=:]\s*\d|\s*급|/[a-z]|-?\d|\.[a-z\d]", nxt):
                continue
            if tok in ("m", "l", "s") and re.search(r"\d\s?$", prv):
                continue
            if tok in ("o", "s", "m", "l", "xo") and re.match(r"[가-힣]", nxt[:1] or " ") and not _SIZE_PARTICLE.match(nxt):
                continue
            if tok.startswith("프리") and re.match(r"[가-힣]", nxt[:1] or " "):
                continue
            if tok == "o" and not re.match(r"\s*(?:사이즈|싸이즈|size|\(|/|,|\)|$)", nxt):
                continue
            r = _size_letter(tok)
            if r:
                sizes.append(r)
        for m in _RE_SIZE_KO.finditer(s):
            g = m.group(0).replace(" ", "")
            if "스몰" in g and "엑스" in g:
                sizes.append(("XS", "XS"))
            elif "엑스" in g or "엑라" in g:
                pre = m.group(1)
                sizes.append(("3XL", "3XL") if pre in ("쓰리", "3") else ("2XL", "2XL") if pre in ("투", "2") else ("XL", "XL"))
            elif "라지" in g:
                sizes.append(("L", "L"))
            elif "스몰" in g:
                sizes.append(("S", "S"))
            else:
                sizes.append(("M", "M"))
    # 16) 단위 없는 숫자
    explicit_len = bool(ski_lens)
    if bare_numbers:
        for m in _RE_BARE.finditer(s):
            before = s[max(0, m.start() - 4):m.start()]
            if (_HEIGHT_BEFORE.search(s, max(0, m.start() - 4), m.start())
                    or re.compile(r"허리|머리|(?<![가-힣])발\s*$").search(s, max(0, m.start() - 4), m.start())):
                continue
            if re.match(r"\s*[-~]\s*\d", s[m.end():m.end() + 4]):   # '50-55' 같은 범위의 앞값은 단독 해석하지 않는다
                continue
            raw = m.group(1)
            v = float(raw)
            iv = int(v) if v.is_integer() else None
            if hint in ("ski", "snowboard"):
                if iv and 100 <= iv <= 215:
                    if explicit_len:
                        cands.append(("ski_length_cm", v))
                    else:
                        ski_lens.append(iv)
            elif hint == "boots":
                if iv and 180 <= iv <= 315 and iv % 5 == 0:
                    if "boot_mondo_mm" not in o:
                        o["boot_mondo_mm"] = iv
                    else:
                        cands.append(("boot_mondo_mm", v))
                elif iv is None and 18.0 <= v <= 31.5 and (v * 2).is_integer():
                    o.setdefault("boot_mondo_mm", int(v * 10))
                elif iv and 40 <= iv <= 150 and iv % 5 == 0:
                    if "boot_flex" not in o:
                        o["boot_flex"] = iv
                    else:
                        cands.append(("boot_flex", v))
            elif hint == "poles":
                if iv and 70 <= iv <= 145:
                    o.setdefault("pole_length_cm", iv)
            elif hint == "apparel":
                if iv and 85 <= iv <= 125 and iv % 5 == 0:
                    o.setdefault("apparel_kr", iv)
            elif hint == "helmet":
                if iv and 48 <= iv <= 66:
                    o.setdefault("helmet_cm", (iv, iv))
            elif hint is None:
                if iv is None:
                    if 18.0 <= v <= 31.5 and (v * 2).is_integer():
                        cands.append(("boot_mondo_mm", v * 10))
                    continue
                if 100 <= iv <= 215:
                    cands.append(("ski_length_cm", v))
                if 180 <= iv <= 315 and iv % 5 == 0:
                    cands.append(("boot_mondo_mm", v))
                if 50 <= iv <= 150 and iv % 10 == 0:
                    cands.append(("boot_flex", v))
                if 70 <= iv <= 140:
                    cands.append(("pole_length_cm", v))
                if 85 <= iv <= 120 and iv % 5 == 0:
                    cands.append(("apparel_kr", v))

    ski_lens = _uniq(ski_lens)
    if ski_lens:
        o["ski_length_cm"] = ski_lens[0]
        o["ski_lengths_cm"] = tuple(ski_lens)
        if len(ski_lens) > 1:
            flags.append("multi_length")
    sizes = _uniq(sizes)
    if sizes:
        if hint == "helmet":
            o.setdefault("helmet_size", sizes[0][0])
        else:
            o["apparel_size"], o["apparel_size_raw"] = sizes[0]
            o["apparel_sizes"] = tuple(x[0] for x in sizes)
            if len({x[0] for x in sizes}) > 1:
                flags.append("multi_size")
    if cands and hint is None:
        flags.append("needs_hint")
    return LengthResult(**o, candidates=tuple(_uniq(cands)), flags=tuple(_uniq(flags)))


# ---------------------------------------------------------------------------
# 사용 횟수
# ---------------------------------------------------------------------------

USES_PER_SEASON = 20      # 한 시즌 ≈ 20일(주말 스키어 기준)
USES_PER_MONTH = 8
USES_PER_WEEK = 3
USAGE_BUCKETS = ("new_unused", "le5", "le20", "one_season", "multi_season", "unknown")


@dataclass(frozen=True)
class UsageResult:
    uses: float | None    # 대략의 사용 일수(회)
    bucket: str           # new_unused | le5 | le20 | one_season | multi_season | unknown
    kind: str             # count | season | duration | hours | new | weekend | vague_few | vague_many | bare | none
    flags: tuple[str, ...]


_KNUM = {"한": 1, "단한": 1, "하": 1, "두": 2, "세": 3, "네": 4, "다섯": 5, "반": 0.5}
_U_PLACEHOLDER = re.compile(r"^[\s.\-?!,ㅇㅌㄴㄱxX_~ᄀ-ᇿ]*$|^\s*(?:모름|모르겠|무관|상관\s*없음|상관x|협|본문\S*|미정|보통|중|양호|기타|a급?|s급?|"
                            r"적당|적당히|정상|x)\s*[.!?]*\s*$")
_U_NEW = re.compile(r"미\s*사용|새\s*상품|새\s*제품|새재품|새샅품|새거|새\s*것|새옷|신품|미개봉|미착용|택\s*(?:달린|있|그대로)|텍\s*(?:달린|있)|"
                    r"사용\s*(?:x|안\s*함|안\s*했|하지\s*않)|한\s*번도\s*(?:안|사용\s*안|착용\s*안)|착용\s*(?:x|안)|^\s*무\s*(?:$|[,(/])|^없음|시착")
_U_LIKE_NEW = re.compile(r"(?:새\s*상품|새\s*제품|신품)\s*급")
_U_CNT = re.compile(r"(\d+(?:\.\d+)?)\s*(?:[~\-]\s*(\d+(?:\.\d+)?))?\s*(여)?\s*(회|번|일(?![요본주])|착용|days?)")
_U_KCNT = re.compile(r"(단\s*한|한|두|세|네|다섯)\s*(?:번|회)|(?<![가-힣])(일회|하루|이틀|사흘|열흘|보름)")
_U_BOUND = re.compile(r"^\s*(?:정도|가량|쯤|내외|전후)?\s*(미만|이하|이내|안쪽|아래|이상|넘게|넘|~)")
_U_FREQ_BEFORE = re.compile(r"(?:주|년|연|시즌\s*당|시즌\s*별|시즌당|시즌별|월|달에|회당|매주|1년에)\s*$")
_U_SEASON = re.compile(r"(\d+(?:\.\d+)?|한|두|세|네|반)\s*(?:[~\-/]\s*(\d+(?:\.\d+)?))?\s*시즌(\s*반)?")
_U_YEARS = re.compile(r"(\d+(?:\.\d+)?)\s*년(?!\s*(?:도|식|형|\d|전|째))")
_U_MONTH = re.compile(r"(\d+(?:\.\d+)?|한|두|세)\s*(?:달|개월)(\s*반)?")
_U_WEEK = re.compile(r"(\d+|한|두|세|일)\s*주(?!말|\s*\d|\s*에|당)(?:일)?")
_U_HOURS = re.compile(r"(\d+|몇)\s*시간|\d+\s*분(?![가-힣])|슬롭|슬로프|(?<![가-힣])런(?![가-힣])")
_U_SEASON_WORD = re.compile(r"이번\s*시즌|올\s*시즌|지난\s*시즌|저번\s*시즌|시즌\s*(?:초반|중반|후반|동안)|(?<![\d가-힣])시즌(?!\s*(?:당|별))")
_U_WEEKEND = re.compile(r"주말")
_U_FEW = re.compile(r"(?<!을수록\s)적음|적어요|적습|거의\s*(?:없|안|새)|많이\s*(?:사용\s*)?(?:안|못)|많지\s*않|별로\s*(?:안|없)|극히|"
                    r"횟수\s*적|사용감\s*(?:적|거의\s*없|없|미비)|몇\s*(?:번|회)|(?<![가-힣])수\s*회|드문\s*드문|가끔|조금|잠깐|최소|잘\s*안")
_U_MANY = re.compile(r"다수|많음|많이\s*(?:사용|탐|탔|입|신|씀)|여러\s*번|여러\s*시즌|다회|수십|자주|빈번|많아요|많습|몇\s*년|여러\s*해")
_U_SECONDHAND = re.compile(r"중고|장터|전\s*주인|당근")


def _bucket_from_uses(u: float) -> str:
    if u <= 0:
        return "new_unused"
    if u <= 5:
        return "le5"
    if u <= 20:
        return "le20"
    if u <= 40:
        return "one_season"
    return "multi_season"


def _bucket_from_seasons(s: float) -> str:
    if s <= 0:
        return "new_unused"
    if s < 1:
        return "le20"
    if s < 1.5:
        return "one_season"
    return "multi_season"


def _knum(x: str) -> float:
    x = re.sub(r"\s+", "", x)
    return float(_KNUM.get(x, x)) if x in _KNUM or re.fullmatch(r"\d+(?:\.\d+)?", x) else float("nan")


def parse_usage(usage_count) -> UsageResult:
    """사용 횟수 칸을 대략의 사용 일수와 구간으로 바꾼다."""
    t = norm_text(usage_count).lower().strip()
    if not t:
        return UsageResult(None, "unknown", "none", ("missing",))
    if _U_PLACEHOLDER.search(t) and not re.search(r"\d|시즌|회|번", t):
        return UsageResult(None, "unknown", "none", ("placeholder",))
    flags: list[str] = []
    if _U_SECONDHAND.search(t):
        flags.append("secondhand_prior_use_unknown")
    # 시즌·연도·날짜 표기 제거(25/26, 2526, 26시즌, 26년 1월)
    s = t
    for tk in _season_tokens(s, cell=False):
        m = re.compile(r"(?:19|20)?\d{2}\s*[/\-~._,]?\s*(?:(?:19|20)?\d{2})?(?:\s*년\s*\d{1,2}\s*월)?").match(s, tk.pos)
        if not m or m.end() <= m.start():
            continue
        tail = s[m.end():m.end() + 3]
        # '10-11회' 같은 횟수 범위는 지우지 않는다: '/'·4자리 붙임·뒤에 '시즌'이 올 때만 시즌 표기로 본다
        if tk.kind != "two_year" or "/" in m.group(0) or re.fullmatch(r"\d{4}", m.group(0).strip()) or re.match(r"\s*시즌", tail):
            s = _mask(s, m)
    s = re.sub(r"(?<!\d)\d{2,4}\s*년\s*\d{1,2}\s*월(?:\s*\d{1,2}\s*일)?|(?<!\d)\d{1,2}\s*월\s*\d{1,2}\s*일|(?<!\d)\d{1,2}\s*월(?:\s*(?:초|말|중순))?",
               lambda m: " " * len(m.group(0)), s)
    s = re.sub(r"(?<!\d)(?:1\d|2\d)\s*시즌|(?<!\d)(?:19|20)\d{2}(?!\d)", lambda m: " " * len(m.group(0)), s)

    # 1) 횟수
    counts: list[tuple[float, list[str]]] = []
    freq: list[tuple[float, list[str]]] = []
    for m in _U_CNT.finditer(s):
        is_freq = bool(_U_FREQ_BEFORE.search(s[max(0, m.start() - 5):m.start()]))
        if is_freq:
            flags.append("frequency")
        a = float(m.group(1))
        if m.group(2):
            a = (a + float(m.group(2))) / 2
            fl = ["range"]
        else:
            fl = []
        if m.group(3):
            a += 2
            fl.append("approx")
        bm = _U_BOUND.match(s[m.end():m.end() + 8])
        if bm:
            b = bm.group(1)
            if b in ("미만", "아래"):
                a = max(a - 1, 1) if a > 1 else a
                fl.append("upper_bound")
            elif b in ("이하", "이내", "안쪽"):
                fl.append("upper_bound")
            else:
                fl.append("lower_bound")
        if a > 300:
            continue
        (freq if is_freq else counts).append((a, fl))
    for m in _U_KCNT.finditer(s):
        w = (m.group(1) or m.group(2) or "").replace(" ", "")
        a = {"단한": 1, "한": 1, "두": 2, "세": 3, "네": 4, "다섯": 5, "일회": 1, "하루": 1, "이틀": 2, "사흘": 3,
             "열흘": 10, "보름": 15}.get(w)
        if a:
            counts.append((float(a), []))
    if counts:
        a, fl = counts[0]
        if len({c[0] for c in counts}) > 1:
            flags.append("multi_values")
        if a == 0:
            return UsageResult(0.0, "new_unused", "count", tuple(_uniq(flags + fl)))
        return UsageResult(a, _bucket_from_uses(a), "count", tuple(_uniq(flags + fl)))
    # 2) 시즌 수
    for m in _U_SEASON.finditer(s):
        n = _knum(m.group(1))
        if math.isnan(n) or n >= 10:
            continue
        fl = []
        if m.group(2):
            n = (n + float(m.group(2))) / 2
            fl.append("range")
        if m.group(3):
            n += 0.5
        return UsageResult(n * USES_PER_SEASON, _bucket_from_seasons(n), "season", tuple(_uniq(flags + fl)))
    if re.search(r"(?<![\d가-힣])반\s*시즌", s):
        return UsageResult(0.5 * USES_PER_SEASON, "le20", "season", tuple(flags))
    m = _U_YEARS.search(s)
    if m and float(m.group(1)) < 30:
        n = float(m.group(1))
        return UsageResult(n * USES_PER_SEASON, _bucket_from_seasons(n), "season", tuple(_uniq(flags + ["years_as_seasons"])))
    # 3) 기간
    m = _U_MONTH.search(s)
    if m:
        n = _knum(m.group(1)) + (0.5 if m.group(2) else 0)
        u = n * USES_PER_MONTH
        b = "one_season" if n >= 3 else _bucket_from_uses(u)
        return UsageResult(u, b, "duration", tuple(_uniq(flags + ["months"])))
    m = _U_WEEK.search(s)
    if m:
        n = 1.0 if m.group(1) == "일" else _knum(m.group(1))
        u = n * USES_PER_WEEK
        return UsageResult(u, _bucket_from_uses(u), "duration", tuple(_uniq(flags + ["weeks"])))
    if _U_HOURS.search(s):
        return UsageResult(1.0, "le5", "hours", tuple(flags))
    if freq:   # '시즌당 3회 미만', '년4-5회': 기간당 횟수만 있으면 그 값을 하한 근사로 쓴다
        a, fl = freq[0]
        return UsageResult(a, _bucket_from_uses(a), "count", tuple(_uniq(flags + fl + ["per_period"])))
    # 4) 새 제품
    if _U_LIKE_NEW.search(t):
        return UsageResult(None, "le5", "vague_few", tuple(_uniq(flags + ["like_new"])))
    if _U_NEW.search(t):
        fl = ["tried_on"] if "시착" in t else []
        return UsageResult(0.0, "new_unused", "new", tuple(_uniq(flags + fl)))
    # 5) 시즌 단어·주말
    if _U_SEASON_WORD.search(s):
        return UsageResult(float(USES_PER_SEASON), "one_season", "season", tuple(_uniq(flags + ["season_word"])))
    if _U_WEEKEND.search(s):
        return UsageResult(float(USES_PER_SEASON), "one_season", "weekend", tuple(_uniq(flags + ["weekend_assumed"])))
    # 6) 막연한 표현
    if _U_FEW.search(t) and "적을수록" not in t:
        b = "le5" if re.search(r"몇\s*(?:번|회)", t) else "le20"
        return UsageResult(None, b, "vague_few", tuple(_uniq(flags + ["vague"])))
    if _U_MANY.search(t):
        return UsageResult(None, "multi_season", "vague_many", tuple(_uniq(flags + ["vague"])))
    # 7) 숫자만
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(?:[~\-]\s*(\d+(?:\.\d+)?))?\s*[?]*\s*", s)
    if m:
        raw = m.group(1)
        if len(raw) >= 3 and len(set(raw)) == 1:
            return UsageResult(None, "unknown", "none", tuple(_uniq(flags + ["placeholder"])))
        a = float(raw)
        if m.group(2):
            a = (a + float(m.group(2))) / 2
        if a > 100:
            return UsageResult(None, "unknown", "none", tuple(_uniq(flags + ["placeholder"])))
        return UsageResult(a, _bucket_from_uses(a), "bare", tuple(_uniq(flags + ["bare_number"])))
    return UsageResult(None, "unknown", "none", tuple(_uniq(flags + ["unparsed"])))


# ---------------------------------------------------------------------------
# 구입 시기
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PurchaseResult:
    year: int | None
    month: int | None
    season_start: int | None   # 구입 시점이 속한 시즌(9~12월 → 그해, 1~4월 → 전해, 시즌 표기 → 그 시작 연도)
    kind: str                  # year_month | year | season | month_only | relative | none
    mentions_shop: bool        # 판매처(샵·브랜드 매장·공홈 등)를 언급했는지. 이름은 돌려주지 않는다.
    flags: tuple[str, ...]


_P_SHOP_KW = re.compile(r"샵|샾|shop|스포츠|sports?|매장|공홈|홈페이지|홈피|사이트|본사|코리아|korea|백화점|아울렛|outlet|"
                        r"(?<![가-힣])몰|쇼핑몰|스토어|store|레포츠|직영|대리점|오프라인|팝업|공식", re.I)
_P_SECONDHAND = re.compile(r"중고|장터|닥팍|닥장|닥팤|닥터\s*스파크|당근|번개|중고나라|전\s*주인")
_P_GIFT = re.compile(r"지인|선물|받음|받았|물려")
_P_NEWBUY = re.compile(r"신품|새\s*상품|새\s*제품|새거|미개봉|정품|선\s*오더|선오던|(?<![가-힣])오더|선주문|예약|이월")
_P_OVERSEAS = re.compile(r"해외|직구|일본|미국|유럽|오스트리아|독일|이탈리아|캐나다|스위스|프랑스|amazon|아마존")
_P_ONLINE = re.compile(r"온라인|인터넷|online|네이버|스마트\s*스토어|쿠팡|크림|무신사")
_P_STOP_TOKEN = re.compile(
    r"^(?:구매|구입|샀|삼|했|했음|했습니다|습니다|입니다|에서|으로|에게|부터|경|쯤|정도|초|말|중|중순|시즌|시즌초|시즌말|막시즌|"
    r"년도|년|월|일|봄|여름|가을|겨울|상품|제품|모름|기억|안남|안나|미정|무관|상관|없음|있음|협|모르|잘|정확|직접|본인|"
    r"인|소유|최신|최근|작년|올해|재작년|제작년|지난|저번|이번|올|전|후|및|등|의|에|용|이곳|여기|국내|해외|직구|지인|선물|"
    r"받음|중고|장터|신품|새상품|새제품|정품|이월|오더|선오더|온라인|인터넷|the|가|이|은|는|후기|추정|아마|약|대략|쯤에|"
    r"구매함|구입함|구매했|구입했|구매후|구입후|당시|때|개월|년전|사용|x|ㅇ|ㅡ|거래|처분|처분중|가물가물|기어|사기|조심|"
    r"모릅니다|몰라요|잘모름|불명|확인|불가|구매처|기억이|안나요|안납니다)+$")
_P_REL = re.compile(r"재작년|제작년|작년|올해|올\s|금년|(\d+)\s*년\s*전|지난\s*시즌|저번\s*시즌|이번\s*시즌|올\s*시즌")


def parse_purchased_at(purchased_at, *, observed_year: int | None = None) -> PurchaseResult:
    """구입 시기 칸에서 연·월(또는 시즌)을 읽고, 판매처 언급 여부만 불리언으로 남긴다."""
    t = norm_text(purchased_at)
    tl = t.lower()
    flags: list[str] = []
    if not tl.strip():
        return PurchaseResult(None, None, None, "none", False, ("missing",))
    for rx, f in ((_P_SECONDHAND, "secondhand"), (_P_GIFT, "gift_or_acquaintance"), (_P_NEWBUY, "bought_new"),
                  (_P_OVERSEAS, "overseas"), (_P_ONLINE, "online")):
        if rx.search(tl):
            flags.append(f)
    year = month = season = None
    kind = "none"
    toks = _season_tokens(tl, cell=True)
    dts = [x for x in toks if x.kind == "date"]
    tws = [x for x in toks if x.kind == "two_year"]
    sgl = [x for x in toks if x.kind == "single"]
    if dts:
        year, month, kind = dts[0].value, dts[0].month, "year_month"
    elif tws:
        season, kind = tws[0].value, "season"
        if re.search(r"시즌\s*초|초반", tl):
            year = season
        elif re.search(r"시즌\s*말|막\s*시즌|후반", tl):
            year = season + 1
    elif sgl:
        year, kind = sgl[0].value, "year"
        after = tl[sgl[0].pos:sgl[0].pos + 10]
        if re.search(r"년?\s*(?:초|봄)", after):
            month = 1 if "초" in after else 4
            flags.append("month_approx")
            kind = "year_month"
        elif re.search(r"년?\s*(?:말|겨울)", after):
            month = 12 if "말" in after else None
            if month:
                flags.append("month_approx")
                kind = "year_month"
    if year is None and season is None:
        rel = _P_REL.search(tl)
        if rel:
            flags.append("relative")
            kind = "relative"
            if observed_year:
                w = rel.group(0)
                if "재작년" in w or "제작년" in w:
                    year = observed_year - 2
                elif "작년" in w:
                    year = observed_year - 1
                elif rel.group(1):
                    year = observed_year - int(rel.group(1))
                elif "올" in w or "금년" in w:
                    year = observed_year if "시즌" not in w else None
    mo = re.search(r"(?<!\d)(\d{1,2})\s*월", tl)
    if month is None and mo and 1 <= int(mo.group(1)) <= 12:
        month = int(mo.group(1))
        if kind in ("none",):
            kind = "month_only"
        elif kind == "relative" or kind == "year":
            kind = "year_month" if year else kind
    if season is None and year and month:
        if month >= 9:
            season = year
        elif month <= 4:
            season = year - 1
    if year is not None and not (2000 <= year <= 2027):
        flags.append("implausible")
    # 판매처 언급 여부: 날짜·숫자·일반어를 걷어내고 남는 이름이 있으면 True
    shop = bool(_P_SHOP_KW.search(tl))
    if not shop:
        rest = re.sub(r"\d+", " ", tl)
        rest = re.sub(r"[^\w가-힣]+", " ", rest)
        for rx in (_P_SECONDHAND, _P_GIFT, _P_NEWBUY, _P_OVERSEAS, _P_ONLINE):
            rest = rx.sub(" ", rest)
        toks2 = [w for w in rest.split() if not _P_STOP_TOKEN.match(w)]
        shop = any(len(re.sub(r"[^가-힣a-z]", "", w)) >= 2 for w in toks2)
    return PurchaseResult(year, month, season, kind, shop, tuple(_uniq(flags)))


# ---------------------------------------------------------------------------
# 상태 등급
# ---------------------------------------------------------------------------

GRADE_ORD = {"미개봉": 0, "S급": 1, "A급": 2, "B급": 3, "C급": 4}


@dataclass(frozen=True)
class GradeResult:
    grade_ord: int | None        # 미개봉=0, S=1, A=2, B=3, C=4, 그 외 None
    grade: str | None            # 'sealed' | 'S' | 'A' | 'B' | 'C' | None
    is_new_claim: bool           # 제목·본문에 '새상품/새제품/미사용/택 있음' 류 주장
    like_new_claim: bool         # '새상품급/신품급/거의 새것' 류(새 제품이라는 주장은 아님)
    flags: tuple[str, ...]


_G_NEW = re.compile(r"새\s*상품|새\s*제품|새재품|미\s*사용|신품|새거|새\s*것|미개봉|미착용|"
                    r"(?:택|태그|텍)\s*(?:이\s*)?(?:있|달|그대로|안\s*뗀|부착|붙어)")
_G_NEW_EXCL_AFTER = re.compile(r"^[\s,/·]*(?:급|같|처럼|수준|느낌|대비|이나|이랑|과|와|가(?![가-힣])|가격|가\s*\d|\s*\d|"
                               r"이\s*아|은\s*아|는\s*아|아님|아니|으로\s*(?:구매|구입|샀)|(?:구매|구입)|사서|"
                               r"이든|든|상관|중고\s*상관)")
_G_NEW_EXCL_BEFORE = re.compile(r"(?:거의|준|같은|처럼)\s*$")
_G_LIKE_NEW = re.compile(r"(?:새\s*상품|새\s*제품|신품)\s*급|거의\s*새|새\s*것\s*같|새거\s*같|준\s*신품|신동품|새\s*상품\s*같")


def parse_grade(status, title=None, body=None) -> GradeResult:
    st = norm_text(status).strip()
    ordv = GRADE_ORD.get(st)
    grade = {0: "sealed", 1: "S", 2: "A", 3: "B", 4: "C"}.get(ordv) if ordv is not None else None
    flags: list[str] = []
    if ordv is None:
        flags.append("status_other" if st else "status_missing")
    text = norm_text(title) + "\n" + norm_text(body)
    is_new = False
    for m in _G_NEW.finditer(text):
        if _G_NEW_EXCL_AFTER.match(text[m.end():m.end() + 8]):
            continue
        if _G_NEW_EXCL_BEFORE.search(text[max(0, m.start() - 4):m.start()]):
            continue
        is_new = True
        break
    like_new = bool(_G_LIKE_NEW.search(text))
    if is_new and ordv is not None and ordv >= 3:
        flags.append("new_claim_vs_low_grade")
    if ordv == 0 and not is_new:
        flags.append("sealed_without_text_claim")
    return GradeResult(ordv, grade, is_new, like_new, tuple(flags))
