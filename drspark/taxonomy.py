"""Phase 1-c. 규칙 기반 카테고리·브랜드·성별 분류기.

- detect_brand(text) -> (brand_slug, matched_alias, confidence)
- detect_brands_all(text) -> [(slug, alias, confidence, pos), ...]  (등장 순서, 슬러그 중복 제거)
- detect_brand_fields(title, model_serial, spec, features, body) -> (slug, alias, confidence, field)
- classify(title, model_serial, spec, features, body) -> Classification
      (category, subcategory, gender, confidence, rule_id)

카테고리·하위 카테고리는 schema.CATEGORIES, 브랜드 슬러그는 schema.BRAND_SLUGS를 따른다.
schema에 없는 브랜드는 BRAND_SLUGS_EXT(공식 영문명 소문자·하이픈)로 확장한다.

판정 원칙(자세한 설명과 점검 결과는 phase1/sections/03_taxonomy.md)
1. 필드 우선순위: title > model_serial > spec > features. 단서가 나온 첫 필드가 결정한다.
   네 필드에 단서가 없으면 비하드굿 브랜드 영역 → 숫자(스키 길이 / 부츠 몬도·플렉스) → 스키·부츠 전용 브랜드
   → 본문 → 겸용 브랜드 기본값 → other 순.
2. 제목은 '+', ',', ' & ', '(', ')', '[', ']', ' / ', '//', '및', '이랑', ' - ', 한글 사이 '.' 로 구간(segment)을 나누고
   앞 구간부터 본다(번들이면 처음 언급된 품목이 대표 품목).
3. 구간 안에서는 단서 등급이 높은 것(명사 > 범용 명사 '스키'/'보드' > 모델 라인 > 서술어)이 이기고,
   같은 등급이면 먼저 나온 것이 이긴다. 겹치는 패턴은 긴 쪽(복합어: 부츠백, 스키복 등)이 이긴다.
4. 바인딩 단서는 '약한' 단서다. 스키 단서(모델 라인, 길이 등)가 하나라도 있으면 스키로 본다.
5. 서술어(월드컵·데모·플렉스 등)만 있으면 숫자·브랜드 영역으로 교차 확인한다.
"""
from __future__ import annotations

import re
import unicodedata
from typing import NamedTuple

from .schema import BRAND_SLUGS, CATEGORIES

# ---------------------------------------------------------------------------
# 브랜드
# ---------------------------------------------------------------------------
# schema.BRAND_SLUGS에 없는 브랜드(공식 영문명 → 소문자·하이픈). 데이터에서 실제로 관측된 것만 넣었다.
BRAND_SLUGS_EXT: dict[str, str] = {
    # 스키·부츠·바인딩
    "k2": "K2", "kessler": "Kessler", "bluemoris": "Bluemoris", "van-deer": "Van Deer", "vist": "Vist",
    "rexxam": "REXXAM", "roxa": "Roxa", "heierling": "Heierling", "full-tilt": "Full Tilt",
    "scarpa": "Scarpa", "ski-trab": "Ski Trab", "dps": "DPS", "black-crows": "Black Crows",
    "faction": "Faction", "line": "Line", "armada": "Armada", "volant": "Volant", "bomber": "Bomber",
    "bogner": "Bogner", "intuition": "Intuition", "sidas": "Sidas", "zipfit": "ZipFit",
    "therm-ic": "Therm-ic", "lenz": "Lenz",
    # 폴·장갑
    "one-way": "One Way", "gabel": "Gabel", "skkil": "SKKIL", "goode": "Goode", "vacek": "Vacek",
    "hestra": "Hestra", "rudis": "Rudis", "montbell": "Montbell", "black-diamond": "Black Diamond",
    "slytech": "Slytech",
    # 헬멧·고글
    "scott": "Scott", "alpina": "Alpina", "bolle": "Bollé", "kask": "Kask", "hmr": "HMR",
    "crnk": "CRNK", "neon": "Neon", "mango": "Mango", "julbo": "Julbo", "dragon": "Dragon",
    "electric": "Electric", "ix-nine": "IX NINE", "neve": "Neve", "indigo": "Indigo", "sena": "Sena",
    # 가방·튜닝
    "skitzo": "Skitzo", "db": "Db", "evoc": "Evoc", "sportube": "Sportube",
    "high-sierra": "High Sierra", "dakine": "Dakine", "jaguy": "Jaguy", "thule": "Thule",
    # 의류
    "schoffel": "Schöffel", "millet": "Millet", "j-lindeberg": "J.Lindeberg",
    "peak-performance": "Peak Performance", "capranea": "Capranea", "lacroix": "Lacroix",
    "toni-sailer": "Toni Sailer", "watts": "Watts", "nepa": "Nepa", "kolon-sport": "Kolon Sport",
    "dynafit": "Dynafit", "cmp": "CMP", "halti": "Halti", "quiksilver": "Quiksilver",
    "kappa": "Kappa", "annue": "Annue", "dimito": "Dimito", "alps-and-meters": "Alps & Meters",
    "aztech-mountain": "Aztech Mountain", "perfect-moment": "Perfect Moment",
    "mountain-force": "Mountain Force", "the-mountain-studio": "The Mountain Studio",
    "glennski": "GLENNSKI", "zuica": "ZUICA", "dotout": "Dotout", "fablice": "Fablice", "sos": "SOS",
    "norrona": "Norrøna", "helly-hansen": "Helly Hansen", "smartwool": "Smartwool", "falke": "Falke",
    "mountain-warehouse": "Mountain Warehouse", "chums": "Chums", "klim": "Klim", "tobe": "Tobe",
    "rutt": "Rutt", "atika": "Atika", "bsrabbit": "BSRABBIT", "roxy": "Roxy", "nike": "Nike",
    "mammut": "Mammut", "bca": "BCA", "ark": "ARK", "limit": "Limit", "bc-stream": "BC Stream",
    "fubuki": "Fubuki",
}
ALL_BRANDS: dict[str, str] = {**BRAND_SLUGS, **BRAND_SLUGS_EXT}

# (slug, 정규식, 신뢰도, 종류) 종류: name(정식 표기), misspell(오기·변형), line(모델 라인으로 브랜드 추정)
# 정규식은 normalize() 결과(NFKC·소문자)에 적용한다. HK = 한글 경계(앞에 한글이 붙지 않음)
HK = r"(?<![가-힣])"
EB = r"(?<![a-z0-9])"   # 영문 앞 경계
EA = r"(?![a-z])"       # 영문 뒤 경계
BRAND_ALIASES: list[tuple[str, str, float, str]] = [
    # --- 하드굿 주요 브랜드
    ("atomic", r"아토믹|atomic", 0.95, "name"),
    ("atomic", r"아토밋|아토미(?=\s)", 0.85, "misspell"),
    ("atomic", rf"레드스터|레드스타|러드스터|redster|{EB}혹스(?=\s*(프라임|prime|마그나|magna|울트라|ultra|\d))|hawx|hawks|hwax|훅스(?=\s*(프라임|마그나|\d))|"
               rf"{EB}s9\s*i|{EB}s9(?![0-9])|{EB}s8i|{EB}g9(?![0-9])|{EB}x9(?![0-9])|블루스터|bluester|더블\s*데크|doubledeck|"
               rf"클라우드\s*c?\d|cloud\s*c?\d", 0.75, "line"),
    ("rossignol", r"로시뇰|로시놀|rossignol", 0.95, "name"),
    ("rossignol", rf"로시니올|료시놀|료시뇰|로시롤|로시뇽|{HK}로시(?![가-힣])|{EB}rossi{EA}", 0.85, "misspell"),
    ("rossignol", rf"히어로|{EB}hero{EA}|슈퍼\s*비라지|슈퍼비라쥐|super\s*virage|비라지|비라쥐", 0.75, "line"),
    ("fischer", r"피셔|fischer|휘셔", 0.95, "name"),
    ("fischer", rf"{EB}fisher{EA}|패셔(?=\s*월드컵)", 0.85, "misspell"),
    ("fischer", rf"{EB}rc4|{EB}rc\s*one|더\s*커브|the\s*curv|{EB}curv\s*(dt|pro)", 0.75, "line"),
    ("head", rf"{HK}헤드(?!셋|폰|라이트|밴드|램프|기어|헌팅|업)|{EB}head{EA}", 0.95, "name"),
    ("head", rf"{HK}해드(?=\s|스키|$)", 0.85, "misspell"),
    ("head", rf"월드컵\s*레벨|worldcup\s*rebels|wc\s*rebels|{EB}rebels?{EA}|레블스|{EB}[ei]\s?[-.]?\s?sl(r|\s*pro|\s*rd)?(?![a-z])|"
             rf"{EB}e[-.]?gs{EA}|{EB}[ei][-.]?xsr|{EB}[ei][-.\s]?race(?!r)|{EB}e[-.\s]?speed|supershape|슈퍼\s*쉐이프|슈퍼\s*셰이프|슈퍼세이프|"
             rf"랩터|raptor|{EB}rator{EA}|포뮬러|포뮬라|포뮤라|formula\s*(wcr|x|1\d0)|edge\s*lyt|advant\s*edge|이지조이|easy\s*joy", 0.75, "line"),
    ("nordica", r"노르디카|nordica", 0.95, "name"),
    ("nordica", r"노르니카|노르디키|노르딕카|nordca", 0.85, "misspell"),
    ("nordica", rf"도베르만|도베르반|도메르만|도배르만|dobermann|doberman|{EB}dob\.|스피드\s*머신|speed\s*machine|speedmachine|"
                rf"스포츠\s*머신|sport\s*machine|sportmachine|프로\s*머신|pro\s*machine|스핏파이어|spitfire|몰티가라", 0.75, "line"),
    ("volkl", r"뵐클|볼클|뵐끌|völkl|volkl|voelkl", 0.95, "name"),
    ("volkl", rf"뷀클|뷜클|빌클|폴클|볼끌|valkl", 0.85, "misspell"),
    ("volkl", rf"레이스\s*타이거|레이스타이커|레이싱타이거|레이스터이거|레이수타이거|레이스타이어|타이거레이스|racetiger|race\s*tiger|"
              rf"디콘|deacon|만트라|mantra|페레그린|페페그린|peregrine|켄도|{EB}kendo", 0.75, "line"),
    ("salomon", r"살로몬|salomon", 0.95, "name"),
    ("salomon", r"살로먼|살로문|솔로몬|실로몬|salomol|salmon\s*prime", 0.85, "misspell"),
    ("salomon", rf"{EB}s\s*/\s*race|{EB}s/max|{EB}s/pro|{EB}s/lab|에스레이스|{EB}x\s*-?\s*pro(?=\s*\d)|{EB}xpro\d|{EB}x\s*-?\s*max|addikt|에딕트", 0.75, "line"),
    ("blizzard", r"블리자드|blizzard", 0.95, "name"),
    ("blizzard", r"블리쟈드|블리지드", 0.85, "misspell"),
    ("blizzard", r"파이어버드(?!\s*r\s*\d)|화이어버드|firebird(?!\s*r\s*\d)|썬더버드|thunderbird|러슬러|rustler|블랙펄|black\s*pearl", 0.7, "line"),
    ("ogasaka", r"오가사카|ogasaka", 0.95, "name"),
    ("ogasaka", r"오카사카|오가사가|오가사키", 0.85, "misspell"),
    ("ogasaka", rf"{EB}tc\s*-?\s*(s|sa|sb|su|st|sk|ss|l|lb|lt|lu|lh|lx|mc|mk|mu|mf|b){EA}|{EB}tcs[abtu]?{EA}|{EB}tcmu|트라이언|{EB}triun|keo'?s", 0.75, "line"),
    ("dynastar", r"다이나스타|dynastar", 0.95, "name"),
    ("dynastar", r"오메글라스|오메그라스|omeglass", 0.75, "line"),
    ("elan", rf"{HK}엘란(?!트라)|{EB}elan{EA}", 0.95, "name"),
    ("stockli", r"스토클리|스톡클리|스퇴클리|stöckli|stockli|stoeckli", 0.95, "name"),
    ("kastle", rf"캐슬레|케슬레|케슬래|{HK}캐슬(?!러)|kästle|kastle", 0.9, "name"),
    ("kessler", r"케슬러|캐슬러|kessler", 0.9, "name"),
    ("augment", r"어그먼트|augment", 0.95, "name"),
    ("hart", rf"{EB}hart{EA}|하트스키|{HK}하트(?!쉘|모양|무늬)", 0.8, "name"),
    ("lange", rf"랑게|{EB}lange{EA}", 0.95, "name"),
    ("lange", rf"랑케|{HK}랑스(?=\s*스키)|{EB}range(?=\s*(rs|worldcup|world\s*cup))", 0.8, "misspell"),
    ("tecnica", r"테크니카|tecnica", 0.95, "name"),
    ("tecnica", r"테크니타|테크니칸|티크니카", 0.85, "misspell"),
    ("tecnica", rf"마하\s*1|마하\s*원|{EB}mach\s*1|mach1|{EB}mach\s*(sport|boa|hv|mv|lv)|machi\s*team|마하\s*(스포츠|보아)", 0.75, "line"),
    ("dalbello", r"달벨로|dalbello", 0.95, "name"),
    ("dalbello", rf"{EB}drs\s*\d|{EB}drs{EA}|일모로|il\s*moro|벨로체|veloce\s*space", 0.75, "line"),
    ("marker", rf"{HK}마커|{EB}marker{EA}", 0.9, "name"),
    ("marker", rf"{EB}x\s*-?\s*comp|xcomp|엑스컴프|{EB}xcell|그리폰|griffon|{EB}jester|킹핀|kingpin", 0.7, "line"),
    ("look", rf"{EB}look{EA}", 0.8, "name"),
    ("look", rf"{HK}룩(?=\s*(바인딩|피벗|피봇|피폿|spx|px|nx|18|14|12|그립))|룩바인딩", 0.9, "name"),
    ("look", rf"피벗|피봇|피폿|{EB}pivot\s*\d", 0.7, "line"),
    ("tyrolia", r"티롤리아|tyrolia", 0.95, "name"),
    ("k2", rf"{EB}k2{EA}|케이투", 0.95, "name"),
    ("k2", r"마인드\s*벤더|마인드밴더|mindbender|{EB}recon\s*1\d0|레콘", 0.7, "line"),
    ("id-one", rf"아이디원|idone|{EB}id\s*one|{EB}id1{EA}", 0.95, "name"),
    ("id-one", rf"{EB}mr\s*-?\s*(ce|c|g|sg|d|j){EA}|{EB}mrce|{EB}mrg{EA}", 0.7, "line"),
    ("bluemoris", r"블루모리스|블로모리스|bluemoris", 0.9, "name"),
    ("van-deer", r"반디어|van\s*deer|vandeer", 0.9, "name"),
    ("vist", rf"{EB}vist{EA}|{HK}비스트(?!로)", 0.85, "name"),
    ("rexxam", rf"렉삼|rexxam|{EB}rexam", 0.9, "name"),
    ("rexxam", rf"{EB}r\s*-?\s*evo|레보\s*1\d0|렉삼레보", 0.7, "line"),
    ("roxa", rf"{EB}roxa{EA}", 0.9, "name"),
    ("heierling", r"하이어링|heierling", 0.9, "name"),
    ("full-tilt", r"풀\s*틸트|full\s*tilt", 0.9, "name"),
    ("full-tilt", r"드롭\s*킥|drop\s*kick", 0.7, "line"),
    ("scarpa", r"스카르파|scarpa", 0.9, "name"),
    ("ski-trab", r"스키트라브|skitrab|ski\s*trab", 0.9, "name"),
    ("dps", rf"{EB}dps{EA}", 0.9, "name"),
    ("black-crows", r"블랙\s*크로우|black\s*crows|blackcrows", 0.9, "name"),
    ("faction", rf"팩션|{EB}faction{EA}", 0.9, "name"),
    ("line", rf"{EB}line\s+(sunset|mastermind|blend|chronic|sakana|vision|pandora)|라인\s*(선셋|마스터마인드)", 0.85, "name"),
    ("armada", r"아르마다|armada", 0.9, "name"),
    ("volant", r"볼란트|volant", 0.9, "name"),
    ("bomber", rf"{HK}봄버|{EB}bomber{EA}", 0.8, "name"),
    ("bogner", r"보그너|bogner", 0.9, "name"),
    ("intuition", r"인투이션|인튜이션|intuition", 0.9, "name"),
    ("sidas", r"시다스|sidas", 0.9, "name"),
    ("zipfit", r"zipfit|짚핏|집핏", 0.9, "name"),
    ("therm-ic", r"썰믹|therm-?ic", 0.9, "name"),
    ("lenz", rf"렌쯔|{EB}lenz{EA}", 0.9, "name"),
    # --- 의류
    ("descente", r"데상트|데쌍트|데센트|descente", 0.95, "name"),
    ("descente", rf"{HK}데상(?=\s|$)|{EB}descent{EA}", 0.8, "misspell"),
    ("phenix", r"피닉스|phenix", 0.9, "name"),
    ("goldwin", r"골드윈|goldwin|goldwim", 0.95, "name"),
    ("onyone", r"온요네|onyone", 0.95, "name"),
    ("onyone", r"욘요네|온여네|온요니", 0.85, "misspell"),
    ("carve", rf"{HK}카브(?!링|라)|{EB}karv{EA}|{EB}carve?{EA}", 0.9, "name"),
    ("spyder", r"스파이더|spyder", 0.95, "name"),
    ("kjus", rf"{HK}큐스|{EB}kjus{EA}|츄스|쉬즈|{HK}쉬스(?=\s)", 0.9, "name"),
    ("fusalp", r"퓨잡|퓨살프|fusalp", 0.95, "name"),
    ("arcteryx", r"아크테릭스|어크테릭스|arc'?teryx|arcteryx", 0.95, "name"),
    ("ark", rf"{EB}ark{EA}|아크(?=\s*ark)", 0.85, "name"),
    ("arcteryx", rf"{HK}아크(?=\s*(\||$|알파|베타|세이버|러쉬|마카이|아톰|l\b))", 0.6, "misspell"),
    ("colmar", r"콜마르|콜마|colmar", 0.95, "name"),
    ("mizuno", r"미즈노|mizuno", 0.95, "name"),
    ("energiapura", r"에너지\s*아\s*퓨라|에너지\s*아\s*푸라|에너지아프라|energiapura|energy\s*apura", 0.95, "name"),
    ("rh-plus", rf"{EB}rh\+|알에이치\s*플러스", 0.95, "name"),
    ("reusch", r"로이쉬|로아쉬|로이시|reusch", 0.95, "name"),
    ("patagonia", r"파타고니아|파티고니아|파타고나아|파타고니으|patagonia", 0.95, "name"),
    ("the-north-face", rf"노스\s*페이스|north\s*face|{HK}노페(?![가-힣])", 0.95, "name"),
    ("maloja", r"말로야|maloja", 0.95, "name"),
    ("burton", rf"{EB}burton{EA}|(?<![가-힣])버튼(?=\s*(상의|자켓|보드|척추|고어|gore|ak))|{EB}ak\s*버튼|ak버튼", 0.9, "name"),
    ("dainese", r"다이네즈|다이네스|다이네제|dainese", 0.95, "name"),
    ("schoffel", r"쉐펠|쉐펄|쇼펠|schoffel|schöffel", 0.9, "name"),
    ("millet", rf"{HK}밀레(?!니엄)|{EB}millet{EA}", 0.9, "name"),
    ("j-lindeberg", r"제이\s*린드버그|린드버그|lindeberg|lindeburg", 0.9, "name"),
    ("peak-performance", r"피크\s*퍼포먼스|픽\s*퍼포먼스|peak\s*per(?:f|)ormance|peakpreformance", 0.9, "name"),
    ("capranea", r"카프라니아|capranea", 0.9, "name"),
    ("lacroix", r"라크루아|라크로와|lacroix", 0.9, "name"),
    ("toni-sailer", r"토니\s*세일러|tony\s*sailer|toni\s*sailer", 0.9, "name"),
    ("watts", rf"{HK}와츠|{EB}watts{EA}", 0.9, "name"),
    ("nepa", rf"{HK}네파(?![가-힣])", 0.9, "name"),
    ("kolon-sport", r"코오롱", 0.9, "name"),
    ("dynafit", r"다이나핏|다이너핏|dynafit", 0.9, "name"),
    ("cmp", rf"{EB}cmp{EA}", 0.85, "name"),
    ("halti", rf"{HK}할티|{EB}halti{EA}", 0.9, "name"),
    ("quiksilver", r"퀵실버|quiksilver", 0.9, "name"),
    ("kappa", rf"{HK}카파(?![가-힣])|{EB}kappa{EA}", 0.85, "name"),
    ("annue", rf"엔뉴|{EB}annue|{EB}ennue", 0.85, "name"),
    ("dimito", r"디미토|dimito", 0.85, "name"),
    ("alps-and-meters", r"alps\s*&\s*meters|알프스\s*앤\s*미터스", 0.85, "name"),
    ("aztech-mountain", r"아즈[텍택]\s*마운틴|aztech", 0.85, "name"),
    ("perfect-moment", r"퍼펙트\s*모먼트|perfect\s*moment", 0.85, "name"),
    ("mountain-force", r"마운틴\s*포스|mountain\s*force", 0.85, "name"),
    ("the-mountain-studio", r"(더\s*)?마운틴\s*스튜디오|mountain\s*studio", 0.85, "name"),
    ("mountain-warehouse", r"마운틴\s*웨어하우스|mountain\s*warehouse", 0.85, "name"),
    ("glennski", rf"{HK}글렌|{HK}글랜|glenski|{EB}glenn?\s*ski|{EB}glen{EA}", 0.85, "name"),
    ("zuica", r"주이카|zuika", 0.85, "name"),
    ("dotout", r"닷아웃|dotout", 0.85, "name"),
    ("fablice", r"파블리스|파플리스|fablice", 0.8, "name"),
    ("sos", rf"{EB}sos{EA}(?=\s*여성|\s*스키)", 0.8, "name"),
    ("norrona", r"노로나|norrona|norrøna", 0.85, "name"),
    ("helly-hansen", r"헨리\s*한센|헬리\s*한센|helly\s*hansen", 0.85, "name"),
    ("smartwool", r"스마트울|smartwool", 0.85, "name"),
    ("falke", rf"{EB}falke{EA}|팔케", 0.85, "name"),
    ("chums", rf"{HK}첨스|{EB}chums{EA}", 0.85, "name"),
    ("klim", rf"{EB}klim{EA}", 0.85, "name"),
    ("tobe", rf"{EB}tobe{EA}", 0.8, "name"),
    ("rutt", rf"{EB}rutt{EA}|{HK}루츠(?=\s*모글)", 0.8, "name"),
    ("atika", r"아티카", 0.8, "name"),
    ("bsrabbit", r"비에스\s*래빗|bs\s*rabbit", 0.85, "name"),
    ("roxy", rf"{EB}roxy{EA}|록시", 0.85, "name"),
    ("nike", r"나이키|nike", 0.85, "name"),
    ("limit", rf"{HK}리미트(?=\s*(스키|여성|보드))", 0.75, "name"),
    ("montbell", r"몽벨|montbell|mont-bell", 0.85, "name"),
    ("black-diamond", r"블랙\s*다이아\s*[몬온]드|black\s*diamond", 0.9, "name"),
    ("mammut", r"마무트|mammut", 0.9, "name"),
    ("slytech", r"슬라이텍|slytech", 0.85, "name"),
    # --- 폴·장갑
    ("leki", rf"{HK}레키|{HK}렉키|{EB}leki{EA}|{EB}reki{EA}", 0.95, "name"),
    ("komperdell", r"컴퍼델|콤퍼델|콤페르델|콤파델|komperdell", 0.95, "name"),
    ("sinano", r"시나노|shinano|sinano", 0.95, "name"),
    ("kizaki", r"키자키|kizaki", 0.95, "name"),
    ("one-way", rf"원웨이|온웨이|one\s*way|oneway", 0.9, "name"),
    ("gabel", r"가벨|gabel", 0.9, "name"),
    ("skkil", rf"skkil|{HK}스킬(?=\s*(\(|벙어리|신가드|월드컵|주니어|폴|스키\s*보호|길이|$))", 0.85, "name"),
    ("goode", rf"{EB}goode{EA}", 0.85, "name"),
    ("vacek", rf"{HK}바첵", 0.75, "name"),
    ("hestra", r"헤스트라|hestra", 0.95, "name"),
    ("rudis", r"루디스|rudis", 0.85, "name"),
    # --- 헬멧·고글
    ("smith", rf"스미스|스미흐|스미수|{EB}smith{EA}", 0.95, "name"),
    ("smith", rf"포디\s*맥|{EB}4d\s*ma[gc]|4dmag|아이오\s*맥|{EB}i/o\s*mag|넥서스|{EB}nexus{EA}|스쿼드\s*(맥|xl|mag)|{EB}squad\s*(mag|xl)|밴티지|벤티지|vantage\s*2", 0.75, "line"),
    ("giro", rf"{HK}지로|{EB}giro{EA}", 0.95, "name"),
    ("giro", rf"컨투어|{EB}contour{EA}|테넷|{EB}tenet{EA}|{HK}토르(?![가-힣])|오웬(?=\s*(스페|밉스|헬멧|m|l|$))|타거[트드]|taggert", 0.7, "line"),
    ("uvex", r"우벡스|uvex|ubex", 0.95, "name"),
    ("poc", rf"{EB}poc{EA}|피오시|포크\s*헬멧", 0.95, "name"),
    ("poc", rf"조눌라|zonula|오벡스|{EB}obex{EA}|스컬\s*듀?두?라|skull\s*dura|{EB}fovea|{EB}opsin", 0.7, "line"),
    ("oakley", r"오클리|oakley", 0.95, "name"),
    ("oakley", r"라인\s*마이너|line\s*miner", 0.75, "line"),
    ("sweet-protection", r"스[윗위]트?\s*프로텍션|sweet\s*protection|스윗(?=\s*헬멧)", 0.95, "name"),
    ("briko", r"브리코|briko", 0.95, "name"),
    ("scott", rf"{HK}스캇|{HK}스콧|{EB}scott{EA}|{EB}scoot{EA}", 0.9, "name"),
    ("alpina", r"알피나|alpina", 0.9, "name"),
    ("bolle", rf"{HK}볼레(?![가-힣])|{EB}boll[eé]{EA}", 0.9, "name"),
    ("kask", r"카스크|{EB}kask{EA}", 0.9, "name"),
    ("hmr", rf"{EB}hmr{EA}|햄머|hammer", 0.85, "name"),
    ("crnk", rf"{EB}crnk{EA}|크랭크", 0.85, "name"),
    ("neon", rf"{HK}네온(?![가-힣])|{EB}neon{EA}", 0.8, "name"),
    ("mango", rf"{HK}망고(?=\s)|{EB}mango{EA}", 0.8, "name"),
    ("julbo", r"줄보|julbo", 0.85, "name"),
    ("dragon", rf"{HK}드래곤(?=\s*(고글|pxv|r1))|{EB}dragon{EA}", 0.85, "name"),
    ("electric", rf"일렉트릭(?=\s*(스키|고글))|{EB}electric{EA}", 0.8, "name"),
    ("ix-nine", rf"{EB}ix\s*nine|{EB}ix9", 0.85, "name"),
    ("neve", rf"{HK}네베(?![가-힣])", 0.8, "name"),
    ("indigo", rf"{HK}인디고|{EB}indigo{EA}", 0.8, "name"),
    ("sena", rf"{HK}세나(?=\s*(sk|\d|장착|거치|$|\)))|{EB}sena{EA}", 0.8, "name"),
    # --- 가방·튜닝·기타
    ("blackbriar", r"블랙\s*브라이?어|blackbriar|black\s*briar|블브", 0.95, "name"),
    ("skitzo", rf"스킷조|skitzo|{HK}스키조(?=\s*(가방|부츠|웜미트|투어|\())", 0.9, "name"),
    ("db", r"두시백|douchebag", 0.85, "name"),
    ("evoc", rf"에복|{EB}evoc{EA}", 0.85, "name"),
    ("sportube", r"스포튜브|sportube", 0.85, "name"),
    ("high-sierra", r"하이\s*시에라|high\s*sierra", 0.85, "name"),
    ("dakine", r"다카인|dakine", 0.85, "name"),
    ("jaguy", rf"{EB}jaguy{EA}|자가이", 0.8, "name"),
    ("thule", rf"{HK}툴레|{EB}thule{EA}", 0.85, "name"),
    ("swix", r"스윅스|swix", 0.95, "name"),
    ("toko", rf"{EB}toko{EA}|{HK}토코(?![가-힣])", 0.9, "name"),
    ("bca", rf"{EB}bca{EA}", 0.85, "name"),
    ("bc-stream", r"bc\s*stream", 0.85, "name"),
    ("fubuki", r"fubuki|후부키", 0.85, "name"),
]

_KIND_RANK = {"name": 0, "misspell": 0, "line": 1}
_BRAND_RE = [(slug, re.compile(pat), conf, kind) for slug, pat, conf, kind in BRAND_ALIASES]
for _slug, *_ in BRAND_ALIASES:
    assert _slug in ALL_BRANDS, _slug


def normalize(text: str | None) -> str:
    """NFKC + 소문자 + 제로폭 문자 제거 + 공백 정리."""
    if not text:
        return ""
    t = unicodedata.normalize("NFKC", str(text))
    t = re.sub(r"[​-‏⁠﻿]", "", t)
    # NFKC가 호환 자모 'ㅡ'(U+3161)를 조합형(U+1173)으로 바꾼다. 'ㅡ'는 대부분 범위 구분자('260ㅡ65')로 쓰인다
    t = t.replace("ᅳ", "-").replace("ㅡ", "-")
    t = t.lower().replace("’", "'")
    return re.sub(r"\s+", " ", t).strip()


def detect_brands_all(text: str | None) -> list[tuple[str, str, float, int]]:
    """텍스트에 나온 브랜드 전부(첫 등장 위치순, 슬러그별 1회)."""
    t = normalize(text)
    found: dict[str, tuple[str, str, float, int, int]] = {}
    for slug, rx, conf, kind in _BRAND_RE:
        for m in rx.finditer(t):
            if not m.group(0).strip():
                continue
            cur = found.get(slug)
            rank = _KIND_RANK[kind]
            key = (rank, m.start())
            if cur is None or key < (cur[4], cur[3]):
                found[slug] = (slug, m.group(0).strip(), conf, m.start(), rank)
            break
    out = sorted(found.values(), key=lambda x: x[3])
    return [(s, a, c, p) for s, a, c, p, _ in out]


def detect_brand(text: str | None) -> tuple[str | None, str | None, float]:
    """대표 브랜드 1개. 정식 표기·오기 중 가장 먼저 나온 것 > 모델 라인으로 추정한 것 중 가장 먼저 나온 것."""
    t = normalize(text)
    best = None  # (rank, pos, -conf, slug, alias, conf)
    for slug, rx, conf, kind in _BRAND_RE:
        m = rx.search(t)
        if not m or not m.group(0).strip():
            continue
        key = (_KIND_RANK[kind], m.start(), -conf)
        if best is None or key < best[:3]:
            best = (*key, slug, m.group(0).strip(), conf)
    if best is None:
        return (None, None, 0.0)
    return (best[3], best[4], best[5])


def detect_brand_fields(title=None, model_serial=None, spec=None, features=None, body=None):
    """필드 우선순위로 브랜드 결정. 반환 (slug, alias, confidence, field)."""
    cands = []
    for field, text, w in (("title", title, 1.0), ("model_serial", model_serial, 0.95),
                           ("spec", spec, 0.85), ("features", features, 0.85),
                           ("body", (normalize(body)[:400] if body else None), 0.7)):
        slug, alias, conf = detect_brand(text)
        if slug:
            kind_line = any(k == "line" for s, rx, c, k in _BRAND_RE
                            if s == slug and rx.search(normalize(alias)) and c == conf)
            cands.append((field, slug, alias, conf * w, kind_line))
    if not cands:
        return (None, None, 0.0, None)
    # 정식 표기 우선(제목 > 모델 > ...), 없으면 모델 라인(제목 > 모델 > ...)
    names = [c for c in cands if not c[4] and c[0] != "body"]
    pick = names[0] if names else cands[0]
    return (pick[1], pick[2], round(pick[3], 3), pick[0])


# 브랜드별 주력 카테고리(앞쪽이 기본값). 명사·모델·숫자 단서가 없을 때만 쓴다.
BRAND_DOMAIN: dict[str, list[str]] = {
    **{b: ["apparel"] for b in [
        "onyone", "carve", "descente", "phenix", "goldwin", "kjus", "colmar", "mizuno", "energiapura",
        "rh-plus", "spyder", "fusalp", "arcteryx", "maloja", "patagonia", "the-north-face", "millet",
        "schoffel", "j-lindeberg", "peak-performance", "capranea", "lacroix", "toni-sailer", "watts",
        "nepa", "kolon-sport", "dynafit", "cmp", "halti", "quiksilver", "kappa", "annue", "dimito",
        "alps-and-meters", "aztech-mountain", "perfect-moment", "mountain-force", "the-mountain-studio",
        "glennski", "zuica", "dotout", "fablice", "sos", "norrona", "helly-hansen", "falke", "chums",
        "klim", "tobe", "rutt", "atika", "bsrabbit", "roxy", "nike", "limit", "montbell"]},
    "smartwool": ["apparel"], "mountain-warehouse": ["apparel"],
    "reusch": ["apparel", "gloves"], "dainese": ["apparel", "protector", "helmet"],
    "sweet-protection": ["helmet", "goggle", "apparel"], "black-crows": ["apparel", "ski"],
    "bogner": ["apparel", "ski"], "black-diamond": ["apparel", "poles", "gloves"],
    "hestra": ["gloves"], "rudis": ["gloves"], "lenz": ["gloves", "boots"],
    **{b: ["helmet", "goggle"] for b in ["smith", "giro", "uvex", "poc", "oakley", "briko", "alpina",
                                          "bolle", "kask", "hmr", "crnk", "neon", "mango", "julbo"]},
    **{b: ["goggle"] for b in ["dragon", "electric", "ix-nine", "neve", "scott"]},
    "indigo": ["helmet"],
    **{b: ["poles"] for b in ["leki", "komperdell", "gabel", "one-way", "sinano", "vacek", "goode"]},
    "kizaki": ["poles", "goggle", "gloves"], "skkil": ["poles", "gloves", "protector"],
    **{b: ["bag"] for b in ["blackbriar", "db", "evoc", "sportube", "high-sierra", "jaguy"]},
    "skitzo": ["bag", "gloves"], "dakine": ["bag", "gloves"],
    "swix": ["tuning", "bag", "poles"], "toko": ["tuning"],
    **{b: ["boots"] for b in ["lange", "dalbello", "tecnica", "rexxam", "roxa", "heierling",
                               "full-tilt", "scarpa", "intuition", "zipfit", "sidas"]},
    "therm-ic": ["boots", "gloves"],
    "marker": ["binding", "helmet", "goggle"], "look": ["binding"], "tyrolia": ["binding"],
    **{b: ["ski"] for b in ["ogasaka", "stockli", "augment", "kastle", "kessler", "bluemoris", "vist",
                             "hart", "dps", "faction", "line", "volant", "bomber", "ski-trab", "blizzard",
                             "elan", "dynastar", "armada"]},
    "id-one": ["ski", "apparel"], "van-deer": ["ski", "boots"], "k2": ["ski", "boots"],
    **{b: ["ski", "boots"] for b in ["atomic", "rossignol", "fischer", "head", "nordica", "salomon", "volkl"]},
    "burton": ["snowboard", "apparel"], "bc-stream": ["snowboard"], "ark": ["protector"],
    "slytech": ["protector", "gloves"], "fubuki": ["other"],
    "sena": ["other"], "bca": ["other"], "mammut": ["other"], "thule": ["other"],
}

# ---------------------------------------------------------------------------
# 카테고리 단서 패턴
# ---------------------------------------------------------------------------
# tier: 1 명사 / 1.5 범용 명사('스키','보드') / 2 모델 라인 / 3 서술어(월드컵·데모·플렉스 등)
# weak: True면 강한 단서가 전혀 없을 때만 채택(바인딩 계열)


class _Pat(NamedTuple):
    rule: str
    rx: re.Pattern
    cat: str
    sub: str | None
    tier: float
    weak: bool


_PATS: list[_Pat] = []


def _p(rule: str, pattern: str, cat: str, sub: str | None = None, tier: float = 1, weak: bool = False):
    _PATS.append(_Pat(rule, re.compile(pattern), cat, sub, tier, weak))


SIZE_TOKEN = r"(?<![a-z0-9])(xxs|xs|s|m|l|xl|xxl|xxxl|2xl|3xl|xo|o|ss|3s)(?![a-z0-9])|라지|미디움|미듐|스몰|엑스라지"

# --- 기타(비품목·이용권·전자기기) ---------------------------------------------
_p("other_ticket", r"시즌권|리프트권|회원권|숙박권|워터파크|콘도|라커|락커|프렌즈|주간권|곤도라|객실", "other", "other")
_p("other_voucher", r"상품권|기프트\s*쿠폰|쿠폰|교환권", "other", "other")
_p("other_device", r"캠코더|카메라|액션캠|고프로|gopro|{0}cam{1}|무전기|{0}ridio{1}|{0}radio{1}".format(EB, EA), "other", "other")
_p("other_sena", rf"{HK}세나(?=\s*(sk|\d|$))|{EB}sena\s*\d", "other", "other", tier=2.5)
_p("other_misc", r"비콘|beacon|하네스|harness|스키\s*머신|운동기구|인라인|가로바|루프\s*박스|윈드가드|스키\s*보드\s*캐리어|스키\s*캐리어(?!\s*(백|가방))|툴레|{0}thule{1}|유일캐리어".format(EB, EA), "other", "other")
_p("other_fashion_boots", r"로우\s*부츠|윈터\s*부츠|방한화|패딩\s*부츠", "other", "other")

# --- 의류 ----------------------------------------------------------------------
_p("apparel_race_suit", r"레이(싱|스|셍)\s*(슈|수)트|레이싱(슈|수)트|(gs|sl|fis|알파인|스피드)\s*(슈|수)트|하프\s*(슈|수)트|레이싱복|경기복|{0}race\s*suit".format(EB),
   "apparel", "apparel_race_suit")
_p("apparel_suit", r"{0}(슈|수)트(?![가-힣])|{1}suit(?!case)".format(HK, EB), "apparel", "apparel_race_suit")
_p("apparel_onepiece", r"원피스|올인원|스노우\s*수트|one-?piece", "apparel", "apparel_set")
_p("apparel_mid", r"미들러|미드\s*레이어|미들\s*레이어|미들\s*웨어|미드레이어|{0}mid\s*-?layer|베이스\s*레이어|baselayer|base\s*layer|"
   r"이너\s*(웨어|자켓|재킷|스키웨어)|이네웨어|{1}이너(?!\s*(글러브|장갑|부츠|부스터|새것|발목))|{0}inner{2}|플리스|fleece|후디|후드\s*티|티셔츠|하프\s*집|집업|"
   r"아톰\s*(sl|후디|lt)|나노\s*퍼프|베터\s*스웨터|타이즈|tights|기모\s*트레이닝".format(EB, HK, EA), "apparel", "apparel_mid_base")
_p("apparel_vest", r"조끼|{0}베스트(?!\s*(셀러|상품))|{1}vest{2}".format(HK, EB, EA), "apparel", "apparel_vest")
_p("apparel_other_acc", r"양말|삭스|{0}socks?{1}|비니|{0}beanie|바라클라바|balaclava|넥\s*워머|넥게이터|{2}모자(?![가-힣])|와펜|멜빵|서스펜더|suspender".format(EB, EA, HK),
   "apparel", "apparel_other")
_p("apparel_set", r"상\s*[/.·,]?\s*하\s*의|상하\s*(세트|셋트|한벌|1\s*set)|상하의|투\s*피스|쓰리\s*피스|two\s*piece", "apparel", "apparel_set")
_p("apparel_jacket", r"자켓|재킷|쟈켓|자캣|jacket|{0}jk{1}|아우터|{0}outer{1}|점퍼|아노락|anorak|바람막이|우의|우비|rain\s*coat|"
   r"패딩(?!\s*부츠)|덕\s*다운|구스\s*다운|다운\s*(자켓|패딩|재킷)|{2}상의".format(EB, EA, HK), "apparel", "apparel_jacket")
_p("apparel_pants", r"바지|팬츠|{0}pants?{1}|{0}pts{1}|빕\s*팬츠|빕팬츠|빕스|{0}bibs?{1}|{2}하의".format(EB, EA, HK), "apparel", "apparel_pants")
_p("apparel_noun", r"스키\s*복|스캐복|스키\s*웨어|skiwear|ski\s*wear|보드\s*복|팀\s*복|데모\s*복|데몬\s*복|데목\s*복|데모\s*웨어|게임\s*복|코치\s*복|"
   r"모글\s*(복|바지|팬츠|웨어|쉘)|mogul\s*(pants|wear|j0)|레플리카|{0}웨어(?!하우스)|{1}wear{2}|"
   r"(?<!아웃)(?<!아웃\s)(?<!아웃쉘\s)쉘(?!만|\s*\d{{3}}|\d{{3}})|하드\s*쉘|하트\s*쉘|{1}shell{2}|팀\s*라인|데모\s*라인|데모라인|데모\s*마스터|데모마스터|고어\s*텍스|gore-?tex|{1}gtx{2}".format(HK, EB, EA),
   "apparel", None)
_p("apparel_code", r"{0}(cpm|csm|cdt|chs|chp|cpt|cpp|jpm|crm|cdj|cbp|csw|com)\s*-?\s*\d{{2}}|{0}(dw5f|dwuw|dwmu|dwmy|dwj|onj|onp|esw|psm|pfb|psb|roj|rop)\w*\d".format(EB),
   "apparel", None, tier=2)
_p("apparel_line", r"{0}s\.?i\.?o{1}|시오(?![가-힣])|퍼포먼스\s*(라인|모델)|퍼포\s*(라인|먼스)|퍼포라인|{0}zone{1}(?!\s*mips)|엣지\s*쉘|오버핏|{0}ski\s*pants".format(EB, EA),
   "apparel", None, tier=2)

# --- 장갑 ----------------------------------------------------------------------
_p("gloves_noun", r"장갑|글러브|{0}gloves?{1}|미튼|mittens?|{2}미트(?!\s*레)|웜\s*미트|벙어리|삼지|삼자|[35]지\s*장갑|[35]지(?![가-힣])|오지\s*장갑|{0}lobster".format(EB, EA, HK),
   "gloves", "gloves")
_p("gloves_line", r"폴\s*라인|{0}fall\s*line{1}|와카야마|그리핀\s*(s|프로)|프로\s*그립|마르벡|marbec|스카디".format(EB, EA), "gloves", "gloves", tier=2)

# --- 헬멧 ----------------------------------------------------------------------
_p("helmet_race_noun", r"친\s*가드|턱\s*보호대|chin\s*guard|chinguard", "helmet", "helmet_race")
_p("helmet_noun", r"헬멧|헬맷|핼멧|핼맷|헐멧|helmet|바이저|바이져|visor|헬고|고글\s*일체형\s*(헬멧|헬맷)?|일체형\s*헬멧", "helmet", None)
_p("helmet_mips", r"밉스|{0}mips{1}".format(EB, EA), "helmet", None, tier=2)
_p("helmet_line", r"넥서스|{0}nexus{1}|{2}레벨(?=\s*(밉스|mips|헬멧|$|\(|m\b|l\b))|level\s*mips|밴티지|벤티지|{0}vantage\s*2|{2}미션(?=\s*(헬멧|mat|$))|{0}mission{1}|퀀텀|quantum|메이즈|{0}maze{1}|디센[드스]|descend|liberty\s*mips|프로스펙트|"
   r"{2}토르(?![가-힣])|{0}tor{1}(?=\s*(l|m|s|/|$))|테넷|{0}tenet|오웬|{0}owen{1}|잭슨|jackson\s*mips|레인지(?=\s*(mips|헬멧|m|$))|{0}ratio\s*mips|그리드\s*헬멧|타거[트드]|taggert|avera|"
   r"스탠스|스텐스|{0}stance{1}|{2}자크(?![가-힣])|{0}jakk|wanted_|levitate|울트라\s*밉스|ultra\s*mips|우벡스울트라|rocket\s*jr|레졸루션|resolution|"
   r"오벡스|{0}obex{1}|포닉스|fornix|스컬\s*듀?두?라|skull\s*dura|"
   r"{2}드라이버\s*(프로|시그마|헬멧|prime)|{0}driver\s*(pro|prime|sigma)|pioneer\s*lt|허스크|{0}husk{1}|오스모|"
   r"{0}mod\s*[1357]|모드\s*[1357]|트루퍼|trooper|스위처|switcher|{2}메소드\s*(프로|밉스)|{0}method\s*(pro|mips)|"
   r"vulcano|stromboli|페이토|faito|{2}아란(?=\s*(헬멧|$))|{0}aran{1}|캐니언|mammoth\s*epp|"
   r"컨피던트|confidant|피닉스\s*2|phoenix\s*2|titano|{0}h2{1}|cheos|alto\s*v|cusna|쿠아나|the\s*peak\s*lt|symbol\s*2|summit\s*(blook|helmet)".format(EB, EA, HK),
   "helmet", None, tier=2)

# --- 고글 ----------------------------------------------------------------------
_p("goggle_noun", r"고글|고굴|{0}보글|goggles?|선글라스|sunglass".format(HK), "goggle", "goggle")
_p("goggle_lens_noun", r"렌즈|{0}lens{1}".format(EB, EA), "goggle", "goggle_lens", tier=2.5)
_p("goggle_line", r"포디\s*맥|퍼디맥|{0}4d\s*ma[gc]|4dmag|아이오\s*맥|아이오mag|{0}i/o\s*mag|{0}io\s*mag|스쿼드(?!론)|{0}squad{1}|스쿼트\s*xl|스카이라인|{0}skyline|프리뷰(?=\s*(변색|고글|$))|{0}preview{1}|리즌\s*otg|reason\s*otg|"
   r"로우다운|lowdown|컨투어|{0}contour{1}|엑시스|{0}axis{1}|{2}지로\s*메소드|giro\s*method|{2}메소드\s*(아시안|af)|method\s*af|지로\s*콤프|giro\s*comp|{2}콤프\s*(스톤|아시아|구합)|인덱스\s*2|인댁스|{0}index\s*2|{2}리볼트(?!\s*\d)|cruz\s*af|"
   r"다운힐\s*2100|downhill\s*2100|evidnt|빅토리[어우]스|victorious|athletic\s*fm|contest\s*cv|"
   r"조눌라|zonula|{0}fovea|{0}opsin|{0}retina{1}|{0}orb{1}|센트리|센추리|{0}sentry|"
   r"라인\s*마이너|line\s*miner|플라이트\s*트레커|flight\s*(deck|tracker)|"
   r"클락웍|clockwork|인터스텔라|interstellar|분닥|boondock|코너\s*고글|더든|"
   r"리엑터|{0}reactor{1}|{0}lcg\s*evo|squadron|스쿼드론|{0}pxv{1}|r1\s*otg|{0}eg2{1}|{0}ix\s*nine|{0}ix9|{0}ix3|카멜레온|마테호른|네바다\s*네오|las\s*-?\s*\d{{3}}|laa\s*-?\s*\d{{3}}".format(EB, EA, HK),
   "goggle", "goggle", tier=2)

# --- 보호대 --------------------------------------------------------------------
_p("protector_noun", r"보호대|프로텍터|{0}protector(?!\s*strips){1}|척추|신\s*가드|정강이|{0}shin\s*guard|암\s*가드|{0}arm\s*guard|포어암|forearm|rhyo\s*lite|스카라베오|scarabeo|"
   r"{2}보호\s*(패드|장비)".format(EB, EA, HK), "protector", "protector")

# --- 폴 ------------------------------------------------------------------------
_p("poles_race_noun", r"(대?회전|gs|sl|레이싱|레이셍|월드컵|곡선|휜|가드)\s*폴(?![라리더란로딩링클])|폴\s*가드|회전\s*가드|오픈\s*가드|레이싱\s*바스켓", "poles", "poles_race")
_p("poles_noun", r"스키\s*폴|스키폴|폴대|{0}poles?{1}|팽귄\s*폴|펭귄\s*폴|폴(?!\s*라인|라|리|더|란|로|딩|링|클|짝|스|섬|로|더블|\s*스미스)|스틱(?!커)|샤프트".format(EB, EA),
   "poles", None)
_p("poles_line", r"길이\s*조[절정]|길리조정|{0}vario{1}|바리오|베놈|배놈|{0}venom{1}|피크\s*바?베?리오|트리거\s*s|trigger\s*s|wcr\s*lite|센티넬\s*스키|nationalteam|내셔널\s*팀\s*카본|"
   r"free\s*(sv|fast|x)|프리\s*패스트|프로시드|x-cursion|gt\s*vario|드리프터".format(EB, EA), "poles", None, tier=2)
_p("poles_strap", r"{0}스트랩|{1}strap{2}|바스켓".format(HK, EB, EA), "poles", "poles_allround", tier=3)

# --- 가방 ----------------------------------------------------------------------
_p("bag_boot", r"(스키\s*)?부츠\s*(백|가방|bag|백팩)|부츠백|부츠가방|스키부츠가방|스키조\s*부츠백팩|tech\s*pack|rs\s*pack|race\s*boots\s*bag", "bag", "bag_boot")
_p("bag_ski", r"스키\s*(백(?!\s*컨트리)|가방|케이스|슬리브|보관백)|스키가방|스키강바|스키백|보드\s*스키백|sportube|스포튜브|스테이\s*와일드|와일드\s*스테이|플레이트백", "bag", "bag_ski")
_p("bag_wheel", r"휠\s*(백|가방)|휠백|횔백|{0}흴|{1}wheel|롤러|캐리어(?!\s*가로바)|{1}carrier{2}".format(HK, EB, EA), "bag", "bag_wheel")
_p("bag_backpack", r"백팩|backpack|배낭|더플\s*백팩|기어\s*백팩|아이콘\s*백팩|hugger", "bag", "bag_backpack")
_p("bag_other", r"가방|{0}bags?{1}|힙\s*쌕|힙\s*색|힙색|파우치|기어\s*백|투어\s*백|투어백|보조\s*가방|스키\s*밴드|스키밴드|악세사리\s*팩".format(EB, EA), "bag", "bag_other")

# --- 튜닝 ----------------------------------------------------------------------
_p("tuning_noun", r"튜너|튜닝\s*(용품|툴|도구|세트)|튜닝용품|에지\s*튜닝|에지튜닝|엣지\s*튜닝|엣지\s*튜너|edger|{0}왁스(?!\s*칠)|{1}wax{2}(?!ing)|왁싱\s*(다리미|브러[쉬시]|세트|솔)|다리미|"
   r"바이스|{1}vise{2}|다이아몬드\s*스톤|스크래퍼|scraper|클램프|clamp|브러[쉬시]|{1}brush{2}|정비\s*(도구|제품|용품)|보호\s*테이프|protector\s*strips|에지\s*정비\s*도구".format(HK, EB, EA),
   "tuning", "tuning")

# --- 스노보드 ------------------------------------------------------------------
_p("board_boots", r"보드\s*부츠|스노우?\s*보드\s*부츠", "snowboard", "board_boots")
_p("board_binding", r"보드\s*바인딩", "snowboard", "board_binding")
_p("board_deck", r"스노우?\s*보드(?!\s*(복|헬멧|헬맷|장갑|고글|부츠|바인딩|일체형|하체|보호))|snowboard|(?<!더블)(?<!더블\s)데크|{0}deck{1}".format(EB, EA), "snowboard", "board_deck")
_p("board_generic", r"{0}보드(?!\s*(복|헬멧|헬맷|장갑|고글|타러|헬고|캐리어))".format(HK), "snowboard", None, tier=1.5)

# --- 부츠 ----------------------------------------------------------------------
_p("boots_acc", r"이너\s*부츠|이너부츠|부츠\s*(건조기|드라이어|히터|워머)|부프\s*건조기|건조기|{0}liners?{1}|라이너|인솔|{0}insoles?|깔창|부스터\s*스트랩|부스터스트랩|부츠\s*스트[랩렙]|"
   r"발열\s*(깔창|양말|인솔)|열선\s*(세트|배터리)?|{0}c-?\s*pack|c1700|1700\s*b|1300\s*b?\s*배터리|heat\s*kit".format(EB, EA), "boots", "boots_other")
_p("boots_noun", r"스키\s*부츠|스키부츠|스키\s*화|스키화|레이싱\s*부츠|투어링\s*부츠|핀\s*부츠|텔레마크|부츠(?!\s*(백|가방|팩|bag|건조기|드라이어|컷|사이즈\s*\d))|{0}boots?{1}(?!\s*bag)".format(EB, EA),
   "boots", None)
_p("boots_line", r"{0}혹스|hawx|hawks|hwax|훅스|마그나|{0}magna{1}|마그마|tracker\s*1\d0|live\s*fit|redster\s*(cs|club|sti|jr|wc\s*1)|레드스터\s*(cs|주니어\s*부츠|jr)|{0}cs\s*1[13]0|레메디|"
   r"랩터|raptor|{0}rator{1}|포뮬러|포뮬라|포뮤라|{0}formula{1}|advant\s*edge|edge\s*lyt|엣지\s*(lyt|1[0-3]0|25/26|250)|헤드\s*엣지|헤드엣지|{0}edge\s*1[0-3]0|next\s*edge|넥스트\s*엣지|벡터\s*에보|vector\s*evo|challenger\s*1|칼리버|코어\s*\(?core|"
   r"speed\s*machine|speedmachine|스피드\s*머신|스피드머신|sport\s*machine|sportmachine|스포츠\s*머신|스포츠머신|pro\s*machine|프로\s*머신|프로머신|hf\s*pro|크루즈|{0}cruise|one\s*cx|"
   r"(도베르만|도베르반|도메르만|dobermann|doberman)\s*(gp|5|edt|rd|dp|1[0-4]0|부츠|월드컵\s*1)|{0}gp\s*1[0-4]0|gp1[0-4]0|1[34]0\s*gp|{0}edt\s*1\d0|5\s*rd\s*m|"
   r"마하\s*1|마하\s*원|마하1|{0}mach\s*1|mach1|{0}mach\s*(sport|boa|hv|mv|lv)|마하\s*(스포츠|보아|9\d|1\d0)|machi\s*team|cochise|race\s*pro\s*\d0|{0}jtr\s*\d|(테크니카|tecnica|티크니카)\s*(파이어버드|firebird)|파이어버드\s*r\s*\d|firebird\s*r\s*\d|"
   r"{0}drs\s*\d|{0}drs{1}|디알에스|일모로|il\s*moro|벨로체|벨리체|veloce|panterra|{0}ds\s*(ax|mx)|ltd\s*ax|{0}mx\s*\d0|"
   r"{0}rs\s*1[0-5]0|{0}rs1[0-5]0|{0}rs\s*(90|wide|mv|zb|za)|{0}rs\s*130|{0}lx\s*1[0-3]0|{0}lx110|l10\s*acd|world\s*cup\s*rs|월드컵\s*rs|{0}z[abcj]{1}|{0}z[abcj]\s*1\d0|랑게\s*와이드|랑게와이드|"
   r"히어로\s*(월드컵\s*(\d{{2,3}}|z[abcj]|sc|부츠|플렉스)|듀얼|1\d0|elite\s*1)|hero\s*(world\s*cup\s*(\d{{2,3}}|z[abcj]|sc)|1\d0)|alltrack|올트랙|pure\s*(elite|pro|heat)|{0}speed\s*1[0-2]0|스피드\s*1[0-2]0|하이\s*스피드|{0}track\s*1[0-3]0|sc\s*demo|demo\s*world\s*cup|"
   r"{0}rc4\s*(podium|boa|보아|mv|hv|lv|1[0-3]0|부츠|curv\s*부츠)|rc4\s*1\d0\s*mv|{0}rc\s*one|{0}rc\s*pro\s*1\d0|{0}rc85|하이브리드\s*1\d0|{0}hybrid\s*1\d0|the\s*curv\s*1\d0|더\s*커브\s*(1\d0|플렉스)|podium\s*gt|"
   r"{0}s\s*/\s*race\s*1\d0|{0}s\s*/?\s*pro\s*(alpha|supra|delta|hv|1\d0)|{0}s\s*프로\s*(델타|알파)|{0}x\s*-?\s*pro\s*\d|{0}xpro\s*\d|{0}x\s*-?\s*max|{0}xmax|{0}x\s*-?\s*access|supra\s*boa|{0}supra{1}|select\s*hv|s\s*max\s*1\d0|"
   r"{0}r\s*-?\s*evo|레보\s*1\d0|revo\s*m|렉삼레보|렉삼에보|{0}roxa\s*fit|드롭\s*킥|drop\s*kick|디센던트|maestrale|{0}voodoo|레콘|{0}recon\s*1\d0|{0}recon\s*boa|revolve\s*(kf|pro)|리볼브|"
   r"{0}tx\s*130|{0}ti\s*130|{0}asy\s*pro|sport\s*fit|zipfit|인[투튜]이션|intuition".format(EB, EA),
   "boots", None, tier=2)
_p("boots_flex", r"(?<!슈퍼)(?<!프리)(?<!super)(?<!free)(플렉스|플랙스|{0}flex{1}|{0}flx)\s*:?\s*\d{{2,3}}|\d{{2,3}}\s*(플렉스|플랙스|{0}flex{1}|플(?![가-힣]))|{0}flex\s*\d|플렉스|플랙스|발볼|{0}last\s*\d{{2,3}}|라스트\s*\d{{2,3}}|아웃\s*솔".format(EB, EA),
   "boots", None, tier=3)

# --- 바인딩(약한 단서) ---------------------------------------------------------
_p("binding_noun", r"바인딩|바인더|비인딩|바운딩|{0}binding{1}".format(EB, EA), "binding", None, tier=1, weak=True)
_p("binding_parts", r"브레이크|{0}brake{1}|힐\s*피스|heel\s*piece|토\s*피스|스토퍼".format(EB, EA), "binding", "binding_other", tier=1, weak=True)
_p("binding_plate", r"리프터|{0}lifter{1}|race\s*plate|레이스\s*플레이트|junior\s*race\s*plate".format(EB, EA), "binding", "binding_plate", tier=1, weak=True)
_p("binding_line", r"{0}x\s*-?\s*comp|xcomp|엑스컴프|{0}comp\s*1[0-8]|{0}com\s*16|컴프\s*1\d|{0}xcell|griffon|그리폰|그리핀\s*13|{0}jester|kingpin|킹핀|squire|스콰이어|{0}attack\s*\d|어택|"
   r"{0}warden|{0}strive|{0}sth2?{1}|{0}shift\s*\d|시프트|쉬프트|피벗|피봇|피폿|{0}pivot|{0}px\s*1[2-8]|{0}spx\s*1[0-5]|{0}nx\s*1\d|{0}fks{1}|icon\s*(rs\s*)?1[0-6]|아이콘\s*(rs\s*)?(1[0-6]|바인딩)|"
   r"{0}z\s*1[0-7]{1}|{0}z20|rsx\s*12|freeflex|프리플렉스|{0}ff\s*(st\s*)?1[0-6]|{0}wcr\s*1[0-6]|[vri]\s*-?motion|tpc\s*10|{0}tpx|{0}er\s*1[0-7]|{0}prd\s*1[12]|{0}fdt|{0}mi\s*12|{0}pr\s*1[0-3]{1}|"
   r"{0}evo\s*1[0-4]|{0}tcx{1}|{0}race\s*1[0-6]|더비|{0}derby|sr\s*-?\s*585|fm\s*-?\s*585|gr585|fl585|피스톤|{0}piston|rockerace|딘\s*수치|{0}din\s*\d".format(EB, EA),
   "binding", None, tier=2, weak=True)

# --- 스키 ----------------------------------------------------------------------
# '스키' 뒤에 붙으면 스키 본체가 아닌 복합어(스키복·스키부츠·스키백…)
_SKI_NOT = (r"(?!\s*(복|웨어|바지|팬츠|자켓|재킷|쟈켓|부츠|화|백(?!\s*컨트리)|가방|케이스|슬리브|장갑|글러브|헬멧|헬맷|핼멧|고글|폴|스틱|양말|삭스|밴드|하네스|머신|강바|조|트라브|정비|튜닝|"
            r"캐리어|보드|[/&]\s*보드|,\s*보드|원정|장|강습|레이싱\s*슈트|레이싱슈트|슈트|팀|국가|회원권|보호|베이스|보관백|휠백|팬|웨|레이싱\s*,|"
            r"자켓|재킷|상의|하의|멜빵|서스펜더|이너|미들|비니|모자))")
_p("ski_noun", r"스키\s*판|스키판|데모\s*판|데모판|월드컵\s*판(?!매)|월드컵판(?!매)|회전\s*판(?!매)|대회전\s*판(?!매)|기선\s*판|레이싱\s*판|스키\s*세트|스키세트|"
   r"(회전|모글|주니어|쥬니어|여성|여자|파우더|프리|올라운드|올마운틴|아동|성인|입문|초급|중급|상급|레이싱|월드컵|수제|데모|중고|헤드|피셔|뵐클|엘란|살로몬|아토믹|로시뇰|블리자드)\s*스키" + _SKI_NOT + "|"
   r"스키\s*플레이트|스키플레이트|{0}ski{1}(?!\s*(wear|boots?|bag|pants|jacket|glove|helmet|pole|team|inner|skin|mittens))|skis{1}".format(EB, EA),
   "ski", None)
_p("ski_gs", r"대회전(?!\s*폴|폴)", "ski", "ski_race_gs")
_p("ski_sl", r"(?<!대)회전(?!\s*반경|반경|\s*폴|폴|\s*가드|성향|계|\s*반지름|력|형|\s*충격|판매)", "ski", "ski_race_sl")
_p("ski_mogul", r"모글\s*스키|모글스키|모굴|{0}모글(?!\s*(복|바지|팬츠|웨어|부츠|폴|쉘|pants|wear))|{1}mogul{2}(?!\s*(pants|wear|j0))".format(HK, EB, EA), "ski", "ski_mogul")
_p("ski_allround_noun", r"올라운드|올라운더", "ski", "ski_allround")
_p("ski_generic", r"(?<![a-z])스키" + _SKI_NOT + r"|스키(?=\s*$)", "ski", None, tier=1.5)


_p("ski_line", r"{0}s9\s*i|{0}s9\s*(fis|rs|pro|rvsk|$|\s)|{0}s9(?![0-9])|{0}s8i|{0}s7(?![0-9])|{0}g9|{0}x9(?![0-9])|9si|레드스터(?!\s*(cs|주니어\s*부츠|jr\s*\d))|러드스터|레드스타|redster(?!\s*(cs|club|sti|jr\s*\d|wc\s*1))|레지스터mi|"
   r"클라우드|{0}cloud\s*(c?\d|ltd)|블루스[터타]|bluester|더블\s*데크|doubledeck|{0}bent\s*(chetler|\d)|{0}벤트\s*\d|{0}race\s*st|"
   r"{0}[ei]\s?[-.]?\s?sl{1}|{0}[ei]\s?[-.]?\s?slr|{0}eslr?{1}|{0}esl\d|{0}isl{1}|{0}[ei][-.]?\s?gs{1}|{0}e[-.]?\s?race|{0}i[-.]?\s?race|이레이스|{0}[ei][-.]?xsr|{0}e[-.\s]?speed|"
   r"wc\s*rebels|worldcup\s*rebels|wcr\s*e|레블스|{0}rebels?{1}|supershape|슈퍼\s*쉐이프|슈퍼\s*셰이프|슈퍼세이프|매그넘|magnum|{0}[ie]\s?\.?\s?rally|랠리|{0}titan{1}|타이탄|{0}e-v8|shape\s*sx|"
   r"{0}i\.?shape|xr\s*ti|이지조이|easy\s*joy|absolut\s*joy|"
   r"레이스\s*타이[거커]|레이스타이[거커어]|레이싱\s*타이거|레이싱타이거|레이스터이거|레이수타이거|타이거레이스|타이커레이스|{0}racetiger|{0}race\s*tiger|{0}rtm\s*\d|"
   r"페레그린|페페그린|peregrine|디콘|deacon|만트라|mantra|{0}m7{1}|켄도|{0}kendo|시크릿\s*\d|volkl\s*secret|{0}bmt\s*\d|{0}revolt\s*\d|바쉬\s*\d|völkl\s*wall|플래티넘|모션타이거|타이거샤크|플레어\s*st|"
   r"(도베르만|dobermann|doberman|{0}dob\.?)\s*(sl|slr|slc|slj|gs|gsr|gsj|scl|swc|spitfire|스핏파이어|wc\s*dept|sl\s*wc)|{0}slr{1}|{0}sl\s*wc|wc\s*dept|{0}dept\s*-?\s*m|{0}swc{1}|스핏파이어|spitfire|transfire|트랜스파이어|몰티가라|{0}slj{1}|{0}gsj{1}|"
   r"hero\s*(athlete|master|elite|mogul|fis|jr)|히어로\s*(마스터|엘리트|주니어|숏턴|sl|fis)|athlete\s*(fis|gs|sl)|슈퍼\s*비라지|슈퍼비라[지쥐]|비라[지쥐]|super\s*virage|슈퍼\s*미라지|데모\s*(알파|베타)|demo\s*(alpha|beta)|"
   r"{0}nova\s*\d|노바\s*\d|아케이드\s*\d|팔마레스|코브라|cobra\s*vas|soul\s*7|"
   r"{0}rc4\s*(wc\s*)?(sl|gs|sc|rc|race|pro|power|노이즈|noize|파워|worldcup|world\s*cup|jr)|rc4\s*(1[2-6]\d|150)|{0}sc\s*mt|{0}wc\s*sc|{0}sc\s*pro|노이즈\s*(st|sl)|{0}noize|power\s*ti|파워\s*(ti|ar)|power\s*ar|"
   r"the\s*curv\s*(dtx|dti|gt)|더\s*커브\s*(dtx|dti)|{0}curv\s*(dtx|pro)|커브\s*dtx|카브\s*프로|progressor|fisher\s*fire|피셔\s*파이어|{0}ct\s*175|"
   r"파이어버드(?!\s*r\s*\d)|화이어버드|firebird(?!\s*r\s*\d)|{0}src{1}|{0}wrc{1}|썬더버드|썬더보드|thunderbird|러슬러|rustler|brahma|bonafide|블랙펄|black\s*pearl|alight\s*pro|{0}rc\s*ti|{0}rc\s*carbon|{0}rtx{1}|{0}rti{1}|sl\s*r\.?d\.?m?{1}|{0}r\.d\.m|sl\s*rd{1}|"
   r"{0}tc\s*-?\s*(s|sa|sb|su|st|sk|ss|l|lb|lt|lu|lh|lx|mc|mk|mu|mf|b){1}|{0}tcs[abtu]?{1}|{0}tssb|{0}tcmu|{0}tc\s*\(?(회전|대회전|플레이트)|오가사카\s*tc|오가사카\s*(sa|sb|ss|st|su|sk)(?![a-z])|트라이언|{0}triun|keo'?s|{0}ks\s*-?\s*(up|pv|ps)|{0}u\s*-?\s*(fs\d|pe){1}|"
   r"{0}s\s*/\s*race|{0}s\s*race|에스레이스|{0}s\s*/\s*max|{0}e\s*s/max|{0}s\s*max\s*n?\s*\d{1}|addikt|에딕트|{0}qst{1}|{0}lab\s*ski|salomon\s*lab|{0}s\s*/\s*lab|살로몬\s*lab|{0}equipe|{0}x\s*-?\s*race|street\s*racer|wpro\s*sw|3v\s*lab|"
   r"{0}sl\s*-?\s*pro|{0}sl\s*(10|12|16){1}|{0}sl1[026]{1}|{0}gs\s*pro|{0}gs\s*prime|{0}r\s*race\s*prime|프라임|{0}prime{1}|"
   r"오메글라스|오메그라스|omeglass|스피드\s*마스터|스피드마스터|speed\s*master|course\s*wc|speed\s*zone|이라이트|레전드\s*걸|legend\s*girl|"
   r"{0}ace\s*(slx|rc|sl){1}|{0}slx{1}|amphibio|jett\s*jr|블러드라인|bloodline|{0}gsx\s*team|absolut\s*joy|"
   r"{0}laser\s*(sl|gs|sc|sx|wrt|ax){1}|레이저\s*sl|레이서\s*sc|sc\s*ra[cs]er|{0}rx\s*1[12](?!\d)|{0}rx1[12](?!\d)|팬텀\s*s|phantom\s*-?\s*s{1}|{0}python{1}|판도라\s*s|race\s*team\s*jr|{0}rt\s*주니어|120\s*rt|"
   r"{0}mr\s*-?\s*(ce|c|g|sg|d|j){1}|{0}mrce|{0}mrg{1}|{0}fr\s*ng|{0}tt[rm]\s*-?\s*\d*{1}|tr-ttr|"
   r"s\s*포션|{0}s\s*-?\s*potion|{0}sportion|s-poton|g-potion|vist\s*sl|mindbender\s*\d{{2}}\s*ti|마인드\s*[벤밴]더\s*\d{{2}}\s*ti|disruption\s*sti|{0}wayback|{0}koala|카이젠|kaizen|파고다|pagoda|"
   r"dictator|선셋\s*98|{0}sunset\s*98|마스터마인드|mastermind|navis\s*freebird|volant\s*pulse|augment\s*race|어그먼트\s*sl|bamboo\s*vt8|밤부\s*vt8|{0}vt8{1}|altavia|"
   r"{0}j[24]{1}|redster\s*j\d|{0}jr\s*j\d|team\s*plate|{0}jrs{1}|주니어\s*프로|jr\s*pro|레드스터\s*j\d|{0}fis\s*jr|{0}speed\s*omeglass|hero\s*athlete|{0}gs\s*jr|{0}sl\s*jr|jr\s*gs|"
   r"숏턴|숏\s*턴|{0}ski\s*plate|{0}rc4\s*(월드컵|wc|world)|{0}9i\s*pro|{0}me\s*-?\s*ce{1}|{0}sc\s*\.\s*pro|{0}st\s*ti{1}|{0}sl\s*(마스터|master|pro|wc|fis|데모|r{1})|sl마스터|{0}gs\s*(마스터|master|wc|fis|r{1})|"
   r"(뵐클|볼클|volkl|völkl|살로몬|salomon|아토믹|atomic|피셔|fischer|헤드|head|엘란|elan|다이나스타|dynastar|노르디카|nordica|스토클리|블리자드|blizzard|로시뇰|로시놀|rossignol|어그먼트|아이디원)\s*(sl|gs)(?![a-z])".format(EB, EA),
   "ski", None, tier=2)
_p("ski_desc", r"월드컵|월드겁|올드컵|{0}worldcup|{0}world\s*cup|{0}wc{1}|{0}fis{1}|{0}pis{1}|반드컵|반\s*월드컵|준\s*월드컵|데모(?!\s*(복|웨어|라인|마스터|팀|outer|jk|pts|jacket|pants))|{0}demo{1}(?!\s*(wear|outer|team|jacket|pants|jk|pts|set|master|skiwear))|데몬(?!\s*(복|팀복))|"
   r"기선|기술\s*선수권|티칭|올\s*마운틴|올마운틴|{0}sl{1}|{0}gs{1}|{0}마스터(?!\s*(라지|xl|l|핑크|새상품|미사용|outer|jk))|{0}master{1}|{0}all\s*mountain|카빙(?!\s*스키\s*복)|파우더|프리\s*라이드|프리라이드|프리스타일(?!\s*부츠)|트윈\s*팁|백컨트리|{0}powder{1}|"
   r"회전\s*반경|회전반경|레디어스|{0}radius{1}|\d{{1,2}}\s*(메다|메타|미터)".format(EB, EA),
   "ski", None, tier=3)
_p("ski_plate_word", r"플레이트(?!\s*백)|{0}plate{1}".format(EB, EA), "ski", None, tier=3)
# 제목 속 스키 길이(140~199). 사이즈·호칭·키·몬도 문맥은 제외
_p("ski_len_num", r"(?<![\d.~/\-])(?<!칭)(?<!칭\s)(?<!키)(?<!키\s)(?<!장)(?<!장\s)(?<!이즈)(?<!이즈\s)(1[4-9]\d)\s*(?:cm|센티)?(?![\d.%~/\-])"
   r"(?!\s*(?:mm|미리|사이즈|싸이즈|호칭|l(?![a-z])|리터|만|원|kg|키로|ml|/\s*\d{2,3}\s*(?:mm|사이즈)))", "ski", None, tier=3)

_ALL_PATS = _PATS

# 단서 바로 뒤에 '포함/증정/드림…'이 오면 덤으로 주는 물건이라 대표 품목 판정에서 뺀다
_ACCESSORY_CTX = re.compile(r"^\s*\S{0,6}\s*(포함|증정|드림|드려요|드립니다|서비스|덤|같이\s*드|무료|별도|도\s*같이)")

# '스키&보드헬멧', '스키/보드'처럼 붙여 쓴 &·/ 는 나누지 않는다
_SEG_SPLIT = re.compile(r"\s*(?:\+|＋|\s&|&\s|,|，|\(|\)|\[|\]|\s/\s|//|\s및\s|이랑\s|\s-\s|\||(?<=[가-힣])\.\s*(?=[가-힣]))\s*")


class Hit(NamedTuple):
    start: int
    end: int
    rule: str
    cat: str
    sub: str | None
    tier: float
    weak: bool
    text: str


def find_hits(text: str) -> list[Hit]:
    """겹치는 단서는 긴 쪽 → 등급이 높은 쪽(숫자가 작은 쪽)이 이긴다."""
    raw: list[Hit] = []
    for p in _ALL_PATS:
        for m in p.rx.finditer(text):
            if m.end() <= m.start():
                continue
            raw.append(Hit(m.start(), m.end(), p.rule, p.cat, p.sub, p.tier, p.weak, m.group(0)))
    raw.sort(key=lambda h: (-(h.end - h.start), h.tier, h.start))
    taken: list[Hit] = []
    for h in raw:
        if any(not (h.end <= t.start or h.start >= t.end) for t in taken):
            continue
        taken.append(h)
    taken.sort(key=lambda h: h.start)
    # 액세서리 문맥(…포함/증정/드림)은 제외
    out = []
    for h in taken:
        tail = text[h.end:h.end + 14]
        if _ACCESSORY_CTX.match(tail):
            continue
        out.append(h)
    return out


def segments(text: str) -> list[str]:
    return [s for s in _SEG_SPLIT.split(text) if s and s.strip()]


# ---------------------------------------------------------------------------
# 숫자 단서
# ---------------------------------------------------------------------------
_RX_LEN_CM = re.compile(r"(?<![\d.])(\d{2,3})\s*(?:cm|센티|센치|cmm)(?![a-z])")
_RX_LEN_BARE = re.compile(r"(?<![\d./~\-])(1[4-9]\d|200)(?![\d.%])(?!\s*(?:mm|미리|l\b|리터|만|사이즈|싸이즈|호칭|원|kg|키로|ml|/\s*\d{2,3}\s*mm|cm?\s*~|\s*-\s*1\d\d))")
_RX_RADIUS = [
    re.compile(r"(?:(?<![a-z])r|반경|회전반경|레디어스|radius|r=|r\s*:)\s*[=:.]?\s*(\d{1,2}(?:\.\d)?)\s*(?:m|미터)?(?![\d])"),
    re.compile(r"(?<![\d.])(\d{1,2}(?:\.\d)?)\s*(?:m|미터|메다|메타)(?![a-z0-9])"),
]
_RX_LEN_RAD = re.compile(r"(?<![\d.])(1[3-9]\d)\s*(?:cm)?\s*[/\-]\s*(?:r\s*)?(\d{1,2}(?:\.\d)?)(?![\d])")
_RX_MONDO_RANGE = re.compile(r"(?<![\d.])(1[89][05]|2[0-9][05]|3[0-2][05])\s*(?:mm|미리)?\s*[-~/ㅡ.,]\s*(?:1[89][05]|2[0-9][05]|[0-9][05])(?![\d])")
_RX_MONDO_ONE = re.compile(r"(?<![\d.])(1[89][05]|2[0-9][05])\s*(?:mm|미리|사이즈|싸이즈|size)(?![a-z])")
# 몬도(cm): '26/26.5', '25.5', 'MP26.5'. '24/25'·'25/26'은 시즌 표기라 소수점이 없으면 쓰지 않는다
_RX_MONDO_CM = re.compile(r"(?<![\d.])(2[2-9])\.5\s*/\s*2[2-9](?:\.5)?(?![\d])|(?<![\d./])(2[2-9])\s*/\s*2[2-9]\.5(?![\d])|"
                          r"(?:mp|몬도|mondo)\s*(2[2-9](?:\.5)?)|(?<![\d./])(2[2-9]\.5)(?![\d/])")
_RX_OUTSHELL = re.compile(r"(아웃\s*[쉘셀셸]|outshell|out\s*shell|bsl|아웃솔)\s*:?\s*\d{3}")
_RX_FLEX = re.compile(r"(?<!슈퍼)(?<!프리)(?:플렉스|플랙스|플렉|flex|flx|강도)\s*[:(]?\s*(\d{2,3})|(\d{2,3})\s*(?:플렉스|플랙스|플렉|flex|플(?![가-힣]))")
_RX_FLEX_MONDO_PAIR = re.compile(r"(?<![\d./])(1[0-5]0|[5-9][05])\s*[,/]?\s+(2[2-9][05])(?![\d.])|(?<![\d./])(2[2-9][05])\s*[,/]?\s+(1[0-5]0|[5-9][05])(?![\d.])")
_FLEX_SET = {50, 60, 65, 70, 75, 80, 85, 90, 95, 100, 105, 110, 115, 120, 125, 130, 140, 150}
_RX_WAIST = re.compile(r"(?<![\d.])(1[0-4]\d)\s*[-/]\s*(\d{2,3})\s*[-/]\s*(1[0-4]\d)(?![\d])")


def numeric_signals(text: str) -> dict:
    """스키 길이(cm)·회전반경(m)·부츠 몬도(mm)·플렉스·허리폭(mm)."""
    t = text
    sig: dict = {}
    ctx_bad = re.compile(r"(키|신장|height|허리|가슴|기장|길이\s*$|하의|상의|호칭|사이즈)\s*:?\s*$")
    lens = []
    for m in _RX_LEN_CM.finditer(t):
        v = int(m.group(1))
        pre = t[max(0, m.start() - 6):m.start()]
        if 70 <= v <= 200 and not ctx_bad.search(pre):
            lens.append((m.start(), v, "cm"))
    for m in _RX_LEN_BARE.finditer(t):
        v = int(m.group(1))
        pre = t[max(0, m.start() - 6):m.start()]
        if ctx_bad.search(pre) or re.search(r"(아웃\s*[쉘셀]|bsl|라스트|last|flex|플렉스|플랙스)\s*:?\s*$", pre):
            continue
        lens.append((m.start(), v, "bare"))
    if lens:
        lens.sort()
        sig["ski_length_cm"] = lens[0][1]
        sig["ski_length_kind"] = lens[0][2]
    rad = None
    m = _RX_LEN_RAD.search(t)
    if m and 9 <= float(m.group(2)) <= 35:
        rad = float(m.group(2))
        sig.setdefault("ski_length_cm", int(m.group(1)))
    if rad is None:
        for rx in _RX_RADIUS:
            for m in rx.finditer(t):
                v = float(m.group(1))
                if 8 <= v <= 35:
                    rad = v
                    break
            if rad is not None:
                break
    if rad is not None:
        sig["ski_radius_m"] = rad
    clean = _RX_OUTSHELL.sub(" ", t)
    mondo = None
    m = _RX_MONDO_RANGE.search(clean)
    if m:
        mondo = int(m.group(1))
    else:
        m = _RX_MONDO_ONE.search(clean)
        if m:
            mondo = int(m.group(1))
        else:
            m = _RX_MONDO_CM.search(clean)
            if m:
                v = m.group(1) or m.group(2) or m.group(3) or m.group(4)
                mondo = int(round(float(v) * 10))
    flex = None
    for m in _RX_FLEX.finditer(t):
        v = int(m.group(1) or m.group(2))
        if 40 <= v <= 150:
            flex = v
            break
    # '랑게 120 265', '테크니카 265 120'처럼 단위 없이 플렉스와 몬도가 나란히 있는 경우
    m = _RX_FLEX_MONDO_PAIR.search(clean)
    if m:
        a, b = int(m.group(1) or m.group(4)), int(m.group(2) or m.group(3))
        mondo = mondo or b
        flex = flex or a
    if mondo:
        sig["boot_mondo_mm"] = mondo
    if flex:
        sig["boot_flex"] = flex
    m = _RX_WAIST.search(t)
    if m:
        sig["ski_waist_mm"] = int(m.group(2))
    else:
        m = re.search(r"(?:waist|허리|폭)\s*(\d{2,3})\s*mm|(?<![\d])(\d{2,3})\s*mm\s*(?:허리|waist)", t)
        if m:
            sig["ski_waist_mm"] = int(m.group(1) or m.group(2))
    return sig


def _model_flex(text: str) -> int | None:
    """모델명 속 플렉스 숫자(HAWX PRIME 130, DRS 140, RS 130 등). 몬도·연식·라스트는 제외."""
    for m in re.finditer(r"(?<![\d./])(\d{2,3})(?![\d/])(?!\.\d)(?!\s*(cm|mm|l\b|리터|사이즈|만|원|m\b|미터))", text):
        v = int(m.group(1))
        pre = text[max(0, m.start() - 8):m.start()]
        if re.search(r"(라스트|last|발볼|lv|mv|hv)\s*:?\s*$", pre) and v in (92, 95, 96, 97, 98, 100, 101, 102, 103, 104, 106):
            continue
        if v in _FLEX_SET and v >= 50:
            return v
    return None


# ---------------------------------------------------------------------------
# 분류
# ---------------------------------------------------------------------------
class Classification(NamedTuple):
    category: str
    subcategory: str
    gender: str
    confidence: float
    rule_id: str


_RX_JUNIOR = re.compile(r"주니어|쥬니어|주니[ㅓᅥ]|주니에|{0}jr\.?{1}|{0}j\.r|junior|아동|어린이|키즈|{0}kids?{1}|유아|초등|중학생|청소년|아이용|남아용|여아용|{2}남아\s*\d|{2}여아\s*\d|"
                        r"(?<![\d])(\d{{1,2}})\s*(y|세)(?![a-z가-힣])|{0}y\s*1[0-6]{1}|자녀".format(EB, EA, HK))
_RX_WOMEN = re.compile(r"여성|여자|{0}여(?=\s*(스키|m\b|s\b))|우먼|레이디|{1}lady|{1}ladies|{1}women|{1}womens?|{1}woman|걸스|(?<=\d)\s?w(?![a-z])|{1}w(?=\s*(gw|boa|wide|\d{{2}}(?!\d)))|{1}w[sml]{2}|여아용".format(HK, EB, EA))
_RX_MEN = re.compile(r"남성|남자|{0}남(?=\s*(m\b|l\b|xl\b))|맨즈|{1}mens?{2}|{1}men's|{1}male{2}".format(HK, EB, EA))
_RX_UNISEX = re.compile(r"남녀\s*공용|남여\s*공용|{0}공용|unisex|남자\s*여자|남녀".format(HK))


def detect_gender(*texts: str | None) -> tuple[str, str]:
    """(gender, field_index) — 첫 번째로 성별 단서가 나온 필드에서 가장 먼저 나온 단서."""
    for i, raw in enumerate(texts):
        t = normalize(raw)
        if not t:
            continue
        if i == len(texts) - 1:  # 본문은 '여성용/남성용/주니어용' 같은 강한 표현만
            m = re.search(r"(여성용|여자용|여성\s*전용|남성용|남자용|주니어용|아동용|어린이용)", t[:400])
            if not m:
                continue
            w = m.group(1)
            return ("W" if w.startswith("여") else "M" if w.startswith("남") else "J", f"f{i}")
        if _RX_UNISEX.search(t):
            return ("U", f"f{i}")
        cands = []
        for g, rx in (("J", _RX_JUNIOR), ("W", _RX_WOMEN), ("M", _RX_MEN)):
            m = rx.search(t)
            if m:
                if g == "J" and m.group(0).strip().endswith(("y", "세")) and not re.search(r"\d", m.group(0)):
                    continue
                cands.append((m.start(), g))
        if cands:
            cands.sort()
            g = cands[0][1]
            if "여아" in t[cands[0][0]:cands[0][0] + 2]:
                g = "J"
            return (g, f"f{i}")
    return ("U", "")


_BUNDLE_WORD = re.compile(r"세트|셋트|일괄|풀\s*셋|풀세트|함께|같이")
_SECONDARY_CATS = {"boots", "binding", "poles", "bag", "helmet", "goggle", "gloves", "protector", "tuning"}


def _resolve_segment(hits: list[Hit], seg: str = "") -> Hit | None:
    strong = [h for h in hits if not h.weak]
    pool = strong or hits
    if not pool:
        return None
    best_tier = min(h.tier for h in pool)
    top = [h for h in pool if h.tier == best_tier]
    best = min(top, key=lambda h: h.start)
    # 번들: '스키 모델 … 부츠 … 세트', '아토믹 s9 157cm 레키 폴'처럼 스키 모델이 먼저 나오면 스키가 대표 품목
    if best.cat in _SECONDARY_CATS:
        ski_first = [h for h in pool if h.cat == "ski" and h.tier <= 2 and h.rule != "ski_generic" and h.start < best.start]
        if ski_first:
            seg_len = numeric_signals(seg).get("ski_length_cm") or 0
            if _BUNDLE_WORD.search(seg) or (best.cat != "boots" and seg_len >= 100):
                return ski_first[0]
    return best


def _decide_text(text: str) -> tuple[Hit | None, str, list[Hit]]:
    """텍스트 한 필드 안에서 대표 단서. 반환 (hit, segment_text, all_hits_of_field).

    앞 구간부터 보고, 강한 단서(약하지 않은 단서)가 있는 첫 구간이 결정한다. 강한 단서가
    어느 구간에도 없으면 약한 단서(바인딩)가 있는 첫 구간을 쓴다.
    """
    all_hits = find_hits(text)
    if not all_hits:
        return None, text, []
    best_weak = None
    for seg in segments(text):
        sh = find_hits(seg)
        if not sh:
            continue
        strong = [h for h in sh if not h.weak]
        weak = [h for h in sh if h.weak]
        # '플레이트'만 있고 바인딩 단서가 있으면 바인딩 플레이트(리프터 등)
        if strong and weak and all(h.rule == "ski_plate_word" for h in strong):
            strong = []
        if strong:
            return _resolve_segment(strong, seg), seg, all_hits
        if best_weak is None and weak:
            best_weak = (_resolve_segment(weak, seg), seg)
    if best_weak is not None:
        return best_weak[0], best_weak[1], all_hits
    return None, text, all_hits


def _has_ski_evidence(hits: list[Hit], sig: dict) -> bool:
    if any(h.cat == "ski" and h.tier <= 2 for h in hits):
        return True
    L = sig.get("ski_length_cm")
    return bool(L and 100 <= L <= 200 and not sig.get("boot_mondo_mm"))


_RX_NOTICE = re.compile(r"사기|신고|블럭\s*처리|장난질|조심하세요|주의하세요|글쓰기\s*테스트|장터\s*테스트|^\s*테스트\s*$|"
                        r"^사진\s*참고해주세요$|^시즌정리\s*급처$|^여러가지$")
_RX_SPARE_LENS = re.compile(r"(spare|교체|여분|스페어)\s*(lens|렌즈)\s*(판매|팝니다|$)")


def _seg_says_boots(seg: str, sig: dict) -> bool:
    """서술어(월드컵 등)만 있는 구간이 실제로는 부츠인가: 그 구간에 몬도·플렉스가 있고 스키 길이가 없거나,
    전체에 몬도·플렉스만 있고 스키 길이가 없을 때."""
    s = numeric_signals(seg)
    if (s.get("boot_mondo_mm") or s.get("boot_flex")) and not s.get("ski_length_cm"):
        return True
    if s.get("ski_length_cm"):
        return False
    return bool((sig.get("boot_mondo_mm") or sig.get("boot_flex")) and not sig.get("ski_length_cm"))


def _is_apparel_context(text: str, brand_dom: list[str]) -> bool:
    if brand_dom and brand_dom[0] == "apparel" and "ski" not in brand_dom:
        return True
    return bool(re.search(SIZE_TOKEN, text)) and not re.search(r"\d{3}\s*cm|(?<!\d)1[4-9]\d(?!\d)", text)


def classify(title=None, model_serial=None, spec=None, features=None, body=None) -> Classification:
    """카테고리·하위 카테고리·성별(M/W/U/J)·신뢰도·규칙 ID.

    rule_id 형식: '<필드>:<단서종류>:<패턴>|<하위분류 규칙>'
      단서종류 noun / noun_generic / line / desc / weak, 그 밖에 num:*, brand_domain, body:*, fallback:none, title:notice
    """
    t_title, t_model, t_spec, t_feat = (normalize(x) for x in (title, model_serial, spec, features))
    t_body = normalize(body)[:400] if body else ""
    brand, _alias, _bc, _bf = detect_brand_fields(title, model_serial, spec, features, None)
    dom = BRAND_DOMAIN.get(brand, []) if brand else []
    gender, _gf = detect_gender(title, model_serial, spec, features, body)

    # 0) 공지·사기 신고·테스트 글(제목 기준)
    if _RX_NOTICE.search(t_title):
        return Classification("other", "other", gender, 0.85, "title:notice|single")

    numtext = " ".join(x for x in (t_title, t_model, t_spec) if x)
    sig = numeric_signals(numtext)

    chosen = None
    for fname, ftext, fconf in (("title", t_title, 0.95), ("model", t_model, 0.85),
                                ("spec", t_spec, 0.75), ("features", t_feat, 0.7)):
        if not ftext:
            continue
        hit, seg, fhits = _decide_text(ftext)
        if hit is not None:
            chosen = (fname, fconf, hit, seg, fhits)
            break

    cat = sub_hint = None
    rule = ""
    conf = 0.0
    seg_text = t_title
    hits_for_sub: list[Hit] = []
    L = sig.get("ski_length_cm")
    mondo, flex = sig.get("boot_mondo_mm"), sig.get("boot_flex")
    if chosen:
        fname, fconf, hit, seg_text, hits_for_sub = chosen
        cat, sub_hint = hit.cat, hit.sub
        if hit.weak:
            # 바인딩 단서뿐: 스키 길이 등 스키 단서가 있으면 스키(바인딩 포함 판매)
            if _has_ski_evidence(hits_for_sub, sig) and not re.search(r"바인딩\s*(만|단품)|플레이트\s*만|리프터", seg_text):
                cat, sub_hint, rule, conf = "ski", None, f"{fname}:weak+len:ski", fconf * 0.8
            else:
                rule, conf = f"{fname}:weak:{hit.rule}", fconf * 0.85
        elif hit.tier >= 3:
            rule, conf = f"{fname}:desc:{hit.rule}", fconf * 0.8
            # 서술어만 있을 때는 숫자·브랜드로 교차 확인
            if cat == "ski":
                if dom and "ski" not in dom and dom[0] != "other":
                    cat, sub_hint, rule = dom[0], None, f"{fname}:desc>brand_domain"
                elif re.search(r"데모|데몬|demo|{0}sl{1}|{0}gs{1}|마스터|master".format(EB, EA), hit.text) and _is_apparel_context(numtext, dom):
                    cat, sub_hint, rule = "apparel", None, f"{fname}:desc:apparel_ctx"
                elif _seg_says_boots(seg_text, sig):
                    cat, sub_hint, rule = "boots", None, f"{fname}:desc+num:boots"
            elif cat == "boots" and L and not mondo and not flex:
                cat, sub_hint, rule = "ski", None, f"{fname}:desc+num:ski"
        else:
            kind = {1: "noun", 1.5: "noun_generic", 2: "line"}.get(hit.tier, "line")
            rule = f"{fname}:{kind}:{hit.rule}"
            conf = fconf * (1.0 if hit.tier == 1 else 0.95 if hit.tier == 1.5 else 0.92)
            # 스키·부츠 공용 모델 라인(레드스터·히어로 등)인데 몬도만 있고 스키 길이가 없으면 부츠
            if cat == "ski" and hit.tier == 2 and mondo and not L and not any(
                    h.cat == "ski" and h.tier < 2 for h in hits_for_sub):
                cat, sub_hint, rule = "boots", None, rule + ">mondo:boots"
    else:
        nondom_hard = dom and dom[0] not in ("ski", "boots", "other")
        single_hard = dom and dom[0] in ("ski", "boots") and not ({"ski", "boots"} <= set(dom))
        if nondom_hard:
            cat, rule, conf = dom[0], "brand_domain", 0.6
        elif (mondo or flex) and not L and (not dom or "boots" in dom):
            cat, rule, conf = "boots", "num:boot_size", 0.7
        elif L and (sig.get("ski_length_kind") == "cm" or L >= 150) and (not dom or "ski" in dom):
            cat, rule, conf = "ski", "num:ski_length", 0.7
        elif (mondo or flex) and L and (not dom or "boots" in dom):
            cat, rule, conf = "boots", "num:boot_size+len", 0.55
        elif single_hard:
            # 부츠 전용(랑게·달벨로·테크니카…)·스키 전용(오가사카·스토클리…) 브랜드는 본문보다 브랜드가 확실
            cat, rule, conf = dom[0], "brand_domain", 0.55
        else:
            hit, seg, fhits = _decide_text(t_body) if t_body else (None, "", [])
            if hit is not None and not hit.weak and hit.tier < 3:
                cat, sub_hint, seg_text, hits_for_sub = hit.cat, hit.sub, seg, fhits
                rule, conf = f"body:{'noun' if hit.tier <= 1.5 else 'line'}:{hit.rule}", 0.5
            elif dom:
                cat, rule, conf = dom[0], "brand_domain", 0.45 if dom[0] in ("ski", "boots") else 0.55
            elif _model_flex(numtext) and brand:
                cat, rule, conf = "boots", "num:model_flex", 0.45
            else:
                return Classification("other", "other", gender, 0.3, "fallback:none|single")

    # 브랜드 영역과 모순되는 약한 판정 보정: 의류 전용 브랜드 + 범용 '스키' 명사 → 의류
    if cat == "ski" and rule.split(":")[-1] == "ski_generic" and dom and dom[0] == "apparel" and "ski" not in dom:
        cat, sub_hint, rule = "apparel", None, rule + ">brand_apparel"
    if cat == "snowboard" and rule.endswith("board_generic") and dom and dom[0] not in ("snowboard",):
        if dom[0] in CATEGORIES:
            cat, sub_hint, rule = dom[0], None, rule + ">brand"
    # 번들 제목에서 대표 구간보다 앞 구간이 스키 전용 브랜드명뿐이면('주니어 오가사카 / 아토믹부츠 세트') 스키가 대표 품목
    if chosen and chosen[0] == "title" and cat in _SECONDARY_CATS:
        segs = segments(t_title)
        if seg_text in segs:
            for prev in segs[:segs.index(seg_text)]:
                pb = detect_brand(prev)[0]
                if pb and BRAND_DOMAIN.get(pb) == ["ski"] and not find_hits(prev):
                    cat, sub_hint, seg_text, rule = "ski", None, prev + " " + seg_text, rule + ">prev_seg_ski_brand"
                    break
    # '아토믹 스키폴세트 172cm'처럼 폴 명사가 있어도 스키 길이(150cm 이상)가 제목에 있으면 스키 세트
    if cat == "poles" and (numeric_signals(t_title).get("ski_length_cm") or 0) >= 150 and re.search(r"cm|센티", t_title) \
            and not re.search(r"(대?회전|gs|sl)\s*폴", t_title):
        cat, sub_hint, rule = "ski", None, rule + ">ski_length"
    # 바이저 헬멧의 '스페어 렌즈'만 파는 글
    if cat == "helmet" and _RX_SPARE_LENS.search(t_title) and not re.search(r"헬멧|헬맷|helmet", t_title):
        cat, sub_hint, rule = "goggle", "goggle_lens", rule + ">spare_lens"

    full = " ".join(x for x in (t_title, t_model, t_spec, t_feat) if x)
    ms = " ".join(x for x in (t_model, t_spec, t_feat) if x)
    sub, sub_rule, sub_f = _subcategory(cat, sub_hint, t_title if cat == "goggle" else seg_text, full, sig, brand,
                                        hits_for_sub, ms=ms)
    # 성별·주니어 정합
    if sub in ("ski_junior", "boots_junior") and gender == "U":
        gender = "J"
    if gender == "J" and cat == "ski" and sub not in ("ski_junior", "ski_mogul"):
        sub, sub_rule = "ski_junior", "gender_j"
    if gender == "J" and cat == "boots" and sub not in ("boots_junior", "boots_other"):
        sub, sub_rule = "boots_junior", "gender_j"
    assert sub in CATEGORIES[cat], (cat, sub)
    return Classification(cat, sub, gender, round(min(conf * sub_f, 0.99), 3), f"{rule}|{sub_rule}")


# ---------------------------------------------------------------------------
# 하위 분류
# ---------------------------------------------------------------------------
_SKI_TECH = re.compile(r"데모|데몬|{0}demo{1}|기선|기술\s*선수권|티칭|숏턴|숏\s*턴|"
                       r"{0}tc\s*-?\s*(s|sa|sb|su|st|sk|ss|b){1}|{0}tcs[abtu]?{1}|{0}tssb|오가사카\s*tc(?!\s*-?\s*l)|오가사카\s*(sa|sb|ss|st|su|sk)(?![a-z])|{0}ks\s*-?\s*(up|pv)|슈퍼\s*비라지|슈퍼비라[지쥐]|비라[지쥐]|super\s*virage|"
                       r"{0}sc\s*mt|{0}wc\s*sc|{0}rc4\s*(wc\s*)?sc|{0}sc\s*\.?\s*pro|레이스\s*타이[거커]\s*sc|레이스타이거\s*sc|racetiger\s*sc|뵐클\s*sc|{0}sc\s*(ltd|limited|리미티드)|"
                       r"{0}src{1}|{0}ace\s*slx|엘란\s*slx|{0}[ei][-.\s]?race(?!\s*pro)|이레이스|블루스[터타]|bluester|더블\s*데크|doubledeck|스피드\s*마스터|스피드마스터|speed\s*master|"
                       r"{0}tt[rm]{1}|hero\s*elite\s*st|히어로\s*엘리트\s*st|laser\s*sc|레이서\s*sc|sc\s*ra[cs]er".format(EB, EA))
_SKI_GS = re.compile(r"대회전|{0}gs{1}|giant|{0}g9|g-potion|{0}e[-.]?gs|{0}i[-.]?gs|{0}tc\s*-?\s*l\w*{1}|{0}tc\s*-?\s*lb|트라이언\s*(gs|대회전)|triun\s*gs|"
                     r"speed\s*wc|course\s*wc\s*gs|{0}e[-.\s]?speed|{0}gsj{1}|{0}gs\s*jr|중회전|{0}tc\s*-?\s*m[cfku]{1}".format(EB, EA))
_SKI_SL = re.compile(r"(?<!대)회전(?!\s*반경|반경|성향|계)|{0}sl{1}|{0}slr{1}|slalom|{0}s9|9si|{0}9i\s*pro|{0}s8i|{0}[ei][-.\s]?slr?{1}|{0}eslr?{1}|{0}isl{1}|{0}esl\d|"
                     r"sl\s*wc|wc\s*dept|{0}dept\s*-?\s*m|{0}swc{1}|{0}slj{1}|{0}slx{1}|프라임|{0}prime{1}|반드컵|반\s*월드컵|준\s*월드컵|s\s*포션|s\s*-?\s*potion|sportion|"
                     r"레이저\s*sl|laser\s*sl|{0}rx\s*1[12]\b|sl\s*r\.?d|{0}r\.d\.m".format(EB, EA))
_SKI_ALL = re.compile(r"올라운드|올라운더|{0}all\s*-?\s*round|올\s*마운틴|올마운틴|{0}all\s*mountain|카빙|중회전|"
                      r"{0}x9{1}|redster\s*(ti|q|sc|s7|xt)|레드스터\s*(ti|sc|s7)|{0}s7(?![0-9])|{0}xt\s*1\d\d|클라우드|{0}cloud|supershape|슈퍼\s*쉐이프|슈퍼\s*셰이프|슈퍼세이프|매그넘|magnum|랠리|rally|{0}titan|타이탄|"
                      r"{0}e-v8|shape\s*sx|{0}i\.?shape|xr\s*ti|이지조이|easy\s*joy|absolut\s*joy|페레그린|페페그린|peregrine|디콘|deacon|플레어|타이거샤크|모션타이거|"
                      r"hero\s*elite(?!\s*st)|히어로\s*엘리트(?!\s*st)|{0}nova|노바\s*\d|pursuit|아케이드|코브라|팔마레스|thunderbird|썬더버드|썬더보드|alight|{0}gs\s*pro|addikt|에딕트|s\s*/\s*max|{0}s\s*max|"
                      r"{0}e[-.\s]?speed\s*pro|e[-.\s]?race\s*pro|{0}u\s*-?\s*fs|{0}ks\s*-?\s*ps|spitfire|스핏파이어|transfire|{0}wrc{1}|{0}rtx{1}|{0}rti{1}|power\s*ti|파워\s*(ti|ar)|power\s*ar|"
                      r"the\s*curv|더\s*커브|{0}curv{1}|progressor|street\s*racer|race\s*st|laser\s*(sx|wrt|ax)|e-?lite|이라이트|speed\s*zone|vt8|kendo|켄도|입문|초급|초중급".format(EB, EA))
_SKI_FREE = re.compile(r"파우더|프리\s*라이드|프리라이드|프리스타일|프리\s*스키|트윈\s*팁|트윈팁|twin\s*-?\s*tip|백컨트리|{0}powder{1}|{0}park{1}|투어링|투어용|"
                       r"만트라|mantra|{0}m7{1}|{0}bmt|revolt|리볼트|바쉬|{0}bash{1}|völkl\s*wall|volkl\s*wall|시크릿|secret|블랙펄|black\s*pearl|러슬러|rustler|brahma|bonafide|{0}bent{1}|벤트|"
                       r"mindbender|마인드\s*[벤밴]더|wayback|{0}koala|카이젠|kaizen|파고다|pagoda|dictator|sunset|선셋|mastermind|마스터마인드|navis|soul\s*7|{0}qst{1}|armada|altavia|"
                       r"(?<![\d])(8[5-9]|9\d|1[01]\d)\s*(ti|mm)?(?=\s|$)(?=.*(만트라|mantra|켄도|kendo|qst|mindbender|bent))".format(EB, EA))
_SKI_SPEED = re.compile(r"슈퍼\s*지(?![가-힣])|super\s*-?\s*g{0}|{1}sg{0}|(?<!종합)(?<!종합\s)활강|{1}dh{0}(?!\s*-)|다운힐\s*스키".format(EA, EB))
_SKI_RACE_CTX = re.compile(rf"{HK}마스터|{EB}master{EA}|" r"월드컵|월드겁|올드컵|{0}worldcup|{0}world\s*cup|{0}wc{1}|{0}fis{1}|{0}pis{1}|레이싱|{0}race(?!\s*(st|carving|tiger))|{0}rd{1}|반드컵|대회\s*출전|경기용|race\s*department|factory|athlete|{0}lab{1}|equipe".format(EB, EA))


def _ski_sub(seg: str, full: str, sig: dict, rule_hint: str | None, ms: str = "") -> tuple[str, str, float]:
    """스키 하위 분류. 대표 구간 → 모델·스펙 → 제목 전체 순으로 단어를 찾고, 없으면 길이·회전반경."""
    L = sig.get("ski_length_cm")
    R = sig.get("ski_radius_m")
    W = sig.get("ski_waist_mm")
    texts = [(seg, 1.0)] + ([(ms, 0.97)] if ms else []) + [(full, 0.95)]
    for text, f in ((seg, 1.0), (full, 0.95)):
        if re.search(r"모글|모굴|{0}mogul{1}|{0}me\s*-?\s*ce{1}|{0}mr\s*-?\s*(ce|c|g|sg|d|j){1}|{0}mrce|{0}mrg{1}|{0}fr\s*ng|하트\s*모글".format(EB, EA), text):
            return "ski_mogul", "word:mogul", f
        if _RX_JUNIOR.search(text) and not re.search(r"주니어\s*(부츠|스키복)", text):
            m = _RX_JUNIOR.search(text)
            if not (m.group(0).endswith(("y", "세")) and not re.search(r"\d", m.group(0))):
                return "ski_junior", "word:junior", f
        if re.search(r"{0}j[24]{1}|redster\s*j\d|레드스터\s*j\d|jett\s*jr|team\s*plate|{0}slj{1}|{0}gsj{1}|{0}jrs{1}|{0}rt\s*주니어|120\s*rt|race\s*team\s*jr".format(EB, EA), text):
            return "ski_junior", "line:junior", f
    if L and L <= 140:
        return "ski_junior", "len<=140", 0.95
    for text, f in texts:
        if _SKI_SPEED.search(text):
            return "ski_race_speed", "word:speed", f
        if _SKI_FREE.search(text) or (W and W >= 85):
            return "ski_freeride_park", "word:freeride" if not (W and W >= 85) else "waist>=85", f
        gs = _SKI_GS.search(text)
        sl = _SKI_SL.search(text)
        tech = _SKI_TECH.search(text)
        allr = _SKI_ALL.search(text)
        band = re.search(r"반드컵|반\s*월드컵|준\s*월드컵", text)
        if gs and (not sl or gs.start() <= sl.start()):
            if allr and re.search(r"올라운드|올라운더", text) and not re.search(r"대회전", text):
                return "ski_allround", "word:allround", f
            return "ski_race_gs", "word:gs", f
        if band and not tech:
            return "ski_race_sl", "word:bandcup", f
        if tech:
            return "ski_technical", "word:technical", f
        if sl:
            return "ski_race_sl", "word:sl", f
        if allr:
            return "ski_allround", "word:allround", f
    race = _SKI_RACE_CTX.search(seg) or _SKI_RACE_CTX.search(full)
    if race:
        if (L and L >= 175) or (R and R >= 19):
            return "ski_race_gs", "race+len/r", 0.9
        if L and 150 <= L <= 172:
            return "ski_race_sl", "race+len", 0.9
        if R and R <= 14:
            return "ski_race_sl", "race+r", 0.85
        return "ski_race_other", "race_ctx", 0.85
    if R:
        if R >= 20 and (not L or L >= 175):
            return "ski_race_gs", "r>=20", 0.8
        return "ski_allround", "radius", 0.8
    if L and L >= 150:
        return "ski_allround", "len_only", 0.7
    return "ski_other", "none", 0.7


_BOOT_RACE = re.compile(r"레이싱|{0}race(?!\s*pro\s*\d0)|레이스|월드컵|월드겁|{0}wc{1}|{0}fis{1}|플러그|{0}plug|{0}z[abcj]{1}|{0}z[abcj]\s*1\d0|{0}rd{1}|rd\s*-?\s*[sm]{1}|5\s*rd|podium|포디움|"
                        r"도베르만|도베르반|도메르만|dobermann|doberman|{0}gp\s*1[0-4]0|gp1[0-4]0|1[34]0\s*gp|{0}edt{1}|랩터\s*(wcr|wc|we|rs|pro|1[2-4]0|월드컵|255|플랙스\s*130)|raptor\s*(wcr|wrc|wc|rs|pro|1[2-4]0|255)|{0}rator{1}|"
                        r"redster|레드스터|{0}cs\s*1[13]0|club\s*sport|레메디|히어로\s*(월드컵|듀얼)|hero\s*(world|1[2-4]0)|{0}hero{1}|히어로(?=\s*1\d0)|{0}rs\s*1[0-5]0|{0}rs1[0-5]0|{0}rs\s*(90|wide|mv|zb|za)|랑게\s*rs|"
                        r"{0}drs{1}|{0}drs\s*\d|파이어버드|firebird|{0}s\s*/\s*race|{0}r\s*-?\s*evo|레보|렉삼|{0}x\s*max\s*race|소프트\s*월드".format(EB, EA))

_BOOT_JR = re.compile(r"{0}jr{1}|주니어|쥬니어|아동|어린이|키즈|{0}kids?{1}|hawx\s*jr|{0}jr\s*\d0|raptor\s*[4-7]0{1}|랩터\s*[4-7]0|drs\s*60|race\s*pro\s*60|{0}jtr|xmax\s*60|x\s*max\s*60|team\s*td".format(EB, EA))
_BOOT_LINER = re.compile(r"이너\s*부츠|이너부츠|{0}이너(?!\s*(새것|글러브|장갑))|라이너|{1}liners?{2}|인[투튜]이션|intuition|zipfit|짚핏|건조기|드라이어|"
                         r"{1}c-?\s*pack|c1700|1700\s*b|1300\s*b|heat\s*kit|배터리".format(HK, EB, EA))
_BOOT_MINOR = re.compile(r"(인솔|insoles?|깔창|부스터|스트[랩렙]|열선|발열)(?!\s*x(?![a-z]))(?!\s*없)")
_BOOT_OTHER_TYPE = re.compile(r"텔레마크|telemark|{0}voodoo|(?<![\d\-~/])75\s*mm".format(EB))


def _boots_sub(seg: str, full: str, sig: dict, hits: list[Hit]) -> tuple[str, str, float]:
    has_model = any(h.cat == "boots" and h.tier == 2 for h in hits)
    # 제목 구간의 플렉스가 스펙의 범위 표기('110-120 플렉스')보다 우선
    flex = numeric_signals(seg).get("boot_flex") or _model_flex(seg) or sig.get("boot_flex") or _model_flex(full)
    mondo = sig.get("boot_mondo_mm")
    # 액세서리: 이너·라이너·건조기·열선 배터리는 그것만 파는 글. 인솔·부스터·스트랩은 '부츠 인솔'처럼 붙어 있거나 부츠 본체 단서가 없을 때만
    if _BOOT_LINER.search(seg) and not (sig.get("boot_flex") and not re.search(r"이너|라이너|liner|인[투튜]이션|intuition|zipfit", seg)):
        return "boots_other", "word:liner_dryer_heater", 0.95
    m = _BOOT_MINOR.search(seg)
    if m:
        bw = re.search(r"부츠|boots?|스키화", seg)
        if bw and bw.start() < m.start() and seg[bw.end():m.start()].strip() == "":
            return "boots_other", "word:boot_accessory", 0.95
        if not bw and not has_model and not sig.get("boot_flex"):
            return "boots_other", "word:accessory", 0.9
    if _BOOT_OTHER_TYPE.search(seg) or _BOOT_OTHER_TYPE.search(full):
        return "boots_other", "word:telemark", 0.9
    # '여성 및 주니어'처럼 여성용이 먼저 나오면 여성 부츠로 본다
    if (_BOOT_JR.search(seg) and not re.search(r"여성\s*(및|/|,|&|과|이나)?\s*(주니어|아동)", seg)) or (mondo and mondo <= 215):
        return "boots_junior", "word/size:junior", 0.95
    if flex and flex <= 70 and mondo and mondo <= 245 and not re.search(r"여성|여자|{0}w(?![a-z])|(?<=\d)w(?![a-z])".format(EB), full):
        return "boots_junior", "flex<=70&small", 0.85
    if _BOOT_RACE.search(seg) or _BOOT_RACE.search(full) or (flex and flex >= 110 and re.search(r"랩터|raptor", full)):
        return "boots_race", "word/line:race", 0.95
    if flex:
        if flex >= 120:
            return "boots_performance", f"flex{flex}", 0.95
        return "boots_allround", f"flex{flex}", 0.95
    return "boots_allround", "default", 0.75


_RX_TOP = re.compile(r"상의|자켓|재킷|쟈켓|jacket|{0}jk{1}|아우터|outer|점퍼|아노락|패딩".format(EB, EA))
_RX_BOTTOM = re.compile(r"하의|바지|팬츠|{0}pants?{1}|{0}pts{1}|빕|{0}bibs?{1}".format(EB, EA))
_RX_ONLY = re.compile(r"(상의|하의|자켓|바지|팬츠)\s*만|단품|바지만|상의만|하의만")


def _apparel_sub(seg: str, full: str, hits: list[Hit], brand: str | None = None) -> tuple[str, str, float]:
    # 제목 구간이 '자켓'뿐이어도 제목·모델·스펙 전체에 상의와 하의가 함께 나오면 세트('자켓,팬츠', '상의 L 하의 XL')
    full_set = bool(_RX_TOP.search(full) and _RX_BOTTOM.search(full) and not _RX_ONLY.search(full))
    # '스키바지 멜빵/서스펜더'는 바지가 아니라 멜빵(액세서리). '멜빵 팬츠'(빕 팬츠)는 바지
    if re.search(r"(바지|팬츠)\s*\(?\s*(바지\s*)?(멜빵|서스펜더)|서스펜더|^\s*\S*\s*멜빵\s*$|스키\s*멜빵", seg) and not re.search(r"멜빵\s*(팬츠|바지)", seg):
        return "apparel_other", "word:suspender", 1.0
    for text, f in ((seg, 1.0), (full, 0.9)):
        hs = [h for h in find_hits(text) if h.cat == "apparel" and h.sub]
        subs = {h.sub for h in hs}
        top = bool(re.search(r"상의|자켓|재킷|쟈켓|jacket|{0}jk{1}|아우터|outer|점퍼|아노락|패딩".format(EB, EA), text))
        bottom = bool(re.search(r"하의|바지|팬츠|{0}pants?{1}|{0}pts{1}|빕|{0}bibs?{1}".format(EB, EA), text))
        only_top = re.search(r"상의\s*만|자켓\s*만|단품.*(상의|자켓)|(상의|자켓)\s*(단품|만)", text)
        only_bottom = re.search(r"하의\s*만|바지\s*만|팬츠\s*만|(바지|하의|팬츠)\s*(단품|만)|바지만", text)
        if "apparel_race_suit" in subs:
            if any(h.sub == "apparel_race_suit" and re.search(r"스노우\s*수트|올인원", h.text) for h in hs):
                return "apparel_set", "word:onepiece", f
            return "apparel_race_suit", "word:race_suit", f
        if "apparel_mid_base" in subs:
            return "apparel_mid_base", "word:mid_base", f
        if "apparel_vest" in subs and not (top or bottom):
            return "apparel_vest", "word:vest", f
        if only_top and not only_bottom:
            return "apparel_jacket", "word:top_only", f
        if only_bottom and not only_top:
            return "apparel_pants", "word:bottom_only", f
        if "apparel_set" in subs or (top and bottom):
            return "apparel_set", "word:set", f
        if re.search(r"세트|셋트|{0}set{1}|한\s*벌|1\s*벌".format(EB, EA), text) and not re.search(r"헬멧|고글", text):
            return "apparel_set", "word:set2", f
        if (top or bottom) and full_set:
            return "apparel_set", "word:set_full", f * 0.95
        if top:
            return "apparel_jacket", "word:top", f
        if bottom:
            return "apparel_pants", "word:bottom", f
        if "apparel_vest" in subs:
            return "apparel_vest", "word:vest", f
        if "apparel_other" in subs:
            return "apparel_other", "word:accessory", f
    for text, f in ((seg, 1.0), (full, 0.9)):
        if re.search(r"스키\s*복|스캐복|팀\s*복|데모|데몬|데목|게임\s*복|경기복|모글\s*복|웨어|wear|쉘|shell|레플리카|코치\s*복|zone|sio|s\.i\.o|퍼포|"
                     r"{0}(cpm|cdt|csm)\s*-?\s*\d{{2}}".format(EB), text):
            return "apparel_set", "default:skiwear_set", f * 0.8
    # 의류 브랜드명 + 사이즈뿐인 글(예: '2425 카브 네이비 M')은 스키복 세트가 대부분
    if (brand and BRAND_DOMAIN.get(brand, [""])[0] == "apparel") or re.search(SIZE_TOKEN, full):
        return "apparel_set", "default:brand_or_size", 0.7
    return "apparel_other", "default", 0.7


def _subcategory(cat: str, hint: str | None, seg: str, full: str, sig: dict, brand: str | None, hits: list[Hit],
                 ms: str = ""):
    if cat == "ski":
        return _ski_sub(seg, full, sig, hint, ms)
    if cat == "boots":
        return _boots_sub(seg, full, sig, hits)
    if cat == "apparel":
        return _apparel_sub(seg, full, hits, brand)
    if cat == "binding":
        if hint == "binding_other" or re.search(r"브레이크|brake|힐\s*피스|토\s*피스|스토퍼", seg):
            return "binding_other", "word:parts", 1.0
        if hint == "binding_plate" or re.search(r"플레이트|plate|리프터|lifter|더비|derby|585|피스톤|piston", seg):
            return "binding_plate", "word:plate", 1.0
        if re.search(r"레이스|레이싱|{0}race{1}|x\s*-?\s*comp|xcomp|{0}comp\s*1[0-8]|{0}fks|px\s*1[5-8]|spx\s*15|wc\s*rockerace|{0}z\s*1[7-9]|{0}z20|월드컵|{0}wc{1}|{0}fis{1}|xcell\s*16|icon\s*rs\s*16|x\s*1[6-9]".format(EB, EA), seg):
            return "binding_race", "word:race", 0.95
        if re.search(r"그리폰|griffon|어택|attack|warden|strive|{0}sth\s*2?|{0}shift|시프트|쉬프트|jester|kingpin|킹핀|그립\s*워크|gw|spx\s*1[0-4]|{0}z1[0-3]|xcell\s*1[0-4]|프리|파우더|올마운틴|\d{{2}}\s*바인딩|바인딩".format(EB), seg):
            return "binding_allround", "word:allround", 0.85
        return "binding_other", "default", 0.7
    if cat == "snowboard":
        if hint in CATEGORIES["snowboard"]:
            return hint, "word", 1.0
        if re.search(r"부츠|boots?", seg):
            return "board_boots", "word:boots", 0.9
        if re.search(r"바인딩|binding", seg):
            return "board_binding", "word:binding", 0.9
        if re.search(r"데크|deck|보드(?!\s*(복|헬멧|장갑|가방))|\d{3}\s*(cm)?", seg) or brand in ("bc-stream", "burton"):
            return "board_deck", "word:deck", 0.85
        return "board_other", "default", 0.7
    if cat == "helmet":
        if hint == "helmet_race" or re.search(r"{0}fis{1}|레이싱|레이스|{0}race{1}|친\s*가드|턱|chin|스컬\s*듀?두?라|skull\s*dura|vulcano|fis\s*6\.8|다운힐\s*헬멧|월드컵\s*헬멧".format(EB, EA), seg + " " + full):
            return "helmet_race", "word:race", 1.0
        return "helmet_allround", "default", 0.95
    if cat == "goggle":
        return _goggle_sub(seg, full)
    if cat == "poles":
        if hint == "poles_race" or re.search(r"(대?회전|gs|sl|레이싱|레이셍|곡선|휜|가드)\s*폴|폴\s*가드|회전\s*가드|오픈\s*가드|가드\s*폴|레이싱\s*(폴|바스켓)|{0}wcr{1}|월드컵\s*(레이스|레이싱)|"
                                     r"레이싱|{0}race{1}|{0}gs{1}".format(EB, EA), seg):
            return "poles_race", "word:race", 1.0
        return "poles_allround", "default", 0.95
    if cat == "bag":
        if hint in CATEGORIES["bag"]:
            # 블랙브라이어 '익스트림 백팩 65L'처럼 부츠백 겸용 백팩은 백팩으로 둔다
            return hint, "word", 1.0
        if brand == "blackbriar":
            if re.search(r"캐리어|휠", seg):
                return "bag_wheel", "brand:blackbriar", 0.9
            return "bag_backpack", "brand:blackbriar", 0.85
        return "bag_other", "default", 0.8
    if cat in ("gloves", "protector", "tuning", "other"):
        return CATEGORIES[cat][0], "single", 1.0
    return CATEGORIES[cat][-1], "default", 0.7


_GOG_WITH_LENS = re.compile(r"(\+|포함|및|&|추가|여분|케이스)\s*.{0,12}렌즈|렌즈\s*\d\s*개|렌즈\s*2|주야|주간\s*/?\s*야간|렌즈\s*포함|(판매|팝니다)[^렌]*렌즈|렌즈\s*\+|\s/\s.*렌즈")
_LENS_ONLY = re.compile(r"(주간|야간|클리어|변색|미러|레드|그린|교체|스페어|spare|새|여분)\s*렌즈|렌즈\s*(판매|팝니다|새거|구[해합]|구매|새상품|미사용|\(?하자)|{0}lens{1}".format(EB, EA))


def _goggle_sub(title: str, full: str) -> tuple[str, str, float]:
    """고글 본체 vs 교체 렌즈. 제목 기준."""
    t = title
    if re.search(r"렌즈\s*만|렌즈만|lens\s*only", t) or re.search(r"렌즈\s*만|렌즈만", full):
        return "goggle_lens", "word:lens_only", 1.0
    if not re.search(r"렌즈|{0}lens{1}".format(EB, EA), t):
        return "goggle", "default", 1.0
    if re.search(r"고글|goggle|보글", t) and not re.search(r"고글\s*렌즈", t):
        return "goggle", "word:goggle", 1.0
    if _GOG_WITH_LENS.search(t):
        return "goggle", "word:goggle_with_lens", 0.9
    if _LENS_ONLY.search(t) or re.search(r"고글\s*렌즈", t):
        return "goggle_lens", "word:lens", 0.9
    return "goggle", "word:goggle_lens_desc", 0.8


__all__ = [
    "ALL_BRANDS", "BRAND_SLUGS_EXT", "BRAND_ALIASES", "BRAND_DOMAIN", "Classification",
    "classify", "detect_brand", "detect_brands_all", "detect_brand_fields", "detect_gender",
    "normalize", "numeric_signals", "find_hits",
]
