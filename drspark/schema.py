"""공통 규약: 카테고리 체계, 브랜드 슬러그, canonical ID 형식, 식별 등급 규칙.

규칙 모듈(parse/taxonomy/listing_units)과 LLM 라벨, 적정가 모델이 모두 이 값을 쓴다.
"""
from __future__ import annotations

import re

# 상위 카테고리 → 하위 카테고리
CATEGORIES: dict[str, list[str]] = {
    "ski": ["ski_race_sl", "ski_race_gs", "ski_race_speed", "ski_race_other", "ski_technical",
            "ski_allround", "ski_mogul", "ski_freeride_park", "ski_junior", "ski_other"],
    "boots": ["boots_race", "boots_performance", "boots_allround", "boots_junior", "boots_other"],
    "binding": ["binding_race", "binding_allround", "binding_plate", "binding_other"],
    "snowboard": ["board_deck", "board_boots", "board_binding", "board_other"],
    "apparel": ["apparel_jacket", "apparel_pants", "apparel_set", "apparel_race_suit",
                "apparel_mid_base", "apparel_vest", "apparel_other"],
    "gloves": ["gloves"],
    "helmet": ["helmet_race", "helmet_allround"],
    "goggle": ["goggle", "goggle_lens"],
    "protector": ["protector"],
    "poles": ["poles_race", "poles_allround"],
    "bag": ["bag_boot", "bag_ski", "bag_wheel", "bag_backpack", "bag_other"],
    "tuning": ["tuning"],
    "other": ["other"],
}
CATEGORY_KO = {
    "ski": "스키", "boots": "부츠", "binding": "바인딩/플레이트", "snowboard": "스노보드",
    "apparel": "의류", "gloves": "장갑", "helmet": "헬멧", "goggle": "고글", "protector": "보호대",
    "poles": "폴", "bag": "가방", "tuning": "튜닝용품", "other": "기타",
}
SUBCATEGORY_KO = {
    "ski_race_sl": "회전(SL)", "ski_race_gs": "대회전(GS)", "ski_race_speed": "슈퍼G/활강",
    "ski_race_other": "레이스(기타)", "ski_technical": "기술선수권·데모", "ski_allround": "올라운드·카빙",
    "ski_mogul": "모글", "ski_freeride_park": "프리라이드·파크", "ski_junior": "주니어", "ski_other": "스키(기타)",
}

# 매물 유형: sale(판매), wanted(구매 희망), non_sale(공지·사기 신고·잡담 등)
LISTING_TYPES = ["sale", "wanted", "non_sale"]
GENDERS = ["M", "W", "U", "J"]  # 남성, 여성, 공용/불명, 주니어

# 브랜드 슬러그(소문자 ASCII). 여기에 없는 브랜드는 공식 영문명을 소문자·하이픈으로 바꿔 쓴다.
BRAND_SLUGS: dict[str, str] = {
    "atomic": "Atomic", "rossignol": "Rossignol", "fischer": "Fischer", "head": "Head",
    "nordica": "Nordica", "volkl": "Völkl", "salomon": "Salomon", "blizzard": "Blizzard",
    "ogasaka": "Ogasaka", "dynastar": "Dynastar", "elan": "Elan", "stockli": "Stöckli",
    "kastle": "Kästle", "augment": "Augment", "hart": "Hart", "lange": "Lange",
    "tecnica": "Tecnica", "dalbello": "Dalbello", "marker": "Marker", "look": "Look",
    "tyrolia": "Tyrolia", "descente": "Descente", "phenix": "Phenix", "goldwin": "Goldwin",
    "onyone": "ONYONE", "carve": "CARVE", "spyder": "Spyder", "kjus": "KJUS", "fusalp": "Fusalp",
    "arcteryx": "Arc'teryx", "colmar": "Colmar", "mizuno": "Mizuno", "energiapura": "Energiapura",
    "rh-plus": "rh+", "reusch": "Reusch", "leki": "Leki", "komperdell": "Komperdell",
    "sinano": "SINANO", "smith": "Smith", "giro": "Giro", "uvex": "Uvex", "poc": "POC",
    "oakley": "Oakley", "sweet-protection": "Sweet Protection", "briko": "Briko",
    "dainese": "Dainese", "blackbriar": "Blackbriar", "kizaki": "Kizaki", "id-one": "ID one",
    "swix": "Swix", "toko": "Toko", "patagonia": "Patagonia", "the-north-face": "The North Face",
    "maloja": "Maloja", "burton": "Burton",
    "backcountry-access": "BCA",
}


def brand_name(slug: str | None) -> str | None:
    """표시용 브랜드명. 사전에 없으면 슬러그를 단어 단위로 대문자화한다."""
    if not slug:
        return slug
    if slug in BRAND_SLUGS:
        return BRAND_SLUGS[slug]
    from .taxonomy import BRAND_SLUGS_EXT  # 규칙 모듈의 확장 브랜드 사전(순환 import 방지로 지연 로드)
    return BRAND_SLUGS_EXT.get(slug) or slug.replace("-", " ").title()

# canonical ID 형식: brand / brand/family / brand/family/model (소문자 ASCII, 숫자, 하이픈)
# 모델 슬러그에는 시즌·길이·사이즈·색상을 넣지 않는다(속성으로 따로 둔다).
_SLUG = r"[a-z0-9]+(?:-[a-z0-9]+)*"
FAMILY_ID_RE = re.compile(rf"^{_SLUG}/{_SLUG}$")
MODEL_ID_RE = re.compile(rf"^{_SLUG}/{_SLUG}/{_SLUG}$")


def id_grade(model_resolved: bool, season_resolved: bool, family_id: str | None) -> str:
    """식별 신뢰도 등급.

    A: 정확한 모델 + 모델 연식(시즌) 확정
    B: 모델 패밀리까지 확정(정확한 모델이어도 시즌 불명이면 B)
    C: 브랜드·카테고리 수준 또는 불명
    """
    if model_resolved and season_resolved:
        return "A"
    if family_id or model_resolved:
        return "B"
    return "C"


def season_label(start_year: int | None) -> str | None:
    """2024 → '24/25'."""
    if start_year is None:
        return None
    return f"{start_year % 100:02d}/{(start_year + 1) % 100:02d}"
