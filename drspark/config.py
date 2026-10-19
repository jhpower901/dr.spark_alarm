"""dr.spark_alarm용 경로 설정(분석 저장소 drspark/config.py 대체본, export_alarm_bundle.py가 생성).

분석 모듈(resolver, privacy)이 `from . import config`로 읽는 값만 둔다.
DRSPARK_PRICING_DIR 환경변수로 사전·모델 폴더를 바꿀 수 있다(기본: 저장소의 pricing_data/).
"""
from __future__ import annotations

import os
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
CURATED = Path(os.environ.get("DRSPARK_PRICING_DIR") or BASE / "pricing_data")  # 빈 값이면 기본 폴더
SCRATCH = BASE / ".cache"
SCRATCH_INTERIM = SCRATCH
SECRET_KEY = SCRATCH / "hmac.key"
TZ = "Asia/Seoul"
