# dr.spark_alarm: 드스파크 중고 장비 새 글 → Discord 알림(+ 적정가 보강)
#
#   docker compose -p drspark-alarm build
#   docker compose -p drspark-alarm up -d
#
# 데이터(SQLite DB)와 로그(drspark.log)는 모두 /app/data에 남는다(compose.yaml에서 ./data를 마운트).
# source.py는 로그를 "현재 폴더"의 drspark.log에 쓰므로 작업 폴더를 /app/data로 둔다.
FROM python:3.12-slim

# 호스트의 ./data 소유자와 맞출 UID/GID(기본 1000). 다르면 빌드할 때 바꾼다:
#   docker compose -p drspark-alarm build --build-arg APP_UID=$(id -u) --build-arg APP_GID=$(id -g)
ARG APP_UID=1000
ARG APP_GID=1000

ENV PYTHONIOENCODING=utf-8 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=Asia/Seoul \
    PYTHONPATH=/app \
    PYTHONSAFEPATH=1 \
    DRSPARK_DB=/app/data/drspark_seen.db

# PYTHONPATH=/app: 작업 폴더(/app/data)에서도 `import source`, `python -c ...`가 된다.
# PYTHONSAFEPATH=1: 쓰기 가능한 작업 폴더(볼륨)를 import 경로에 넣지 않는다(심어진 .py 실행 방지,
#   Windows Docker Desktop 바인드 마운트의 readdir EINVAL 회피).

# libgomp1: lightgbm(OpenMP) 런타임, tzdata: TZ=Asia/Seoul(로그 시각·스케줄러)
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgomp1 tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

RUN groupadd --gid "${APP_GID}" alarm \
 && useradd --uid "${APP_UID}" --gid "${APP_GID}" --no-create-home --home-dir /app --shell /usr/sbin/nologin alarm

# 코드·사전·모델(.dockerignore로 DB·로그·.env·.git은 뺀다)
COPY --chown=root:root . /app
# /app/data: DB·로그(볼륨), /app/.cache: drspark 패키지 임시 폴더(쓰기 필요)
RUN mkdir -p /app/data /app/.cache \
 && chown alarm:alarm /app/data /app/.cache

USER alarm
WORKDIR /app/data
VOLUME ["/app/data"]

# 스케줄러는 1분(±20초)마다 목록을 가져오며 매번 로그를 남긴다. 5분 넘게 로그가 멈추면 비정상으로 본다.
HEALTHCHECK --interval=1m --timeout=10s --start-period=3m --retries=3 \
  CMD python -c "import os,sys,time; sys.exit(0 if time.time() - os.path.getmtime('drspark.log') < 300 else 1)"

CMD ["python", "/app/source.py"]
