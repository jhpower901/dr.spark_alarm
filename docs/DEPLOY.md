# 배포와 업그레이드

적정가 알림 버전을 설치하거나 기존 설치를 업그레이드하는 절차다. 배치 방식에 따라 절을 고른다.

1. [기존 Docker Compose 배포를 제자리에서 업그레이드](#1-기존-docker-compose-배포를-제자리에서-업그레이드): 앱 폴더를 컨테이너에 바인드 마운트해 돌리던 배포. 운영 서버는 이 방식으로 업그레이드했다.
2. [이 저장소의 compose.yaml로 설치](#2-이-저장소의-composeyaml로-설치): 새로 설치할 때.
3. [systemd + venv](#3-systemd--venv): Docker 없이 돌릴 때.

어느 방식이든 지킬 것은 같다.

- **DB는 그대로 이어 쓴다.** `seen` 테이블이 "이미 알린 글" 목록이자 재게시 탐지 이력이다. DB 경로가 바뀌어 빈 DB로 시작하면 첫 실행에서 목록 첫 페이지의 글이 모두 새 글로 다시 알림이 가고, 재게시·가격 인하 탐지도 이력 없이 시작한다.
- **업그레이드 전에 멈추고 백업한다.** `drspark_seen.db`는 WAL 모드라 `-wal`, `-shm` 파일까지 함께 복사한다.
- **`drspark/`, `pricing/`, `pricing_data/`를 함께 넣는다.** `source.py`만 바꾸면 적정가 보강이 꺼진 채(`pricing package not importable` 경고) 예전 알림만 나간다.
- **lightgbm에는 OpenMP 런타임이 필요하다.** Debian/Ubuntu 계열 이미지·서버에는 `libgomp1`을 설치한다.
- **메모리.** 보강을 켜면 프로세스가 약 250MB를 쓴다(끄면 약 60MB). 컨테이너 메모리 제한이 있으면 512MB 이상으로 둔다.

## 1. 기존 Docker Compose 배포를 제자리에서 업그레이드

### 전제: 기존 배치

이 절은 다음과 같이 돌던 배포를 가정한다. 다른 서비스와 같은 compose 파일에 들어 있어도 된다. 이 서비스만 멈추고 다시 띄운다.

```yaml
services:
  <service>:
    build: ./<app-dir>                 # 앱 폴더의 작은 Dockerfile
    working_dir: /app
    volumes:
      - ./<app-dir>:/app               # 코드·DB·로그가 모두 이 폴더에 있다
    env_file: ./<app-dir>/.env.local   # DISCORD_WEBHOOK_URL=...
    restart: unless-stopped
```

```dockerfile
# <app-dir>/Dockerfile (운영용, 이 저장소의 Dockerfile과 다르다)
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "source.py"]
```

- 코드는 바인드 마운트로 들어간다. 이미지에는 파이썬과 의존성만 있다. 그래서 새 코드는 앱 폴더에 복사하고, 새 의존성은 이미지를 다시 빌드해서 넣는다.
- `DRSPARK_DB`를 설정하지 않았으므로 DB는 작업 폴더(`/app`)의 `drspark_seen.db`, 즉 `<app-dir>/drspark_seen.db`다. 로그 `drspark.log`도 같은 폴더에 생긴다. 새 코드도 `DRSPARK_DB`가 없으면 같은 파일을 쓴다.

**이 저장소의 `Dockerfile`·`compose.yaml`을 앱 폴더에 덮어쓰지 않는다.** 저장소의 Dockerfile은 새 설치용이다. `DRSPARK_DB=/app/data/drspark_seen.db`와 `WORKDIR /app/data`를 설정하고 코드를 이미지 안에 복사하며 UID 1000으로 돈다. 이 배치에 덮어쓰면 DB 경로가 바뀌어 빈 DB로 시작하고, 첫 실행에서 목록 첫 페이지 전체를 다시 알린다.

### 변수

명령은 compose 파일이 있는 폴더에서 차례대로 실행한다. 아래 값은 환경에 맞게 채운다.

```bash
cd <compose-dir>
SVC=<service>                       # 알림 서비스 이름(docker compose config --services)
APP=$(realpath <app-dir>)           # 바인드 마운트한 앱 폴더(호스트 경로)
NEW=$(realpath <new-code-dir>)      # 이 저장소를 받은 폴더(git clone 또는 압축 해제)
CID=$(docker compose ps -q "$SVC"); IMG=$(docker inspect --format '{{.Config.Image}}' "$CID"); echo "$IMG"
ROLLBACK=drspark-alarm-rollback:$(date +%Y%m%d-%H%M)
```

`IMG`는 compose가 이 서비스에 붙인 이미지 이름이다(보통 `<project>-<service>`). 변수는 같은 셸에서만 살아 있다. 셸을 새로 열었으면 `docker image ls drspark-alarm-rollback`과 `ls -d <app-dir>.bak-*`로 값을 찾아 `ROLLBACK`, `BK`를 다시 정한다.

### 1단계: 롤백용 이미지 태그

```bash
docker image tag "$(docker inspect --format '{{.Image}}' "$CID")" "$ROLLBACK"
docker image ls drspark-alarm-rollback
```

지금 돌고 있는 컨테이너의 이미지(ID 기준)에 이름을 하나 더 붙여 둔다. 다시 빌드해도 이 이미지는 남는다.

### 2단계: 이 서비스만 중지

```bash
docker compose stop "$SVC"
docker compose ps
```

같은 compose 파일의 다른 서비스는 그대로 돈다.

### 3단계: 앱 폴더 전체 백업

```bash
BK="$APP.bak-$(date +%Y%m%d-%H%M)"
cp -a "$APP" "$BK" && ls -la "$BK"
```

DB(`drspark_seen.db`, `-wal`, `-shm`), 로그, `.env.local`, 운영 Dockerfile이 모두 들어간다. 백업에는 개인정보(작성자·연락처)가 있으므로 권한을 그대로 두고, 며칠 운영해 본 뒤 지운다.

### 4단계: 코드 파일만 복사

```bash
cp "$NEW/source.py" "$NEW/requirements.txt" "$NEW/README.md" "$APP/"
for d in drspark pricing pricing_data tools; do
  rm -rf "$APP/$d" && cp -r "$NEW/$d" "$APP/$d"
done
head -8 "$APP/pricing_data/BUNDLE.json"
```

복사하는 것은 `source.py`, `requirements.txt`, `README.md`, `drspark/`, `pricing/`, `pricing_data/`, `tools/`뿐이다. `Dockerfile`, `compose.yaml`, `.dockerignore`, `deploy/`, `docs/`, `tests/`는 복사하지 않는다. DB·로그·`.env.local`도 건드리지 않는다.

컨테이너가 root로 돌면 앱 폴더 안에 root 소유 `__pycache__`가 생긴다. 다음 업그레이드에서 폴더를 지울 권한이 없으면 `sudo`를 붙인다.

### 5단계: 운영 Dockerfile에 libgomp1 추가

`<app-dir>/Dockerfile`의 `FROM` 다음 줄에 다음을 넣는다. 나머지 줄은 그대로 둔다.

```dockerfile
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgomp1 \
 && rm -rf /var/lib/apt/lists/*
```

선택: 앱 폴더가 빌드 컨텍스트라 DB·로그도 docker 데몬으로 전송된다(이미지에 복사되지는 않는다). 빌드를 가볍게 하려면 앱 폴더에 `.dockerignore`를 두고 `*`와 `!requirements.txt` 두 줄만 적는다.

### 6단계: 빌드와 시작 전 점검

```bash
docker compose build "$SVC"

# 작업 폴더와 DB 경로: "/app None"이 나와야 한다(DB = <app-dir>/drspark_seen.db)
docker compose run --rm --no-deps "$SVC" python -c "import os; print(os.getcwd(), os.environ.get('DRSPARK_DB'))"

# 적정가 번들 로드: 버전이 나와야 한다(None이면 보강 꺼짐)
docker compose run --rm --no-deps "$SVC" python -c "from pricing.enrich import Enricher; e = Enricher.from_env(); print('pricing:', e and e.version)"
```

두 번째 명령은 `pricing: fairprice-1.0@2026-10-19T12:32:24+00:00`처럼 번들 버전을 출력해야 한다. 이 점검은 알림을 보내지 않는다.

선택: 운영 DB 사본으로 파이프라인을 재생해 오류가 없는지 본다. 원본은 읽기만 하고 Discord로 보내지 않으며 집계 값만 출력한다.

```bash
SNAP=$(mktemp -d) && cp -a "$BK"/drspark_seen.db* "$SNAP"/
docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -v "$APP":/app:ro -v "$SNAP":/snap -w /tmp "$IMG" \
  python /app/tools/replay_backup.py --db /snap/drspark_seen.db -n 800 --listings "" --oof "" --tmp /tmp
rm -rf "$SNAP"
```

출력 요약에서 `오류 {'by_stage': {}, …}`와 `연락처 유출 0/…`을 확인한다. 이번 업그레이드의 스테이징 점검에서는 800건 오류 0건, 연락처 유출 0건, 적정가 산출 87.9%였다. 사본 폴더에는 개인정보가 있으므로 바로 지운다.

### 7단계: 이 서비스만 다시 띄우기

```bash
docker compose up -d --no-deps "$SVC"
docker compose logs -f --since 5m "$SVC"
```

`--no-deps`로 다른 서비스는 다시 만들지 않는다.

### 8단계: 확인

첫 수집은 시작 1분 뒤에 돈다. 2~3분 뒤에 다음을 확인한다.

```bash
docker compose logs --since 10m "$SVC" | grep -E 'Pricing enrichment|Watcher started'
docker compose logs --since 10m "$SVC" | grep -c 'NEW: '
docker compose logs --since 10m "$SVC" | grep -E 'ERROR|Traceback' | head
```

- `Pricing enrichment: on (fairprice-1.0@…)`과 `Watcher started`가 있어야 한다. `off`이면 바로 위 경고에 이유가 나온다.
- `NEW:` 줄 수는 0에 가까워야 한다(그사이 실제로 올라온 글만). 첫 수집에서 목록 첫 페이지 전체(수십 건)가 `NEW:`로 찍히면 다른 DB를 열고 있다는 뜻이다. 바로 `docker compose stop "$SVC"`로 멈추고 작업 폴더와 `DRSPARK_DB`를 다시 확인한다.
- `ERROR`, `Traceback`이 없어야 한다.
- 새 글이 올라오면 `Enriched <글번호> | grade=… p50=… flag=…` 줄이 찍히고, Discord 알림에 모델·적정가·재게시 필드가 붙는다.

기록은 SQLite로 확인한다(호스트에 `sqlite3`가 없으면 `docker compose exec "$SVC" python -c …`로 같은 쿼리를 돌린다).

```sql
SELECT COUNT(*), MAX(computed_at) FROM enrichment;
SELECT status, COUNT(*) FROM alert_outbox GROUP BY 1;
```

시작할 때 `enrichment`·`alert_outbox` 테이블이 없으면 만든다. `seen`은 건드리지 않는다.

### 알림 설정 바꾸기(선택)

새 환경변수는 모두 기본값이 기존 동작과 같다(`ALERT_MODE=all`). 바꾸려면 `<app-dir>/.env.local`에 줄을 더하고 컨테이너를 다시 만든다. `docker compose restart`는 `env_file`을 다시 읽지 않는다.

```bash
echo 'ALERT_MODE=deals' >> "$APP/.env.local"
docker compose up -d --no-deps --force-recreate "$SVC"
```

변수 목록은 README의 '환경변수'와 `.env.example`에 있다.

### 되돌리기

보강만 끄려면 `.env.local`에 `DRSPARK_PRICING=0`을 넣고 위처럼 컨테이너를 다시 만든다.

이전 코드로 되돌릴 때는 **코드만** 되돌린다. DB는 지금 것을 그대로 둔다. 업그레이드 뒤에 본 글이 `seen`에 기록되어 있으므로, 백업 DB로 되돌리면 그 글들을 다시 알린다. 이전 코드는 새 테이블(`enrichment`, `alert_outbox`)을 무시한다.

```bash
docker compose stop "$SVC"
for d in drspark pricing pricing_data tools; do
  rm -rf "$APP/$d"; [ -e "$BK/$d" ] && cp -a "$BK/$d" "$APP/$d"
done
for f in source.py requirements.txt Dockerfile README.md; do
  [ -e "$BK/$f" ] && cp -a "$BK/$f" "$APP/$f"
done
docker image tag "$ROLLBACK" "$IMG"
docker compose up -d --no-deps --no-build --force-recreate "$SVC"
```

`--no-build`로 다시 빌드하지 않고 태그를 되돌린 이전 이미지로 컨테이너를 만든다. 로그에서 `Watcher started`를 확인한다.

### 정리

며칠 운영해 이상이 없으면 백업 폴더와 롤백 이미지를 지운다.

```bash
rm -rf "$BK"
docker image rm "$ROLLBACK"
```

### 번들만 갱신할 때(재학습 후)

의존성(`requirements.txt`)이 그대로면 `drspark/`와 `pricing_data/`만 바뀐다. 2·3단계(중지·백업) 뒤에 두 폴더만 복사하고 `docker compose up -d --no-deps "$SVC"`로 다시 띄운다. 이미지 재빌드는 필요 없다. 모델은 시작할 때만 읽으므로 재시작해야 반영된다. `requirements.txt`가 바뀌었으면 위 절차를 처음부터 따른다.

## 2. 이 저장소의 compose.yaml로 설치

새로 설치할 때 쓴다. 저장소의 `Dockerfile`은 코드를 이미지 안에 복사하고, DB·로그를 `./data` 볼륨(`/app/data`)에 둔다.

```bash
git clone https://github.com/jhpower901/dr.spark_alarm.git && cd dr.spark_alarm
cp .env.example .env                 # DISCORD_WEBHOOK_URL 등을 채운다
mkdir -p data && sudo chown 1000:1000 data
docker compose -p drspark-alarm up -d --build
docker compose -p drspark-alarm logs -f alarm
```

- 기존 DB를 옮겨 오려면 첫 시작 **전에** 기존 프로세스를 멈추고 `drspark_seen.db`(`-wal`, `-shm` 포함)를 `data/`에 복사한 뒤 소유자를 1000으로 맞춘다. 빈 DB로 시작하면 첫 실행에서 목록 첫 페이지 전체를 알린다.
- 업그레이드: 먼저 지금 이미지를 태그해 두고(`docker image tag drspark-alarm:latest drspark-alarm:rollback`) `docker compose -p drspark-alarm stop alarm`으로 멈춘 뒤 `data/`를 백업한다. 그다음 `git pull && docker compose -p drspark-alarm up -d --build`로 다시 띄우고 1절 8단계와 같이 로그를 확인한다.
- 되돌리기: `docker image tag drspark-alarm:rollback drspark-alarm:latest && docker compose -p drspark-alarm up -d --no-build --force-recreate`. `data/`(DB)는 지금 것을 그대로 둔다.
- 컨테이너는 UID 1000으로 돈다. `./data` 소유자가 다르면 `.env`에 `APP_UID`/`APP_GID`를 넣고 다시 빌드한다.
- 자세한 내용은 README의 '설치와 실행 > Docker'에 있다.

## 3. systemd + venv

Docker 없이 전용 사용자와 venv로 돌린다. [`deploy/drspark-alarm.service`](../deploy/drspark-alarm.service)는 `/opt/dr.spark_alarm`, 사용자 `drspark`, DB·로그 폴더 `data/`를 가정한 예시이고, 설치 명령은 파일 머리 주석에 있다.

기존 venv 설치를 업그레이드할 때:

1. 지금 서비스의 `WorkingDirectory`, `ExecStart`, 환경변수를 `systemctl cat <서비스>`로 확인한다. DB는 작업 폴더의 `drspark_seen.db`이거나 `DRSPARK_DB`가 가리키는 파일이다.
2. `systemctl stop <서비스>` 뒤 DB(`-wal`, `-shm` 포함)와 코드 폴더를 백업한다.
3. `git fetch && git checkout <배포 브랜치> && git pull`로 코드를 받는다. `pricing_data/BUNDLE.json`이 있는지 확인한다.
4. `sudo apt install -y libgomp1`을 설치하고 venv에서 `pip install -r requirements.txt`를 실행한다. 시스템 파이썬으로 돌던 서비스면 venv를 새로 만들고 `ExecStart`의 파이썬 경로를 바꾼다(Debian 12 이상은 시스템 pip 설치가 막혀 있다).
5. `WorkingDirectory`와 `DRSPARK_DB`는 바꾸지 않는다. 그래야 같은 DB를 이어 쓴다.
6. `systemctl start <서비스>` 뒤 로그에서 `Pricing enrichment: on (…)`, 첫 수집의 `NEW:` 줄 수(0에 가까워야 함), `ERROR` 없음을 1절 8단계와 같이 확인한다.

되돌리기는 `git checkout <이전 커밋>` 뒤 재시작이다. DB는 지금 것을 그대로 둔다.
