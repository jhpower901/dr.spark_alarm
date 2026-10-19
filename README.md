# dr.spark_alarm

[드스파크 중고 장터(ski_sell2)](https://www.drspark.net/ski_sell2)에 새 판매글이 올라오면 Discord로 알려 주는 프로그램이다.
새 글마다 **모델을 식별하고 과거 시세로 적정가를 매겨** 알림에 붙인다(적정가 알림).

- 1분(±20초)마다 목록 첫 페이지를 가져온다.
- 처음 보는 글이면 상세 페이지를 읽어 판매 정보(출시연도, 모델/시리얼, 스펙, 사용횟수 등)를 파싱한다.
- 파싱한 글을 SQLite `seen` 테이블에 저장한다.
- 적정가를 계산해 `enrichment` 테이블에 저장한다.
- Discord 웹후크로 임베드 알림을 보낸다. 전송에 실패한 알림은 `alert_outbox` 테이블에 남겨 두었다가 다음 실행에서 다시 보낸다.

적정가 보강은 켜고 끌 수 있다. 끄거나(`DRSPARK_PRICING=0`) 사전·모델 폴더(`pricing_data/`)나 `pricing/` 패키지가 없으면 예전과 같은 알림을 보낸다. 보강 중에 오류가 나도 알림은 그대로 나간다.

## 목차

1. [새 기능: 적정가 알림](#새-기능-적정가-알림)
2. [알림 읽는 법](#알림-읽는-법)
3. [환경변수](#환경변수)
4. [알림 필터](#알림-필터)
5. [설치와 실행](#설치와-실행)
6. [운영 서버 업데이트](#운영-서버-업데이트)
7. [적정가 번들 갱신과 재학습](#적정가-번들-갱신과-재학습)
8. [한계](#한계)
9. [개인정보](#개인정보)
10. [파일 구조](#파일-구조)

## 새 기능: 적정가 알림

과거 매물 5,584건(관측 2026-03-05 ~ 2026-10-16)을 분석해 만든 규칙·사전·모델을 쓴다. 매물마다 LLM이나 외부 API를 부르지 않고 로컬에서 계산한다. 시작할 때 모델을 한 번 읽는 데 3초 안팎이 걸린다. 그 뒤로는 글 1건에 보통 0.1초 안팎이 걸린다(재게시 조회 포함, 95%가 0.3초 이내).

| 기능 | 내용 |
|---|---|
| 모델 식별 등급 | **A** 모델과 시즌을 모두 확인했다. **B** 모델군(패밀리)까지만 확인했다. **C** 브랜드나 종류만 알아냈다. 제목·모델/시리얼·스펙을 별칭 사전(모델 755개, 별칭 약 3,900개)에 맞춰 본다. |
| 적정가 범위 | **P50**(중앙 추정값)과 **P10~P90**(80% 범위)을 낸다. 비슷한 매물 10건 중 8건의 호가가 이 범위에 든다는 뜻이다. 교차검증 포함률은 80%다. |
| 판정과 신뢰도 | 호가를 범위와 비교해 저평가, 적정 범위, 고평가, 가격 확인 필요 중 하나로 판정한다. 신뢰도는 식별 등급과 근거 매물 수로 정한다(신뢰 높음, 신뢰 보통, 참고용). |
| 근거 줄 | 어떤 비교군(같은 모델·시즌, 모델, 모델군, 브랜드·종류 등)의 몇 건에서 출발했는지 보여 준다. 이어서 연식, 게시 월, 길이, 상태 같은 요인이 값을 몇 % 올리고 내렸는지 적는다. |
| 정가 대비 | 사전에 정가가 있는 모델(약 350개)이면 호가가 정가의 몇 %인지 보여 준다. |
| 재게시·가격 인하 | 같은 판매자가 180일 안에 같은 물건을 다시 올린 글인지 찾는다(이번 글보다 글번호가 작은 글만). 찾으면 이전 글 수, 처음·직전 게시일, 직전 호가 대비 변화율을 보여 준다. 직전 글보다 **5% 이상** 내렸으면 제목 앞에 `[가격인하]`를 붙인다. 소폭 인하(끌어올리기용 재게시 등)에는 붙이지 않는다. |
| 거래 상태 | 상세 페이지의 `거래` 칸(판매중 / 거래완료)을 읽는다. 판매중이 아니면 `상태` 필드 앞에 붙이고(예: `거래완료 / S급 / 네고○`), 거래완료 글은 `[저평가]`·멘션·`deals` 모드에서 뺀다. 거래완료 여부는 `enrichment.trade_status`에 남겨 다음 재학습 때 마감 신호로 쓸 수 있다. |
| `enrichment` 테이블 | 위 결과를 `seen`과 같은 DB의 별도 테이블에 저장한다(`seen` 스키마는 바꾸지 않는다). 연락처와 작성자는 저장하지 않는다. |

### 사이트 변경 대응(2026-10)

드스파크 상세 페이지의 가격 라벨이 `판매가격`에서 `희망가`로 바뀌었다(2026-10-18 확인). 기존 코드는 `판매가격`만 읽어서 상세 가격이 목록 가격을 덮어쓰지 못했다. 거래완료처럼 목록에 가격이 없는 글은 가격이 비어 저장됐다. 이제 두 라벨을 모두 읽는다(`판매가격` 우선). 새로 생긴 `거래`·`결정순서` 칸 가운데 `거래`는 위 '거래 상태'로 쓴다.

### 판정 기준

| 판정 | 조건 | 임베드 색 |
|---|---|---|
| ▼ 저평가 | 호가 < P10 | 초록 |
| ● 적정 범위 | P10 ≤ 호가 ≤ P90 | 파랑(기존 알림 색) |
| ▲ 고평가 | 호가 > P90 | 주황 |
| ? 가격 확인 필요 | 호가가 P50의 0.3배 미만이거나 3배 초과 | 회색 |

가격 확인 필요는 싸거나 비싼 매물이라기보다 가격 오기(0을 빼먹거나 더 붙임), 부품만 판매, 다른 품목일 가능성이 크다는 뜻이다.

판정에 쓰는 호가는 가격 칸 값을 규칙으로 보정한 값이다. 예를 들어 `65`는 65만원, 신품 레이스 스키의 `3,600,000`은 0이 하나 더 붙은 것으로 보고 36만원, 가격 칸이 비어 있으면 본문의 가격을 쓴다. 이렇게 보정한 값으로 판정했으면 `적정가` 필드 첫 줄에 `판정 호가 36만원(입력값 보정 …)`처럼 그 값을 밝힌다(`가격` 필드는 예전처럼 입력값 그대로다).

| 신뢰도 | 조건 |
|---|---|
| 신뢰 높음 | 식별 A이고 같은 모델(또는 같은 모델·시즌) 학습 매물이 3건 이상 |
| 신뢰 보통 | 식별 A나 B이고 같은 모델·모델군 수준 비교군이 있음 |
| 참고용 | 그 밖(C등급, 브랜드·종류 수준 추정 등). 또 **호가가 불확실한 글**은 등급과 상관없이 참고용이다: 0 하나를 빼는 보정(÷10)이나 만원 단위 가정을 한 호가, 본문의 정가보다 12배 넘게 싼 호가, 레이스 스키·부츠의 5만원 미만 호가. 적정가 모델 학습에서도 뺀 글들이다. |

제목 앞 `[저평가]` 태그, `DEAL_MENTION` 멘션, `ALERT_MODE=deals`의 저평가 기준은 모두 **저평가이면서 신뢰 높음·보통**인 글이다. 참고용 저평가는 색만 초록으로 바뀐다.

### 적정가를 내지 않는 글

다음 글에는 모델·재게시 정보만 붙이고 `적정가: 산출 안 함(사유)`로 표시한다.

- 구매 희망 글, 판매 글이 아닌 글(나눔·공지 등)
- 여러 물건 묶음(스키+부츠 일괄 등). 본문의 조건부 금액('타게 되면 55만원', '네고 시', '택포')이나 정가·매장가·이전 호가는 두 번째 물건 가격으로 세지 않는다. 묶음 판정 규칙과 검수는 분석 저장소 `phase1/sections/04_listing_units.md` 4.3절에 있다.
- 장비 외 품목(시즌권·회원권, `other` 카테고리)
- 가격이 없거나 5,000원 미만

## 알림 읽는 법

아래는 **합성 예시**다. 실제 글이 아니다. 작성자·본문·이전 글은 지어냈고, 적정가 수치는 현재 번들(`fairprice-1.0`)로 실제 계산한 값이다. Discord에서는 제목이 글 링크이고, 왼쪽 띠 색이 판정을 나타낸다(이 예시는 초록).

```text
@here
[저평가] [가격인하] 24/25 아토믹 레드스터 G9 FIS 183cm 판매합니다
──────────────────────────────────────────────
가격                         상태
520,000원                    A급 / 네고가능 / 직거래
작성자
예시판매자
작성시간
2026년 10월 18일 오후 5:11 (1분 전)
모델
Atomic Redster G9 FIS Revoshock S · 24/25 시즌 · 스키 · 식별 A(모델·시즌 확인)
적정가
75.6만원 (80% 범위 57.9만원 ~ 89만원)
▼ 저평가 · 호가/적정가 0.69 · 신뢰 높음
정가 227만원 대비 23%
적정가 근거
같은 모델·시즌 5건 기준 · 동일 모델·시즌 5건 중앙값 70만원 · 기준 47만원 · 2시즌 경과 +20% ·
10월 게시 +15% · 길이 183cm +13% → 76만원
(요인별 %는 같은 종류 평균 매물 대비)
재게시
같은 판매자의 이전 글 1건 · 처음 10/06 · 직전 10/06
직전 호가 60만원 → 이번 52만원 (-13.3%)
판매 정보
출시연도: 24/25
Model/Serial: Redster G9 FIS Revoshock S
제품스펙: 183cm R24
구입처/시기: 24년 11월 매장
사용횟수: 10회 미만
거래지역
서울
본문내용
시즌 10회 사용. 엣지 상태 좋습니다. 직거래 우선.
```

읽는 순서:

1. **제목 태그와 색.** `[저평가]`는 믿을 만한 저평가이고, `[가격인하]`는 같은 판매자가 값을 5% 이상 내려 다시 올린 글이다. 첫 줄의 멘션은 `DEAL_MENTION`을 설정했을 때만 붙는다.
2. **모델.** 사전의 공식 모델명, 시즌, 종류, 식별 등급을 보여 준다. B등급이면 모델군 이름이, C등급이면 브랜드 이름만 나온다. 등급이 낮을수록 적정가가 거칠다.
3. **적정가.** 굵은 값이 P50이고 괄호 안이 80% 범위다. `호가/적정가 0.69`는 호가가 P50의 69%라는 뜻이다. 정가가 있으면 정가 대비 비율도 나온다. 판정에 쓴 호가가 `가격` 필드와 다르면(단위 보정, 본문 가격) 맨 위에 `판정 호가 …` 줄이 붙는다. 이 줄에 `단위·오기 불확실`이 있으면 판정은 참고용이다.
4. **적정가 근거.** 앞부분은 출발한 비교군과 건수다(이 예시는 같은 모델·시즌 5건). 뒷부분은 요인별 보정과 최종값이다. 건수가 적거나 비교군이 넓을수록(같은 종류, 같은 카테고리) 참고만 한다. 표시된 요인은 일부라 % 값을 더해도 최종값과 정확히 맞지 않는다.
5. **재게시.** 이 프로그램의 DB(`seen`)에 남아 있는 같은 작성자의 글 중 같은 물건으로 판단한 글을 센다. DB를 새로 만들면 이력이 없다. 두 호가가 만원 단위로 같아 보이면(160만원 → 159.5만원) 원 단위로 쓴다.

보강이 꺼져 있거나 실패한 글은 예전 알림과 똑같다(파란색, 모델·적정가·재게시 필드 없음).

## 환경변수

`.env.example`을 복사해 쓴다. `source.py`는 `.env` 파일을 스스로 읽지 않는다. Docker(`env_file`), systemd(`EnvironmentFile`), 셸(`set -a; . ./.env; set +a`)에서 환경변수로 넘긴다. 값을 비워 둔 변수(`DRSPARK_DB=`처럼)는 설정하지 않은 것으로 보고 기본값을 쓴다.

| 변수 | 기본값 | 설명 |
|---|---|---|
| `DISCORD_WEBHOOK_URL` | (없음) | Discord 웹후크 주소. 없으면 DB에 기록만 하고 알림은 보내지 않는다(경고 로그). |
| `DRSPARK_DB` | `drspark_seen.db` (실행 폴더) | SQLite DB 경로. Docker 이미지는 `/app/data/drspark_seen.db`. |
| `DRSPARK_PRICING` | `1` | `0`, `false`, `off`, `no`면 적정가 보강을 끈다(기존 동작). |
| `DRSPARK_PRICING_DIR` | `<저장소>/pricing_data` | 사전·모델 폴더. 다른 위치의 번들을 쓸 때만 바꾼다. |
| `ALERT_MODE` | `all` | `all`은 모든 새 글, `deals`는 저평가(신뢰 높음·보통)나 적정가 이하로 내린 재게시만 알린다([알림 필터](#알림-필터)). 모르는 값이면 시작할 때 경고를 남기고 `all`로 동작한다. |
| `ALERT_CATEGORIES` | (전체) | 쉼표로 구분한 카테고리. 예: `ski,boots`. 대소문자는 가리지 않는다. 모르는 값은 시작할 때 경고를 남기고 무시하며, 남는 값이 없으면 카테고리 필터를 끈다. |
| `ALERT_SKIP_WANTED` | `0` | `1`이면 구매 희망 글은 알리지 않는다. |
| `DEAL_MENTION` | (없음) | 저평가 알림의 메시지 본문에 붙일 멘션. 예: `@here`, `"<@&역할ID>"` |
| `ALERT_RETRY_HOURS` | `6` | Discord 전송에 실패한 알림을 다시 보내는 기간(시간). 이보다 오래 못 보낸 알림은 버린다([전송 실패와 재전송](#전송-실패와-재전송)). |
| `TZ` | 시스템 | 로그 시각용. `Asia/Seoul` 권장(Docker·systemd 예시에 들어 있음). 재게시 날짜와 시즌 계산은 `TZ`와 상관없이 한국 시간으로 한다. |

카테고리 값: `ski`(스키), `boots`(부츠), `binding`(바인딩/플레이트), `snowboard`(스노보드), `apparel`(의류), `gloves`(장갑), `helmet`(헬멧), `goggle`(고글), `protector`(보호대), `poles`(폴), `bag`(가방), `tuning`(튜닝용품), `other`(기타)

## 알림 필터

필터는 보강이 성공한 글에만 걸린다. 보강이 꺼져 있거나 실패한 글은 놓치지 않도록 **항상 알린다**. `enrichment` 테이블 저장만 실패한 경우(DB 잠김 등)에는 계산한 결과로 그대로 거른다. 필터로 걸러진 글도 `seen`과 `enrichment`에는 기록되고(`alert_outbox`에는 `filtered`), 로그에 `Alert filtered: <글번호> (mode=…, flag=…, category=…, type=…)`가 남는다.

검사 순서:

1. `ALERT_SKIP_WANTED=1`이고 구매 희망 글이면 알리지 않는다.
2. `ALERT_CATEGORIES`가 있고 분류된 카테고리가 목록에 없으면 알리지 않는다.
3. `ALERT_MODE=deals`이면 다음 글만 알린다.
   - `[저평가]`인 글(저평가이면서 신뢰 높음·보통).
   - `[가격인하]`(5% 이상 인하)이면서 **내린 값이 적정가(P50) 이하**이고 판정이 적정 범위나 저평가인 글. 값을 내렸어도 여전히 P50보다 비싸거나 고평가·가격 확인 필요인 글은 알리지 않는다.
   - 적정가가 없는 글(묶음 등)은 10% 이상 내린 경우만.

예시:

```dotenv
# 스키·부츠 판매글 중 싸게 나온 것만 받고, 저평가는 역할 멘션
ALERT_MODE=deals
ALERT_CATEGORIES=ski,boots
ALERT_SKIP_WANTED=1
DEAL_MENTION="<@&123456789012345678>"
```

카테고리는 규칙 분류기(과거 데이터에서 정확도 약 97%)가 정한다. 잘못 분류되면 원하는 글이 걸러질 수 있다. 처음에는 `ALERT_MODE=all`로 며칠 돌려 보고 좁히기를 권한다.

참고로 과거 백업 DB의 최근 800건(약 12일, 하루 약 66건)을 관측 순서대로 다시 돌려 본 결과는 다음과 같다(재게시 조회는 그 이전 이력을 포함, 현재 번들 `fairprice-1.0@2026-10-19T12:32:24+00:00` 기준, 자세한 내용은 [docs/VERIFICATION.md](docs/VERIFICATION.md)).

| 항목 | 비율 |
|---|---:|
| 식별 등급 | A 39% · B 22% · C 39% |
| 적정가를 낸 글 | 86% (나머지: 묶음 7%, 구매 희망 5%, 장비 외 품목 1% 등) |
| 판정 | 적정 범위 77% · 고평가 5.1% · 저평가 2.8% · 가격 확인 필요 1.1% |
| `[저평가]`(신뢰 높음·보통) | 1.5% |
| 호가가 불확실해 판정을 참고용으로 낮춘 글 | 0.6% |
| 재게시 / `[가격인하]`(5% 이상) | 36% / 12% |
| `deals` 모드였다면 알림 | 8% (하루 약 5건, 대부분 적정가 이하로 내린 재게시) |

인하 폭과 상관없이 모든 인하를 알리던 이전 규칙으로는 `deals` 알림이 14%(하루 약 9건)였고, 그중 약 40%가 내린 뒤에도 P50보다 비싼 글이었다.

이 글들은 모델 학습에 쓰인 글이라 판정이 실제보다 '적정 범위'에 몰려 있다. 새 글에서는 저평가·고평가가 더 자주 나올 수 있다.

아래 SQL로 운영 DB에서 직접 확인할 수 있다.

```sql
-- 최근 7일 판정 분포
SELECT deal_flag, deal_confidence, COUNT(*) FROM enrichment
 WHERE computed_at >= strftime('%s','now','-7 days') GROUP BY 1, 2 ORDER BY 3 DESC;
-- 식별 등급 분포, 적정가를 내지 않은 사유
SELECT id_grade, COUNT(*) FROM enrichment GROUP BY 1;
SELECT skipped_reason, COUNT(*) FROM enrichment WHERE skipped_reason IS NOT NULL GROUP BY 1;
-- 최근 저평가 글(제목은 seen에서). price_flags가 있으면 보정·불확실한 호가
SELECT e.post_id, s.title, e.price_krw, e.price_kind, e.price_flags, e.fair_p50, e.deal_ratio, e.deal_confidence
  FROM enrichment e JOIN seen s USING (post_id)
 WHERE e.deal_flag = 'undervalued' ORDER BY e.computed_at DESC LIMIT 20;
-- 알림 전송 상태(최근 7일), 아직 못 보낸 알림
SELECT status, COUNT(*) FROM alert_outbox WHERE created_at >= strftime('%s','now','-7 days') GROUP BY 1;
SELECT post_id, attempts, last_error, datetime(created_at, 'unixepoch', 'localtime') FROM alert_outbox
 WHERE status IN ('pending', 'failed', 'expired') ORDER BY created_at DESC LIMIT 20;
```

### 전송 실패와 재전송

알림마다 전송 상태를 `alert_outbox` 테이블에 남긴다(`seen` 스키마는 그대로).

| 상태 | 뜻 |
|---|---|
| `sent` | 보냈다. |
| `filtered` | 알림 필터로 걸렀다. |
| `pending` | 아직 못 보냈다. 다음 실행(1분 뒤) 맨 처음에 오래된 것부터 다시 보낸다. |
| `failed` | Discord가 페이로드를 거부했다(HTTP 400·413). 다시 보내지 않는다. |
| `expired` | `ALERT_RETRY_HOURS`(기본 6시간) 동안 못 보내 버렸다. 오래된 알림을 한꺼번에 쏟아내지 않기 위해서다. |
| `no_webhook` | 웹후크가 설정되지 않았다. |

- 연결 실패, 5xx, 429 반복처럼 Discord 쪽 장애면 그 글은 `pending`으로 남는다. 같은 실행의 나머지 새 글은 시도하지 않고 `pending`으로만 기록한다(1분 주기를 넘기지 않게). 장애가 풀리면 처음 순서대로 한 번씩 나간다. 늦게 나간 알림의 작성시간은 원래 관측 시각을 그대로 보여 준다.
- 예전 코드는 전송이 3번 실패하면 그 글의 알림을 영영 잃었고(이미 `seen`에 기록돼 다시 보내지 않음) 같은 회차의 나머지 글 처리도 멈췄다. 429를 3번 받으면 경고만 남기고 조용히 잃었다.
- 글 하나의 알림 단계에서 오류가 나도 같은 회차의 나머지 새 글은 계속 처리한다.
- 보강이 꺼져 있고 Discord가 정상이면 보내는 메시지는 예전과 같다. 달라진 점은 `alert_outbox` 행이 쌓인다는 것뿐이다.

## 설치와 실행

요구 사항:

- Python 3.10 이상(3.12에서 검증)
- Debian/Ubuntu에서는 `libgomp1`(lightgbm 런타임)
- 메모리 약 200MB(적정가 보강을 켰을 때. Docker 이미지에서 최대 RSS 약 180MB. 끄면 수십 MB)

적정가 모델(`pricing_data/fairprice.pkl`)은 pickle이다. 그래서 `requirements.txt`의 pandas·numpy·scikit-learn·lightgbm·rapidfuzz는 학습 환경과 같은 버전으로 고정했다. 버전을 임의로 올리지 않는다.

### venv로 직접 실행(Linux)

```bash
sudo apt install -y python3-venv libgomp1
git clone https://github.com/jhpower901/dr.spark_alarm.git && cd dr.spark_alarm
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env && chmod 600 .env     # DISCORD_WEBHOOK_URL 등 채우기
set -a; . ./.env; set +a
python source.py
```

Windows(PowerShell)에서는 `py -m venv .venv`, `.venv\Scripts\Activate.ps1`, `$env:DISCORD_WEBHOOK_URL="…"`로 바꿔 쓴다. libgomp1 대신 Visual C++ 재배포 패키지가 필요할 수 있다(lightgbm).

DB(`drspark_seen.db`)와 로그(`drspark.log`, 5MB×6개 순환)는 **실행한 폴더**에 생긴다. DB 위치는 `DRSPARK_DB`로 바꿀 수 있다.

시작 로그에서 보강이 켜졌는지 확인한다.

```text
… | INFO | drspark.pricing | __init__:… | Enricher loaded fairprice-1.0@2026-10-19T12:32:24+00:00 from …/pricing_data in 2.9s (models=755, aliases=3905)
… | INFO | drspark | <module>:… | Pricing enrichment: on (fairprice-1.0@2026-10-19T12:32:24+00:00) | alert mode=all categories=all skip_wanted=False mention=off
```

`Pricing enrichment: off`이면 바로 위 경고에 이유(`Pricing data not found …`, `failed to load` 등)가 나온다. 알림 없이 설정만 점검하려면 다음을 실행한다(버전이 나오면 정상, `None`이면 보강 꺼짐).

```bash
python -c "from pricing.enrich import Enricher; e = Enricher.from_env(); print(e and e.version)"
```

처음 실행(빈 DB)하면 목록 첫 페이지의 글이 모두 새 글이라 한꺼번에 알림이 간다. 이는 기존 동작과 같다. 기존 DB를 옮겨 오면 생기지 않는다.

### systemd(전용 사용자 + venv)

[`deploy/drspark-alarm.service`](deploy/drspark-alarm.service)는 **예시**다. `/opt/dr.spark_alarm`, 사용자 `drspark`, `data/` 폴더를 가정했다. 설치 명령은 파일 머리 주석에 있다. 운영 서버의 실제 경로·사용자에 맞춰 고친 뒤 쓴다.

```bash
sudo systemctl enable --now drspark-alarm
journalctl -u drspark-alarm -f                  # 표준 출력 로그
tail -f /opt/dr.spark_alarm/data/drspark.log    # 파일 로그
```

### Docker

```bash
cp .env.example .env                 # 값 채우기
mkdir -p data && sudo chown 1000:1000 data
docker compose -p drspark-alarm up -d --build
docker compose -p drspark-alarm logs -f alarm
```

- 이미지는 `python:3.12-slim`이고 `libgomp1`·`tzdata`가 들어 있다. 비root 사용자(UID 1000)로 돈다.
- DB와 로그는 컨테이너 `/app/data`, 즉 호스트 `./data`에 남는다(`data/drspark_seen.db`, `data/drspark.log`). 작업 폴더를 `/app/data`로 두어 `source.py`가 현재 폴더에 쓰는 로그도 볼륨에 남는다.
- `./data`를 다른 UID가 소유하면 쓰기에 실패한다. `.env`에 `APP_UID`/`APP_GID`를 넣고 다시 빌드한다(`docker compose -p drspark-alarm build`).
- 헬스체크: 5분 넘게 `drspark.log`가 갱신되지 않으면 unhealthy가 된다. 1분마다 목록을 가져오며 로그를 남기기 때문이다.
- 설정만 점검: `docker compose -p drspark-alarm run --rm --no-deps alarm python -c "from pricing.enrich import Enricher; e = Enricher.from_env(); print(e and e.version)"`(Git Bash에서는 앞에 `MSYS_NO_PATHCONV=1`).
- Proxmox LXC 안에서 Docker를 쓰려면 컨테이너에 `nesting`(비특권이면 `keyctl`도)이 켜져 있어야 한다. 그렇지 않으면 venv + systemd가 간단하다.

## 운영 서버 업데이트

단계별 명령과 되돌리기는 [docs/DEPLOY.md](docs/DEPLOY.md)에 있다. 운영 서비스는 앱 폴더를 컨테이너에 바인드 마운트한 Docker Compose 서비스로 돌고, 그 절차(1절)대로 제자리 업그레이드했다. venv·systemd 설치도 같은 문서에 있다. 요점은 다음과 같다.

1. **기존 DB를 그대로 쓴다.** 기존 코드는 DB를 실행 폴더의 `drspark_seen.db`에 둔다. 새 코드도 `DRSPARK_DB`가 없으면 같은 곳을 쓴다. DB 경로가 바뀌어 빈 DB로 시작하면 첫 실행에서 목록 첫 페이지 전체를 다시 알린다. 그래서 기존 배포에 이 저장소의 `Dockerfile`·`compose.yaml`(DB를 `/app/data`에 둔다)을 덮어쓰지 않는다.
2. **멈추고 백업.** 롤백용으로 지금 이미지에 태그를 붙이고 이 서비스만 멈춘 뒤, 앱 폴더 전체(`drspark_seen.db`, `-wal`, `-shm` 포함)를 복사한다. 멈추지 않고 DB만 백업하려면 SQLite 온라인 백업을 쓴다.
   ```bash
   python3 -c "import sqlite3; s = sqlite3.connect('drspark_seen.db'); d = sqlite3.connect('drspark_seen.backup.db'); s.backup(d); d.close()"
   ```
3. **코드 파일만 교체.** `source.py`, `requirements.txt`, `README.md`, `drspark/`, `pricing/`, `pricing_data/`, `tools/`를 함께 넣는다(`pricing_data/BUNDLE.json` 확인). `source.py` 파일 하나만 바꿔 넣으면 적정가 보강이 켜지지 않는다. 이때는 `pricing package not importable` 경고를 남기고 예전과 같은 알림만 보낸다(전송 재시도는 동작한다).
4. **의존성.** `libgomp1`을 설치하고(Docker면 운영 Dockerfile에 추가) `requirements.txt`로 다시 설치한다(Docker면 이미지 재빌드).
5. **설정.** 환경변수(`.env` 등)는 그대로 두면 된다. 새 변수는 모두 기본값이 기존 동작과 같다(`ALERT_MODE=all`).
6. **다시 띄운 뒤 확인.** 이 서비스만 다시 띄우고(`docker compose up -d --no-deps <서비스>`) 로그에서 `Pricing enrichment: on (…)`을 확인한다. 첫 수집의 `NEW:` 줄은 0에 가까워야 한다(수십 건이면 다른 DB를 연 것이다). `ERROR`가 없어야 한다. 다음 새 글에서 `Enriched <글번호> | grade=… p50=…` 줄과 Discord 알림을 확인한다.

시작할 때 `enrichment`·`alert_outbox` 테이블이 없으면 만든다(`CREATE TABLE IF NOT EXISTS`). 새 버전에서 `enrichment`에 열이 늘었으면 기존 테이블에 빠진 열을 붙인다(`ALTER TABLE … ADD COLUMN`, 기존 행은 그대로). `seen`은 건드리지 않는다. 이미 저장된 글은 다시 계산하지 않는다. 재게시 탐지는 기존 `seen` 이력을 그대로 쓰므로 기존 DB를 유지하는 것이 좋다.

시작 로그에 설정 경고(`ALERT_MODE=… 알 수 없는 값`, `ALERT_CATEGORIES의 알 수 없는 카테고리 …`)가 있으면 `.env`를 고친다.

**되돌리기:** 보강만 끄려면 `DRSPARK_PRICING=0`으로 두고 다시 시작한다(Docker `env_file`을 고쳤으면 컨테이너를 다시 만든다). 이전 코드(`fc71c35`)로 돌아갈 때는 코드만 되돌리고 DB는 지금 것을 그대로 둔다. 백업한 DB로 되돌리면 업그레이드 뒤에 본 글을 다시 알린다. `enrichment`·`alert_outbox` 테이블이 남아 있어도 이전 코드는 이를 무시한다. 다만 이전 코드는 `DRSPARK_DB`를 읽지 않고 실행 폴더의 `drspark_seen.db`만 쓴다. DB를 다른 곳(`data/` 등)으로 옮겼다면 실행 폴더를 그곳으로 맞춘다.

## 적정가 번들 갱신과 재학습

`drspark/`(규칙·리졸버·적정가 모듈)와 `pricing_data/`(사전·모델)는 분석 저장소에서 동기화한 **산출물**이다. 이 저장소에서 직접 고치지 않는다. 문제가 있으면 분석 저장소에서 고쳐 다시 내보낸다. pickle 모델이 `drspark.fairprice` 모듈을 참조하므로 두 폴더는 항상 같이 갱신한다.

| 파일 | 내용 |
|---|---|
| `pricing_data/canonical_models.csv`, `canonical_families.csv` | 모델·모델군 사전(공식명, 정가, 시즌) |
| `pricing_data/model_aliases.csv` | 매물 표기 → 모델 별칭 |
| `pricing_data/fairprice.pkl`, `fairprice.json` | 적정가 모델과 메타데이터(버전, 학습 시각, CV 지표) |
| `pricing_data/BUNDLE.json` | 내보낸 시각, 원본, 파일별 SHA-256 |

### 번들 다시 내보내기

분석 저장소(`analysis/drspark`) 루트에서 실행한다. 표준 라이브러리만 쓰므로 호스트 Python으로 실행해도 된다. 인자를 생략하면 분석 저장소 안의 `alarm/`으로 내보낸다.

```bash
python integration/export_alarm_bundle.py /path/to/dr.spark_alarm
```

이 스크립트는 다음을 복사하고 `BUNDLE.json`을 새로 쓴다.

- `src/drspark/*.py` → `drspark/` (`config.py`는 알림용 경로 설정으로 대체)
- `data/curated/*.csv` → `pricing_data/`
- 가장 최근 `integration/artifacts/fairprice-*.pkl/.json` → `pricing_data/fairprice.pkl/.json`

그다음 테스트(`python -m pytest -q`)와 위의 설정 점검 명령을 돌린다. 이상이 없으면 `drspark/`와 `pricing_data/`를 한 커밋으로 올리고 운영 서버에서 재시작한다. 모델은 시작할 때만 읽으므로 재시작해야 반영된다.

### 언제 다시 학습하나

- **시즌 시작 전후(9~10월).** 새 시즌 모델이 나오고 전 시즌 장비가 한 시즌 더 묵는다. 학습 데이터에 없는 시즌은 연식 보정이 외삽된다.
- **분기마다.** 호가 수준과 거래 시기 효과(게시 월)가 바뀐다.
- **수시.** `enrichment`에서 C등급·`모델 미확인` 비율이 눈에 띄게 늘거나, `price_typo`가 잦아지거나, 판정이 한쪽(고평가 등)으로 쏠릴 때.

재학습 절차(분석 저장소, 자세한 내용은 그 저장소의 README):

1. 운영 DB를 백업해 분석 저장소의 원본 위치에 둔다. 원본 경로·SHA-256은 분석 저장소 `src/drspark/config.py`(`RAW_DB`, `RAW_SHA256`)와 `docker/compose.yaml`의 마운트에서 바꾼다.
2. 전체 파이프라인을 실행한다(`bin/dx bash src/pipeline/run_all.sh`). 적재·파싱·사전·적정가 학습·`export_model.py`까지 한 번에 돈다. 이 스크립트는 LLM을 다시 부르지 않는다.
3. 새 모델·별칭을 사전에 넣으려면 LLM 라벨링·검증 단계가 따로 필요하다. 이 단계를 하지 않으면 학습 기간 뒤에 처음 등장한 모델은 B·C등급으로만 잡힌다.
4. `export_alarm_bundle.py`로 이 저장소에 내보내고 위 순서대로 배포한다.
5. 분석 환경의 라이브러리 버전이 바뀌었으면 `requirements.txt`의 고정 버전도 같이 바꾼다.

현재 번들(`pricing_data/BUNDLE.json`): `fairprice-1.0@2026-10-19T12:32:24+00:00`, 학습 행 3,141개. 판매자 단위 5겹 교차검증 결과는 MdAPE 22.3%(A 17.8% · B 26.5% · C 30.1%), P10~P90 포함률 80.1%다. 시간 홀드아웃(2026-09-01 이후 글 1,312건)은 MdAPE 22.9%, 포함률 80.5%다. 이 번들부터 묶음 판정이 조건부·참고 금액을 물건별 가격으로 세지 않는다. 같은 800건 재생에서 묶음으로 빠진 글은 54건에서 57건이 되었다(단품 오탐 4건이 풀리고, 품목 나열·'둘 다' 같은 실제 묶음 7건이 새로 잡힘).

배포 전 스테이징 점검(운영 DB 사본, 새 이미지 컨테이너 안): 테스트 383 passed · 1 skipped, 마지막 800건 재생 오류 0건, 연락처 유출 0건, 적정가 산출 87.9%.

## 한계

- **호가 ≠ 거래가.** 학습 데이터는 판매글에 적힌 호가다. 실제 거래 가격이나 네고 결과는 모른다. 적정가는 "비슷한 매물이 보통 얼마에 올라오는가"이고, 저평가는 "같은 조건의 다른 글보다 싸게 올라왔다"는 뜻이다.
- **학습 기간.** 2026-03 ~ 2026-10(시즌 말·비시즌) 관측분만으로 학습했다. 시즌 중(11~2월) 가격 수준과 새 시즌 모델은 반영되지 않았다. 게시 월 효과도 이 기간 안에서만 추정했다. 학습 기간 밖(2026-11 이후)에 올라온 글은 게시 월 효과를 2026-10 수준으로 계산하고, 근거 줄에는 `게시 월(10월 기준)`으로 표시한다. 11월 이후에는 재학습을 권한다.
- **C등급 추정은 거칠다.** 브랜드·종류 수준 비교군이라 오차가 크다(교차검증 MdAPE 30%). 그래서 판정을 참고용으로 두고 `[저평가]`·멘션·`deals` 모드에서 뺀다. 의류·소품은 모델 식별률이 낮아 C등급이 많다.
- **모델 식별 재현율.** 새 매물에서 리졸버가 모델까지 맞히는 비율은 대략 60~70%로 본다. 과거 데이터 표본 안에서는 재현율 81%, 정밀도 95%였지만 별칭 사전이 같은 데이터로 만들어져 낙관적인 값이다. 처음 보는 표기, 오타, 새 모델은 모델군·브랜드 수준으로 떨어진다.
- **글에 적힌 정보만 쓴다.** 사진 속 상태, 손상, 튜닝 이력은 모른다. 본문에서 키워드로 잡은 상태·하자 플래그만 반영한다.
- **재게시 탐지 범위.** 작성자 닉네임이 정확히 같은 글만 비교한다. 이 프로그램이 저장한 `seen` 이력(180일) 안에서만 찾는다. 닉네임을 바꾸거나 제목·스펙을 크게 바꾸면 놓친다.
- **가격 파싱.** "65"(만원 생략), "6.5" 같은 표기와 0이 하나 더 붙은 것으로 보이는 값(신품 레이스 스키 360만원 → 36만원)은 규칙으로 보정한다. 보정이 틀리면 판정도 틀린다. 그래서 ÷10 보정·단위 가정·오기 의심 호가로 낸 판정은 항상 참고용으로 두어 `[저평가]`·멘션·`deals` 모드에서 빼고, 임베드에 `판정 호가 …` 줄로 해석한 값을 보여 준다. 본문 단위로 확인된 보정("65" + 본문 "65만원")이나 본문 가격은 신뢰도를 낮추지 않는다.

## 개인정보

- `enrichment` 테이블에는 파생값(분류, 모델 ID, 가격·적정가, 판정, 가격 파서 표시, 재게시 글번호·호가 변화)만 저장한다. 작성자, 전화번호, 이메일, 본문은 없다. 모델 식별에 쓰는 텍스트는 계산 전에 전화번호·이메일·URL·메신저 ID를 마스킹한다(본문은 앞 5,000자, 나머지 필드는 1,000자까지만 본다).
- `alert_outbox`의 `payload`는 Discord로 보낼 임베드 그대로라 작성자·연락처·본문이 들어 있다. 아직 못 보낸 알림(`pending`)에만 두고, 보내거나 거부·만료되면 지운다.
- 재게시 탐지는 `seen`에 이미 저장된 작성자 닉네임으로 같은 판매자의 이전 글을 찾는다. 찾은 결과로는 이전 글번호만 남긴다.
- **기존 동작은 바꾸지 않았다.** `seen` 테이블은 예전처럼 작성자·전화번호·이메일·본문을 저장한다. Discord 알림의 `작성자`, `판매 정보`(전화번호·E-mail 포함), `본문내용` 필드도 예전과 같다. 웹후크가 연결된 채널의 공개 범위에 유의한다.
- DB 파일(`*.db*`), 로그(`*.log*`), `.env`(`.env.example` 제외), `data/`는 git과 Docker 이미지에 넣지 않는다(`.gitignore`, `.dockerignore`). 확인: `git check-ignore -v .env`. 로그에는 글 제목과 URL이 남는다.
- `drspark/`·`pricing_data/` 번들에는 개인정보가 없다. 사전과 모델은 정제된 데이터로 만들었고, 원본 글·연락처·작성자는 들어 있지 않다.

## 파일 구조

```text
source.py                 수집·파싱·저장·필터·Discord 전송·재전송 대기열(진입점)
pricing/                  적정가 보강(알림 쪽 코드)
  enrich.py               Enricher(모델 식별 + 적정가 + 판정), Enrichment, enrichment 테이블 DDL
  repost.py               같은 판매자 재게시·가격 변화 탐지
  discord_fields.py       임베드 필드·색·제목 태그
drspark/                  분석 저장소에서 동기화한 규칙·리졸버·적정가 모듈(직접 수정 금지)
pricing_data/             사전·별칭·적정가 모델·BUNDLE.json(직접 수정 금지)
requirements.txt          의존성(적정가 관련은 버전 고정)
Dockerfile, compose.yaml  Docker 실행(프로젝트 이름 drspark-alarm)
.env.example              환경변수 예시
deploy/drspark-alarm.service  systemd 유닛 예시
tests/                    pytest(모두 합성 데이터. HTML fixture는 tests/fixtures/make_fixtures.py로 생성). 실행: python -m pytest -q
tools/replay_backup.py    과거 백업 DB로 파이프라인을 재생해 오류·지연·판정 분포를 재는 도구(전송 안 함)
docs/VERIFICATION.md      재생 검증 결과(집계값만)
docs/DEPLOY.md            배포·업그레이드·되돌리기 절차(Docker Compose 제자리 업그레이드, 새 설치, systemd)
```
