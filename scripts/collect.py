#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
청년 뉴스 댓글 랭킹 — 수집기

네이버 검색 API(NAVER API HUB)로 '청년' 기사를 찾고, 그중 네이버 뉴스에 올라온 기사만 골라
댓글 수와 추천 스티커(공감·쏠쏠·흥미·분석·후속) 수를 붙여 docs/data 에 저장합니다.
GitHub Actions가 매시간 실행하며, 파이썬 기본 라이브러리만 사용합니다.

    python3 scripts/collect.py              # 평소 수집
    python3 scripts/collect.py --diagnose   # 연결 점검(기사 1건으로 단계별 응답 출력)

필요한 환경변수: NAVER_CLIENT_ID, NAVER_CLIENT_SECRET
  - NAVER API HUB(네이버클라우드) Application의 Client ID / Client Secret
  - 2026년 7월 이전에 네이버 개발자센터에서 받은 '검색' 키도 2027년 6월까지는 그대로 쓸 수 있습니다
"""

import argparse
import html
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time as dtime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

KST = timezone(timedelta(hours=9), "KST")
ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT / "config.json"
DATA_DIR = ROOT / "docs" / "data"
DAYS_DIR = DATA_DIR / "days"
INDEX_FILE = DATA_DIR / "index.json"
STATE_FILE = DATA_DIR / "state.json"

# 접속 주소 (테스트할 때만 환경변수로 바꿉니다)
# 뉴스 검색: NAVER API HUB(새 방식) → 안 되면 개발자센터(옛 방식, 2027년 6월 종료) 순서로 시도
SEARCH_ENDPOINTS = [
    ("API HUB", os.environ.get("NAVER_HUB_SEARCH_URL", "https://naverapihub.apigw.ntruss.com/search/v1/news"),
     "X-NCP-APIGW-API-KEY-ID", "X-NCP-APIGW-API-KEY"),
    ("개발자센터", os.environ.get("NAVER_SEARCH_URL", "https://openapi.naver.com/v1/search/news.json"),
     "X-Naver-Client-Id", "X-Naver-Client-Secret"),
]
ARTICLE_URL = os.environ.get("NAVER_ARTICLE_URL", "https://n.news.naver.com/mnews/article/{oid}/{aid}")
COMMENT_URLS = os.environ.get(
    "NAVER_COMMENT_URLS",
    "https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json "
    "https://apis.naver.com/commentBox/cbox/web_neo_list_jsonp.json",
).split()
LIKE_URL = os.environ.get("NAVER_LIKE_URL", "https://news.like.naver.com/v1/search/contents")

PUBLIC_ARTICLE = "https://n.news.naver.com/mnews/article/{oid}/{aid}"
PUBLIC_COMMENTS = "https://n.news.naver.com/mnews/article/comment/{oid}/{aid}"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

SECTIONS = {"100": "정치", "101": "경제", "102": "사회", "103": "생활/문화",
            "104": "세계", "105": "IT/과학", "110": "오피니언"}

# 추천 스티커: 내부 이름 → 화면 표기 (기사 페이지에서 실제 이름을 읽으면 그것으로 바뀝니다)
DEFAULT_REACTIONS = {"touched": "공감백배", "useful": "쏠쏠정보", "wow": "흥미진진",
                     "analytical": "분석탁월", "recommend": "후속강추"}
REACTION_ORDER = ["touched", "useful", "wow", "analytical", "recommend"]

# 기사 페이지를 못 읽었을 때만 쓰는 언론사 이름표 (네이버 언론사 번호)
PRESS_BY_OID = {
    "001": "연합뉴스", "002": "프레시안", "003": "뉴시스", "005": "국민일보", "006": "미디어오늘",
    "008": "머니투데이", "009": "매일경제", "011": "서울경제", "014": "파이낸셜뉴스", "015": "한국경제",
    "016": "헤럴드경제", "018": "이데일리", "020": "동아일보", "021": "문화일보", "022": "세계일보",
    "023": "조선일보", "024": "매경이코노미", "025": "중앙일보", "028": "한겨레", "029": "디지털타임스",
    "030": "전자신문", "031": "아이뉴스24", "032": "경향신문", "033": "주간경향", "036": "한겨레21",
    "037": "주간동아", "044": "코리아헤럴드", "047": "오마이뉴스", "050": "한경비즈니스", "052": "YTN",
    "053": "주간조선", "055": "SBS", "056": "KBS", "057": "MBN", "079": "노컷뉴스", "081": "서울신문",
    "082": "부산일보", "087": "강원일보", "088": "매일신문", "092": "지디넷코리아", "119": "데일리안",
    "123": "조세일보", "138": "디지털데일리", "214": "MBC", "215": "한국경제TV", "243": "이코노미스트",
    "262": "신동아", "277": "아시아경제", "293": "블로터", "308": "시사IN", "346": "헬스조선",
    "366": "조선비즈", "374": "SBS Biz", "417": "머니S", "421": "뉴스1", "422": "연합뉴스TV",
    "437": "JTBC", "448": "TV조선", "449": "채널A", "469": "한국일보", "584": "동아사이언스",
    "607": "뉴스타파", "629": "더팩트", "648": "비즈워치", "656": "대전일보", "658": "국제신문",
    "666": "경기일보",
}

DEFAULT_CONFIG = {
    "site_title": "청년 뉴스 댓글 랭킹",
    "search_queries": ["청년", "청년층", "청년들", "청년세대", "청년 정책", "청년 일자리",
                       "청년 주거", "청년 고용", "청년 취업", "청년 지원"],
    "title_keywords": ["청년"],
    "exclude_title_keywords": [],
    "sections": [],
    "track_days": 7,
    "max_pages_per_query": 10,
    "request_delay_sec": 0.15,
    "workers": 4,
}

# 갱신 주기: (기사 나이 상한 시간, 최소 갱신 간격 시간)
REFRESH_TIERS = [(24, 0.15), (72, 3), (10 ** 6, 12)]
SEARCH_OVERLAP_HOURS = 3


class FatalError(Exception):
    """설정·인증 문제처럼 계속 진행할 수 없는 오류"""


# ───────────────────────── 공통 도구 ─────────────────────────

def now_kst():
    forced = os.environ.get("COLLECT_NOW")  # 테스트용
    if forced:
        return datetime.fromisoformat(forced).astimezone(KST)
    return datetime.now(KST)


def iso(dt):
    return dt.astimezone(KST).isoformat(timespec="seconds") if dt else None


def parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s).astimezone(KST)
    except ValueError:
        return None


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def write_text_if_changed(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if path.read_text(encoding="utf-8") == text:
            return False
    except OSError:
        pass
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)
    return True


TAG_RE = re.compile(r"<[^>]+>")
SPACE_RE = re.compile(r"\s+")


def clean_text(s):
    return SPACE_RE.sub(" ", html.unescape(TAG_RE.sub("", s or ""))).strip()


def load_config():
    cfg = dict(DEFAULT_CONFIG)
    user = load_json(CONFIG_FILE, {})
    if not isinstance(user, dict):
        raise FatalError("config.json 형식이 올바르지 않습니다. 쉼표·따옴표를 확인해 주세요.")
    for k, v in user.items():
        if not k.startswith("_"):
            cfg[k] = v
    for key in ("search_queries", "title_keywords", "exclude_title_keywords", "sections"):
        val = cfg.get(key) or []
        if isinstance(val, str):
            val = [val]
        cfg[key] = [str(x).strip() for x in val if str(x).strip()]
    if not cfg["search_queries"]:
        raise FatalError("config.json 의 search_queries 가 비어 있습니다.")
    cfg["track_days"] = max(1, min(31, int(cfg.get("track_days") or 7)))
    cfg["max_pages_per_query"] = max(1, min(10, int(cfg.get("max_pages_per_query") or 10)))
    cfg["request_delay_sec"] = max(0.0, float(cfg.get("request_delay_sec") or 0))
    cfg["workers"] = max(1, min(8, int(cfg.get("workers") or 4)))
    return cfg


class Http:
    def __init__(self, delay):
        self.delay = delay
        self.requests = 0
        self._lock = threading.Lock()

    def get(self, url, headers=None, timeout=20, retries=2):
        h = {"User-Agent": UA, "Accept": "*/*", "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.5"}
        if headers:
            h.update(headers)
        status, body = 0, ""
        for attempt in range(retries + 1):
            with self._lock:
                self.requests += 1
            try:
                with urlopen(Request(url, headers=h), timeout=timeout) as r:
                    raw = r.read()
                    charset = r.headers.get_content_charset() or "utf-8"
                    status, body = r.status, raw.decode(charset, errors="replace")
            except HTTPError as e:
                try:
                    body = e.read().decode("utf-8", errors="replace")
                except Exception:
                    body = ""
                status = e.code
            except (URLError, TimeoutError, ConnectionError, OSError) as e:
                status, body = 0, f"{type(e).__name__}: {e}"
            finally:
                if self.delay:
                    time.sleep(self.delay)
            if status in (0, 429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(1.5 * (attempt + 1))
                continue
            return status, body
        return status, body


def parse_jsonp(text):
    """'_callback({...});' 같은 JSONP 응답에서 JSON만 꺼냅니다."""
    if not text:
        return None
    s, e = text.find("{"), text.rfind("}")
    if s < 0 or e < s:
        return None
    try:
        return json.loads(text[s:e + 1])
    except ValueError:
        return None


# ───────────────────────── 1. 기사 검색 ─────────────────────────

NAVER_HOSTS = {"n.news.naver.com", "news.naver.com", "m.news.naver.com"}


def parse_naver_link(link):
    """네이버 뉴스 주소에서 (언론사 번호, 기사 번호, 섹션 번호)를 꺼냅니다."""
    try:
        u = urlparse(link or "")
    except ValueError:
        return None
    if u.netloc not in NAVER_HOSTS:
        return None
    q = parse_qs(u.query)
    m = re.search(r"/article/(?:comment/)?(\d{3})/(\d{6,})", u.path)
    if m:
        oid, aid = m.group(1), m.group(2)
    elif q.get("oid") and q.get("aid"):
        oid, aid = q["oid"][0], q["aid"][0]
    else:
        return None
    sid = (q.get("sid") or q.get("sid1") or [None])[0]
    return oid, aid, sid


def parse_pubdate(s):
    try:
        return parsedate_to_datetime(s).astimezone(KST)
    except (TypeError, ValueError, IndexError):
        return None


class Searcher:
    """뉴스 검색 API 호출. 처음 성공한 방식(API HUB / 개발자센터)을 이번 실행 내내 씁니다."""

    def __init__(self, http, keys, log):
        self.http, self.keys, self.log = http, keys, log
        self.endpoint = None

    def get(self, params, retries=3):
        query = urlencode(params)
        tried = []
        for ep in ([self.endpoint] if self.endpoint else SEARCH_ENDPOINTS):
            name, url, id_header, secret_header = ep
            status, body = self.http.get(url + "?" + query, retries=retries,
                                         headers={id_header: self.keys[0], secret_header: self.keys[1]})
            if status in (401, 403):
                tried.append((name, status, body))
                continue
            if self.endpoint is None and status == 200:
                self.endpoint = ep
                self.log(f"검색 API 연결: {name}")
            return status, body
        raise FatalError(self._auth_message(tried))

    @staticmethod
    def _auth_message(tried):
        detail = " / ".join(f"{n} HTTP {st}: {SPACE_RE.sub(' ', b or '')[:120]}" for n, st, b in tried)
        hub = next((st for n, st, _ in tried if n == "API HUB"), None)
        if hub == 403:
            msg = ("네이버 API HUB 권한이 없습니다. 네이버클라우드 콘솔의 NAVER API HUB → Application에서 "
                   "'검색 - 뉴스' API를 골랐는지 확인해 주세요.")
        else:
            msg = ("네이버 API 키 인증에 실패했습니다. NAVER_CLIENT_ID 에는 NAVER API HUB Application의 Client ID를, "
                   "NAVER_CLIENT_SECRET 에는 Client Secret을 넣었는지 확인해 주세요. "
                   "(네이버클라우드 계정의 Access Key가 아닙니다. 값 앞뒤 빈칸도 확인해 주세요.)")
        return f"{msg} [{detail}]"


def search_query(searcher, query, since, max_pages, log):
    """검색어 하나로 최신순 결과를 since 시점까지 거슬러 올라가며 가져옵니다."""
    items, calls = [], 0
    for page in range(max_pages):
        start = 1 + page * 100
        if start > 1000:
            break
        status, body = searcher.get({"query": query, "display": 100, "start": start, "sort": "date"})
        calls += 1
        if status == 429:
            log(f"  · '{query}': 호출 한도 초과(429) — 이번 회차는 여기까지 검색합니다.")
            return items, calls, False
        if status != 200:
            log(f"  · '{query}' {start}번째부터: 검색 실패 HTTP {status} {body[:160]}")
            return items, calls, False
        try:
            data = json.loads(body)
        except ValueError:
            log(f"  · '{query}': 검색 응답을 해석하지 못했습니다.")
            return items, calls, False
        page_items = data.get("items") or []
        items.extend(page_items)
        if len(page_items) < 100:
            break
        dates = [d for d in (parse_pubdate(it.get("pubDate")) for it in page_items) if d]
        if dates and min(dates) < since:
            break
    return items, calls, True


# ───────────────────────── 2. 기사 페이지(언론사 이름 등) ─────────────────────────

META_TAG_RE = re.compile(r"<meta\b[^>]*>", re.I)
ATTR_RE = re.compile(r'([a-zA-Z_:.-]+)\s*=\s*"([^"]*)"')
IMG_TAG_RE = re.compile(r"<img\b[^>]*>", re.I)


def _meta_map(page):
    out = {}
    for tag in META_TAG_RE.findall(page):
        attrs = {k.lower(): html.unescape(v) for k, v in ATTR_RE.findall(tag)}
        key = attrs.get("property") or attrs.get("name")
        if key and "content" in attrs and key not in out:
            out[key] = attrs["content"]
    return out


def _clean_press(name):
    name = clean_text(name)
    name = re.split(r"\s*[|｜]\s*", name)[0]
    name = re.sub(r"\s*[-:]\s*네이버.*$", "", name).strip().lstrip("@")
    if not name or "네이버" == name or len(name) > 30:
        return None
    return name


def parse_article_page(page, oid, aid):
    info = {}
    meta = _meta_map(page)
    for key in ("og:article:author", "me2:category1", "twitter:creator", "article:author"):
        name = _clean_press(meta.get(key, ""))
        if name:
            info["press"] = name
            break
    if "press" not in info:
        for tag in IMG_TAG_RE.findall(page):
            if "media_end_head_top_logo" in tag or "press_logo" in tag:
                attrs = {k.lower(): html.unescape(v) for k, v in ATTR_RE.findall(tag)}
                name = _clean_press(attrs.get("alt") or attrs.get("title") or "")
                if name:
                    info["press"] = name
                    break
    # 추천 스티커 이름표: data-type="useful" … u_likeit_list_name">쏠쏠정보<
    labels = {}
    marks = list(re.finditer(r'data-type="([a-z_]+)"', page))
    for i, m in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else m.end() + 400
        chunk = page[m.end():min(end, m.end() + 400)]
        lm = re.search(r'u_likeit_list_name[^>]*>\s*([^<]{2,8}?)\s*<', chunk)
        if lm and m.group(1) not in labels:
            labels[m.group(1)] = clean_text(lm.group(1))
    if len(labels) >= 3:
        info["labels"] = labels
    # 추천 스티커 조회용 식별자: data-sid="NEWS" data-cid="ne_001_0012345678"
    cm = re.search(r'data-cid="(ne[^",]*' + re.escape(aid) + r')"', page)
    if cm:
        cid = cm.group(1)
        fmt = cid.replace(aid, "{aid}", 1).replace(oid, "{oid}", 1)
        if fmt.count("{oid}") == 1 and fmt.count("{aid}") == 1:
            info["cid_fmt"] = fmt
        before = page[max(0, cm.start() - 300):cm.start()]
        sm = re.findall(r'data-sid="([A-Z_]+)"', before)
        if sm:
            info["like_service"] = sm[-1]
    return info


def fetch_article_page(http, oid, aid):
    status, body = http.get(ARTICLE_URL.format(oid=oid, aid=aid), headers={"Referer": "https://news.naver.com/"})
    if status != 200:
        return {"_error": f"HTTP {status}"}
    return parse_article_page(body, oid, aid)


# ───────────────────────── 3. 댓글 수 ─────────────────────────

def fetch_comment_count(http, art):
    oid, aid = art["oid"], art["aid"]
    params = {
        "ticket": "news", "templateId": "default_society", "pool": "cbox5", "lang": "ko",
        "country": "KR", "objectId": f"news{oid},{aid}", "pageSize": 1, "indexSize": 10,
        "listType": "OBJECT", "pageType": "more", "page": 1, "initialize": "true",
        "useAltSort": "true", "replyPageSize": 20, "sort": "FAVORITE", "_callback": "_callback",
    }
    referer = PUBLIC_ARTICLE.format(oid=oid, aid=aid) + (f"?sid={art['sid']}" if art.get("sid") else "")
    last_err = "응답 없음"
    for base in COMMENT_URLS:
        status, body = http.get(base + "?" + urlencode(params), headers={"Referer": referer}, retries=1)
        if status != 200:
            last_err = f"HTTP {status}"
            continue
        data = parse_jsonp(body)
        if not isinstance(data, dict):
            last_err = "응답 해석 실패"
            continue
        result = data.get("result") if isinstance(data.get("result"), dict) else {}
        count = result.get("count") if isinstance(result.get("count"), dict) else None
        if data.get("success") and count is not None:
            n = count.get("comment")
            if n is None:
                n = count.get("total")
            try:
                return True, int(n or 0)
            except (TypeError, ValueError):
                last_err = "댓글 수 형식 오류"
                continue
        last_err = f"code {data.get('code')}: {str(data.get('message'))[:60]}"
    return False, last_err


# ───────────────────────── 4. 추천 스티커 수 ─────────────────────────

def _extract_reactions(data):
    """응답에서 {contentsId: {스티커: 수}} 를 꺼냅니다."""
    out = {}
    contents = data.get("contents") if isinstance(data, dict) else None
    if not isinstance(contents, list):
        return None
    for c in contents:
        if not isinstance(c, dict):
            continue
        cid = c.get("contentsId") or c.get("contentId")
        if not cid:
            continue
        counts = {}
        reacts = c.get("reactions")
        if isinstance(reacts, list):
            for r in reacts:
                if isinstance(r, dict) and r.get("reactionType"):
                    try:
                        counts[r["reactionType"]] = counts.get(r["reactionType"], 0) + int(r.get("count") or 0)
                    except (TypeError, ValueError):
                        pass
        elif isinstance(reacts, dict):
            for k, v in reacts.items():
                n = v.get("count") if isinstance(v, dict) else v
                try:
                    counts[k] = int(n or 0)
                except (TypeError, ValueError):
                    pass
        out[str(cid)] = counts
    return out


def like_request(http, service, cids, mode):
    if mode == "comma":
        q = f"{service}[{','.join(cids)}]"
    else:  # pipe / single
        q = "|".join(f"{service}[{c}]" for c in cids)
    params = {"suppress_response_codes": "true", "q": q, "isDuplication": "false",
              "cssIds": "MULTI_MOBILE,NEWS_MOBILE"}
    status, body = http.get(LIKE_URL + "?" + urlencode(params), headers={"Referer": "https://n.news.naver.com/"},
                            retries=1)
    if status != 200:
        return None
    return _extract_reactions(parse_jsonp(body))


def fetch_reactions(http, arts, state, workers, log):
    """여러 기사 스티커 수를 묶어서 조회하고, 안 되면 한 건씩 조회합니다."""
    fmt = state.get("like_cid_fmt") or "ne_{oid}_{aid}"
    service = state.get("like_service") or "NEWS"
    cid_of = {a["id"]: fmt.format(oid=a["oid"], aid=a["aid"]) for a in arts}
    wanted = list(dict.fromkeys(cid_of.values()))
    found = {}
    chunks = [wanted[i:i + 20] for i in range(0, len(wanted), 20)]
    mode = None
    if chunks and len(chunks[0]) >= 2:
        for m in ("comma", "pipe"):
            got = like_request(http, service, chunks[0], m)
            if got and len([c for c in chunks[0] if c in got]) >= 2:
                mode = m
                found.update(got)
                break
    if mode:
        def batch(ch):
            return like_request(http, service, ch, mode) or {}
        with ThreadPoolExecutor(workers) as ex:
            for got in ex.map(batch, chunks[1:]):
                found.update(got)
    missing = [c for c in wanted if c not in found]

    def single(c):
        return c, like_request(http, service, [c], "single")
    with ThreadPoolExecutor(workers) as ex:
        for c, got in ex.map(single, missing):
            if got is not None:  # 응답이 정상이면 목록에 없을 때 0건으로 봅니다
                found[c] = got.get(c, {})
    log(f"  · 추천 스티커 조회 방식: {mode or '한 건씩'} (묶음 {len(chunks) if mode else 0}회, 단건 {len(missing)}회)")
    return {aid: found.get(cid) for aid, cid in cid_of.items()}, mode or "single"


# ───────────────────────── 5. 저장 ─────────────────────────

def day_path(d):
    return DAYS_DIR / f"{d}.json"


def load_day(d):
    data = load_json(day_path(d), {})
    arts = data.get("articles") if isinstance(data, dict) else None
    return arts if isinstance(arts, list) else []


def dump_day(d, arts):
    arts = sorted(arts, key=lambda a: (a.get("published") or "", a["id"]))
    lines = ",\n".join(json.dumps(a, ensure_ascii=False, separators=(",", ":")) for a in arts)
    return ('{"date":"%s","count":%d,"articles":[\n%s\n]}\n' % (d, len(arts), lines)) if arts else \
        '{"date":"%s","count":0,"articles":[]}\n' % d


def build_index(cfg, state, status, now):
    days, seen_types = {}, set()
    if DAYS_DIR.exists():
        for p in sorted(DAYS_DIR.glob("????-??-??.json")):
            arts = load_day(p.stem)
            days[p.stem] = {
                "count": len(arts),
                "comments": sum(int(a.get("comments") or 0) for a in arts),
            }
            for a in arts:
                seen_types.update((a.get("reactions") or {}).keys())
    labels_full = dict(DEFAULT_REACTIONS)
    labels_full.update(state.get("reaction_labels") or {})
    for t in seen_types - set(labels_full):
        labels_full[t] = t
    order = [k for k in REACTION_ORDER if k in labels_full] + \
            [k for k in labels_full if k not in REACTION_ORDER and k in seen_types]
    return {
        "updated": iso(now),
        "site_title": cfg.get("site_title") or DEFAULT_CONFIG["site_title"],
        "title_keywords": cfg["title_keywords"],
        "track_days": cfg["track_days"],
        "reaction_order": order,
        "reaction_labels": {k: labels_full[k][:2] for k in order},
        "reaction_names": {k: labels_full[k] for k in order},
        "days": dict(sorted(days.items(), reverse=True)),
        "status": status,
    }


def write_index(index):
    write_text_if_changed(INDEX_FILE, json.dumps(index, ensure_ascii=False, indent=1) + "\n")


# ───────────────────────── 실행 ─────────────────────────

def refresh_due(art, now):
    checked = parse_iso(art.get("checked"))
    if checked is None or (art.get("comments") is None and int(art.get("cfail") or 0) < 3):
        return True
    pub = parse_iso(art.get("published")) or now
    age_h = (now - pub).total_seconds() / 3600
    since_h = (now - checked).total_seconds() / 3600
    for max_age, every in REFRESH_TIERS:
        if age_h <= max_age:
            return since_h >= every
    return False


def collect(cfg, keys, log):
    t0 = time.time()
    now = now_kst()
    state = load_json(STATE_FILE, {})
    if not isinstance(state, dict):
        state = {}
    http = Http(cfg["request_delay_sec"])
    searcher = Searcher(http, keys, log)
    stats = {"search_calls": 0, "search_items": 0, "naver_items": 0, "matched": 0, "new": 0,
             "refreshed": 0, "comment_ok": 0, "comment_fail": 0, "comment_fail_new": 0,
             "like_ok": 0, "like_fail": 0,
             "page_fetches": 0}
    errors = []

    today = now.date()
    first_day = today - timedelta(days=cfg["track_days"] - 1)
    window_start = datetime.combine(first_day, dtime(0, 0), KST)

    # 1) 최근 기사 불러오기
    store = {}
    d = first_day
    while d <= today:
        for a in load_day(d.isoformat()):
            if isinstance(a, dict) and a.get("id"):
                store[a["id"]] = a
        d += timedelta(days=1)
    log(f"기존 기사 {len(store)}건 (최근 {cfg['track_days']}일)")

    # 2) 검색
    last_ok = parse_iso(state.get("last_search_ok"))
    since = window_start
    if last_ok:
        since = max(window_start, last_ok - timedelta(hours=SEARCH_OVERLAP_HOURS))
    log(f"검색 범위: {iso(since)} 이후 · 검색어 {len(cfg['search_queries'])}개")
    all_ok = True
    new_arts = []
    seen = set()
    for q in cfg["search_queries"]:
        items, calls, ok = search_query(searcher, q, since, cfg["max_pages_per_query"], log)
        all_ok &= ok
        stats["search_calls"] += calls
        stats["search_items"] += len(items)
        for it in items:
            parsed = parse_naver_link(it.get("link"))
            if not parsed:
                continue
            stats["naver_items"] += 1
            oid, aid, sid = parsed
            aid_key = f"{oid}_{aid}"
            if aid_key in seen:
                continue
            seen.add(aid_key)
            title = clean_text(it.get("title"))
            if cfg["title_keywords"] and not any(k in title for k in cfg["title_keywords"]):
                continue
            if any(k in title for k in cfg["exclude_title_keywords"]):
                continue
            pub = parse_pubdate(it.get("pubDate"))
            if not pub or pub < window_start or pub > now + timedelta(hours=1):
                continue
            section = SECTIONS.get(sid or "", "기타")
            if cfg["sections"] and section not in cfg["sections"]:
                continue
            stats["matched"] += 1
            if aid_key in store:
                continue
            art = {"id": aid_key, "oid": oid, "aid": aid, "title": title, "press": None,
                   "section": section, "sid": sid, "published": iso(pub),
                   "url": PUBLIC_ARTICLE.format(oid=oid, aid=aid),
                   "orig": it.get("originallink") or "", "comments": None, "prev": None,
                   "prev_at": None, "reactions": {}, "checked": None, "first_seen": iso(now)}
            store[aid_key] = art
            new_arts.append(art)
    stats["new"] = len(new_arts)
    log(f"검색 {stats['search_calls']}회 · 결과 {stats['search_items']}건 · 네이버 뉴스 {stats['naver_items']}건 "
        f"· 조건 일치 {stats['matched']}건 · 새 기사 {stats['new']}건")

    # 3) 언론사 이름 (새 언론사만 기사 페이지를 한 번 읽어 확인)
    press_cache = state.setdefault("press", {})
    targets = {}
    for a in new_arts:
        if a["oid"] not in press_cache and a["oid"] not in targets:
            targets[a["oid"]] = a
    if "reaction_labels" not in state and not targets and store:
        sample = next(iter(store.values()))
        targets[sample["oid"]] = sample
    if targets:
        with ThreadPoolExecutor(cfg["workers"]) as ex:
            pages = list(ex.map(lambda a: (a, fetch_article_page(http, a["oid"], a["aid"])), targets.values()))
        stats["page_fetches"] = len(pages)
        for a, info in pages:
            if info.get("press"):
                press_cache[a["oid"]] = info["press"]
            if info.get("labels") and "reaction_labels" not in state:
                state["reaction_labels"] = info["labels"]
            if info.get("cid_fmt") and "like_cid_fmt" not in state:
                state["like_cid_fmt"] = info["cid_fmt"]
                state["like_service"] = info.get("like_service") or "NEWS"
            if info.get("_error"):
                errors.append(f"기사 페이지 {a['oid']}/{a['aid']}: {info['_error']}")
    for a in store.values():
        if not a.get("press") or a.get("press_guess"):
            name = press_cache.get(a["oid"])
            if name:
                a["press"] = name
                a.pop("press_guess", None)
            elif not a.get("press"):
                host = urlparse(a.get("orig") or "").netloc.replace("www.", "")
                a["press"] = PRESS_BY_OID.get(a["oid"]) or host or f"언론사 {a['oid']}"
                a["press_guess"] = True

    # 4) 댓글 수 · 추천 스티커 갱신
    due = [a for a in store.values() if refresh_due(a, now)]
    log(f"갱신 대상 {len(due)}건 / 전체 {len(store)}건")
    if due:
        with ThreadPoolExecutor(cfg["workers"]) as ex:
            results = list(ex.map(lambda a: fetch_comment_count(http, a), due))
        reactions, like_mode = fetch_reactions(http, due, state, cfg["workers"], log)
        state["like_mode"] = like_mode
        for a, (ok, val) in zip(due, results):
            if ok:
                stats["comment_ok"] += 1
                if a.get("comments") is not None and a.get("checked"):
                    a["prev"], a["prev_at"] = a["comments"], a["checked"]
                a["comments"] = val
                a["checked"] = iso(now)
                a.pop("cfail", None)
            else:
                stats["comment_fail"] += 1
                if not a.get("cfail"):
                    stats["comment_fail_new"] += 1
                a["cfail"] = int(a.get("cfail") or 0) + 1
                if a["cfail"] >= 3:  # 계속 실패하는 기사는 다른 기사와 같은 주기로만 다시 시도
                    a["checked"] = iso(now)
                if len(errors) < 12:
                    errors.append(f"댓글 {a['oid']}/{a['aid']}: {val}")
            r = reactions.get(a["id"])
            if r is not None:
                stats["like_ok"] += 1
                a["reactions"] = r
            else:
                stats["like_fail"] += 1
        stats["refreshed"] = len(due)
        if stats["like_ok"] == 0 and state.get("like_cid_fmt"):
            # 기사 페이지에서 읽은 식별자 형식이 맞지 않았던 것 — 다음 회차에 기본 형식으로 되돌립니다
            state.pop("like_cid_fmt", None)
            state.pop("like_service", None)
    log(f"댓글 수 성공 {stats['comment_ok']} · 실패 {stats['comment_fail']} | "
        f"스티커 성공 {stats['like_ok']} · 실패 {stats['like_fail']}")

    # 5) 저장
    by_day = {}
    for a in store.values():
        pub = parse_iso(a.get("published"))
        if pub:
            by_day.setdefault(pub.date().isoformat(), []).append(a)
    changed = 0
    for day, arts in by_day.items():
        changed += write_text_if_changed(day_path(day), dump_day(day, arts))
    if all_ok:
        state["last_search_ok"] = iso(now)

    stats["http_requests"] = http.requests
    stats["seconds"] = round(time.time() - t0, 1)
    level, msg = "ok", "정상"
    tried = stats["comment_ok"] + stats["comment_fail"]
    if tried and stats["comment_ok"] == 0:
        level, msg = "warn", "댓글 수를 하나도 가져오지 못했습니다. 네이버 쪽 변경일 수 있어요 — 연결 점검을 실행해 보세요."
    elif tried and stats["comment_fail_new"] > tried * 0.3:
        level, msg = "warn", f"댓글 수 조회 실패가 많습니다 ({stats['comment_fail_new']}/{tried}건)."
    elif (stats["like_ok"] + stats["like_fail"]) and stats["like_ok"] == 0:
        level, msg = "warn", "추천 스티커 수를 가져오지 못했습니다(댓글 수는 정상)."
    elif not all_ok:
        level, msg = "warn", "일부 검색어 조회에 실패했습니다. 다음 회차에 다시 시도합니다."
    status = {"level": level, "message": msg, "at": iso(now), "stats": stats, "errors": errors[:12]}
    state["last_run"] = status
    write_text_if_changed(STATE_FILE, json.dumps(state, ensure_ascii=False, indent=1, sort_keys=True) + "\n")
    write_index(build_index(cfg, state, status, now))
    log(f"저장: 날짜 파일 {changed}개 변경 · 요청 {http.requests}회 · {stats['seconds']}초 · 상태: {msg}")
    return status


def diagnose(cfg, keys, log):
    """기사 1건으로 각 단계 응답을 그대로 보여줍니다. 문제가 생기면 이 출력을 확인하세요."""
    http = Http(0.2)
    q = cfg["search_queries"][0]
    log(f"[1/4] 네이버 뉴스 검색 API — 검색어 '{q}'")
    status, body = Searcher(http, keys, log).get({"query": q, "display": 100, "start": 1, "sort": "date"})
    log(f"  HTTP {status}")
    if status != 200:
        log("  " + body[:500])
        raise FatalError(f"검색 API 오류 HTTP {status}")
    items = json.loads(body).get("items") or []
    naver = [(it, parse_naver_link(it.get("link"))) for it in items]
    naver = [(it, p) for it, p in naver if p]
    log(f"  결과 {len(items)}건 중 네이버 뉴스 {len(naver)}건")
    if not naver:
        raise FatalError("네이버 뉴스 기사를 찾지 못했습니다.")
    picked = [(it, p) for it, p in naver if any(k in clean_text(it.get("title")) for k in cfg["title_keywords"])] or naver
    it, (oid, aid, sid) = picked[0]
    art = {"id": f"{oid}_{aid}", "oid": oid, "aid": aid, "sid": sid}
    log(f"  기사: {clean_text(it.get('title'))}")
    log(f"  주소: {it.get('link')}  (pubDate {it.get('pubDate')})")

    log("[2/4] 기사 페이지")
    status, page = http.get(ARTICLE_URL.format(oid=oid, aid=aid), headers={"Referer": "https://news.naver.com/"})
    info = parse_article_page(page, oid, aid) if status == 200 else {}
    log(f"  HTTP {status} · 읽어낸 정보: {json.dumps(info, ensure_ascii=False)}")

    log("[3/4] 댓글 수")
    ok, val = fetch_comment_count(http, art)
    log(f"  결과: {'성공 ' + str(val) + '개' if ok else '실패 — ' + str(val)}")
    if not ok:
        params = {"ticket": "news", "templateId": "default_society", "pool": "cbox5", "lang": "ko",
                  "country": "KR", "objectId": f"news{oid},{aid}", "pageSize": 1, "page": 1,
                  "sort": "FAVORITE", "_callback": "_callback"}
        status, body = http.get(COMMENT_URLS[0] + "?" + urlencode(params),
                                headers={"Referer": PUBLIC_ARTICLE.format(oid=oid, aid=aid)})
        log(f"  원본 응답 HTTP {status}: {body[:600]}")

    log("[4/4] 추천 스티커")
    st = {}
    if info.get("cid_fmt"):
        st["like_cid_fmt"], st["like_service"] = info["cid_fmt"], info.get("like_service") or "NEWS"
    fmt = st.get("like_cid_fmt") or "ne_{oid}_{aid}"
    cid = fmt.format(oid=oid, aid=aid)
    params = {"suppress_response_codes": "true", "q": f"{st.get('like_service') or 'NEWS'}[{cid}]",
              "isDuplication": "false", "cssIds": "MULTI_MOBILE,NEWS_MOBILE"}
    status, body = http.get(LIKE_URL + "?" + urlencode(params), headers={"Referer": "https://n.news.naver.com/"})
    parsed = _extract_reactions(parse_jsonp(body))
    log(f"  HTTP {status} · 해석 결과: {json.dumps(parsed, ensure_ascii=False)}")
    if not parsed:
        log(f"  원본 응답: {body[:600]}")
    if len(naver) >= 3:
        res, mode = fetch_reactions(http, [{"id": f"{p[0]}_{p[1]}", "oid": p[0], "aid": p[1]} for _, p in naver[:5]],
                                    st, 2, log)
        log(f"  여러 건 묶음 조회: {mode} · {sum(1 for v in res.values() if v is not None)}/{len(res)}건 성공")
    log("점검 끝. 위 결과에 '실패'가 있으면 이 로그를 복사해 문의해 주세요.")


def main():
    ap = argparse.ArgumentParser(description="청년 뉴스 댓글 랭킹 수집기")
    ap.add_argument("--diagnose", action="store_true", help="연결 점검만 하고 저장하지 않습니다")
    args = ap.parse_args()

    def log(msg):
        print(msg, flush=True)

    try:
        cfg = load_config()
    except FatalError as e:
        log(f"[오류] {e}")
        return 1
    keys = (os.environ.get("NAVER_CLIENT_ID", "").strip(), os.environ.get("NAVER_CLIENT_SECRET", "").strip())
    try:
        if not all(keys):
            raise FatalError("네이버 API 키가 없습니다. 저장소 Settings → Secrets and variables → Actions 에 "
                             "NAVER_CLIENT_ID 와 NAVER_CLIENT_SECRET 을 등록해 주세요. "
                             "(NAVER API HUB Application의 Client ID / Client Secret)")
        if args.diagnose:
            diagnose(cfg, keys, log)
        else:
            collect(cfg, keys, log)
        return 0
    except FatalError as e:
        log(f"[오류] {e}")
        if not args.diagnose:
            state = load_json(STATE_FILE, {})
            now = now_kst()
            status = {"level": "error", "message": str(e), "at": iso(now), "stats": {}, "errors": []}
            write_index(build_index(cfg, state if isinstance(state, dict) else {}, status, now))
        return 1


if __name__ == "__main__":
    sys.exit(main())
