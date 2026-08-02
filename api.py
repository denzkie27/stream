import re
import json
import httpx
import asyncio
import time
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from bs4 import BeautifulSoup

app = FastAPI(
    title="Stream API",
    description="Ad‑free streaming API for movies and TV shows",
    version="1.0.2"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_URL = "https://moviebox.ph"
API_BASE = "https://h5-api.aoneroom.com/wefeed-h5api-bff"

_bearer_token: str | None = None
_token_lock = asyncio.Lock()
REQUEST_TIMEOUT = 30.0

# Cache for stream data (key -> (data, domain, ref, timestamp))
_stream_cache = {}
CACHE_TTL = 15  # seconds

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
    "Referer": "https://moviebox.ph/",
    "Origin": "https://moviebox.ph",
    "X-Client-Info": '{"timezone":"Asia/Dhaka"}',
    "X-Request-Lang": "en",
    "Accept": "application/json",
    "sec-ch-ua": '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
}

PLAYER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "sec-ch-ua": '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

# ---------- HELPERS ----------
async def _get_bearer_token() -> str:
    global _bearer_token
    if _bearer_token:
        return _bearer_token
    async with _token_lock:
        if _bearer_token:
            return _bearer_token
        last_exc = None
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(follow_redirects=True, timeout=REQUEST_TIMEOUT) as client:
                    resp = await client.get(f"{API_BASE}/home?host=moviebox.ph", headers=DEFAULT_HEADERS)
                    x_user = resp.headers.get("x-user")
                    if x_user:
                        _bearer_token = json.loads(x_user).get("token")
                    if not _bearer_token:
                        cookie = resp.headers.get("set-cookie", "")
                        m = re.search(r"token=([^;]+)", cookie)
                        if m:
                            _bearer_token = m.group(1)
                    if _bearer_token:
                        return _bearer_token
            except Exception as e:
                last_exc = e
                await asyncio.sleep(1)
        raise HTTPException(status_code=502, detail=f"Could not acquire guest token. Last error: {last_exc}")

async def _make_request(url: str, method: str = "GET", payload: dict = None, custom_headers: dict = None) -> dict:
    token = await _get_bearer_token()
    headers = {**DEFAULT_HEADERS}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if payload:
        headers["Content-Type"] = "application/json"
    if custom_headers:
        headers.update(custom_headers)

    async with httpx.AsyncClient(follow_redirects=True, timeout=REQUEST_TIMEOUT) as client:
        try:
            if method == "POST":
                resp = await client.post(url, headers=headers, json=payload)
            else:
                resp = await client.get(url, headers=headers)
            x_user = resp.headers.get("x-user")
            if x_user:
                new_token = json.loads(x_user).get("token")
                if new_token:
                    _bearer_token = new_token
            if resp.status_code != 200:
                raise HTTPException(status_code=502, detail=f"Upstream API error: {resp.status_code} {resp.text[:300]}")
            return resp.json()
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Request failed: {str(e)}")

async def _get_content_type(slug: str) -> str:
    """Robust content type detection using resourceType, subjectType, and seasons."""
    if not hasattr(_get_content_type, "cache"):
        _get_content_type.cache = {}
    if slug in _get_content_type.cache:
        return _get_content_type.cache[slug]

    detail = await _make_request(f"{API_BASE}/detail?detailPath={slug}")
    subject_data = detail.get("data", {}).get("subject", {})
    resource = detail.get("data", {}).get("resource", {})

    rtype = subject_data.get("resourceType", "").strip().lower()
    if rtype in ("movie",):
        _get_content_type.cache[slug] = "movie"
        return "movie"
    if rtype in ("tvseries", "tv_series", "tv"):
        _get_content_type.cache[slug] = "tv"
        return "tv"
    if rtype in ("animation", "anime"):
        _get_content_type.cache[slug] = "animation"
        return "animation"

    subj_type = subject_data.get("subjectType", 0)
    if subj_type == 2:
        seasons = resource.get("seasons", [])
        if seasons and any(s.get("se", 0) > 0 for s in seasons):
            _get_content_type.cache[slug] = "tv"
            return "tv"
        else:
            _get_content_type.cache[slug] = "movie"
            return "movie"
    elif subj_type == 3:
        _get_content_type.cache[slug] = "animation"
        return "animation"

    seasons = resource.get("seasons", [])
    if seasons and any(s.get("se", 0) > 0 for s in seasons):
        _get_content_type.cache[slug] = "tv"
        return "tv"

    _get_content_type.cache[slug] = "movie"
    return "movie"

async def _get_player_domain() -> str:
    try:
        data = await _make_request(f"{API_BASE}/media-player/get-domain")
        return data.get("data", "https://netfilm.world").rstrip("/")
    except Exception:
        return "https://netfilm.world"

# ---------- STREAM DATA FETCHER (multi‑domain) ----------
async def _get_stream_data(sid: str, slug: str, se: int = 0, ep: int = 0):
    """Try multiple methods to get stream data – returns (data, domain, ref)"""
    cache_key = f"{sid}|{slug}|{se}|{ep}"
    now = time.time()
    if cache_key in _stream_cache:
        data, domain, ref, ts = _stream_cache[cache_key]
        if now - ts < CACHE_TTL:
            return data, domain, ref

    # Method 1: Get domain from API, then fetch stream
    try:
        domain = await _get_player_domain()
        ref = f"{domain}/spa/videoPlayPage/movies/{slug}?id={sid}&type=/movie/detail&detailSe={se}&detailEp={ep}&lang=en"
        url = f"{domain}/wefeed-h5api-bff/subject/play?subjectId={sid}&se={se}&ep={ep}&detailPath={slug}"

        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as c:
            r = await c.get(url, headers={**PLAYER_HEADERS, "Referer": ref, "Origin": domain})
            data = r.json().get("data", {})
            if data.get("hasResource") and (data.get("streams") or data.get("dash")):
                _stream_cache[cache_key] = (data, domain, ref, now)
                return data, domain, ref
    except Exception as e:
        pass

    # Method 2: Try moviebox.ph domain
    try:
        domain = "https://moviebox.ph"
        ref = f"{domain}/spa/videoPlayPage/movies/{slug}?id={sid}&type=/movie/detail&detailSe={se}&detailEp={ep}&lang=en"
        url = f"https://h5-api.aoneroom.com/wefeed-h5api-bff/subject/play?subjectId={sid}&se={se}&ep={ep}&detailPath={slug}"

        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as c:
            r = await c.get(url, headers={**PLAYER_HEADERS, "Referer": ref, "Origin": domain})
            data = r.json().get("data", {})
            if data.get("hasResource") and (data.get("streams") or data.get("dash")):
                _stream_cache[cache_key] = (data, domain, ref, now)
                return data, domain, ref
    except Exception as e:
        pass

    # Method 3: Try netfilm.world directly
    try:
        domain = "https://netfilm.world"
        ref = f"{domain}/spa/videoPlayPage/movies/{slug}?id={sid}&type=/movie/detail&detailSe={se}&detailEp={ep}&lang=en"
        url = f"{domain}/wefeed-h5api-bff/subject/play?subjectId={sid}&se={se}&ep={ep}&detailPath={slug}"

        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as c:
            r = await c.get(url, headers={**PLAYER_HEADERS, "Referer": ref, "Origin": domain})
            data = r.json().get("data", {})
            if data.get("hasResource") and (data.get("streams") or data.get("dash")):
                _stream_cache[cache_key] = (data, domain, ref, now)
                return data, domain, ref
    except Exception as e:
        pass

    # Fallback: return empty
    return {"hasResource": False, "streams": [], "dash": []}, "", ""

# ---------- DASHBOARD ----------
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    # keep your existing dashboard HTML (shortened for space; use the one from your original api.py)
    html_content = """..."""   # <-- paste your full dashboard HTML here
    return HTMLResponse(content=html_content)

# ---------- HOME ----------
@app.get("/home")
async def get_home():
    url = f"{API_BASE}/home?host=moviebox.ph"
    data = await _make_request(url)
    sections = []
    for op in data.get("data", {}).get("operatingList", []) or []:
        op_type = op.get("type")
        title = op.get("title", "Featured")
        if op_type == "BANNER":
            items = [{
                "name": item.get("title") or (item.get("subject") or {}).get("title"),
                "poster_url": item.get("image", {}).get("url") or (item.get("subject") or {}).get("cover", {}).get("url"),
                "slug": item.get("detailPath") or (item.get("subject") or {}).get("detailPath"),
                "subject_id": (item.get("subject") or {}).get("subjectId"),
                "badge": (item.get("subject") or {}).get("corner")
            } for item in op.get("banner", {}).get("items", []) if item.get("title") and "Communities" not in item.get("title")]
            sections.append({"section": "Banner", "count": len(items), "items": items})
        elif op_type in ["SUBJECTS_MOVIE", "SUBJECTS_TV", "SUBJECTS_ANIMATION"]:
            items = [{
                "name": sub.get("title"),
                "poster_url": sub.get("cover", {}).get("url"),
                "slug": sub.get("detailPath"),
                "subject_id": sub.get("subjectId"),
                "badge": sub.get("corner"),
                "rating": sub.get("imdbRatingValue")
            } for sub in op.get("subjects", [])]
            sections.append({"section": title, "count": len(items), "items": items})
    return {"status": "success", "sections": sections}

# ---------- CATEGORIES ----------
async def _get_category_data(tab_id: int, page: int = 1, per_page: int = 24, sort: str = "RECOMMEND") -> dict:
    url = f"{API_BASE}/subject/filter"
    payload = {"tabId": tab_id, "filter": {"sort": sort, "genre": "ALL", "country": "ALL", "year": "ALL", "language": "ALL"},
               "page": page, "perPage": per_page}
    data = await _make_request(url, method="POST", payload=payload)
    inner = data.get("data", {})
    raw_items = inner.get("items", inner.get("subjects", []))
    items = [{
        "name": sub.get("title"),
        "poster_url": sub.get("cover", {}).get("url"),
        "slug": sub.get("detailPath"),
        "subject_id": sub.get("subjectId"),
        "badge": sub.get("corner"),
        "rating": sub.get("imdbRatingValue"),
        "year": sub.get("releaseDate", "")[:4] if sub.get("releaseDate") else None
    } for sub in raw_items]
    pager = inner.get("pager", {})
    total = pager.get("totalCount") or inner.get("total") or len(items)
    return {"page": page, "per_page": per_page, "total": total, "items": items}

@app.get("/movies")
async def get_movies(page: int = 1, sort: str = "RECOMMEND"):
    return await _get_category_data(tab_id=2, page=page, sort=sort)

@app.get("/tv-series")
async def get_tv_series(page: int = 1, sort: str = "RECOMMEND"):
    return await _get_category_data(tab_id=5, page=page, sort=sort)

@app.get("/animation")
async def get_animation(page: int = 1, sort: str = "RECOMMEND"):
    return await _get_category_data(tab_id=8, page=page, sort=sort)

# ---------- SEARCH ----------
@app.get("/search/suggest")
async def get_search_suggestions(q: str = Query(..., min_length=1)):
    url = f"{API_BASE}/subject/search-suggest"
    data = await _make_request(url, method="POST", payload={"keyword": q, "perPage": 10})
    inner = data.get("data", {})
    raw = inner.get("items", inner.get("list", []))
    suggestions = []
    for item in raw:
        sub = item.get("subject") or {}
        suggestions.append({
            "title": sub.get("title") or item.get("word") or item.get("title"),
            "slug": sub.get("detailPath") or item.get("detailPath"),
            "subject_id": sub.get("subjectId") or item.get("subjectId")
        })
    return {"suggestions": suggestions}

@app.get("/search")
async def search(q: str = Query(..., min_length=1), page: int = 1, per_page: int = 20):
    try:
        search_api_url = f"{API_BASE}/subject/search"
        data = await _make_request(search_api_url, method="POST", payload={"keyword": q, "page": page, "perPage": per_page})
        inner = data.get("data", {})
        raw_items = inner.get("items", inner.get("subjects", []))
        total = inner.get("total", len(raw_items))
        items = [{
            "name": sub.get("title"),
            "slug": sub.get("detailPath"),
            "poster_url": sub.get("cover", {}).get("url") if sub.get("cover") else None,
            "subject_id": sub.get("subjectId"),
            "rating": sub.get("imdbRatingValue"),
            "year": (sub.get("releaseDate", "") or "")[:4]
        } for sub in raw_items]
        return {"query": q, "page": page, "per_page": per_page, "total": total, "items": items, "source": "api"}
    except HTTPException:
        pass

    # Fallback scraping
    search_url = f"https://moviebox.ph/search?q={q.replace(' ', '+')}&page={page}"
    async with httpx.AsyncClient(follow_redirects=True, timeout=REQUEST_TIMEOUT) as client:
        headers = {
            **DEFAULT_HEADERS,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        resp = await client.get(search_url, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Search page returned {resp.status_code}.")
        html = resp.text

    soup = BeautifulSoup(html, 'html.parser')
    items = []
    for a in soup.find_all('a', href=re.compile(r'^/detail/')):
        slug = a['href'].replace('/detail/', '')
        title_tag = a.find(['h3', 'span', 'p'], class_=re.compile(r'title|name', re.I))
        title = title_tag.get_text(strip=True) if title_tag else a.get_text(strip=True)
        title = re.sub(r'^[^a-zA-Z0-9]+', '', title)
        img = a.find('img')
        poster = img.get('src') if img else None
        if title and slug:
            items.append({"name": title, "slug": slug, "poster_url": poster, "subject_id": None})

    seen = set()
    unique_items = []
    for item in items:
        if item['slug'] not in seen:
            seen.add(item['slug'])
            unique_items.append(item)

    total_span = soup.find(['span', 'div'], string=re.compile(r'\d+ results?'))
    total = len(unique_items)
    if total_span:
        m = re.search(r'(\d[\d,]*)', total_span.get_text())
        if m:
            total = int(m.group(1).replace(',', ''))

    return {"query": q, "page": page, "per_page": len(unique_items), "total": total, "items": unique_items, "source": "scraping"}

# ---------- DETAIL ----------
@app.get("/detail/{slug}")
async def get_movie_detail(slug: str):
    return await _make_request(f"{API_BASE}/detail?detailPath={slug}")

# ---------- STREAM INFO ----------
@app.get("/api/stream/{subject_id}")
async def get_stream_sources(
    subject_id: str,
    detail_path: str,
    se: int = Query(None),
    ep: int = Query(None)
):
    content_type = await _get_content_type(detail_path)
    if se is None:
        se = 1 if content_type == "tv" else 0
    if ep is None:
        ep = 1 if content_type == "tv" else 0

    domain = await _get_player_domain()
    type_path = {"movie": "movies", "tv": "tv", "animation": "animation"}.get(content_type, "movies")
    player_referer = (
        f"{domain}/spa/videoPlayPage/{type_path}/{detail_path}"
        f"?id={subject_id}&type=/{content_type}/detail&detailSe={se}&detailEp={ep}&lang=en"
    )
    play_url = f"{API_BASE}/subject/play?subjectId={subject_id}&se={se}&ep={ep}&detailPath={detail_path}&host=moviebox.ph"

    try:
        raw_json = await _make_request(play_url, custom_headers={"Referer": player_referer, "X-Source": "moviebox.ph"})
    except HTTPException as e:
        return {"subject_id": subject_id, "se": se, "ep": ep, "has_resource": False, "note": "Failed to fetch play data.", "debug_error": e.detail}

    data = raw_json.get("data", {})
    has_resource = data.get("hasResource", False)
    streams = [{
        "resolution": (f"{s.get('resolutions')}p" if s.get('resolutions', '').isdigit() else s.get('resolutions')),
        "format": s.get("format"),
        "url": s.get("url"),
        "size": s.get("size"),
        "duration": s.get("duration"),
        "codec": s.get("codecName")
    } for s in data.get("streams", [])]

    return {
        "subject_id": subject_id,
        "se": se, "ep": ep,
        "has_resource": has_resource,
        "sources": streams,
        "hls": data.get("hls", []),
        "dash": data.get("dash", []),
        "free_episodes": data.get("freeNum"),
        "limited": data.get("limited", False),
        "note": None if has_resource else "No stream found for this episode.",
        "player_page": player_referer,
        "detected_type": content_type
    }

# ---------- CAPTIONS ----------
@app.get("/api/stream/{subject_id}/captions")
async def get_captions(
    subject_id: str,
    detail_path: str,
    se: int = Query(None),
    ep: int = Query(None)
):
    content_type = await _get_content_type(detail_path)
    if se is None:
        se = 1 if content_type == "tv" else 0
    if ep is None:
        ep = 1 if content_type == "tv" else 0

    domain = await _get_player_domain()
    type_path = {"movie": "movies", "tv": "tv", "animation": "animation"}.get(content_type, "movies")
    player_referer = (
        f"{domain}/spa/videoPlayPage/{type_path}/{detail_path}"
        f"?id={subject_id}&type=/{content_type}/detail&detailSe={se}&detailEp={ep}&lang=en"
    )
    play_url = f"{API_BASE}/subject/play?subjectId={subject_id}&se={se}&ep={ep}&detailPath={detail_path}&host=moviebox.ph"

    try:
        play_raw = await _make_request(play_url, custom_headers={"Referer": player_referer, "X-Source": "moviebox.ph"})
    except HTTPException as e:
        return {"subject_id": subject_id, "se": se, "ep": ep, "count": 0, "captions": [], "error": e.detail}

    play_data = play_raw.get("data", {})
    streams = play_data.get("streams", [])
    dash = play_data.get("dash", [])
    stream_id = None
    stream_format = None
    if streams:
        stream_id = streams[0].get("id")
        stream_format = streams[0].get("format", "MP4")
    elif dash:
        stream_id = dash[0].get("id")
        stream_format = dash[0].get("format", "DASH")
    if not stream_id:
        return {"subject_id": subject_id, "se": se, "ep": ep, "count": 0, "captions": []}

    cap_url = f"{API_BASE}/subject/caption?format={stream_format}&id={stream_id}&subjectId={subject_id}&detailPath={detail_path}"
    data = await _make_request(cap_url)
    inner = data.get("data", {})
    captions = inner.get("captions", []) if isinstance(inner, dict) else inner
    return {"subject_id": subject_id, "se": se, "ep": ep, "count": len(captions), "captions": captions}

# ---------- STREAM PROXY (the working player) ----------
@app.get("/stream-proxy/{subject_id}")
async def stream_proxy(
    subject_id: str,
    detail_path: str,
    quality: str = "480p",
    se: int = 0,
    ep: int = 0
):
    """
    Proxies the actual video stream (DASH first, then MP4)
    with the correct Referer / Origin headers.
    """
    try:
        data, domain, ref = await _get_stream_data(subject_id, detail_path, se, ep)

        # --- DASH (proxy with full referer) ---
        dash_sources = data.get("dash", [])
        if dash_sources:
            q = quality.replace("p", "")
            sel = next(
                (s for s in dash_sources if q in str(s.get("resolutions", ""))),
                dash_sources[-1]
            )
            dash_url = sel.get("url")
            if dash_url:
                async def gen_dash():
                    cdn_headers = {
                        "User-Agent": PLAYER_HEADERS["User-Agent"],
                        "Accept": "*/*",
                        "Referer": ref,
                        "Origin": domain,
                    }
                    async with httpx.AsyncClient(follow_redirects=True, timeout=300, verify=False) as c:
                        async with c.stream("GET", dash_url, headers=cdn_headers) as r2:
                            async for chunk in r2.aiter_bytes(1048576):
                                yield chunk
                return StreamingResponse(gen_dash(), media_type="application/dash+xml")

        # --- MP4 (proxy with full referer) ---
        mp4_streams = data.get("streams", [])
        if mp4_streams:
            q = quality.replace("p", "")
            sel = next(
                (s for s in mp4_streams if s.get("resolutions") == q),
                mp4_streams[-1]
            )
            mp4_url = sel.get("url")
            if mp4_url:
                async def gen_mp4():
                    cdn_headers = {
                        "User-Agent": PLAYER_HEADERS["User-Agent"],
                        "Accept": "*/*",
                        "Referer": ref,
                        "Origin": domain,
                    }
                    async with httpx.AsyncClient(follow_redirects=True, timeout=300, verify=False) as c:
                        async with c.stream("GET", mp4_url, headers=cdn_headers) as r2:
                            async for chunk in r2.aiter_bytes(1048576):
                                yield chunk
                return StreamingResponse(gen_mp4(), media_type="video/mp4")

        raise HTTPException(404, "No streams available")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Stream proxy error: {str(e)}")

# ---------- WEB UI ----------
@app.get("/stream", response_class=HTMLResponse)
async def web_ui():
    with open("stream.html", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

# ---------- RUN ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
