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
    version="1.0.1"
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
    # (your existing dashboard HTML – unchanged)
    html_content = """..."""   # keep your existing HTML
    return HTMLResponse(content=html_content)

# ---------- HOME ----------
@app.get("/home")
async def get_home():
    # unchanged
    ...

# ---------- CATEGORIES ----------
async def _get_category_data(tab_id: int, page: int = 1, per_page: int = 24, sort: str = "RECOMMEND") -> dict:
    # unchanged
    ...

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
    # unchanged
    ...

@app.get("/search")
async def search(q: str = Query(..., min_length=1), page: int = 1, per_page: int = 20):
    # unchanged
    ...

# ---------- DETAIL ----------
@app.get("/detail/{slug}")
async def get_movie_detail(slug: str):
    return await _make_request(f"{API_BASE}/detail?detailPath={slug}")

# ---------- STREAM INFO (unchanged) ----------
@app.get("/api/stream/{subject_id}")
async def get_stream_sources(
    subject_id: str,
    detail_path: str,
    se: int = Query(None),
    ep: int = Query(None)
):
    # leave as is or update to use _get_stream_data if you want
    ...

# ---------- CAPTIONS (unchanged) ----------
@app.get("/api/stream/{subject_id}/captions")
async def get_captions(...):
    ...

# ---------- NEW: STREAM PROXY (the actual working player) ----------
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

# ---------- WEB UI (unchanged) ----------
@app.get("/stream", response_class=HTMLResponse)
async def web_ui():
    with open("stream.html", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

# ---------- RUN ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
