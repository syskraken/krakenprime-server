import hashlib
import json
import time
import threading
from pathlib import Path
from typing import Optional

import requests
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── Config ───────────────────────────────────────────────────
HEARTBEAT_TIMEOUT_S = 90     # no heartbeat in this long → considered offline
CLEANUP_INTERVAL_S  = 15
WEBPAGE_DIR = Path(__file__).parent / "webpage"
INSTALLS_FILE = Path(__file__).parent / "installs.json"   # persisted set of client_ids ever seen

# If the server sits behind a reverse proxy (nginx, Cloudflare, etc.), the
# real client IP arrives in a header instead of the raw socket address.
# Set this to the header your proxy sets, or None to trust request.client.host.
TRUSTED_FORWARD_HEADER = "X-Forwarded-For"   # set to None if not behind a proxy

# Free IP → city-level lookup. No API key needed, but it's rate limited
# (~45 req/min) — that's why every IP is geolocated only once and cached
# in memory for the life of the process. For higher volume, swap this
# for a local MaxMind GeoLite2 database instead of a network call.
# Accuracy is city/ISP-level, not exact address — good enough to separate
# users within the same country, not precise enough to pinpoint someone.
GEOIP_URL = "http://ip-api.com/json/{ip}?fields=status,country,countryCode,city,lat,lon"
GEOIP_TIMEOUT_S = 3

# ── State ────────────────────────────────────────────────────
_lock = threading.Lock()
_sessions: dict[str, dict] = {}     # client_id -> {"geo": geo_dict|None, "last_seen": ts}
_geo_cache: dict[str, Optional[dict]] = {}   # ip -> geo dict (or None), cached in memory only
_known_client_ids: set[str] = set() # every client_id ever seen -> "installs"

app = FastAPI(title="KRAKEN PRIME Fleet Tracker")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://kraken.protectiva.site"],   # tighten to your webpage's origin once deployed
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ── Persisted install count ─────────────────────────────────
# We only ever persist client_ids (random UUIDs) to disk — never IPs —
# so the count survives server restarts without building an IP log.

def _load_installs():
    if INSTALLS_FILE.exists():
        try:
            with open(INSTALLS_FILE) as f:
                data = json.load(f)
            return set(data.get("client_ids", []))
        except Exception:
            return set()
    return set()


def _save_installs():
    try:
        with open(INSTALLS_FILE, "w") as f:
            json.dump({"client_ids": sorted(_known_client_ids)}, f)
    except OSError:
        pass


_known_client_ids = _load_installs()


# ── IP geolocation ───────────────────────────────────────────

def _client_ip(request: Request) -> str:
    if TRUSTED_FORWARD_HEADER:
        fwd = request.headers.get(TRUSTED_FORWARD_HEADER)
        if fwd:
            return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _geolocate(ip: str) -> Optional[dict]:
    """Return {"country_code", "country", "city", "lat", "lon"} for an IP,
    or None if it can't be resolved (private/loopback IP, lookup failure)."""
    if ip in _geo_cache:
        return _geo_cache[ip]

    if ip in ("unknown", "127.0.0.1", "::1") or ip.startswith(("10.", "192.168.", "172.")):
        _geo_cache[ip] = None
        return None

    geo = None
    try:
        resp = requests.get(GEOIP_URL.format(ip=ip), timeout=GEOIP_TIMEOUT_S)
        data = resp.json()
        if data.get("status") == "success" and data.get("lat") is not None:
            geo = {
                "country_code": data.get("countryCode"),
                "country": data.get("country"),
                "city": data.get("city") or "",
                "lat": data.get("lat"),
                "lon": data.get("lon"),
            }
    except Exception:
        geo = None

    _geo_cache[ip] = geo
    return geo


def _jitter(client_id: str, lat: float, lon: float) -> tuple[float, float]:
    """Deterministically nudge a point a small amount based on the
    client_id, so multiple users resolving to the same city/ISP hub don't
    render as a single overlapping dot on the map."""
    h = hashlib.sha256(client_id.encode()).digest()
    dx = ((h[0] / 255.0) - 0.5) * 0.6   # ~±0.3 degrees
    dy = ((h[1] / 255.0) - 0.5) * 0.6
    return lat + dx, lon + dy


class Heartbeat(BaseModel):
    client_id: str


@app.post("/api/heartbeat")
def heartbeat(hb: Heartbeat, request: Request):
    ip = _client_ip(request)
    geo = _geolocate(ip)

    is_new = False
    with _lock:
        if hb.client_id not in _known_client_ids:
            _known_client_ids.add(hb.client_id)
            is_new = True
        _sessions[hb.client_id] = {"geo": geo, "last_seen": time.time()}
    if is_new:
        _save_installs()   # only touches disk the first time we see a client_id

    return {"ok": True}


@app.post("/api/leave")
def leave(hb: Heartbeat):
    """Called when the app closes, so the counter drops instantly instead
    of waiting for the heartbeat timeout."""
    with _lock:
        _sessions.pop(hb.client_id, None)
    return {"ok": True}


@app.get("/api/active")
def active():
    now = time.time()
    points = []
    country_counts: dict[str, dict] = {}   # code -> {"name": ..., "count": ...}
    total = 0
    with _lock:
        for client_id, sess in _sessions.items():
            if now - sess["last_seen"] > HEARTBEAT_TIMEOUT_S:
                continue
            total += 1
            geo = sess["geo"]
            if geo:
                lat, lon = _jitter(client_id, geo["lat"], geo["lon"])
                points.append({
                    "id": client_id,
                    "country": geo["country"],
                    "country_code": geo["country_code"],
                    "city": geo["city"],
                    "lat": lat,
                    "lon": lon,
                })
                code = geo["country_code"] or "??"
                entry = country_counts.setdefault(code, {"name": geo["country"] or code, "count": 0})
                entry["count"] += 1
        total_installs = len(_known_client_ids)

    regions = [
        {"code": code, "name": info["name"], "count": info["count"]}
        for code, info in country_counts.items()
    ]
    return {"total": total, "points": points, "regions": regions, "total_installs": total_installs}


def _cleanup_loop():
    while True:
        time.sleep(CLEANUP_INTERVAL_S)
        cutoff = time.time() - HEARTBEAT_TIMEOUT_S
        with _lock:
            stale = [cid for cid, s in _sessions.items() if s["last_seen"] < cutoff]
            for cid in stale:
                _sessions.pop(cid, None)


threading.Thread(target=_cleanup_loop, daemon=True).start()

# Serve the map webpage, if present, at the site root.
if WEBPAGE_DIR.exists():
    app.mount("/", StaticFiles(directory=str(WEBPAGE_DIR), html=True), name="webpage")