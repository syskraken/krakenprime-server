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

# Free IP → country lookup. No API key needed, but it's rate limited
# (~45 req/min) — that's why every IP is geolocated only once and cached
# in memory for the life of the process. For higher volume, swap this
# for a local MaxMind GeoLite2 database instead of a network call.
GEOIP_URL = "http://ip-api.com/json/{ip}?fields=status,countryCode"
GEOIP_TIMEOUT_S = 3

# ── Region reference table (for plotting a country on the map) ─
# Centroid coordinates are approximate — good enough for a "fleet map",
# not for anything precision-sensitive. Extend freely.
REGIONS = {
    "US": ("United States", 39.8, -98.6),   "CA": ("Canada", 56.1, -106.3),
    "GB": ("United Kingdom", 54.0, -2.9),    "IE": ("Ireland", 53.4, -8.2),
    "FR": ("France", 46.6, 2.2),             "DE": ("Germany", 51.2, 10.4),
    "ES": ("Spain", 40.5, -3.7),             "PT": ("Portugal", 39.6, -8.0),
    "IT": ("Italy", 42.8, 12.8),             "NL": ("Netherlands", 52.1, 5.3),
    "BE": ("Belgium", 50.6, 4.5),            "CH": ("Switzerland", 46.8, 8.2),
    "AT": ("Austria", 47.6, 14.6),           "SE": ("Sweden", 60.1, 18.6),
    "NO": ("Norway", 60.5, 8.5),             "DK": ("Denmark", 56.0, 9.5),
    "FI": ("Finland", 64.0, 26.0),           "PL": ("Poland", 51.9, 19.1),
    "CZ": ("Czechia", 49.8, 15.5),           "RO": ("Romania", 45.9, 25.0),
    "GR": ("Greece", 39.1, 21.8),            "TR": ("Turkey", 38.9, 35.2),
    "UA": ("Ukraine", 48.4, 31.2),           "RU": ("Russia", 61.5, 105.3),
    "IN": ("India", 20.6, 79.0),             "PK": ("Pakistan", 30.4, 69.3),
    "BD": ("Bangladesh", 23.7, 90.4),        "CN": ("China", 35.9, 104.2),
    "JP": ("Japan", 36.2, 138.3),            "KR": ("South Korea", 35.9, 127.8),
    "TW": ("Taiwan", 23.7, 121.0),           "HK": ("Hong Kong", 22.3, 114.2),
    "PH": ("Philippines", 12.9, 121.8),      "VN": ("Vietnam", 14.1, 108.3),
    "TH": ("Thailand", 15.9, 100.9),         "ID": ("Indonesia", -0.8, 113.9),
    "MY": ("Malaysia", 4.2, 101.9),          "SG": ("Singapore", 1.35, 103.8),
    "AU": ("Australia", -25.3, 133.8),       "NZ": ("New Zealand", -41.0, 174.9),
    "SA": ("Saudi Arabia", 24.0, 45.1),      "AE": ("UAE", 23.4, 53.8),
    "IL": ("Israel", 31.0, 34.9),            "EG": ("Egypt", 26.8, 30.8),
    "ZA": ("South Africa", -30.6, 22.9),     "NG": ("Nigeria", 9.1, 8.7),
    "KE": ("Kenya", -0.02, 37.9),            "MA": ("Morocco", 31.8, -7.1),
    "BR": ("Brazil", -14.2, -51.9),          "AR": ("Argentina", -38.4, -63.6),
    "CL": ("Chile", -35.7, -71.5),           "CO": ("Colombia", 4.6, -74.3),
    "MX": ("Mexico", 23.6, -102.5),          "PE": ("Peru", -9.2, -75.0),
    "MM": ("Myanmar", 21.9, 95.9),           "KH": ("Cambodia", 12.6, 104.9),
    "IQ": ("Iraq", 33.2, 43.7),              "IR": ("Iran", 32.4, 53.7),
}

# ── State ────────────────────────────────────────────────────
_lock = threading.Lock()
_sessions: dict[str, dict] = {}     # client_id -> {"region": code|None, "last_seen": ts}
_geo_cache: dict[str, Optional[str]] = {}   # ip -> country code (or None), cached in memory only
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


def _geolocate(ip: str) -> Optional[str]:
    """Return an ISO country code for an IP, or None if it can't be
    resolved (private/loopback IP, lookup failure, unknown country)."""
    if ip in _geo_cache:
        return _geo_cache[ip]

    if ip in ("unknown", "127.0.0.1", "::1") or ip.startswith(("10.", "192.168.", "172.")):
        _geo_cache[ip] = None
        return None

    code = None
    try:
        resp = requests.get(GEOIP_URL.format(ip=ip), timeout=GEOIP_TIMEOUT_S)
        data = resp.json()
        if data.get("status") == "success":
            cc = data.get("countryCode")
            if cc in REGIONS:
                code = cc
    except Exception:
        code = None

    _geo_cache[ip] = code
    return code


class Heartbeat(BaseModel):
    client_id: str


@app.post("/api/heartbeat")
def heartbeat(hb: Heartbeat, request: Request):
    ip = _client_ip(request)
    region = _geolocate(ip)

    is_new = False
    with _lock:
        if hb.client_id not in _known_client_ids:
            _known_client_ids.add(hb.client_id)
            is_new = True
        _sessions[hb.client_id] = {"region": region, "last_seen": time.time()}
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
    counts: dict[str, int] = {}
    total = 0
    with _lock:
        for sess in _sessions.values():
            if now - sess["last_seen"] > HEARTBEAT_TIMEOUT_S:
                continue
            total += 1
            if sess["region"]:
                counts[sess["region"]] = counts.get(sess["region"], 0) + 1
        total_installs = len(_known_client_ids)

    regions = [
        {"code": code, "name": REGIONS[code][0], "lat": REGIONS[code][1],
         "lon": REGIONS[code][2], "count": c}
        for code, c in counts.items()
    ]
    return {"total": total, "regions": regions, "total_installs": total_installs}


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