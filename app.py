import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import wraps

import json

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, render_template, request, session, url_for

load_dotenv(override=True)

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-change-me").strip()

API_KEY = os.getenv("WHEREBY_API_KEY", "").strip()
APP_USERNAME = os.getenv("APP_USERNAME", "admin").strip()
APP_PASSWORD = os.getenv("APP_PASSWORD", "changeme").strip()
BASE_URL = "https://api.whereby.dev/v1"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

AUDIT_API_TOKEN = os.getenv("AUDIT_API_TOKEN", "").strip()
AUDIT_API_URL = "https://api.codemantra.io/lms/api/v1/audits/teacher-class"
AUDIT_HEADERS = {"X-Access-Token": f"Bearer {AUDIT_API_TOKEN}"}
AUDIT_FETCH_SIZE = 100   # page size when pulling from the remote API
AUDIT_PER_PAGE = 20      # audits shown per dashboard page

_audits = []
_audits_lock = threading.Lock()

# Recordings cache
_recordings = []
_next_cursor = None       # API cursor for fetching more recordings
_all_fetched = False      # True when no more pages from API
_fetching_more = False    # Guard to prevent concurrent fetches
_recordings_ready = False
_load_progress = {"fetched": 0, "total": 0}

# Detail caches
_participants = {}   # recordingId -> list
_urls = {}           # recordingId -> access-link URL

_lock = threading.Lock()

# Prefetch thread pool (5 concurrent detail fetches)
_detail_pool = ThreadPoolExecutor(max_workers=5)

BATCH_SIZE = 2        # API pages (50 each) = 100 recordings per batch
PER_PAGE = 50         # recordings shown per dashboard page
CACHE_FILE = os.path.join(os.getcwd(), "cache.json")


# ---------- Disk cache ----------

def _save_cache():
    with _lock:
        data = {
            "recordings": list(_recordings),
            "participants": dict(_participants),
            "urls": dict(_urls),
            "next_cursor": _next_cursor,
            "all_fetched": _all_fetched,
        }
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(data, f)
        print(f"[cache] saved {len(data['recordings'])} recordings to disk")
    except Exception as e:
        print(f"[cache] save error: {e}")


def _load_cache():
    global _recordings, _participants, _urls, _next_cursor, _all_fetched, _recordings_ready, _load_progress
    if not os.path.exists(CACHE_FILE):
        return False
    try:
        with open(CACHE_FILE) as f:
            data = json.load(f)
        with _lock:
            _recordings = data.get("recordings", [])
            _participants = data.get("participants", {})
            _urls = data.get("urls", {})
            _next_cursor = data.get("next_cursor")
            _all_fetched = data.get("all_fetched", False)
            _load_progress["fetched"] = len(_recordings)
            _load_progress["total"] = len(_recordings) if _all_fetched else 0
            if len(_recordings) >= PER_PAGE:
                _recordings_ready = True
        print(f"[cache] loaded {len(_recordings)} recordings from disk")
        return True
    except Exception as e:
        print(f"[cache] load error: {e}")
        return False


# ---------- API ----------

def _api_get(url, params=None, headers=None, retries=6):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers or HEADERS, params=params, timeout=15)
        except requests.RequestException as e:
            print(f"[api] error: {e}, retrying in 5s")
            time.sleep(5)
            continue
        if resp.status_code == 429:
            wait = int(float(resp.headers.get("Retry-After", "10"))) + 2
            print(f"[api] rate limited — waiting {wait}s")
            time.sleep(wait)
            continue
        return resp
    return resp


def _fetch_participants_for(room_session_id):
    participants = []
    cursor = None
    while True:
        params = {"roomSessionId": room_session_id, "limit": 100}
        if cursor:
            params["cursor"] = cursor
        resp = _api_get(f"{BASE_URL}/insights/participants", params=params)
        if resp.status_code != 200:
            break
        data = resp.json()
        participants.extend(data.get("results", []))
        cursor = data.get("cursor")
        if not cursor:
            break
    return participants


def _fetch_url_for(recording_id):
    resp = _api_get(f"{BASE_URL}/recordings/{recording_id}/access-link")
    if resp.status_code == 200:
        data = resp.json()
        return data.get("url") or data.get("playbackUrl") or data.get("downloadUrl") or data.get("accessLink")
    return None


# ---------- Recordings loader ----------

def _load_batch():
    """Fetch the next BATCH_SIZE API pages (100 recordings) and append to cache."""
    global _next_cursor, _all_fetched, _fetching_more, _recordings_ready

    with _lock:
        if _fetching_more or _all_fetched:
            return
        _fetching_more = True

    try:
        for _ in range(BATCH_SIZE):
            params = {"limit": 50}
            if _next_cursor:
                params["cursor"] = _next_cursor

            resp = _api_get(f"{BASE_URL}/recordings", params=params)
            if resp.status_code != 200:
                print(f"[recordings] error {resp.status_code}")
                break

            data = resp.json()
            batch = data.get("results", [])

            with _lock:
                _recordings.extend(batch)
                _load_progress["fetched"] = len(_recordings)
                if not _recordings_ready and len(_recordings) >= PER_PAGE:
                    _recordings_ready = True

            _next_cursor = data.get("cursor")
            if not _next_cursor:
                with _lock:
                    _all_fetched = True
                    _load_progress["total"] = len(_recordings)
                print(f"[recordings] all {len(_recordings)} loaded")
                break

            time.sleep(0.5)

        print(f"[recordings] batch done — {_load_progress['fetched']} loaded so far")
        _save_cache()
    finally:
        with _lock:
            _fetching_more = False


# ---------- Prefetch worker ----------

def _fetch_details_for(rec):
    rid = rec["recordingId"]
    with _lock:
        need_parts = rid not in _participants
        need_url = rid not in _urls

    if need_parts:
        parts = _fetch_participants_for(rec["roomSessionId"])
        with _lock:
            _participants[rid] = parts

    if need_url:
        url = _fetch_url_for(rid)
        with _lock:
            _urls[rid] = url


def _queue_page_prefetch(recordings):
    for rec in recordings:
        rid = rec["recordingId"]
        with _lock:
            done = rid in _participants and rid in _urls
        if not done:
            _detail_pool.submit(_fetch_details_for, rec)


# ---------- Audits ----------

SCORE_FIELDS = [
    ("classReadiness", "Class Readiness"),
    ("classOpening", "Class Opening"),
    ("contentDelivery", "Content Delivery"),
    ("studentEngagement", "Student Engagement"),
    ("understandingCheck", "Understanding Check"),
    ("techUsage", "Tech Usage"),
    ("classManagement", "Class Management"),
    ("feedbackQuality", "Feedback Quality"),
    ("classClosure", "Class Closure"),
    ("emotionalSafety", "Emotional Safety"),
    ("dataHandling", "Data Handling"),
    ("privacyCompliance", "Privacy Compliance"),
    ("platformCompliance", "Platform Compliance"),
    ("accountSafety", "Account Safety"),
]


def _fetch_all_audits():
    """Pull every audit from the LMS API and replace the in-memory cache."""
    items = []
    page = 0
    while True:
        resp = _api_get(AUDIT_API_URL, params={"page": page, "size": AUDIT_FETCH_SIZE}, headers=AUDIT_HEADERS)
        if resp.status_code != 200:
            print(f"[audits] error {resp.status_code}")
            break
        data = resp.json()
        items.extend(data.get("content", []))
        if data.get("last", True):
            break
        page += 1

    with _audits_lock:
        _audits[:] = items
    print(f"[audits] loaded {len(items)} audits")


def _get_audits(force=False):
    with _audits_lock:
        loaded = bool(_audits)
    if force or not loaded:
        _fetch_all_audits()
    with _audits_lock:
        return list(_audits)


def _round2(value):
    return round(value, 2) if isinstance(value, (int, float)) else value


def _audit_date(a):
    start_iso = (a.get("schedule") or {}).get("scheduledStart", "")
    if not start_iso:
        return None
    return datetime.fromisoformat(start_iso.replace("Z", "+00:00")).date()


def _filter_by_period(audits, month, week):
    """week is the Monday date ("YYYY-MM-DD") of the target ISO week."""
    if week:
        try:
            week_start = datetime.strptime(week, "%Y-%m-%d").date()
        except ValueError:
            return audits
        week_end = week_start + timedelta(days=6)
        return [a for a in audits if (d := _audit_date(a)) and week_start <= d <= week_end]
    if month:
        return [a for a in audits if (a.get("schedule") or {}).get("scheduledStart", "").startswith(month)]
    return audits


def _format_audit(a):
    teacher = a.get("teacher") or {}
    session = a.get("session") or {}
    schedule = a.get("schedule") or {}
    created_by = a.get("createdBy") or {}

    start_iso = schedule.get("scheduledStart")
    start_fmt = "—"
    if start_iso:
        start_fmt = datetime.fromisoformat(start_iso.replace("Z", "+00:00")).strftime("%d %b %Y, %H:%M UTC")

    scores = [
        {"label": label, "score": a.get(f"{key}Score"), "remark": a.get(f"{key}Remark") or ""}
        for key, label in SCORE_FIELDS
    ]

    red_flags = []
    for f in a.get("redFlags") or []:
        if isinstance(f, dict):
            red_flags.append({
                "type": f.get("flagType") or "Flag",
                "severity": f.get("severity") or "",
                "description": f.get("description") or "",
            })
        elif f:
            red_flags.append({"type": str(f), "severity": "", "description": ""})

    return {
        "id": a.get("id"),
        "teacherId": teacher.get("id"),
        "teacherName": teacher.get("name", "Unknown"),
        "teacherEmail": teacher.get("email", ""),
        "courseName": (session.get("course") or {}).get("name", "—"),
        "scheduledStart": start_fmt,
        "overallScore": _round2(a.get("overallScore")),
        "keyStrengths": [s for s in (a.get("keyStrengths") or []) if s and s != "N/A"],
        "keyWeaknesses": [s for s in (a.get("keyWeaknesses") or []) if s and s != "N/A"],
        "redFlags": red_flags,
        "remarks": a.get("remarks") or "",
        "createdBy": created_by.get("name", "Unknown"),
        "scores": scores,
    }


# ---------- Auth ----------

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        if username == APP_USERNAME and password == APP_PASSWORD:
            session["logged_in"] = True
            return redirect(request.args.get("next") or url_for("index"))
        error = "Invalid username or password."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---------- Routes ----------

@app.route("/")
@login_required
def index():
    return render_template("index.html")


@app.route("/api/status")
@login_required
def api_status():
    with _lock:
        return jsonify({
            "ready": _recordings_ready,
            "fetched": _load_progress["fetched"],
            "total": _load_progress["total"],
            "allFetched": _all_fetched,
        })


@app.route("/api/recordings")
@login_required
def api_recordings():
    page = max(1, request.args.get("page", 1, type=int))
    search = request.args.get("search", "").strip().lower()

    with _lock:
        ready = _recordings_ready
        all_recs = list(_recordings)
        fetching = _fetching_more
        all_done = _all_fetched

    if not ready:
        return jsonify({"status": "loading", "recordings": [], "total": 0, "page": 1, "pages": 0})

    date_from = request.args.get("date_from", "").strip()
    date_to   = request.args.get("date_to", "").strip()

    if date_from or date_to:
        filtered = []
        for rec in all_recs:
            rec_date = datetime.fromisoformat(rec["startDate"].replace("Z", "+00:00")).date()
            if date_from and rec_date < datetime.strptime(date_from, "%Y-%m-%d").date():
                continue
            if date_to and rec_date > datetime.strptime(date_to, "%Y-%m-%d").date():
                continue
            filtered.append(rec)
        all_recs = filtered

    if search:
        filtered = []
        for rec in all_recs:
            rid = rec["recordingId"]
            with _lock:
                parts = _participants.get(rid, [])
            names = [p.get("displayName", "").lower() for p in parts]
            if any(search in n for n in names) or search in rec.get("roomName", "").lower():
                filtered.append(rec)
        all_recs = filtered

    total = len(all_recs)
    pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page = max(1, min(page, pages))
    start = (page - 1) * PER_PAGE
    page_recs = all_recs[start:start + PER_PAGE]

    # Trigger loading more recordings when user reaches the last page of what's loaded
    if page >= pages and not all_done and not fetching:
        threading.Thread(target=_load_batch, daemon=True).start()

    _queue_page_prefetch(page_recs)

    formatted = [_format_recording(r) for r in page_recs]
    return jsonify({
        "status": "ok",
        "recordings": formatted,
        "total": total,
        "page": page,
        "pages": pages,
        "loadingMore": not all_done,
    })


@app.route("/api/recordings/<recording_id>/details")
@login_required
def api_recording_details(recording_id):
    with _lock:
        rec = next((r for r in _recordings if r["recordingId"] == recording_id), None)
        cached_parts = _participants.get(recording_id)
        cached_url = _urls.get(recording_id)

    if not rec:
        return jsonify({"error": "Not found"}), 404

    if cached_parts is None:
        cached_parts = _fetch_participants_for(rec["roomSessionId"])
        with _lock:
            _participants[recording_id] = cached_parts

    if cached_url is None:
        cached_url = _fetch_url_for(recording_id)
        with _lock:
            _urls[recording_id] = cached_url

    return jsonify({
        "url": cached_url,
        "participants": [
            {"name": p.get("displayName", p.get("name", "Unknown")), "id": p.get("participantId", "")}
            for p in cached_parts
        ],
    })


def _format_recording(rec):
    start = datetime.fromisoformat(rec["startDate"].replace("Z", "+00:00"))
    rid = rec["recordingId"]
    with _lock:
        participants = _participants.get(rid)
        url = _urls.get(rid)
    return {
        "recordingId": rid,
        "roomSessionId": rec["roomSessionId"],
        "roomName": rec["roomName"],
        "startDate": start.strftime("%d %b %Y, %H:%M UTC"),
        "duration": _fmt_duration(rec["startDate"], rec["endDate"]),
        "sizeInMegaBytes": rec["sizeInMegaBytes"],
        "participantsLoaded": participants is not None,
        "urlLoaded": url is not None,
        "participants": [
            {"name": p.get("displayName", "Unknown"), "id": p.get("participantId", "")}
            for p in (participants or [])
        ],
        "url": url,
    }


@app.route("/clear-cache", methods=["POST"])
@login_required
def clear_cache():
    global _recordings, _participants, _urls, _next_cursor, _all_fetched, _fetching_more, _recordings_ready, _load_progress
    if os.path.exists(CACHE_FILE):
        try:
            os.remove(CACHE_FILE)
        except Exception:
            pass
    with _lock:
        _recordings.clear()
        _participants.clear()
        _urls.clear()
        _next_cursor = None
        _all_fetched = False
        _fetching_more = False
        _recordings_ready = False
        _load_progress["fetched"] = 0
        _load_progress["total"] = 0
    threading.Thread(target=_load_batch, daemon=True).start()
    return jsonify({"status": "ok"})


@app.route("/audits")
@login_required
def audits_page():
    return render_template("audits.html")


@app.route("/api/audits")
@login_required
def api_audits():
    page = max(1, request.args.get("page", 1, type=int))
    month = request.args.get("month", "").strip()      # "YYYY-MM"
    week = request.args.get("week", "").strip()         # Monday date "YYYY-MM-DD"
    teacher_id = request.args.get("teacher", "").strip()

    all_audits = _filter_by_period(_get_audits(), month, week)

    if teacher_id:
        all_audits = [a for a in all_audits if (a.get("teacher") or {}).get("id") == teacher_id]

    all_audits.sort(key=lambda a: (a.get("schedule") or {}).get("scheduledStart", ""), reverse=True)

    total = len(all_audits)
    scores = [a.get("overallScore") for a in all_audits if a.get("overallScore") is not None]
    avg_score = round(sum(scores) / len(scores), 2) if scores else None
    flagged = sum(1 for a in all_audits if a.get("redFlags"))

    pages = max(1, (total + AUDIT_PER_PAGE - 1) // AUDIT_PER_PAGE)
    page = max(1, min(page, pages))
    start = (page - 1) * AUDIT_PER_PAGE
    page_items = all_audits[start:start + AUDIT_PER_PAGE]

    return jsonify({
        "status": "ok",
        "audits": [_format_audit(a) for a in page_items],
        "total": total,
        "page": page,
        "pages": pages,
        "stats": {"avgScore": avg_score, "flagged": flagged},
    })


@app.route("/api/audits/months")
@login_required
def api_audits_months():
    all_audits = _get_audits()
    months = sorted({
        (a.get("schedule") or {}).get("scheduledStart", "")[:7]
        for a in all_audits
        if (a.get("schedule") or {}).get("scheduledStart")
    })
    return jsonify({"months": months})


@app.route("/api/audits/teachers")
@login_required
def api_audits_teachers():
    all_audits = _get_audits()
    teachers = {}
    for a in all_audits:
        t = a.get("teacher") or {}
        tid = t.get("id")
        if tid and tid not in teachers:
            teachers[tid] = t.get("name", "Unknown")
    result = sorted(
        ({"id": tid, "name": name} for tid, name in teachers.items()),
        key=lambda x: x["name"].lower(),
    )
    return jsonify({"teachers": result})


@app.route("/api/audits/teacher-stats")
@login_required
def api_audits_teacher_stats():
    """Per-teacher average score for the given period — always across all teachers,
    regardless of any teacher filter applied to the main audits list."""
    month = request.args.get("month", "").strip()
    week = request.args.get("week", "").strip()

    all_audits = _filter_by_period(_get_audits(), month, week)

    agg = {}
    for a in all_audits:
        t = a.get("teacher") or {}
        tid = t.get("id")
        if not tid:
            continue
        entry = agg.setdefault(tid, {"name": t.get("name", "Unknown"), "scores": []})
        score = a.get("overallScore")
        if score is not None:
            entry["scores"].append(score)

    result = []
    for tid, entry in agg.items():
        scores = entry["scores"]
        avg = round(sum(scores) / len(scores), 2) if scores else None
        result.append({"teacherId": tid, "teacherName": entry["name"], "count": len(scores), "avgScore": avg})

    result.sort(key=lambda x: (x["avgScore"] is None, -(x["avgScore"] or 0)))
    return jsonify({"teachers": result})


@app.route("/api/audits/refresh", methods=["POST"])
@login_required
def api_audits_refresh():
    _fetch_all_audits()
    return jsonify({"status": "ok"})


def _fmt_duration(start_iso, end_iso):
    s = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    e = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    secs = int((e - s).total_seconds())
    h, rem = divmod(secs, 3600)
    m, s2 = divmod(rem, 60)
    return f"{h}h {m}m {s2}s" if h else f"{m}m {s2}s"


# Load from disk cache or fetch from Whereby API on startup
if not _load_cache():
    threading.Thread(target=_load_batch, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
