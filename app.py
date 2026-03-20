import json
import time
from collections import Counter
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
from twilio.rest import Client

from main import (
    validate_env,
    init_db,
    get_db,
    fetch_blr_flats,
    build_whatsapp_body,
    mark_sent,
    get_session_profile,
    MAX_WHATSAPP_RESULTS,
    TWILIO_SID,
    TWILIO_TOKEN,
    TARGET_WHATSAPP,
    TWILIO_SENDER,
    OFFICE_ADDR,
    USER_PROFILE_DEFAULTS,
    now_ts,
)

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="Rental Intelligence Dashboard",
    page_icon="🏠",
    layout="wide"
)

# =========================================================
# STYLING
# =========================================================
st.markdown("""
<style>
    .stApp {
        background:
            radial-gradient(circle at top left, rgba(99,102,241,0.12), transparent 30%),
            radial-gradient(circle at top right, rgba(16,185,129,0.10), transparent 28%),
            linear-gradient(180deg, #eef4ff 0%, #f7faff 40%, #f9fbff 100%);
        color: #0f172a;
    }

    [data-testid="stSidebar"] {
        background:
            linear-gradient(180deg, #0f172a 0%, #172554 45%, #1e3a8a 100%);
        border-right: 1px solid rgba(255,255,255,0.08);
    }

    [data-testid="stSidebar"] * {
        color: #f8fafc !important;
    }

    /* Fix: sidebar inputs need dark background + white text so typed text is visible */
    [data-testid="stSidebar"] input,
    [data-testid="stSidebar"] textarea,
    [data-testid="stSidebar"] [data-baseweb="input"] input,
    [data-testid="stSidebar"] [data-baseweb="textarea"] textarea {
        background-color: rgba(255, 255, 255, 0.10) !important;
        color: #f8fafc !important;
        caret-color: #f8fafc !important;
        border-color: rgba(255, 255, 255, 0.25) !important;
    }

    /* Number input inner box */
    [data-testid="stSidebar"] [data-baseweb="input"] {
        background-color: rgba(255, 255, 255, 0.10) !important;
        border-color: rgba(255, 255, 255, 0.25) !important;
    }

    /* Placeholder text */
    [data-testid="stSidebar"] input::placeholder,
    [data-testid="stSidebar"] textarea::placeholder {
        color: rgba(248, 250, 252, 0.45) !important;
    }

    /* Selectbox dropdown trigger */
    [data-testid="stSidebar"] [data-baseweb="select"] > div {
        background-color: rgba(255, 255, 255, 0.10) !important;
        border-color: rgba(255, 255, 255, 0.25) !important;
        color: #f8fafc !important;
    }

    /* Selectbox selected value text */
    [data-testid="stSidebar"] [data-baseweb="select"] [data-testid="stMarkdownContainer"] p,
    [data-testid="stSidebar"] [data-baseweb="select"] span {
        color: #f8fafc !important;
    }

    /* Slider track and thumb labels */
    [data-testid="stSidebar"] [data-testid="stSlider"] p,
    [data-testid="stSidebar"] [data-testid="stSlider"] span {
        color: #f8fafc !important;
    }

    .hero {
        background: linear-gradient(135deg, #1d4ed8 0%, #4338ca 45%, #0f766e 100%);
        border-radius: 24px;
        padding: 28px 30px;
        color: white;
        box-shadow: 0 18px 40px rgba(37, 99, 235, 0.20);
        margin-bottom: 1.2rem;
        border: 1px solid rgba(255,255,255,0.14);
    }

    .hero-title {
        font-size: 2rem;
        font-weight: 800;
        line-height: 1.1;
        margin-bottom: 0.35rem;
    }

    .hero-subtitle {
        font-size: 1rem;
        color: rgba(255,255,255,0.88);
        margin-bottom: 0;
    }

    .section-shell {
        background: rgba(255,255,255,0.78);
        backdrop-filter: blur(8px);
        border: 1px solid rgba(148,163,184,0.18);
        border-radius: 22px;
        padding: 18px 18px 14px 18px;
        box-shadow: 0 10px 30px rgba(15,23,42,0.06);
        margin-bottom: 1rem;
    }

    .profile-box {
        background: linear-gradient(135deg, rgba(255,255,255,0.94) 0%, rgba(239,246,255,0.96) 100%);
        padding: 1rem;
        border-radius: 20px;
        border: 1px solid rgba(59,130,246,0.14);
        box-shadow: 0 10px 24px rgba(30,41,59,0.06);
    }

    .card {
        background: linear-gradient(180deg, rgba(255,255,255,0.98) 0%, rgba(248,250,252,0.98) 100%);
        padding: 1.2rem 1.2rem 1rem 1.2rem;
        border-radius: 22px;
        box-shadow: 0 14px 30px rgba(15, 23, 42, 0.08);
        border: 1px solid rgba(148,163,184,0.16);
        margin-bottom: 1rem;
    }

    .rent-title {
        font-size: 1.12rem;
        font-weight: 800;
        color: #0f172a;
        margin-bottom: 0.3rem;
        line-height: 1.35;
    }

    .small-muted {
        color: #475569;
        font-size: 0.92rem;
        margin-bottom: 0.55rem;
    }

    .score-pill {
        display: inline-block;
        padding: 0.42rem 0.78rem;
        border-radius: 999px;
        background: linear-gradient(135deg, #dbeafe 0%, #eef2ff 100%);
        color: #312e81;
        font-size: 0.83rem;
        font-weight: 700;
        margin-right: 0.42rem;
        margin-bottom: 0.42rem;
        border: 1px solid rgba(79,70,229,0.10);
    }

    .good-pill {
        display: inline-block;
        padding: 0.35rem 0.7rem;
        border-radius: 999px;
        background: linear-gradient(135deg, #dcfce7 0%, #ecfdf5 100%);
        color: #166534;
        font-size: 0.8rem;
        font-weight: 700;
        margin: 0.18rem 0.3rem 0.18rem 0;
        border: 1px solid rgba(34,197,94,0.12);
    }

    .info-chip {
        display: inline-block;
        padding: 0.32rem 0.68rem;
        border-radius: 999px;
        background: linear-gradient(135deg, #ecfeff 0%, #ecfdf5 100%);
        color: #0f766e;
        font-size: 0.78rem;
        font-weight: 700;
        margin: 0.18rem 0.3rem 0.18rem 0;
        border: 1px solid rgba(20,184,166,0.12);
    }

    .subsection-title {
        font-size: 1rem;
        font-weight: 800;
        color: #0f172a;
        margin-bottom: 0.65rem;
    }

    .label-strong {
        color: #0f172a;
        font-weight: 700;
    }

    div[data-testid="metric-container"] {
        background: linear-gradient(180deg, rgba(255,255,255,0.95) 0%, rgba(241,245,249,0.95) 100%);
        border: 1px solid rgba(148,163,184,0.18);
        padding: 12px 10px;
        border-radius: 18px;
        box-shadow: 0 8px 20px rgba(15,23,42,0.05);
    }

    .stButton > button {
        background: linear-gradient(135deg, #2563eb 0%, #4338ca 100%);
        color: white;
        border: none;
        border-radius: 12px;
        font-weight: 700;
        padding: 0.55rem 1rem;
        box-shadow: 0 10px 20px rgba(59,130,246,0.20);
    }

    .stDownloadButton > button {
        background: linear-gradient(135deg, #0f766e 0%, #1d4ed8 100%);
        color: white;
        border: none;
        border-radius: 12px;
        font-weight: 700;
    }

    .stTextInput input, .stNumberInput input, .stSelectbox div[data-baseweb="select"] > div {
        border-radius: 12px !important;
    }

    hr {
        border: none;
        border-top: 1px solid rgba(148,163,184,0.18);
        margin: 0.8rem 0 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# =========================================================
# SESSION STATE
# =========================================================
if "listings" not in st.session_state:
    st.session_state.listings = []

if "last_status" not in st.session_state:
    st.session_state.last_status = None

if "last_search_input" not in st.session_state:
    st.session_state.last_search_input = {}

# FIX: Per-session profile stored in session_state — never touches module-level USER_PROFILE.
# Populated from DB on first load, then mutated only within this session.
if "session_profile" not in st.session_state:
    st.session_state.session_profile = get_session_profile()


# =========================================================
# DB HELPERS
# =========================================================
def save_search(area: str, bhk: str, max_rent: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO search_history(area, bhk, max_rent, searched_at) VALUES (%s, %s, %s, %s)",
        (area, bhk, max_rent, int(time.time()))
    )
    conn.commit()
    cur.close()
    conn.close()


def get_recent_searches(limit: int = 10) -> List[Dict]:
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT id, area, bhk, max_rent, searched_at FROM search_history ORDER BY searched_at DESC LIMIT %s",
        (limit,)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def infer_user_profile() -> Dict:
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT area, bhk, max_rent FROM search_history ORDER BY searched_at DESC LIMIT 50")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return {
            "preferred_areas": USER_PROFILE_DEFAULTS.get("preferred_areas", []),
            "most_searched_bhk": None,
            "typical_budget": USER_PROFILE_DEFAULTS.get("max_rent", 25000),
            "search_count": 0,
        }

    area_counter: Counter = Counter()
    bhk_counter: Counter = Counter()
    rents = []

    for r in rows:
        if r.get("area"):
            area_counter[r["area"]] += 1
        if r.get("bhk"):
            bhk_counter[r["bhk"]] += 1
        if r.get("max_rent"):
            rents.append(int(r["max_rent"]))

    return {
        "preferred_areas": [a for a, _ in area_counter.most_common(3)],
        "most_searched_bhk": bhk_counter.most_common(1)[0][0] if bhk_counter else None,
        "typical_budget": int(sum(rents) / len(rents)) if rents else USER_PROFILE_DEFAULTS.get("max_rent", 25000),
        "search_count": len(rows),
    }


def save_profile_settings(profile_data: Dict):
    conn = get_db()
    cur = conn.cursor()

    items = {
        "max_rent": str(profile_data.get("max_rent", 25000)),
        "preferred_areas": json.dumps(profile_data.get("preferred_areas", [])),
        "max_commute": str(profile_data.get("max_commute", 35)),
        "budget_flex": str(profile_data.get("budget_flex", 3000)),
        "needs_parking": json.dumps(bool(profile_data.get("needs_parking", False))),
        "needs_furnished": json.dumps(bool(profile_data.get("needs_furnished", False))),
        "lifestyle": str(profile_data.get("lifestyle", "working_professional")),
    }

    for k, v in items.items():
        cur.execute("""
            INSERT INTO user_profile_settings(profile_key, profile_value, updated_at)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                profile_value = VALUES(profile_value),
                updated_at = VALUES(updated_at)
        """, (k, v, int(time.time())))

    conn.commit()
    cur.close()
    conn.close()


def load_profile_settings() -> Dict:
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT profile_key, profile_value FROM user_profile_settings")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = {}
    for row in rows:
        k = row["profile_key"]
        v = row["profile_value"]

        if k == "preferred_areas":
            try:
                result[k] = json.loads(v)
            except Exception:
                result[k] = []
        elif k in ("needs_parking", "needs_furnished"):
            try:
                result[k] = json.loads(v)
            except Exception:
                result[k] = False
        elif k in ("max_rent", "max_commute", "budget_flex"):
            try:
                result[k] = int(v)
            except Exception:
                pass
        else:
            result[k] = v

    return result


def build_effective_profile(saved: Dict, inferred: Dict) -> Dict:
    """Merge saved DB settings + inferred search history into one profile dict.
    Does NOT mutate any module-level state.
    """
    return get_session_profile({
        "preferred_areas": saved.get(
            "preferred_areas",
            inferred.get("preferred_areas", USER_PROFILE_DEFAULTS.get("preferred_areas", []))
        ),
        "max_rent": saved.get(
            "max_rent",
            inferred.get("typical_budget", USER_PROFILE_DEFAULTS.get("max_rent", 25000))
        ),
        "max_commute": saved.get("max_commute", USER_PROFILE_DEFAULTS.get("max_commute", 35)),
        "budget_flex": saved.get("budget_flex", USER_PROFILE_DEFAULTS.get("budget_flex", 3000)),
        "needs_parking": saved.get("needs_parking", USER_PROFILE_DEFAULTS.get("needs_parking", False)),
        "needs_furnished": saved.get("needs_furnished", USER_PROFILE_DEFAULTS.get("needs_furnished", False)),
        "lifestyle": saved.get("lifestyle", USER_PROFILE_DEFAULTS.get("lifestyle", "working_professional")),
        # inferred extras (display only, not used in scoring)
        "_most_searched_bhk": inferred.get("most_searched_bhk"),
        "_search_count": inferred.get("search_count", 0),
    })


# =========================================================
# WHATSAPP
# =========================================================
def send_whatsapp_with_status(props: List[Dict], area: str) -> Dict:
    if not props:
        return {"ok": False, "message": "No listings found, so nothing was sent.", "sent_count": 0}

    if not all([TWILIO_SID, TWILIO_TOKEN, TARGET_WHATSAPP]):
        return {"ok": False, "message": "Twilio configuration is missing in .env.", "sent_count": 0}

    body = build_whatsapp_body(props, area)

    if not body:
        return {
            "ok": True,
            "message": "No new WhatsApp alert was sent because all results were already sent earlier.",
            "sent_count": 0
        }

    try:
        client = Client(TWILIO_SID, TWILIO_TOKEN)
        client.messages.create(from_=TWILIO_SENDER, body=body, to=TARGET_WHATSAPP)

        fresh = [p for p in props if not p.get("already_sent")]
        count = 0
        for p in fresh:
            if count >= MAX_WHATSAPP_RESULTS:
                break
            mark_sent(p["link"], p["title"])
            count += 1

        return {
            "ok": True,
            "message": f"WhatsApp alert sent successfully to {TARGET_WHATSAPP}.",
            "sent_count": count
        }

    except Exception as e:
        return {"ok": False, "message": f"Failed to send WhatsApp: {e}", "sent_count": 0}


# =========================================================
# UI HELPERS
# =========================================================
def get_supermarkets(item: Dict) -> List[Dict]:
    return [p for p in (item.get("raw_places", []) or []) if p.get("type") == "supermarket"]


def get_other_facilities(item: Dict) -> List[Dict]:
    return [p for p in (item.get("raw_places", []) or []) if p.get("type") != "supermarket"]


def render_profile(profile_data: Dict):
    st.markdown('<div class="profile-box">', unsafe_allow_html=True)
    st.subheader("User Search Profile")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Frequent areas**")
        areas = profile_data.get("preferred_areas", [])
        st.write(", ".join(areas) if areas else "Not enough data yet")

    with c2:
        st.markdown("**Most searched BHK**")
        st.write(profile_data.get("_most_searched_bhk") or "Not enough data yet")

    with c3:
        st.markdown("**Typical budget**")
        st.write(f"₹{profile_data.get('max_rent', 0):,}")

    st.caption(f"Built from last {profile_data.get('_search_count', 0)} searches.")
    st.markdown('</div>', unsafe_allow_html=True)


def render_listing_card(idx: int, item: Dict):
    supermarkets = get_supermarkets(item)
    other_facilities = get_other_facilities(item)

    st.markdown('<div class="card">', unsafe_allow_html=True)

    st.markdown(f'<div class="rent-title">{idx}. {item.get("title", "Untitled Listing")}</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="small-muted">{item.get("source", "Unknown Source")} • {item.get("listing_type", "UNKNOWN")}</div>',
        unsafe_allow_html=True
    )

    st.markdown(" ".join([
        f'<span class="score-pill">Fit {item.get("fit_score", "N/A")}/100</span>',
        f'<span class="score-pill">Reliability {item.get("reliability_score", "N/A")}/100</span>',
        f'<span class="score-pill">Lifestyle {item.get("lifestyle_score", "N/A")}/100</span>',
    ]), unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Rent", f"₹{item.get('rent_value', 0):,}" if item.get("rent_value") else "N/A")
    c2.metric("Monthly Total", f"₹{item.get('monthly_total_cost', 0):,}" if item.get("monthly_total_cost") else "N/A")
    c3.metric("Commute", f"{item.get('commute_mins')} min" if item.get("commute_mins") is not None else "N/A")
    c4.metric("Deposit", f"₹{item.get('deposit_estimate', 0):,}" if item.get("deposit_estimate") else "N/A")

    st.markdown(f"<span class='label-strong'>Address:</span> {item.get('address', 'N/A')}", unsafe_allow_html=True)
    st.markdown(
        f"<span class='label-strong'>Why recommended:</span> {item.get('recommendation_reason', 'N/A')}",
        unsafe_allow_html=True
    )
    st.markdown(
        f"<span class='label-strong'>Description / vibe:</span> {item.get('vibe_summary', 'N/A')}",
        unsafe_allow_html=True
    )

    if item.get("amenities_text"):
        st.markdown("<div class='subsection-title'>Nearby facilities found</div>", unsafe_allow_html=True)
        st.write(item["amenities_text"])

    if supermarkets:
        st.markdown("<div class='subsection-title'>Nearby supermarkets</div>", unsafe_allow_html=True)
        chips = ""
        for s in supermarkets:
            rating = f"⭐ {s['rating']}" if s.get("rating") else "No rating"
            chips += f"<span class='info-chip'>{s.get('name', 'Unknown')} • {rating}</span>"
        st.markdown(chips, unsafe_allow_html=True)

    if other_facilities:
        with st.expander("Show more nearby facilities"):
            grouped: Dict[str, list] = {}
            for f in other_facilities:
                grouped.setdefault(f.get("type", "other"), []).append(f)

            for facility_type, values in grouped.items():
                st.markdown(f"**{facility_type.replace('_', ' ').title()}**")
                row_html = ""
                for v in values:
                    rating = f"⭐ {v['rating']}" if v.get("rating") else "No rating"
                    row_html += f"<span class='good-pill'>{v.get('name', 'Unknown')} • {rating}</span>"
                st.markdown(row_html, unsafe_allow_html=True)

    st.markdown(f"**Listing link:** [Open property]({item.get('link', '#')})")
    st.markdown('</div>', unsafe_allow_html=True)


def apply_filters(listings: List[Dict], sidebar_filters: Dict) -> List[Dict]:
    filtered = []

    for item in listings:
        rent_value = item.get("rent_value") or 0
        commute = item.get("commute_mins")
        fit = item.get("fit_score") or 0
        reliability = item.get("reliability_score") or 0
        source = item.get("source", "")
        supermarket_present = len(get_supermarkets(item)) > 0

        if rent_value > sidebar_filters["max_rent_filter"]:
            continue
        if commute is not None and commute > sidebar_filters["max_commute_filter"]:
            continue
        if fit < sidebar_filters["min_fit_filter"]:
            continue
        if reliability < sidebar_filters["min_reliability_filter"]:
            continue
        if sidebar_filters["only_supermarket"] and not supermarket_present:
            continue
        if sidebar_filters["sources"] and source not in sidebar_filters["sources"]:
            continue

        filtered.append(item)

    return filtered


def build_comparison_df(listings: List[Dict]) -> pd.DataFrame:
    rows = []
    for item in listings:
        supermarkets = get_supermarkets(item)
        rows.append({
            "Title": item.get("title"),
            "Source": item.get("source"),
            "Rent": item.get("rent_value"),
            "Monthly Total": item.get("monthly_total_cost"),
            "Commute (min)": item.get("commute_mins"),
            "Fit Score": item.get("fit_score"),
            "Reliability": item.get("reliability_score"),
            "Lifestyle": item.get("lifestyle_score"),
            "Address": item.get("address"),
            "Supermarkets Nearby": len(supermarkets),
            "Vibe": item.get("vibe_summary"),
            "Link": item.get("link"),
        })
    return pd.DataFrame(rows)


# =========================================================
# INIT
# =========================================================
try:
    validate_env()
    init_db()
except Exception as e:
    st.error(f"Startup failed: {e}")
    st.stop()

# =========================================================
# LOAD PROFILE (once per session)
# =========================================================
inferred_profile = infer_user_profile()
saved_profile = load_profile_settings()

# FIX: effective_profile is a fresh dict — never the module-level USER_PROFILE.
effective_profile = build_effective_profile(saved_profile, inferred_profile)

# Sync session_state on first load (or after a profile save triggers a rerun)
if not st.session_state.get("_profile_loaded"):
    st.session_state.session_profile = effective_profile
    st.session_state["_profile_loaded"] = True

# =========================================================
# HERO HEADER
# =========================================================
st.markdown("""
<div class="hero">
    <div class="hero-title">🏠 Rental Intelligence Dashboard</div>
    <div class="hero-subtitle">
        Search Bengaluru rentals, score them, inspect nearby amenities, compare listings, and track WhatsApp alerts in one place.
    </div>
</div>
""", unsafe_allow_html=True)

# =========================================================
# PROFILE SUMMARY CARD
# =========================================================
with st.container():
    st.markdown('<div class="section-shell">', unsafe_allow_html=True)
    render_profile(st.session_state.session_profile)
    st.markdown('</div>', unsafe_allow_html=True)

# =========================================================
# SIDEBAR
# =========================================================
st.sidebar.header("Filters & Profile")

with st.sidebar.expander("Profile Edit Controls", expanded=True):
    profile_areas_input = st.text_input(
        "Preferred Areas (comma separated)",
        value=", ".join(st.session_state.session_profile.get("preferred_areas", []))
    )
    profile_max_rent = st.number_input(
        "Default Budget",
        min_value=5000,
        max_value=200000,
        value=int(st.session_state.session_profile.get("max_rent", 25000)),
        step=1000
    )
    profile_max_commute = st.slider(
        "Preferred Max Commute",
        min_value=5, max_value=120,
        value=int(st.session_state.session_profile.get("max_commute", 35)),
        step=5
    )
    profile_budget_flex = st.slider(
        "Budget Flex",
        min_value=0, max_value=30000,
        value=int(st.session_state.session_profile.get("budget_flex", 3000)),
        step=500
    )
    profile_needs_parking = st.checkbox(
        "Needs Parking",
        value=bool(st.session_state.session_profile.get("needs_parking", False))
    )
    profile_needs_furnished = st.checkbox(
        "Needs Furnished",
        value=bool(st.session_state.session_profile.get("needs_furnished", False))
    )
    _lifestyle_options = ["working_professional", "student", "family"]
    _current_lifestyle = st.session_state.session_profile.get("lifestyle", "working_professional")
    if _current_lifestyle not in _lifestyle_options:
        _current_lifestyle = "working_professional"
    profile_lifestyle = st.selectbox(
        "Lifestyle",
        _lifestyle_options,
        index=_lifestyle_options.index(_current_lifestyle)
    )

    if st.button("Save Profile Settings"):
        new_profile_payload = {
            "preferred_areas": [x.strip() for x in profile_areas_input.split(",") if x.strip()],
            "max_rent": int(profile_max_rent),
            "max_commute": int(profile_max_commute),
            "budget_flex": int(profile_budget_flex),
            "needs_parking": bool(profile_needs_parking),
            "needs_furnished": bool(profile_needs_furnished),
            "lifestyle": profile_lifestyle,
        }
        save_profile_settings(new_profile_payload)
        # FIX: update session_state profile — does NOT touch USER_PROFILE
        st.session_state.session_profile = get_session_profile(new_profile_payload)
        st.session_state["_profile_loaded"] = False  # force re-merge on next run
        st.sidebar.success("Profile settings saved.")

with st.sidebar.expander("Result Filters", expanded=True):
    max_rent_filter = st.slider("Max Rent Filter", 5000, 100000, 40000, 1000)
    max_commute_filter = st.slider("Max Commute Filter", 5, 120, 60, 5)
    min_fit_filter = st.slider("Minimum Fit Score", 0, 100, 0, 5)
    min_reliability_filter = st.slider("Minimum Reliability", 0, 100, 0, 5)
    only_supermarket = st.checkbox("Only show listings with nearby supermarkets", value=False)
    sources = st.multiselect("Source", ["NoBroker", "MagicBricks"], default=[])

sidebar_filters = {
    "max_rent_filter": max_rent_filter,
    "max_commute_filter": max_commute_filter,
    "min_fit_filter": min_fit_filter,
    "min_reliability_filter": min_reliability_filter,
    "only_supermarket": only_supermarket,
    "sources": sources,
}

# =========================================================
# SEARCH FORM
# =========================================================
st.markdown('<div class="section-shell">', unsafe_allow_html=True)
st.subheader("Search Rentals")

with st.form("search_form"):
    c1, c2, c3 = st.columns([2, 1, 1])

    _preferred = st.session_state.session_profile.get("preferred_areas", [])
    default_area = _preferred[0] if _preferred else ""

    with c1:
        area_in = st.text_input("Area", value=default_area, placeholder="HSR Layout / Koramangala / BTM Layout")

    with c2:
        _bhk_default = st.session_state.session_profile.get("_most_searched_bhk", "2")
        if _bhk_default not in ["1", "2", "3"]:
            _bhk_default = "2"
        bhk_in = st.selectbox("BHK", ["1", "2", "3"], index=["1", "2", "3"].index(_bhk_default))

    with c3:
        rent_in = st.text_input("Max Rent", value=str(st.session_state.session_profile.get("max_rent", 25000)))

    submitted = st.form_submit_button("Search Rentals")

st.markdown('</div>', unsafe_allow_html=True)

# =========================================================
# SAVED SEARCHES
# =========================================================
st.markdown('<div class="section-shell">', unsafe_allow_html=True)
st.subheader("Saved Searches")

recent_searches = get_recent_searches(8)

if recent_searches:
    search_options = []
    search_map = {}

    for row in recent_searches:
        label = f"{row['area']} | {row['bhk']} BHK | ₹{row['max_rent']}"
        search_options.append(label)
        search_map[label] = row

    chosen = st.selectbox("Load a previous search", ["Select a saved search"] + search_options)

    if chosen != "Select a saved search":
        picked = search_map[chosen]
        st.info(
            f"Saved search selected: {picked['area']} • {picked['bhk']} BHK • ₹{picked['max_rent']}. "
            f"Copy these values into the form above and run search."
        )

    with st.expander("View recent saved searches"):
        for row in recent_searches:
            st.write(f"- {row['area']} | {row['bhk']} BHK | ₹{row['max_rent']}")
else:
    st.caption("No saved searches yet.")

st.markdown('</div>', unsafe_allow_html=True)

# =========================================================
# RUN SEARCH
# =========================================================
if submitted:
    if not area_in.strip() or not bhk_in.strip() or not rent_in.strip():
        st.error("Area, BHK and Max Rent are required.")
    else:
        try:
            rent_int = int(str(rent_in).replace(",", "").strip())

            save_search(area_in.strip(), bhk_in.strip(), rent_int)

            # FIX: Build a fresh per-request profile from current sidebar values.
            # This is a plain dict — it is passed into fetch_blr_flats() directly
            # and never stored back into USER_PROFILE.
            request_profile = get_session_profile({
                "preferred_areas": [x.strip() for x in profile_areas_input.split(",") if x.strip()],
                "max_rent": rent_int,
                "max_commute": int(profile_max_commute),
                "budget_flex": int(profile_budget_flex),
                "needs_parking": bool(profile_needs_parking),
                "needs_furnished": bool(profile_needs_furnished),
                "lifestyle": profile_lifestyle,
            })

            # Keep session_state in sync for display
            st.session_state.session_profile = request_profile

            with st.spinner("Searching, parsing, scoring, and checking nearby facilities..."):
                listings = fetch_blr_flats(
                    area_in.strip(),
                    bhk_in.strip(),
                    str(rent_int),
                    profile=request_profile,   # FIX: pass session profile, not global
                )

            status = send_whatsapp_with_status(listings, area_in.strip())

            st.session_state.listings = listings
            st.session_state.last_status = status
            st.session_state.last_search_input = {
                "area": area_in.strip(),
                "bhk": bhk_in.strip(),
                "rent": rent_int,
            }

        except ValueError:
            st.error("Max Rent must be a valid number.")
        except Exception as e:
            st.error(f"Something went wrong: {e}")

# =========================================================
# DISPLAY RESULTS
# =========================================================
if st.session_state.listings:
    listings = st.session_state.listings
    filtered_listings = apply_filters(listings, sidebar_filters)

    st.markdown('<div class="section-shell">', unsafe_allow_html=True)
    st.subheader("Search Summary")

    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Results found", len(listings))
    s2.metric("After filters", len(filtered_listings))
    s3.metric("Top fit score", max([x.get("fit_score", 0) for x in filtered_listings], default=0))
    s4.metric("Budget", f"₹{st.session_state.last_search_input.get('rent', 0):,}")

    if st.session_state.last_status:
        if st.session_state.last_status["ok"]:
            st.success(
                f"{st.session_state.last_status['message']} "
                f"Sent count: {st.session_state.last_status['sent_count']}"
            )
        else:
            st.error(st.session_state.last_status["message"])

    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="section-shell">', unsafe_allow_html=True)
    st.subheader("Rental Comparison Table")

    compare_df = build_comparison_df(filtered_listings[:10])

    if not compare_df.empty:
        st.dataframe(compare_df, use_container_width=True, hide_index=True)

        csv_data = compare_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download Comparison CSV",
            data=csv_data,
            file_name="rental_comparison.csv",
            mime="text/csv"
        )
    else:
        st.warning("No listings match the current filters.")

    st.markdown('</div>', unsafe_allow_html=True)

    st.subheader("Top Rentals")
    if not filtered_listings:
        st.warning("No valid rentals match the current filters.")
    else:
        for i, item in enumerate(filtered_listings[:10], start=1):
            render_listing_card(i, item)