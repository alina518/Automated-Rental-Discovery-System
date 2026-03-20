import os
import re
import json
import time
import hashlib
import logging
from typing import Optional, Dict, List, Tuple
from urllib.parse import urlparse
from datetime import datetime

import requests
import mysql.connector
from mysql.connector import pooling
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from twilio.rest import Client
from groq import Groq

load_dotenv()

# =========================================================
# CONFIG
# =========================================================
SEARCH_API_KEY = os.getenv("SEARCH_API_KEY")
OFFICE_ADDR = os.getenv("OFFICE_ADDR")
OFFICE_LAT = os.getenv("OFFICE_LAT")
OFFICE_LNG = os.getenv("OFFICE_LNG")
TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
TARGET_WHATSAPP = os.getenv("YOUR_WHATSAPP")
TWILIO_SENDER = "whatsapp:+14155238886"

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "flat_finder")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))

MAX_RESULTS_FROM_SEARCH = 10
# FIX: Increased to allow more results before WhatsApp truncation kicks in
MAX_WHATSAPP_RESULTS = 5
COMMUTE_CACHE_HOURS = 24
HTTP_TIMEOUT = 20
PLACES_RADIUS_METERS = 1000
# FIX: Twilio WhatsApp body hard limit is 1600 chars; leave headroom
WHATSAPP_BODY_CHAR_LIMIT = 1500

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

ALLOWED_DOMAINS = {
    "www.nobroker.in",
    "nobroker.in",
    "www.magicbricks.com",
    "magicbricks.com"
}

BAD_TITLE_PATTERNS = [
    "property for sale",
    "plot",
    "villa plots",
    "office space",
    "commercial",
    "job",
    "news",
    "youtube",
]

SUSPICIOUS_WORDS = [
    "token advance",
    "call now",
    "investment",
    "best deal",
    "premium project",
    "pre-launch",
    "sale",
    "buy now",
]

NEIGHBORHOOD_TYPES = [
    "gym",
    "cafe",
    "hospital",
    "subway_station",
    "supermarket",
    "bar",
    "bus_station",
    "pharmacy",
    "school",
    "restaurant",
]

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-IN,en;q=0.9",
}

# FIX: USER_PROFILE is no longer mutated at module level; a deep copy is made
# per-request in get_session_profile() so concurrent Streamlit sessions are safe.
USER_PROFILE_DEFAULTS: Dict = {
    "max_rent": 25000,
    "preferred_areas": ["HSR Layout", "Koramangala", "BTM Layout"],
    "max_commute": 35,
    "lifestyle": "working_professional",
    "needs_parking": False,
    "needs_furnished": False,
    "budget_flex": 3000,
}

# Module-level mutable dict retained for CLI usage only.
# Streamlit should call get_session_profile() / update_session_profile() instead.
USER_PROFILE = dict(USER_PROFILE_DEFAULTS)

AREA_ALIASES = {
    "chikkabettahalli": [
        "Chikkabettahalli",
        "Chikka Bettahalli",
        "Vidyaranyapura",
        "Yelahanka",
        "Sahakara Nagar",
    ],
    "hsr layout": ["HSR Layout"],
    "koramangala": ["Koramangala"],
    "btm layout": ["BTM Layout"],
    "electronic city": ["Electronic City", "Electronics City"],
    "marathahalli": ["Marathahalli", "Kalamandir", "Munnekollal"],
}

logging.basicConfig(
    filename="rental_intelligence.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# =========================================================
# SESSION PROFILE HELPERS  (Streamlit-safe)
# =========================================================

def get_session_profile(overrides: Optional[Dict] = None) -> Dict:
    """Return a fresh profile dict, optionally merged with caller-supplied overrides.
    Never touches the module-level USER_PROFILE, so concurrent sessions are safe.
    """
    import copy
    profile = copy.deepcopy(USER_PROFILE_DEFAULTS)
    if overrides:
        profile.update(overrides)
    return profile


# =========================================================
# ENV VALIDATION
# =========================================================
def validate_env():
    required = {
        "SEARCH_API_KEY": SEARCH_API_KEY,
        "GOOGLE_API_KEY": GOOGLE_API_KEY,
    }

    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing required env vars: {', '.join(missing)}")

    has_office_address = bool(OFFICE_ADDR)
    has_office_coords = bool(OFFICE_LAT and OFFICE_LNG)

    if not has_office_address and not has_office_coords:
        raise ValueError("Provide either OFFICE_ADDR or both OFFICE_LAT and OFFICE_LNG")


# =========================================================
# DB — connection pool
# =========================================================
_pool: Optional[pooling.MySQLConnectionPool] = None


def _get_pool() -> pooling.MySQLConnectionPool:
    """Return the shared connection pool, creating it on first call."""
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name="flat_finder_pool",
            pool_size=5,
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            port=MYSQL_PORT,
        )
    return _pool


def get_db():
    """Get a connection from the pool."""
    return _get_pool().get_connection()


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS commute_cache (
            address_hash VARCHAR(64) PRIMARY KEY,
            address TEXT NOT NULL,
            commute_mins INT NULL,
            distance_meters INT NULL,
            updated_at BIGINT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_listings (
            link_hash VARCHAR(64) PRIMARY KEY,
            link TEXT NOT NULL,
            title TEXT,
            sent_at BIGINT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS seen_listings (
            link_hash VARCHAR(64) PRIMARY KEY,
            link TEXT NOT NULL,
            title TEXT,
            first_seen_at BIGINT NOT NULL,
            last_seen_at BIGINT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS geo_cache (
            address_hash VARCHAR(64) PRIMARY KEY,
            address TEXT NOT NULL,
            lat DOUBLE NULL,
            lng DOUBLE NULL,
            updated_at BIGINT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS neighborhood_cache (
            geo_hash VARCHAR(64) PRIMARY KEY,
            lat DOUBLE NOT NULL,
            lng DOUBLE NOT NULL,
            amenities_text TEXT,
            amenity_counts TEXT,
            raw_places TEXT,
            updated_at BIGINT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            link_hash VARCHAR(64) PRIMARY KEY,
            source VARCHAR(100),
            title TEXT,
            link TEXT,
            address TEXT,
            bhk VARCHAR(10),
            listing_type VARCHAR(30),
            rent_value INT,
            commute_mins INT,
            distance_meters INT,
            maintenance_estimate INT,
            deposit_estimate INT,
            travel_cost_estimate INT,
            monthly_total_cost INT,
            reliability_score INT,
            lifestyle_score INT,
            fit_score INT,
            lat DOUBLE,
            lng DOUBLE,
            amenities_text TEXT,
            vibe_summary TEXT,
            neighborhood_red_flags TEXT,
            risk_flags TEXT,
            recommendation_reason TEXT,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS search_history (
            id INT AUTO_INCREMENT PRIMARY KEY,
            area VARCHAR(255),
            bhk VARCHAR(20),
            max_rent INT,
            searched_at BIGINT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_profile_settings (
            profile_key VARCHAR(100) PRIMARY KEY,
            profile_value TEXT,
            updated_at BIGINT NOT NULL
        )
    """)

    conn.commit()
    cur.close()
    conn.close()


# =========================================================
# UTILS
# =========================================================
def now_ts() -> int:
    return int(time.time())


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def hash_text(text: str) -> str:
    return hashlib.sha256(text.strip().lower().encode("utf-8")).hexdigest()


def canonicalize_url(url: str) -> str:
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    return base.rstrip("/")


def allowed_listing_domain(url: str) -> bool:
    try:
        domain = urlparse(url).netloc.lower()
        return domain in ALLOWED_DOMAINS
    except Exception:
        return False


def normalize_bhk(bhk: str) -> str:
    bhk = str(bhk).strip().lower()
    bhk = bhk.replace("bhk", "").strip()
    return bhk


def infer_source(url: str) -> str:
    domain = urlparse(url).netloc.lower()
    if "nobroker" in domain:
        return "NoBroker"
    if "magicbricks" in domain:
        return "MagicBricks"
    return "Unknown"


def safe_json_dumps(value) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return "[]"


def is_near_bengaluru(lat: float, lng: float) -> bool:
    if lat is None or lng is None:
        return False
    return 12.5 <= lat <= 13.3 and 77.2 <= lng <= 77.9


def is_probable_detail_page(url: str) -> bool:
    url_l = url.lower()

    hard_bad_patterns = [
        "/property-for-sale/",
        "/flats-for-sale-",
        "/property-for-sale-in-",
        "/apartments-for-sale-",
        "commercial",
        "office-space",
        "office-space-for-rent",
        "shop-for-rent",
        "warehouse",
    ]

    if any(p in url_l for p in hard_bad_patterns):
        return False

    return True


# =========================================================
# Address validation helpers
# =========================================================
ADDRESS_JUNK_PATTERNS = [
    r"what are",
    r"how to",
    r"types of",
    r"furniture",
    r"options in",
    r"rent in.*mart",
    r"mega mart",
    r"find.*flat",
    r"search.*flat",
    r"apartments? for rent",
    r"flats? for rent",
    r"property for",
    r"bhk flat for rent",
    r"\d+ bhk",
    r"rental properties",
]


def is_seo_garbage(text: str) -> bool:
    tl = text.lower().strip()
    for pat in ADDRESS_JUNK_PATTERNS:
        if re.search(pat, tl):
            return True
    if "?" in tl:
        return True
    if len(tl) > 120 and tl.count(",") < 2:
        return True
    return False


def looks_like_real_address(address: str, area_hint: str = "") -> bool:
    if not address:
        return False

    a = address.lower().strip()

    if is_seo_garbage(address):
        return False

    hard_bad_phrases = [
        "flats for rent",
        "apartments for rent",
        "property for rent",
        "property for sale",
        "office space",
        "commercial property",
    ]
    if any(x in a for x in hard_bad_phrases):
        return False

    weak_only = [
        "bengaluru, karnataka, india",
        "bangalore, karnataka, india",
    ]
    if a in weak_only:
        return False

    if "bengaluru" in a or "bangalore" in a:
        return True

    if area_hint and area_hint.lower() in a and len(address.strip()) >= 10:
        return True

    return len(address.strip()) >= 10


def build_area_fallback_address(area: str) -> str:
    return f"{area}, Bengaluru, Karnataka, India"


def is_fallback_address(address: str) -> bool:
    if not address:
        return True
    parts = [p.strip() for p in address.split(",")]
    if len(parts) == 4 and parts[-1].lower() == "india" and parts[-2].lower() == "karnataka":
        return True
    return False


def has_precise_location_signal(address: str, lat: Optional[float], lng: Optional[float]) -> bool:
    if lat is not None and lng is not None and is_near_bengaluru(lat, lng):
        return True
    if address and not is_fallback_address(address) and looks_like_real_address(address):
        return True
    return False


def get_area_terms(area: str) -> List[str]:
    stripped = normalize_space(area)
    aliases = AREA_ALIASES.get(stripped.lower(), [])
    all_terms = [stripped] + aliases
    unique = []
    seen = set()
    for term in all_terms:
        key = term.lower()
        if key not in seen:
            seen.add(key)
            unique.append(term)
    return unique


# =========================================================
# RENT / CLASSIFICATION
# =========================================================
def parse_price_to_int_strict(text: str) -> Optional[int]:
    if not text:
        return None

    text = text.lower().replace(",", "").strip()

    k_match = re.search(r'(\d+(?:\.\d+)?)\s*k\b', text)
    if k_match:
        val = int(float(k_match.group(1)) * 1000)
        if 3000 <= val <= 300000:
            return val

    lakh_match = re.search(r'(\d+(?:\.\d+)?)\s*lakh', text)
    if lakh_match and any(x in text for x in ["rent", "month", "monthly"]):
        return int(float(lakh_match.group(1)) * 100000)

    cr_match = re.search(r'(\d+(?:\.\d+)?)\s*crore', text)
    if cr_match and any(x in text for x in ["rent", "month", "monthly"]):
        return int(float(cr_match.group(1)) * 10000000)

    patterns = [
        r'(?:rent|monthly rent|expected rent|per month|month rent)\s*[:\-]?\s*₹?\s*(\d{4,6})',
        r'₹\s*(\d{4,6})',
        r'rs\.?\s*(\d{4,6})',
        r'(\d{4,6})\s*/?\s*(?:month|monthly)',
    ]

    reject_words = ["sqft", "sq ft", "builtup", "carpet", "super built", "plot"]

    for pat in patterns:
        for match in re.finditer(pat, text):
            val = int(match.group(1))
            start = max(0, match.start() - 25)
            end = min(len(text), match.end() + 25)
            window = text[start:end]
            if 3000 <= val <= 300000 and not any(word in window for word in reject_words):
                return val

    return None


def classify_listing_text(title: str, snippet: str, page_text: str) -> str:
    text = f"{title} {snippet} {page_text}".lower()

    if any(x in text for x in ["office space", "commercial property", "shop for rent", "warehouse",
                                 "commercial space", "coworking", "co-working"]):
        return "COMMERCIAL"

    if any(x in text for x in ["hostel", "co-living", "flatmate wanted", "pg for rent", "paying guest"]):
        return "PG/HOSTEL"

    sale_signals = ["for sale", "property for sale", "buy property", "sale price"]
    if any(x in text for x in sale_signals) and "rent" not in text:
        return "SALE"

    rent_signals = ["rent", "for rent", "monthly rent", "per month", "/month", "lease"]
    if any(x in text for x in rent_signals):
        return "RENT"

    return "UNKNOWN"


def looks_like_candidate_listing(title: str, snippet: str) -> bool:
    text = f"{title} {snippet}".lower()

    if any(bad in text for bad in BAD_TITLE_PATTERNS):
        return False

    if "sale" in text and "rent" not in text:
        return False

    return True


# =========================================================
# SEARCH
# =========================================================
def build_search_queries(area: str, bhk: str) -> List[str]:
    terms = get_area_terms(area)
    bhk_clean = normalize_bhk(bhk)

    queries = []
    for term in terms:
        queries.append(
            f'site:nobroker.in OR site:magicbricks.com "{bhk_clean} BHK" "for rent" "{term}" Bengaluru'
        )
        queries.append(
            f'site:nobroker.in OR site:magicbricks.com "flat for rent" "{term}" Bengaluru'
        )

    final = []
    seen = set()
    for q in queries:
        if q not in seen:
            seen.add(q)
            final.append(q)

    return final[:4]


def fetch_search_results(area: str, bhk: str, budget: str) -> List[Dict]:
    if not SEARCH_API_KEY:
        raise ValueError("SEARCH_API_KEY missing in .env")

    url = "https://google.serper.dev/search"
    headers = {
        "X-API-KEY": SEARCH_API_KEY,
        "Content-Type": "application/json",
    }

    queries = build_search_queries(area, bhk)
    cleaned = []
    seen_links = set()

    print(f"\n[SEARCH] Running {len(queries)} search queries for area '{area}'")

    for idx, query in enumerate(queries, start=1):
        try:
            payload = {"q": query, "num": 10}
            resp = requests.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT)

            if resp.status_code != 200:
                print(f"[SEARCH {idx}] status={resp.status_code}")
                print(f"[SEARCH {idx}] response={resp.text[:1000]}")
                continue

            data = resp.json()
            organic = data.get("organic", [])
            print(f"[SEARCH {idx}] Raw results: {len(organic)}")

            for item in organic:
                title = normalize_space(item.get("title", ""))
                snippet = normalize_space(item.get("snippet", ""))
                link = canonicalize_url(item.get("link", ""))

                if not link:
                    continue
                if not allowed_listing_domain(link):
                    continue
                if link in seen_links:
                    continue
                if not is_probable_detail_page(link):
                    continue
                if not looks_like_candidate_listing(title, snippet):
                    continue

                seen_links.add(link)
                cleaned.append({
                    "title": title,
                    "snippet": snippet,
                    "link": link,
                    "query_used": query,
                })

        except Exception as e:
            print(f"[SEARCH {idx}] failed: {e}")

    print(f"[SEARCH] Final cleaned candidates: {len(cleaned)}")
    return cleaned


# =========================================================
# PAGE FETCH + PARSE
# =========================================================
def fetch_page(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            return None

        content_type = resp.headers.get("Content-Type", "")
        if "text/html" not in content_type:
            return None

        return resp.text
    except requests.RequestException:
        return None


def extract_json_ld(soup: BeautifulSoup) -> List[dict]:
    results = []
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    for script in scripts:
        text = script.get_text(strip=True)
        if not text:
            continue
        try:
            data = json.loads(text)
            if isinstance(data, list):
                results.extend(data)
            else:
                results.append(data)
        except Exception:
            continue
    return results


def deep_find_strings(obj, keys: Tuple[str, ...]) -> List[str]:
    found = []

    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                if k.lower() in keys and isinstance(v, (str, int, float)):
                    found.append(str(v))
                walk(v)
        elif isinstance(x, list):
            for item in x:
                walk(item)

    walk(obj)
    return found


def deep_find_lat_lng(obj) -> List[Tuple[float, float]]:
    found = []

    def walk(x):
        if isinstance(x, dict):
            lat_keys = ["latitude", "lat"]
            lng_keys = ["longitude", "lng", "lon"]

            lat_val = None
            lng_val = None

            for k, v in x.items():
                kl = str(k).lower()
                if kl in lat_keys:
                    try:
                        lat_val = float(v)
                    except Exception:
                        pass
                if kl in lng_keys:
                    try:
                        lng_val = float(v)
                    except Exception:
                        pass

            if lat_val is not None and lng_val is not None:
                found.append((lat_val, lng_val))

            for _, v in x.items():
                walk(v)

        elif isinstance(x, list):
            for item in x:
                walk(item)

    walk(obj)
    return found


def extract_best_lat_lng(jsonlds: List[dict], html: str) -> Tuple[Optional[float], Optional[float]]:
    for obj in jsonlds:
        pairs = deep_find_lat_lng(obj)
        for lat, lng in pairs:
            if is_near_bengaluru(lat, lng):
                return lat, lng

    patterns = [
        r'"latitude"\s*:\s*"?([0-9]+\.[0-9]+)"?.{0,80}"longitude"\s*:\s*"?([0-9]+\.[0-9]+)"?',
        r'"lat"\s*:\s*([0-9]+\.[0-9]+).{0,80}"lng"\s*:\s*([0-9]+\.[0-9]+)',
        r'"lng"\s*:\s*([0-9]+\.[0-9]+).{0,80}"lat"\s*:\s*([0-9]+\.[0-9]+)',
    ]

    text = html[:150000]

    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            try:
                if "lng" in pat and pat.index("lng") < pat.index("lat"):
                    lng = float(m.group(1))
                    lat = float(m.group(2))
                else:
                    lat = float(m.group(1))
                    lng = float(m.group(2))
                if is_near_bengaluru(lat, lng):
                    return lat, lng
            except Exception:
                pass

    return None, None


def extract_best_address(jsonlds: List[dict], soup: BeautifulSoup, fallback_area: str) -> Optional[str]:
    address_candidates = []

    for obj in jsonlds:
        if isinstance(obj, dict):
            addr = obj.get("address")
            if isinstance(addr, dict):
                parts = [
                    addr.get("streetAddress"),
                    addr.get("addressLocality"),
                    addr.get("addressRegion"),
                    addr.get("postalCode"),
                ]
                parts = [normalize_space(str(p)) for p in parts if p]
                if parts:
                    candidate = ", ".join(parts)
                    if not is_seo_garbage(candidate):
                        address_candidates.append(candidate)

        name_vals = deep_find_strings(obj, ("streetaddress", "addresslocality"))
        for v in name_vals:
            v = normalize_space(v)
            if v and len(v) < 150 and not is_seo_garbage(v):
                address_candidates.append(v)

    for tag in soup.find_all("meta"):
        content = normalize_space(tag.get("content", ""))
        if not content or len(content) > 200:
            continue
        if is_seo_garbage(content):
            continue
        if ("bangalore" in content.lower() or "bengaluru" in content.lower() or
                fallback_area.lower() in content.lower()):
            address_candidates.append(content)

    cleaned = []
    seen = set()
    for candidate in address_candidates:
        c = normalize_space(candidate)
        key = c.lower()
        if key in seen:
            continue
        seen.add(key)
        if len(c) < 8:
            continue
        if is_seo_garbage(c):
            continue
        if ("bangalore" in c.lower() or "bengaluru" in c.lower()
                or fallback_area.lower() in c.lower()):
            cleaned.append(c)

    cleaned.sort(key=lambda x: (x.count(","), len(x)), reverse=True)

    for c in cleaned:
        if looks_like_real_address(c, fallback_area):
            return c

    return None


def extract_best_price(title: str, snippet: str, html_text: str, jsonlds: List[dict]) -> Optional[int]:
    for obj in jsonlds:
        vals = deep_find_strings(obj, ("price", "rent"))
        for v in vals:
            p = parse_price_to_int_strict(v)
            if p:
                return p

    candidates = [title, snippet, html_text[:50000]]

    for text in candidates:
        p = parse_price_to_int_strict(text)
        if p:
            return p

    return None


def extract_title_from_page(soup: BeautifulSoup, fallback: str) -> str:
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return normalize_space(og["content"])

    if soup.title and soup.title.text:
        return normalize_space(soup.title.text)

    h1 = soup.find("h1")
    if h1:
        return normalize_space(h1.get_text(" ", strip=True))

    return fallback


def parse_listing_page(url: str, fallback_title: str, fallback_snippet: str, area: str, bhk: str) -> Optional[Dict]:
    html = fetch_page(url)

    if not html:
        fallback_rent = parse_price_to_int_strict(fallback_title + " " + fallback_snippet)
        return {
            "title": fallback_title,
            "address": build_area_fallback_address(area),
            "is_fallback_address": True,
            "location_precision": "low",
            "rent_value": fallback_rent,
            "link": url,
            "source": infer_source(url),
            "bhk": bhk,
            "page_text": (fallback_title + " " + fallback_snippet).lower(),
            "listing_type": classify_listing_text(fallback_title, fallback_snippet, fallback_snippet),
            "lat": None,
            "lng": None,
        }

    soup = BeautifulSoup(html, "lxml")
    jsonlds = extract_json_ld(soup)

    title = extract_title_from_page(soup, fallback_title)
    address = extract_best_address(jsonlds, soup, area)
    lat, lng = extract_best_lat_lng(jsonlds, html)
    rent_value = extract_best_price(title, fallback_snippet, html, jsonlds)

    page_text_small = normalize_space(
        (title + " " + fallback_snippet + " " + soup.get_text(" ", strip=True)[:5000]).lower()
    )

    listing_type = classify_listing_text(title, fallback_snippet, page_text_small)

    final_address = address
    if not final_address or not looks_like_real_address(final_address, area):
        final_address = build_area_fallback_address(area)

    if lat is not None and lng is not None:
        location_precision = "high"
    elif final_address and not is_fallback_address(final_address):
        location_precision = "medium"
    else:
        location_precision = "low"

    return {
        "title": title,
        "address": final_address,
        "is_fallback_address": final_address == build_area_fallback_address(area),
        "location_precision": location_precision,
        "rent_value": rent_value or parse_price_to_int_strict(fallback_title + " " + fallback_snippet),
        "link": url,
        "source": infer_source(url),
        "bhk": bhk,
        "page_text": page_text_small,
        "listing_type": listing_type,
        "lat": lat,
        "lng": lng,
    }


# =========================================================
# COMMUTE CACHE
# =========================================================
def get_cached_commute(cache_key: str) -> Optional[Tuple[Optional[int], Optional[int]]]:
    conn = get_db()
    cur = conn.cursor(dictionary=True)

    cur.execute(
        "SELECT commute_mins, distance_meters, updated_at FROM commute_cache WHERE address_hash = %s",
        (cache_key,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    age_hours = (now_ts() - row["updated_at"]) / 3600
    if age_hours > COMMUTE_CACHE_HOURS:
        return None

    return row["commute_mins"], row["distance_meters"]


def save_cached_commute(cache_key: str, label: str, commute_mins: Optional[int], distance_meters: Optional[int]):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO commute_cache(address_hash, address, commute_mins, distance_meters, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            address = VALUES(address),
            commute_mins = VALUES(commute_mins),
            distance_meters = VALUES(distance_meters),
            updated_at = VALUES(updated_at)
    """, (cache_key, label, commute_mins, distance_meters, now_ts()))

    conn.commit()
    cur.close()
    conn.close()


# =========================================================
# GEO + NEIGHBORHOOD CACHE
# =========================================================
def get_cached_geo(address: str) -> Optional[Tuple[Optional[float], Optional[float]]]:
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    address_key = hash_text(address)

    cur.execute(
        "SELECT lat, lng, updated_at FROM geo_cache WHERE address_hash = %s",
        (address_key,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    age_hours = (now_ts() - row["updated_at"]) / 3600
    if age_hours > COMMUTE_CACHE_HOURS:
        return None

    return row["lat"], row["lng"]


def save_cached_geo(address: str, lat: Optional[float], lng: Optional[float]):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO geo_cache(address_hash, address, lat, lng, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            address = VALUES(address),
            lat = VALUES(lat),
            lng = VALUES(lng),
            updated_at = VALUES(updated_at)
    """, (hash_text(address), address, lat, lng, now_ts()))

    conn.commit()
    cur.close()
    conn.close()


def geocode_address(address: str) -> Tuple[Optional[float], Optional[float]]:
    if not GOOGLE_API_KEY or not address:
        return None, None

    if is_fallback_address(address):
        return None, None

    cached = get_cached_geo(address)
    if cached is not None:
        return cached

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": GOOGLE_API_KEY}

    try:
        resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "OK":
            save_cached_geo(address, None, None)
            return None, None

        results = data.get("results", [])
        if not results:
            save_cached_geo(address, None, None)
            return None, None

        location = results[0].get("geometry", {}).get("location", {})
        lat = location.get("lat")
        lng = location.get("lng")

        if lat is None or lng is None:
            save_cached_geo(address, None, None)
            return None, None

        save_cached_geo(address, lat, lng)
        return lat, lng

    except requests.RequestException as e:
        logging.warning(f"Google Geocoding API failed for '{address}': {e}")
        return None, None


def geo_to_hash(lat: float, lng: float) -> str:
    return hash_text(f"{round(lat, 4)}|{round(lng, 4)}")


def get_cached_neighborhood(lat: float, lng: float) -> Optional[Dict]:
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    key = geo_to_hash(lat, lng)

    cur.execute("""
        SELECT amenities_text, amenity_counts, raw_places, updated_at
        FROM neighborhood_cache
        WHERE geo_hash = %s
    """, (key,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    age_hours = (now_ts() - row["updated_at"]) / 3600
    if age_hours > COMMUTE_CACHE_HOURS:
        return None

    try:
        amenity_counts = json.loads(row["amenity_counts"]) if row["amenity_counts"] else {}
    except Exception:
        amenity_counts = {}

    try:
        raw_places = json.loads(row["raw_places"]) if row["raw_places"] else []
    except Exception:
        raw_places = []

    return {
        "amenities_text": row["amenities_text"] or "",
        "amenity_counts": amenity_counts,
        "raw_places": raw_places,
    }


def save_cached_neighborhood(lat: float, lng: float, amenities_text: str, amenity_counts: Dict, raw_places: List[Dict]):
    conn = get_db()
    cur = conn.cursor()
    key = geo_to_hash(lat, lng)

    cur.execute("""
        INSERT INTO neighborhood_cache(geo_hash, lat, lng, amenities_text, amenity_counts, raw_places, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            lat = VALUES(lat),
            lng = VALUES(lng),
            amenities_text = VALUES(amenities_text),
            amenity_counts = VALUES(amenity_counts),
            raw_places = VALUES(raw_places),
            updated_at = VALUES(updated_at)
    """, (
        key, lat, lng, amenities_text,
        safe_json_dumps(amenity_counts),
        safe_json_dumps(raw_places),
        now_ts()
    ))

    conn.commit()
    cur.close()
    conn.close()


def get_office_coordinates() -> Tuple[Optional[float], Optional[float]]:
    if OFFICE_LAT and OFFICE_LNG:
        try:
            lat = float(OFFICE_LAT)
            lng = float(OFFICE_LNG)
            return lat, lng
        except ValueError:
            logging.warning("Invalid OFFICE_LAT / OFFICE_LNG values in .env")
            return None, None

    if OFFICE_ADDR:
        return geocode_address(OFFICE_ADDR)

    return None, None


def get_commute_info(
    address: Optional[str] = None,
    origin_lat: Optional[float] = None,
    origin_lng: Optional[float] = None
) -> Tuple[Optional[int], Optional[int]]:
    if not GOOGLE_API_KEY:
        return None, None

    dest_lat, dest_lng = get_office_coordinates()
    if dest_lat is None or dest_lng is None:
        return None, None

    if not is_near_bengaluru(dest_lat, dest_lng):
        logging.warning(f"Office coordinates appear outside Bengaluru: {dest_lat}, {dest_lng}")
        return None, None

    if origin_lat is not None and origin_lng is not None and is_near_bengaluru(origin_lat, origin_lng):
        origin = f"{origin_lat},{origin_lng}"
        cache_key = hash_text(f"coord:{round(origin_lat, 5)},{round(origin_lng, 5)}")
        label = origin
    elif address and not is_fallback_address(address):
        origin = address
        cache_key = hash_text(f"addr:{address}")
        label = address
    else:
        return None, None

    cached = get_cached_commute(cache_key)
    if cached is not None and cached != (None, None):
        return cached

    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": origin,
        "destinations": f"{dest_lat},{dest_lng}",
        "mode": "driving",
        "key": GOOGLE_API_KEY,
    }

    try:
        resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "OK":
            return None, None

        rows = data.get("rows", [])
        if not rows:
            return None, None

        elements = rows[0].get("elements", [])
        if not elements:
            return None, None

        element = elements[0]
        if element.get("status") != "OK":
            return None, None

        duration_seconds = element.get("duration", {}).get("value")
        distance_meters = element.get("distance", {}).get("value")

        mins = round(duration_seconds / 60) if duration_seconds is not None else None
        distance_meters = round(distance_meters) if distance_meters is not None else None

        if mins is not None or distance_meters is not None:
            save_cached_commute(cache_key, label, mins, distance_meters)

        return mins, distance_meters

    except requests.RequestException as e:
        logging.warning(f"Google Distance Matrix API failed for origin={origin}: {e}")
        return None, None


def get_neighborhood_context(lat: float, lng: float) -> Dict:
    if not GOOGLE_API_KEY or lat is None or lng is None:
        return {"amenities_text": "", "amenity_counts": {}, "raw_places": []}

    cached = get_cached_neighborhood(lat, lng)
    if cached is not None:
        return cached

    google_place_types = {
        "gym": "gym",
        "cafe": "cafe",
        "hospital": "hospital",
        "subway_station": "subway_station",
        "supermarket": "supermarket",
        "bar": "bar",
        "bus_station": "bus_station",
        "pharmacy": "pharmacy",
        "school": "school",
        "restaurant": "restaurant",
    }

    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

    found_amenities = []
    amenity_counts = {k: 0 for k in NEIGHBORHOOD_TYPES}
    raw_places = []
    seen_per_type = {}

    for ui_type, google_type in google_place_types.items():
        params = {
            "location": f"{lat},{lng}",
            "radius": PLACES_RADIUS_METERS,
            "type": google_type,
            "key": GOOGLE_API_KEY,
        }

        try:
            resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") not in ("OK", "ZERO_RESULTS"):
                logging.warning(
                    f"Google Places Nearby Search returned status: {data.get('status')} for type '{google_type}'"
                )
                continue

            results = data.get("results", [])
            seen_per_type[ui_type] = 0

            for place in results:
                if seen_per_type[ui_type] >= 2:
                    break

                name = place.get("name")
                if not name:
                    continue

                rating = place.get("rating")
                vicinity = place.get("vicinity", "")

                raw_places.append({
                    "name": name,
                    "type": ui_type,
                    "rating": rating,
                    "vicinity": vicinity,
                })
                amenity_counts[ui_type] += 1
                found_amenities.append(f"{name} ({ui_type})")
                seen_per_type[ui_type] += 1

        except requests.RequestException as e:
            logging.warning(f"Google Places Nearby Search failed for type '{google_type}' at {lat},{lng}: {e}")
            continue

    result = {
        "amenities_text": ", ".join(found_amenities),
        "amenity_counts": amenity_counts,
        "raw_places": raw_places,
    }

    save_cached_neighborhood(lat, lng, result["amenities_text"], result["amenity_counts"], result["raw_places"])
    return result


# =========================================================
# SENT / SEEN
# =========================================================
def mark_seen(link: str, title: str):
    conn = get_db()
    cur = conn.cursor()
    key = hash_text(link)

    cur.execute("""
        INSERT INTO seen_listings(link_hash, link, title, first_seen_at, last_seen_at)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            last_seen_at = VALUES(last_seen_at)
    """, (key, link, title, now_ts(), now_ts()))

    conn.commit()
    cur.close()
    conn.close()


def was_already_sent(link: str) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sent_listings WHERE link_hash = %s", (hash_text(link),))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row is not None


def mark_sent(link: str, title: str):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO sent_listings(link_hash, link, title, sent_at)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            sent_at = VALUES(sent_at)
    """, (hash_text(link), link, title, now_ts()))

    conn.commit()
    cur.close()
    conn.close()


# =========================================================
# COST + RISK + DECISION INTELLIGENCE
# =========================================================
def estimate_hidden_costs(item: Dict, budget: int) -> Dict:
    rent = item.get("rent_value") or 0
    commute_mins = item.get("commute_mins")
    source = item.get("source", "")
    text = item.get("page_text", "")

    if rent <= 12000:
        maintenance = 1000
    elif rent <= 20000:
        maintenance = 1800
    elif rent <= 30000:
        maintenance = 2500
    else:
        maintenance = 3500

    deposit = 2 * rent
    if "deposit" in text:
        dep_match = re.search(r"deposit\s*[:\-]?\s*(\d{4,7})", text)
        if dep_match:
            deposit = int(dep_match.group(1))

    if isinstance(commute_mins, int):
        if commute_mins <= 20:
            travel_cost = 1500
        elif commute_mins <= 35:
            travel_cost = 2500
        elif commute_mins <= 50:
            travel_cost = 3500
        else:
            travel_cost = 5000
    else:
        travel_cost = 2500

    utilities = 1500 if source == "NoBroker" else 1800
    monthly_total_cost = rent + maintenance + travel_cost + utilities

    return {
        "maintenance_estimate": maintenance,
        "deposit_estimate": deposit,
        "travel_cost_estimate": travel_cost,
        "monthly_total_cost": monthly_total_cost,
    }


def compute_reliability_score(item: Dict, budget: int) -> Tuple[int, List[str]]:
    score = 100
    flags = []

    title = (item.get("title") or "").lower()
    address = item.get("address")
    rent = item.get("rent_value")
    link = item.get("link", "")
    page_text = item.get("page_text", "")
    listing_type = item.get("listing_type", "UNKNOWN")
    location_precision = item.get("location_precision", "low")

    if not address or len(address) < 6:
        score -= 15
        flags.append("Incomplete address")
    elif is_fallback_address(address):
        score -= 10
        flags.append("Only area-level address available (no exact street)")

    if rent is None:
        score -= 20
        flags.append("Rent not confidently extracted")

    if rent is not None and rent < 4000:
        score -= 25
        flags.append("Suspiciously low rent")

    if rent is not None and rent > budget + 20000:
        score -= 20
        flags.append("Rent unusually above target budget")

    for word in SUSPICIOUS_WORDS:
        if word in page_text:
            score -= 8
            flags.append(f"Suspicious text: {word}")

    if "magicbricks" in link:
        score -= 5
    elif "nobroker" in link:
        score += 5

    if "sale" in title and "rent" not in title:
        score -= 20
        flags.append("Possible sale listing mismatch")

    if listing_type == "SALE":
        score -= 20
        flags.append("Possible sale-page mismatch")
    elif listing_type == "COMMERCIAL":
        score -= 25
        flags.append("Possible commercial listing")
    elif listing_type == "PG/HOSTEL":
        score -= 15
        flags.append("Possible PG/hostel style listing")
    elif listing_type == "UNKNOWN":
        score -= 5
        flags.append("Listing type unclear")

    if location_precision == "low":
        score -= 12
        flags.append("Exact location not available — scores are area-level estimates")
    elif location_precision == "medium":
        score -= 4

    score = max(0, min(score, 100))
    return score, list(dict.fromkeys(flags))


def compute_lifestyle_fit(item: Dict, profile: Dict) -> Tuple[int, List[str]]:
    score = 0
    reasons = []

    address = (item.get("address") or "").lower()
    commute = item.get("commute_mins")
    total_cost = item.get("monthly_total_cost") or 0
    location_precision = item.get("location_precision", "low")
    rent = item.get("rent_value") or 0

    preferred_areas = [a.lower() for a in profile.get("preferred_areas", [])]
    max_commute = profile.get("max_commute", 35)
    budget_flex = profile.get("budget_flex", 0)
    max_rent = profile.get("max_rent", 0)

    if any(area in address for area in preferred_areas):
        score += 30
        reasons.append("Preferred area match")

    if isinstance(commute, int):
        if commute <= max_commute:
            score += 35
            reasons.append("Commute within preferred limit")
        elif commute <= max_commute + 10:
            score += 15
            reasons.append("Commute slightly above preferred limit")
        else:
            score -= 10
    else:
        score -= 5
        reasons.append("Commute data unavailable — based on area estimate only")

    if total_cost <= max_rent + budget_flex + 5000:
        score += 25
        reasons.append("Estimated total monthly cost is manageable")
    else:
        score -= 10
        reasons.append("Estimated total monthly cost is high")

    amenity_counts = item.get("amenity_counts", {})

    if location_precision in ("high", "medium"):
        if amenity_counts.get("supermarket", 0) > 0:
            score += 8
            reasons.append("Supermarket nearby")
        if amenity_counts.get("hospital", 0) > 0:
            score += 8
            reasons.append("Hospital nearby")
        if amenity_counts.get("subway_station", 0) > 0 or amenity_counts.get("bus_station", 0) > 0:
            score += 10
            reasons.append("Public transport access nearby")
        if amenity_counts.get("gym", 0) > 0:
            score += 5
            reasons.append("Gym nearby")
        if amenity_counts.get("cafe", 0) > 0:
            score += 5
            reasons.append("Cafe options nearby")
    else:
        reasons.append("Neighborhood scores are area-level estimates (no precise address)")

    if rent > 0 and max_rent > 0:
        rent_ratio = rent / max_rent
        if rent_ratio <= 0.75:
            score += 10
            reasons.append("Rent is well below budget")
        elif rent_ratio <= 1.0:
            score += 5
        elif rent_ratio <= 1.15:
            score -= 5

    return max(0, min(score, 100)), reasons


def compute_final_fit_score(item: Dict, budget: int, profile: Dict) -> Tuple[int, List[str]]:
    reasons = []

    rent = item.get("rent_value")
    total_cost = item.get("monthly_total_cost")
    commute = item.get("commute_mins")
    reliability = item.get("reliability_score", 0)
    lifestyle = item.get("lifestyle_score", 0)

    if rent is not None:
        if rent <= budget:
            affordability_score = 100
            reasons.append("Rent is within budget")
        elif rent <= budget + profile.get("budget_flex", 3000):
            affordability_score = 70
            reasons.append("Rent is slightly above budget")
        else:
            affordability_score = 30
            reasons.append("Rent is well above budget")
    else:
        affordability_score = 40
        reasons.append("Rent unavailable, scored conservatively")

    if isinstance(commute, int):
        if commute <= 20:
            commute_score = 100
            reasons.append("Very low commute")
        elif commute <= 35:
            commute_score = 80
            reasons.append("Good commute")
        elif commute <= 50:
            commute_score = 55
            reasons.append("Moderate commute")
        else:
            commute_score = 25
            reasons.append("High commute burden")
    else:
        commute_score = 40
        reasons.append("Commute data unavailable — score is conservative estimate")

    if total_cost and total_cost > budget + 10000:
        reasons.append("Hidden monthly cost is high")

    final_score = round(
        0.35 * affordability_score +
        0.25 * commute_score +
        0.20 * reliability +
        0.20 * lifestyle
    )

    return max(0, min(final_score, 100)), reasons


# =========================================================
# NEIGHBORHOOD VIBE
# =========================================================
def build_rule_based_vibe(item: Dict, area: str) -> str:
    amenity_counts = item.get("amenity_counts", {}) or {}
    rent = item.get("rent_value")
    commute_mins = item.get("commute_mins")
    title = item.get("title", "")
    source = item.get("source", "")
    location_precision = item.get("location_precision", "low")
    risk_flags = item.get("risk_flags", []) or []
    address = item.get("address") or area

    cafes = amenity_counts.get("cafe", 0)
    gyms = amenity_counts.get("gym", 0)
    hospitals = amenity_counts.get("hospital", 0)
    supermarkets = amenity_counts.get("supermarket", 0)
    metro = amenity_counts.get("subway_station", 0)
    bars = amenity_counts.get("bar", 0)
    buses = amenity_counts.get("bus_station", 0)

    phrases = []

    if rent is not None:
        if rent <= 15000:
            phrases.append("budget-friendly on paper")
        elif rent <= 25000:
            phrases.append("mid-range pricing")
        else:
            phrases.append("premium-side pricing")

    if commute_mins is not None:
        if commute_mins <= 20:
            phrases.append("strong office commute")
        elif commute_mins <= 35:
            phrases.append("workable daily commute")
        elif commute_mins <= 50:
            phrases.append("commute is acceptable but not great")
        else:
            phrases.append("commute looks tiring")

    amenity_bits = []
    if metro > 0 or buses > 0:
        amenity_bits.append("transit access nearby")
    if supermarkets > 0:
        amenity_bits.append("daily essentials close")
    if hospitals > 0:
        amenity_bits.append("healthcare access nearby")
    if cafes > 0 or bars > 0:
        amenity_bits.append("social options around")
    if gyms > 0:
        amenity_bits.append("fitness options nearby")

    if amenity_bits:
        phrases.append(", ".join(amenity_bits[:2]))

    if source == "NoBroker":
        phrases.append("source quality is relatively better")
    elif source == "MagicBricks":
        phrases.append("needs a bit more manual verification")

    if "furnished" in title.lower():
        phrases.append("may suit someone prioritizing convenience")
    elif "semi-furnished" in title.lower():
        phrases.append("semi-furnished setup")
    elif "independent" in title.lower():
        phrases.append("more standalone-style setup")

    if location_precision == "low":
        rent_str = f"₹{rent}" if rent else "unknown rent"
        return (
            f"Listing in {area} ({rent_str}) — exact street address unavailable, "
            f"so commute and neighborhood data are area-level estimates only. Verify directly."
        )

    if risk_flags:
        return f"{address} looks like a {phrases[0] if phrases else 'workable'} option; {phrases[1] if len(phrases) > 1 else 'manual checks still matter'}."

    return f"{address} feels like a {phrases[0] if phrases else 'balanced'} option with {phrases[1] if len(phrases) > 1 else 'reasonable overall fit'}."


def derive_neighborhood_red_flags(amenity_counts: Dict, commute_mins: Optional[int], is_precise: bool) -> List[str]:
    flags = []

    if not is_precise:
        flags.append("No precise address — neighborhood data reflects the general area, not this specific listing")
        return flags

    if amenity_counts.get("hospital", 0) == 0:
        flags.append("No hospital found within 1 km")

    if amenity_counts.get("supermarket", 0) == 0:
        flags.append("No supermarket found within 1 km")

    if amenity_counts.get("subway_station", 0) == 0 and amenity_counts.get("bus_station", 0) == 0:
        flags.append("Weak public transport nearby")

    if commute_mins is not None and commute_mins > 50:
        flags.append("Long commute burden")

    return flags


def generate_vibe_summary(item: Dict, area: str) -> str:
    fallback = build_rule_based_vibe(item, area)

    if not GROQ_API_KEY:
        return fallback

    try:
        client = Groq(api_key=GROQ_API_KEY)

        system_prompt = (
            "You are a Bengaluru rental decision assistant. "
            "Write exactly one natural sounding sentence for a house-hunting alert. "
            "Do not sound generic or templated. "
            "Base it only on the provided listing facts. "
            "Mention the likely type of renter it suits OR the main tradeoff. "
            "If exact location is weak, clearly say the location confidence is low. "
            "Do not invent crime, traffic, safety, broker, or noise facts. "
            "Keep it under 28 words."
        )

        user_prompt = f"""
Listing title: {item.get('title')}
Source: {item.get('source')}
Address: {item.get('address')}
Area searched: {area}
Advertised rent: {item.get('rent_value')}
Commute to office/college: {item.get('commute_mins')}
Estimated total monthly cost: {item.get('monthly_total_cost')}
Location precision: {item.get('location_precision')}
Amenities text: {item.get('amenities_text')}
Amenity counts: {json.dumps(item.get('amenity_counts', {}))}
Risk flags: {json.dumps(item.get('risk_flags', []))}
Neighborhood red flags: {json.dumps(item.get('neighborhood_red_flags', []))}

Output exactly one sentence.
"""

        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.55,
            max_completion_tokens=80,
        )

        text = response.choices[0].message.content.strip()
        if not text:
            return fallback

        text = normalize_space(text)
        return text

    except Exception as e:
        logging.warning(f"Groq vibe summary failed for area '{area}': {e}")
        return fallback


def build_recommendation_reason(item: Dict) -> str:
    positives = []

    rent = item.get("rent_value")
    commute = item.get("commute_mins")
    reliability = item.get("reliability_score", 0)
    total = item.get("monthly_total_cost")
    precision = item.get("location_precision", "low")

    if rent is not None:
        positives.append(f"advertised rent ₹{rent}")
    else:
        positives.append("rent needs manual verification")

    if total is not None:
        positives.append(f"estimated total monthly cost ₹{total}")
    if commute is not None:
        positives.append(f"commute about {commute} min")
    elif precision == "low":
        positives.append("commute is an estimate based on area centre")
    if reliability >= 75:
        positives.append("good listing reliability")
    if precision in {"high", "medium"}:
        positives.append("location confidence is decent")
    else:
        positives.append("location is area-level only")

    if positives:
        reason = "Recommended because " + ", ".join(positives[:4]) + "."
    else:
        reason = "Recommended based on overall fit."

    vibe = item.get("vibe_summary")
    if vibe:
        reason += " " + vibe

    return reason


# =========================================================
# STORAGE
# =========================================================
def save_listing_record(item: Dict):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO listings(
            link_hash, source, title, link, address, bhk, listing_type, rent_value, commute_mins,
            distance_meters, maintenance_estimate, deposit_estimate, travel_cost_estimate,
            monthly_total_cost, reliability_score, lifestyle_score, fit_score,
            lat, lng, amenities_text, vibe_summary, neighborhood_red_flags,
            risk_flags, recommendation_reason, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            source = VALUES(source),
            title = VALUES(title),
            link = VALUES(link),
            address = VALUES(address),
            bhk = VALUES(bhk),
            listing_type = VALUES(listing_type),
            rent_value = VALUES(rent_value),
            commute_mins = VALUES(commute_mins),
            distance_meters = VALUES(distance_meters),
            maintenance_estimate = VALUES(maintenance_estimate),
            deposit_estimate = VALUES(deposit_estimate),
            travel_cost_estimate = VALUES(travel_cost_estimate),
            monthly_total_cost = VALUES(monthly_total_cost),
            reliability_score = VALUES(reliability_score),
            lifestyle_score = VALUES(lifestyle_score),
            fit_score = VALUES(fit_score),
            lat = VALUES(lat),
            lng = VALUES(lng),
            amenities_text = VALUES(amenities_text),
            vibe_summary = VALUES(vibe_summary),
            neighborhood_red_flags = VALUES(neighborhood_red_flags),
            risk_flags = VALUES(risk_flags),
            recommendation_reason = VALUES(recommendation_reason),
            updated_at = VALUES(updated_at)
    """, (
        hash_text(item["link"]),
        item.get("source"),
        item.get("title"),
        item.get("link"),
        item.get("address"),
        item.get("bhk"),
        item.get("listing_type"),
        item.get("rent_value"),
        item.get("commute_mins"),
        item.get("distance_meters"),
        item.get("maintenance_estimate"),
        item.get("deposit_estimate"),
        item.get("travel_cost_estimate"),
        item.get("monthly_total_cost"),
        item.get("reliability_score"),
        item.get("lifestyle_score"),
        item.get("fit_score"),
        item.get("lat"),
        item.get("lng"),
        item.get("amenities_text"),
        item.get("vibe_summary"),
        safe_json_dumps(item.get("neighborhood_red_flags", [])),
        safe_json_dumps(item.get("risk_flags", [])),
        item.get("recommendation_reason"),
        now_ts(),
        now_ts()
    ))

    conn.commit()
    cur.close()
    conn.close()


# =========================================================
# MAIN PIPELINE
# =========================================================
def should_skip_listing(parsed: Dict, budget_int: int) -> Tuple[bool, str]:
    listing_type = parsed.get("listing_type", "UNKNOWN")
    rent = parsed.get("rent_value")

    if listing_type in {"SALE", "COMMERCIAL"}:
        return True, f"bad type: {listing_type}"

    if rent is not None and rent > budget_int + 15000:
        return True, f"rent too high ({rent})"

    return False, ""


def enrich_listing(parsed: Dict, area: str, budget_int: int, profile: Optional[Dict] = None) -> Dict:
    """Enrich a parsed listing dict with commute, POI, scores and vibe.

    ``profile`` is an optional per-request profile dict.  When not supplied the
    module-level USER_PROFILE is used (CLI path).  Streamlit callers should
    always pass a session-scoped profile to avoid cross-session mutations.
    """
    if profile is None:
        profile = USER_PROFILE

    if not parsed.get("address"):
        parsed["address"] = build_area_fallback_address(area)

    exact_lat = parsed.get("lat")
    exact_lng = parsed.get("lng")
    address = parsed.get("address")

    parsed["commute_mins"] = None
    parsed["distance_meters"] = None
    parsed["amenities_text"] = ""
    parsed["amenity_counts"] = {}
    parsed["raw_places"] = []
    parsed["neighborhood_red_flags"] = []
    parsed["vibe_summary"] = ""

    if exact_lat is not None and exact_lng is not None and is_near_bengaluru(exact_lat, exact_lng):
        commute_mins, distance_meters = get_commute_info(origin_lat=exact_lat, origin_lng=exact_lng)
        parsed["commute_mins"] = commute_mins
        parsed["distance_meters"] = distance_meters

        neighborhood = get_neighborhood_context(exact_lat, exact_lng)
        parsed["amenities_text"] = neighborhood["amenities_text"]
        parsed["amenity_counts"] = neighborhood["amenity_counts"]
        parsed["raw_places"] = neighborhood["raw_places"]

    elif address and not is_fallback_address(address):
        commute_mins, distance_meters = get_commute_info(address=address)
        parsed["commute_mins"] = commute_mins
        parsed["distance_meters"] = distance_meters

        geo_lat, geo_lng = geocode_address(address)
        if geo_lat is not None and geo_lng is not None and is_near_bengaluru(geo_lat, geo_lng):
            parsed["lat"] = geo_lat
            parsed["lng"] = geo_lng

            neighborhood = get_neighborhood_context(geo_lat, geo_lng)
            parsed["amenities_text"] = neighborhood["amenities_text"]
            parsed["amenity_counts"] = neighborhood["amenity_counts"]
            parsed["raw_places"] = neighborhood["raw_places"]

    else:
        parsed["commute_mins"] = None
        parsed["distance_meters"] = None

    cost_info = estimate_hidden_costs(parsed, budget_int)
    parsed.update(cost_info)

    reliability_score, risk_flags = compute_reliability_score(parsed, budget_int)
    parsed["reliability_score"] = reliability_score
    parsed["risk_flags"] = risk_flags

    is_precise = has_precise_location_signal(parsed.get("address"), parsed.get("lat"), parsed.get("lng"))
    parsed["neighborhood_red_flags"] = derive_neighborhood_red_flags(
        parsed.get("amenity_counts", {}),
        parsed.get("commute_mins"),
        is_precise=is_precise
    )

    lifestyle_score, lifestyle_reasons = compute_lifestyle_fit(parsed, profile)
    parsed["lifestyle_score"] = lifestyle_score
    parsed["lifestyle_reasons"] = lifestyle_reasons

    fit_score, fit_reasons = compute_final_fit_score(parsed, budget_int, profile)
    parsed["fit_score"] = fit_score
    parsed["fit_reasons"] = fit_reasons

    parsed["vibe_summary"] = generate_vibe_summary(parsed, area)
    parsed["recommendation_reason"] = build_recommendation_reason(parsed)
    parsed["already_sent"] = was_already_sent(parsed["link"])

    return parsed


def fetch_blr_flats(area: str, bhk: str, budget: str, profile: Optional[Dict] = None) -> List[Dict]:
    """Full pipeline: search → parse → enrich → rank.

    Pass a session-scoped ``profile`` dict from Streamlit to avoid shared-state
    bugs.  CLI usage leaves ``profile`` as None and falls back to USER_PROFILE.
    """
    budget_int = int(str(budget).replace(",", "").strip())

    if profile is None:
        profile = USER_PROFILE

    search_results = fetch_search_results(area, bhk, budget)
    logging.info(f"Found {len(search_results)} cleaned search results for {area}, {bhk} BHK")

    final_listings = []
    dedupe_links = set()

    if not search_results:
        print(f"[PIPELINE] No search candidates found for area={area}, bhk={bhk}, budget={budget}")
        return []

    for idx, result in enumerate(search_results, start=1):
        link = result["link"]

        if link in dedupe_links:
            continue
        dedupe_links.add(link)

        print(f"\n[{idx}/{len(search_results)}] Parsing: {link}")

        parsed = parse_listing_page(
            url=link,
            fallback_title=result["title"],
            fallback_snippet=result["snippet"],
            area=area,
            bhk=bhk
        )

        if not parsed:
            print("   Skipped: parser returned nothing")
            continue

        print(f"   Title    : {parsed.get('title', '')[:80]}")
        print(f"   Type     : {parsed.get('listing_type', 'UNKNOWN')}")
        print(f"   Rent     : {parsed.get('rent_value')}")
        print(f"   Address  : {parsed.get('address')}")
        print(f"   Precision: {parsed.get('location_precision')}")

        mark_seen(parsed["link"], parsed["title"])

        skip, reason = should_skip_listing(parsed, budget_int)
        if skip:
            print(f"   Skipped: {reason}")
            continue

        parsed = enrich_listing(parsed, area, budget_int, profile=profile)
        save_listing_record(parsed)

        print(f"   Commute  : {parsed.get('commute_mins', 'N/A')} min")
        print(f"   Fit Score: {parsed['fit_score']}/100")

        final_listings.append(parsed)
        time.sleep(1)

    final_listings.sort(
        key=lambda x: (
            x.get("fit_score", 0),
            x.get("reliability_score", 0),
            1 if x.get("rent_value") is not None else 0
        ),
        reverse=True
    )

    print(f"\n[PIPELINE] Final usable listings: {len(final_listings)}")
    return final_listings


# =========================================================
# WHATSAPP
# =========================================================
def _build_listing_entry(p: Dict) -> str:
    """Build a single WhatsApp entry string for one listing."""
    rent_text = f"₹{p['rent_value']}" if p.get("rent_value") is not None else "Not found"
    total_text = f"₹{p['monthly_total_cost']}" if p.get("monthly_total_cost") is not None else "N/A"
    commute_text = f"{p['commute_mins']} min" if p.get("commute_mins") is not None else "Needs check"
    precision_note = "" if p.get("location_precision") != "low" else " | area-level only"

    # FIX: Truncate variable-length fields so one long listing can't consume the
    # entire character budget.
    title_str = (p.get("title") or "")[:55]
    address_str = (p.get("address") or "")[:55]
    vibe_str = (p.get("vibe_summary") or "Vibe unavailable")[:120]

    return (
        f"Fit: {p['fit_score']}/100 | Reliability: {p['reliability_score']}/100{precision_note}\n"
        f"Rent: {rent_text} | Total: {total_text}\n"
        f"Commute: {commute_text}\n"
        f"{title_str}\n"
        f"{address_str}\n"
        f"{vibe_str}\n"
        f"{p['link']}\n\n---\n\n"
    )


def build_whatsapp_body(props: List[Dict], area: str) -> Optional[str]:
    """Build the WhatsApp message body.

    FIX (Objective 8): Each entry is measured before appending, so we never
    silently truncate mid-entry.  The loop stops cleanly when the next entry
    would push the total over WHATSAPP_BODY_CHAR_LIMIT.
    """
    fresh = [p for p in props if not p.get("already_sent")]

    if not fresh:
        return None

    now = datetime.now().strftime("%I:%M %p")
    header = (
        f"🏠 *{area} Rental Intelligence* ({now})\n"
        f"_{OFFICE_ADDR or 'Office configured via coordinates'}_\n\n"
    )

    body = header
    added = 0

    for p in fresh:
        if added >= MAX_WHATSAPP_RESULTS:
            break

        entry = _build_listing_entry(p)

        # FIX: Check length BEFORE appending — never let the body exceed the limit
        if len(body) + len(entry) > WHATSAPP_BODY_CHAR_LIMIT:
            break

        body += entry
        added += 1

    return body if added > 0 else None


def notify(props: List[Dict], area: str):
    if not props:
        print("No listings to notify.")
        return

    if not all([TWILIO_SID, TWILIO_TOKEN, TARGET_WHATSAPP]):
        print("Missing Twilio config in .env")
        return

    body = build_whatsapp_body(props, area)

    if not body:
        print("No new listings to send — everything already sent.")
        return

    try:
        client = Client(TWILIO_SID, TWILIO_TOKEN)
        client.messages.create(
            from_=TWILIO_SENDER,
            body=body,
            to=TARGET_WHATSAPP
        )
        print("WhatsApp message sent")

        fresh = [p for p in props if not p.get("already_sent")]
        count = 0
        for p in fresh:
            if count >= MAX_WHATSAPP_RESULTS:
                break
            mark_sent(p["link"], p["title"])
            count += 1

    except Exception as e:
        print(f"Failed to send WhatsApp: {e}")


# =========================================================
# CONSOLE OUTPUT
# =========================================================
def print_results(listings: List[Dict]):
    print("\n================ TOP RESULTS ================\n")

    if not listings:
        print("No valid listings found.")
        return

    for i, p in enumerate(listings[:10], start=1):
        sent_flag = "YES" if p.get("already_sent") else "NO"
        rent_text = f"₹{p['rent_value']}" if p.get("rent_value") is not None else "Not found"
        precision = p.get("location_precision", "low")
        precision_note = " (area-level estimate)" if precision == "low" else ""

        print(f"{i}. {p['title']}")
        print(f"   Source              : {p.get('source', 'N/A')}")
        print(f"   Listing Type        : {p.get('listing_type', 'N/A')}")
        print(f"   Location Precision  : {precision}{precision_note}")
        print(f"   Rent                : {rent_text}")
        print(f"   Estimated Total     : ₹{p['monthly_total_cost'] if p.get('monthly_total_cost') else 'N/A'}")
        print(f"   Maintenance         : ₹{p.get('maintenance_estimate', 'N/A')}")
        print(f"   Deposit Estimate    : ₹{p.get('deposit_estimate', 'N/A')}")
        print(f"   Travel Cost Estimate: ₹{p.get('travel_cost_estimate', 'N/A')}")
        print(f"   Address             : {p.get('address', 'N/A')}{precision_note}")
        if p.get("commute_mins") is not None:
            print(f"   Commute             : {p['commute_mins']} min")
        else:
            print(f"   Commute             : N/A (area estimate)")
        print(f"   Reliability Score   : {p.get('reliability_score', 'N/A')}/100")
        print(f"   Lifestyle Score     : {p.get('lifestyle_score', 'N/A')}/100")
        print(f"   Fit Score           : {p.get('fit_score', 'N/A')}/100")
        print(f"   Vibe Summary        : {p.get('vibe_summary', 'N/A')}")
        print(f"   Why Recommended     : {p.get('recommendation_reason', 'N/A')}")
        print(f"   Risk Flags          : {', '.join(p.get('risk_flags', [])) if p.get('risk_flags') else 'None'}")
        print(f"   Already sent        : {sent_flag}")
        print(f"   Link                : {p['link']}")
        print("")


# =========================================================
# RUN
# =========================================================
def main():
    try:
        validate_env()
        init_db()

        area_in = input("Area (e.g. HSR Layout): ").strip()
        bhk_in = input("BHK (1/2/3): ").strip()
        rent_in = input("Max Rent: ").strip()

        if not area_in or not bhk_in or not rent_in:
            print("Area, BHK and Max Rent are required.")
            return

        # CLI path: mutate module-level profile as before
        USER_PROFILE["max_rent"] = int(rent_in.replace(",", "").strip())

        print("\n[START] Running rental search pipeline...")
        print(f"[INPUT] Area={area_in} | BHK={bhk_in} | Max Rent={rent_in}")

        listings = fetch_blr_flats(area_in, bhk_in, rent_in, profile=USER_PROFILE)
        print_results(listings)
        notify(listings, area_in)

    except Exception as e:
        logging.exception("Fatal error")
        print(f"Fatal error: {e}")


if __name__ == "__main__":
    main()