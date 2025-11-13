#!/usr/bin/env python3
"""
DamaDam Profile Scraper - Core Logic v3.1
Shared scraping logic for both Online and Target scrapers
"""

import os
import sys
import time
import random
import re
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ Environment loaded from .env")
except ImportError:
    print("⚠️ python-dotenv not installed, using system environment")

# Verify required packages
required_packages = {
    'selenium': 'selenium',
    'gspread': 'gspread',
    'google.auth': 'google-auth'
}

missing_packages = []
for module, package in required_packages.items():
    try:
        __import__(module)
    except ImportError:
        missing_packages.append(package)

if missing_packages:
    print(f"❌ Missing packages: {', '.join(missing_packages)}")
    print(f"Install with: pip install {' '.join(missing_packages)}")
    sys.exit(1)

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

import gspread
from google.auth.exceptions import RefreshError
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIGURATION
# ============================================================

LOGIN_URL = "https://damadam.pk/login/"
COOKIE_FILE = "damadam_cookies.pkl"

# Environment Variables
USERNAME = os.getenv('DAMADAM_USERNAME')
PASSWORD = os.getenv('DAMADAM_PASSWORD')
USERNAME_2 = os.getenv('DAMADAM_USERNAME_2', '')
PASSWORD_2 = os.getenv('DAMADAM_PASSWORD_2', '')
SHEET_URL = os.getenv('GOOGLE_SHEET_URL')
GOOGLE_CREDENTIALS_RAW = os.getenv('GOOGLE_CREDENTIALS_JSON', '')

# Scraper Settings
MAX_PROFILES_PER_RUN = int(os.getenv('MAX_PROFILES_PER_RUN', '0'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
MIN_DELAY = float(os.getenv('MIN_DELAY', '0.2'))
MAX_DELAY = float(os.getenv('MAX_DELAY', '0.4'))
PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '15'))
SHEET_WRITE_DELAY = float(os.getenv('SHEET_WRITE_DELAY', '0.4'))

# Sheet Structure
COLUMN_ORDER = [
    "IMAGE", "NICK NAME", "TAGS", "LAST POST", "LAST POST TIME", "FRIEND", "CITY",
    "GENDER", "MARRIED", "AGE", "JOINED", "FOLLOWERS", "STATUS",
    "POSTS", "PROFILE LINK", "INTRO", "SOURCE", "DATETIME SCRAP"
]

COLUMN_TO_INDEX = {name: idx for idx, name in enumerate(COLUMN_ORDER)}
LOG_SHEET_NAME = "Logs"
LOG_HEADERS = ["Timestamp", "Nickname", "Change Type", "Fields", "Before", "After"]
DASHBOARD_SHEET_NAME = "Dashboard"
HIGHLIGHT_EXCLUDE_COLUMNS = {"IMAGE", "LAST POST", "JOINED", "PROFILE LINK", "SOURCE", "DATETIME SCRAP"}
LINK_COLUMNS = {"IMAGE", "LAST POST", "PROFILE LINK"}

# Validate environment
required_env = ['DAMADAM_USERNAME', 'DAMADAM_PASSWORD', 'GOOGLE_SHEET_URL', 'GOOGLE_CREDENTIALS_JSON']
missing_vars = [var for var in required_env if not os.getenv(var)]

if missing_vars:
    print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
    sys.exit(1)

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_pkt_time():
    """Get current Pakistan time (UTC+5)"""
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5)

def log_msg(message):
    """Log with timestamp"""
    timestamp = get_pkt_time().strftime("%H:%M:%S")
    print(f"  [{timestamp}] {message}")
    sys.stdout.flush()

def clean_text(text):
    """Clean text"""
    if not text:
        return ""
    text = str(text).strip().replace('\xa0', ' ').replace('\n', ' ')
    return re.sub(r'\s+', ' ', text).strip()

def clean_data(value):
    """Clean data by removing unwanted values"""
    if not value:
        return ""
    value = str(value).strip()
    replace_with_blank = [
        "No city", "Not set", "[No Posts]", "N/A",
        "no city", "not set", "[no posts]", "n/a",
        "[No Post URL]", "[Error]", "no set", "none", "null", "no age"
    ]
    return "" if value in replace_with_blank else value

def convert_relative_date_to_absolute(relative_text):
    """Convert '2 months ago' to 'dd-mmm-yy'"""
    if not relative_text:
        return ""
    
    relative_text = relative_text.lower().strip()
    now = get_pkt_time()
    
    try:
        match = re.search(r'(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*ago', relative_text)
        if match:
            amount = int(match.group(1))
            unit = match.group(2)
            
            delta_map = {
                'second': timedelta(seconds=amount),
                'minute': timedelta(minutes=amount),
                'hour': timedelta(hours=amount),
                'day': timedelta(days=amount),
                'week': timedelta(weeks=amount),
                'month': timedelta(days=amount * 30),
                'year': timedelta(days=amount * 365)
            }
            
            if unit in delta_map:
                target_date = now - delta_map[unit]
                return target_date.strftime("%d-%b-%y")
        return relative_text
    except:
        return relative_text

def parse_post_timestamp(timestamp_text):
    """Parse post timestamp to 'DD-MMM-YY'"""
    return convert_relative_date_to_absolute(timestamp_text)

def to_absolute_url(href):
    """Ensure URLs are absolute"""
    if not href:
        return ""
    href = href.strip()
    if href.startswith('/'):
        return f"https://damadam.pk{href}"
    elif not href.startswith('http'):
        return f"https://damadam.pk/{href}"
    return href

def column_letter(col_idx):
    """Convert column index to letter (A, B, C, ...)"""
    result = ""
    col_idx += 1
    while col_idx > 0:
        col_idx -= 1
        result = chr(col_idx % 26 + ord('A')) + result
        col_idx //= 26
    return result

def save_cookies(driver, filepath=COOKIE_FILE):
    """Save browser cookies"""
    try:
        import pickle
        with open(filepath, 'wb') as f:
            pickle.dump(driver.get_cookies(), f)
        log_msg(f"💾 Cookies saved to {filepath}")
        return True
    except Exception as e:
        log_msg(f"⚠️ Failed to save cookies: {e}")
        return False

def load_cookies(driver, filepath=COOKIE_FILE):
    """Load cookies from file"""
    try:
        import pickle
        if not os.path.exists(filepath):
            return False
        
        with open(filepath, 'rb') as f:
            cookies = pickle.load(f)
        
        for cookie in cookies:
            try:
                driver.add_cookie(cookie)
            except:
                pass
        
        log_msg(f"🍪 Loaded {len(cookies)} cookies")
        return True
    except Exception as e:
        log_msg(f"⚠️ Failed to load cookies: {e}")
        return False

def calculate_eta(processed, total, start_time):
    """Calculate estimated time remaining"""
    if processed == 0:
        return "Calculating..."
    
    elapsed = time.time() - start_time
    rate = processed / elapsed
    remaining = total - processed
    eta_seconds = remaining / rate if rate > 0 else 0
    
    if eta_seconds < 60:
        return f"{int(eta_seconds)}s"
    elif eta_seconds < 3600:
        return f"{int(eta_seconds / 60)}m {int(eta_seconds % 60)}s"
    else:
        hours = int(eta_seconds / 3600)
        minutes = int((eta_seconds % 3600) / 60)
        return f"{hours}h {minutes}m"

# ============================================================
# GOOGLE SHEETS SETUP
# ============================================================

print("\n🔑 Initializing Google Sheets...")
try:
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds_json = json.loads(GOOGLE_CREDENTIALS_RAW)
    credentials = Credentials.from_service_account_info(creds_json, scopes=scope)
    client = gspread.authorize(credentials)
    print(f"✅ Authorized: {credentials.service_account_email}")
except Exception as e:
    print(f"❌ Google Sheets authorization failed: {e}")
    sys.exit(1)
