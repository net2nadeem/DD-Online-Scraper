#!/usr/bin/env python3
"""
Browser Setup and Authentication Module
"""

import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from core_scraper import *

# ============================================================
# BROWSER SETUP
# ============================================================

def setup_browser():
    """Setup Chrome browser"""
    try:
        print("\n🔧 Setting up browser...")
        
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--log-level=3")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        print("  ✅ Browser initialized")
        return driver
    
    except Exception as e:
        print(f"  ❌ Browser setup failed: {e}")
        return None

def restart_browser(driver):
    """Restart browser on crash"""
    try:
        if driver:
            driver.quit()
    except:
        pass
    time.sleep(2)
    return setup_browser()

# ============================================================
# AUTHENTICATION
# ============================================================

def login_with_credentials(driver, username, password, account_name):
    """Login with credentials"""
    try:
        log_msg(f"Trying {account_name}: {username}")
        
        login_selectors = [
            {"nick": "#nick", "pass": "#pass", "button": "form button"},
            {"nick": "input[name='nick']", "pass": "input[name='pass']", "button": "button[type='submit']"}
        ]
        
        for sel in login_selectors:
            try:
                nick_field = WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, sel["nick"]))
                )
                pass_field = driver.find_element(By.CSS_SELECTOR, sel["pass"])
                submit_btn = driver.find_element(By.CSS_SELECTOR, sel["button"])
                
                nick_field.clear()
                time.sleep(0.5)
                nick_field.send_keys(username)
                
                pass_field.clear()
                time.sleep(0.5)
                pass_field.send_keys(password)
                
                submit_btn.click()
                time.sleep(4)
                
                if "login" not in driver.current_url.lower():
                    log_msg(f"✅ {account_name} login successful")
                    save_cookies(driver)
                    return True
                break
            except:
                continue
        
        return False
    
    except Exception as e:
        log_msg(f"❌ {account_name} login error: {e}")
        return False

def login_to_damadam(driver):
    """Login to DamaDam"""
    try:
        print("\n🔐 Logging in...")
        
        driver.get("https://damadam.pk/")
        time.sleep(2)
        
        if load_cookies(driver):
            driver.refresh()
            time.sleep(3)
            
            if "login" not in driver.current_url.lower():
                page_source = driver.page_source.lower()
                if any(indicator in page_source for indicator in ['logout', 'profile', 'settings']):
                    print("  ✅ Login via cookies")
                    return True
        
        driver.get(LOGIN_URL)
        time.sleep(3)
        
        if USERNAME and PASSWORD:
            if login_with_credentials(driver, USERNAME, PASSWORD, "Account 1"):
                return True
        
        if USERNAME_2 and PASSWORD_2:
            log_msg("Trying Account 2...")
            driver.get(LOGIN_URL)
            time.sleep(3)
            if login_with_credentials(driver, USERNAME_2, PASSWORD_2, "Account 2"):
                return True
        
        print("  ❌ All login attempts failed")
        return False
    
    except Exception as e:
        print(f"  ❌ Login error: {e}")
        return False
