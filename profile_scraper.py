#!/usr/bin/env python3
"""
Profile Scraping Module
"""

import time
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from core_scraper import *

# ============================================================
# PROFILE SCRAPING
# ============================================================

def get_friend_status(driver):
    """Check friend status"""
    try:
        page_source = driver.page_source.lower()
        if 'action="/follow/remove/"' in page_source or 'unfollow.svg' in page_source:
            return "Yes"
        if 'follow.svg' in page_source and 'unfollow' not in page_source:
            return "No"
        return ""
    except:
        return ""

def extract_text_comment_url(href):
    """Extract text comment URL"""
    pattern = r'/comments/text/(\d+)/'
    match = re.search(pattern, href)
    if match:
        return to_absolute_url(f"/comments/text/{match.group(1)}/").rstrip('/')
    return to_absolute_url(href)

def extract_image_comment_url(href):
    """Extract image comment URL"""
    pattern = r'/comments/image/(\d+)/'
    match = re.search(pattern, href)
    if match:
        return to_absolute_url(f"/content/{match.group(1)}/g/")
    return to_absolute_url(href)

def scrape_recent_post(driver, nickname):
    """Scrape recent post"""
    post_url = f"https://damadam.pk/profile/public/{nickname}"
    try:
        driver.get(post_url)
        
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "article.mbl"))
            )
        except TimeoutException:
            return {'LPOST': '', 'LDATE-TIME': ''}
        
        recent_post = driver.find_element(By.CSS_SELECTOR, "article.mbl")
        post_data = {'LPOST': '', 'LDATE-TIME': ''}
        
        url_selectors = [
            ("a[href*='/content/']", lambda h: to_absolute_url(h)),
            ("a[href*='/comments/text/']", extract_text_comment_url),
            ("a[href*='/comments/image/']", extract_image_comment_url)
        ]
        
        for selector, formatter in url_selectors:
            try:
                link = recent_post.find_element(By.CSS_SELECTOR, selector)
                href = link.get_attribute('href')
                if href:
                    formatted = formatter(href)
                    if formatted:
                        post_data['LPOST'] = formatted
                        break
            except:
                continue
        
        time_selectors = [
            "span[itemprop='datePublished']",
            "time[itemprop='datePublished']",
            "span.cxs.cgy",
            "time"
        ]
        for sel in time_selectors:
            try:
                time_elem = recent_post.find_element(By.CSS_SELECTOR, sel)
                if time_elem.text.strip():
                    post_data['LDATE-TIME'] = parse_post_timestamp(time_elem.text.strip())
                    break
            except:
                continue
        
        return post_data
    
    except Exception as e:
        return {'LPOST': '', 'LDATE-TIME': ''}

def scrape_profile(driver, nickname):
    """Scrape profile"""
    url = f"https://damadam.pk/users/{nickname}/"
    try:
        log_msg(f"📍 Scraping: {nickname}")
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1.cxl.clb.lsp"))
        )
        
        page_source = driver.page_source
        now = get_pkt_time()
        
        data = {
            'NICK NAME': nickname,
            'DATETIME SCRAP': now.strftime("%d-%b-%y %I:%M %p"),
            'PROFILE LINK': url,
            'TAGS': '',
            'CITY': '',
            'GENDER': '',
            'MARRIED': '',
            'AGE': '',
            'JOINED': '',
            'FOLLOWERS': '',
            'POSTS': '',
            'LAST POST': '',
            'LAST POST TIME': '',
            'IMAGE': '',
            'INTRO': '',
            'STATUS': '',
            'FRIEND': ''
        }
        
        # Status detection
        if 'Account suspended' in page_source or 'account suspended' in page_source.lower():
            data['STATUS'] = "Suspended"
        elif 'background:tomato' in page_source or 'style="background:tomato"' in page_source:
            data['STATUS'] = "Unverified"
        else:
            try:
                driver.find_element(By.CSS_SELECTOR, "div[style*='tomato']")
                data['STATUS'] = "Unverified"
            except:
                data['STATUS'] = "Verified"
        
        data['FRIEND'] = get_friend_status(driver)
        
        # Intro
        for sel in ["span.cl.sp.lsp.nos", "span.cl", ".ow span.nos"]:
            try:
                intro = driver.find_element(By.CSS_SELECTOR, sel)
                if intro.text.strip():
                    data['INTRO'] = clean_text(intro.text)
                    break
            except:
                pass
        
        # Profile fields
        fields = {'City:': 'CITY', 'Gender:': 'GENDER', 'Married:': 'MARRIED', 'Age:': 'AGE', 'Joined:': 'JOINED'}
        for field_text, key in fields.items():
            try:
                elem = driver.find_element(By.XPATH, f"//b[contains(text(), '{field_text}')]/following-sibling::span[1]")
                value = elem.text.strip()
                if value:
                    if key == 'JOINED':
                        data[key] = convert_relative_date_to_absolute(value)
                    elif key == 'GENDER':
                        data[key] = "💃" if value.lower() == 'female' else "🕺" if value.lower() == 'male' else value
                    elif key == 'MARRIED':
                        if value.lower() in ['yes', 'married']:
                            data[key] = "💍"
                        elif value.lower() in ['no', 'single', 'unmarried']:
                            data[key] = "❎"
                        else:
                            data[key] = value
                    else:
                        data[key] = clean_data(value)
            except:
                pass
        
        # Followers
        for sel in ["span.cl.sp.clb", ".cl.sp.clb"]:
            try:
                followers = driver.find_element(By.CSS_SELECTOR, sel)
                match = re.search(r'(\d+)', followers.text)
                if match:
                    data['FOLLOWERS'] = match.group(1)
                    break
            except:
                pass
        
        # Posts count
        for sel in ["a[href*='/profile/public/'] button div:first-child", "a[href*='/profile/public/'] button div"]:
            try:
                posts = driver.find_element(By.CSS_SELECTOR, sel)
                match = re.search(r'(\d+)', posts.text)
                if match:
                    data['POSTS'] = match.group(1)
                    break
            except:
                pass
        
        # Profile image
        for sel in ["img[src*='avatar-imgs']", "img[src*='avatar']", "div[style*='whitesmoke'] img[src*='cloudfront.net']"]:
            try:
                img = driver.find_element(By.CSS_SELECTOR, sel)
                src = img.get_attribute('src')
                if src and ('avatar' in src or 'cloudfront.net' in src):
                    src = src.replace('/thumbnail/', '/')
                    data['IMAGE'] = src
                    break
            except:
                pass
        
        # Recent post
        if data['POSTS'] and data['POSTS'] != '0':
            time.sleep(1)
            post_data = scrape_recent_post(driver, nickname)
            data['LAST POST'] = clean_data(post_data['LPOST'])
            data['LAST POST TIME'] = post_data.get('LDATE-TIME', '')
        
        log_msg(f"✅ Extracted: {data['GENDER']}, {data['CITY']}, Posts: {data['POSTS']}")
        return data
    
    except WebDriverException:
        log_msg(f"⚠️ Browser crashed for {nickname}")
        return None
    except Exception as e:
        log_msg(f"❌ Error: {str(e)[:50]}")
        return None
