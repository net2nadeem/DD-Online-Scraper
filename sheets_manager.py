#!/usr/bin/env python3
"""
Google Sheets Manager - Optimized for append-only operations
"""

import time
import json
import gspread
from core_scraper import *

class SheetsManager:
    def __init__(self):
        self.client = client
        self.profiles_sheet = None
        self.target_sheet = None
        self.tags_sheet = None
        self.log_sheet = None
        self.dashboard_sheet = None
        self.tags_mapping = {}
        self.existing_profiles = {}
    
    def setup(self):
        """Setup sheets"""
        try:
            print("\n📊 Connecting to Google Sheets...")
            
            spreadsheet = self.client.open_by_url(SHEET_URL)
            
            def get_or_create_worksheet(name, cols=None, rows=1000):
                try:
                    ws = spreadsheet.worksheet(name)
                    return ws
                except gspread.exceptions.WorksheetNotFound:
                    return spreadsheet.add_worksheet(title=name, rows=rows, cols=cols or 20)
            
            self.profiles_sheet = get_or_create_worksheet("Profiles", len(COLUMN_ORDER))
            self.target_sheet = get_or_create_worksheet("Target", 4)
            
            # Initialize headers if sheet is empty
            if not self.profiles_sheet.get_all_values():
                self.profiles_sheet.append_row(COLUMN_ORDER)
            if not self.target_sheet.get_all_values():
                self.target_sheet.append_row(["Nickname", "Status", "Remarks", "Source"])
            
            try:
                self.tags_sheet = spreadsheet.worksheet("Tags")
                self.load_tags_mapping()
            except gspread.exceptions.WorksheetNotFound:
                self.tags_sheet = None
            
            try:
                self.log_sheet = spreadsheet.worksheet(LOG_SHEET_NAME)
            except gspread.exceptions.WorksheetNotFound:
                self.log_sheet = spreadsheet.add_worksheet(title=LOG_SHEET_NAME, rows=2000, cols=len(LOG_HEADERS))
                self.log_sheet.append_row(LOG_HEADERS)
            
            try:
                self.dashboard_sheet = spreadsheet.worksheet(DASHBOARD_SHEET_NAME)
            except gspread.exceptions.WorksheetNotFound:
                self.dashboard_sheet = spreadsheet.add_worksheet(title=DASHBOARD_SHEET_NAME, rows=50, cols=8)
            
            self.load_existing_profiles()
            self.format_profiles_sheet()
            
            return True
        
        except Exception as e:
            print(f"❌ Sheets setup failed: {e}")
            return False
    
    def format_profiles_sheet(self):
        """Format sheets without clearing data"""
        try:
            # Apply formatting to entire sheet
            self.safe_update(
                self.profiles_sheet.format,
                "A:R",
                {
                    "backgroundColor": {"red": 1, "green": 1, "blue": 1},
                    "textFormat": {"fontFamily": "Bona Nova SC", "fontSize": 8}
                }
            )
            
            # Format header row
            self.safe_update(
                self.profiles_sheet.format,
                "A1:R1",
                {
                    "textFormat": {"bold": True, "fontSize": 9, "fontFamily": "Bona Nova SC"},
                    "horizontalAlignment": "CENTER"
                }
            )
        except Exception as e:
            log_msg(f"⚠️ Formatting failed: {e}")
    
    def load_tags_mapping(self):
        """Load tags mapping"""
        try:
            all_data = self.tags_sheet.get_all_values()
            if not all_data or len(all_data) < 2:
                return
            
            headers = all_data[0]
            
            for col_idx, tag_name in enumerate(headers):
                if not tag_name or not tag_name.strip():
                    continue
                
                tag_name_clean = tag_name.strip()
                
                for row_idx in range(1, len(all_data)):
                    if col_idx < len(all_data[row_idx]):
                        nickname = all_data[row_idx][col_idx].strip()
                        if nickname:
                            nickname_lower = nickname.lower()
                            if nickname_lower in self.tags_mapping:
                                self.tags_mapping[nickname_lower] += f", {tag_name_clean}"
                            else:
                                self.tags_mapping[nickname_lower] = tag_name_clean
            
            log_msg(f"📋 Loaded {len(self.tags_mapping)} tags")
        except Exception as e:
            log_msg(f"⚠️ Tags loading failed: {e}")
    
    def get_tags_for_nickname(self, nickname):
        """Get tags for nickname"""
        return self.tags_mapping.get(nickname.lower(), "")
    
    def load_existing_profiles(self):
        """Load existing profiles for duplicate checking"""
        try:
            self.existing_profiles = {}
            rows = self.profiles_sheet.get_all_values()[1:]  # Skip header
            for idx, row in enumerate(rows, start=2):
                if row and len(row) > 1:
                    nickname = row[1].strip().lower()  # NICK NAME column
                    if nickname:
                        self.existing_profiles[nickname] = {'row': idx, 'data': row}
            log_msg(f"📋 Loaded {len(self.existing_profiles)} existing profiles")
        except Exception as e:
            log_msg(f"⚠️ Profile loading failed: {e}")
    
    def get_target_nicknames(self):
        """Get target nicknames with Pending status"""
        try:
            rows = self.target_sheet.get_all_values()[1:]  # Skip header
            targets = []
            
            for idx, row in enumerate(rows, start=2):
                if not row or len(row) == 0:
                    continue
                
                nickname = row[0].strip() if len(row) > 0 else ""
                status = row[1].strip() if len(row) > 1 else ""
                source = row[3].strip() if len(row) > 3 else "Manual"
                
                if nickname and status.lower() in ["pending", "pending 🚨"]:
                    targets.append({'nickname': nickname, 'row': idx, 'source': source})
            
            if MAX_PROFILES_PER_RUN > 0:
                targets = targets[:MAX_PROFILES_PER_RUN]
            
            print(f"  ✅ Found {len(targets)} PENDING targets")
            return targets
        except Exception as e:
            print(f"  ❌ Failed to get targets: {e}")
            return []
    
    def get_online_nicknames(self, driver):
        """Get online nicknames"""
        try:
            print("\n  🌐 Fetching online users...")
            driver.get("https://damadam.pk/online_kon/")
            time.sleep(2)
            
            nicknames = []
            list_items = driver.find_elements(By.CSS_SELECTOR, "li.mbl.cl.sp")
            
            for li in list_items:
                try:
                    bold_elem = li.find_element(By.TAG_NAME, "b")
                    nick = bold_elem.text.strip()
                    
                    if nick and len(nick) >= 3 and not nick.isdigit() and any(c.isalpha() for c in nick):
                        nicknames.append(nick)
                except:
                    continue
            
            # Fallback method
            if not nicknames:
                profile_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/users/']")
                for link in profile_links:
                    href = link.get_attribute('href')
                    if href and '/users/' in href:
                        nick = href.split('/users/')[-1].rstrip('/')
                        if nick and len(nick) >= 3 and not nick.isdigit() and any(c.isalpha() for c in nick) and nick not in nicknames:
                            nicknames.append(nick)
            
            print(f"  ✅ Found {len(nicknames)} online users")
            return [{'nickname': nick, 'row': 0, 'source': 'Online'} for nick in nicknames]
        
        except Exception as e:
            print(f"  ❌ Failed to get online users: {e}")
            return []
    
    def safe_update(self, func, *args, max_retries=3, **kwargs):
        """Safe update with retry logic"""
        for attempt in range(max_retries):
            try:
                result = func(*args, **kwargs)
                time.sleep(SHEET_WRITE_DELAY)
                return result
            except Exception as e:
                if '429' in str(e) or 'quota' in str(e).lower():
                    wait_time = (attempt + 1) * 5
                    log_msg(f"⏳ Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    if attempt == max_retries - 1:
                        log_msg(f"❌ Update failed after {max_retries} attempts: {e}")
                        return None
        return None
    
    def update_target_status(self, row_num, status, remarks):
        """Update target status"""
        try:
            self.safe_update(self.target_sheet.update, values=[[status]], range_name=f'B{row_num}')
            self.safe_update(self.target_sheet.update, values=[[remarks]], range_name=f'C{row_num}')
        except Exception as e:
            log_msg(f"⚠️ Target status update failed: {e}")
    
    def apply_link_formulas(self, row_idx, data):
        """Apply link formulas to specific cells"""
        for col_name in LINK_COLUMNS:
            value = data.get(col_name)
            if not value:
                continue
            
            col_idx = COLUMN_TO_INDEX[col_name]
            cell = f"{column_letter(col_idx)}{row_idx}"
            
            if col_name == "IMAGE":
                formula = f'=IMAGE("{value}", 4, 50, 50)'
            elif col_name == "LAST POST":
                formula = f'=HYPERLINK("{value}", "Post")'
            else:
                formula = f'=HYPERLINK("{value}", "Profile")'
            
            self.safe_update(
                self.profiles_sheet.update,
                values=[[formula]],
                range_name=cell,
                value_input_option='USER_ENTERED'
            )
    
    def log_change(self, nickname, change_type, changed_fields, before=None, after=None):
        """Log changes to log sheet"""
        if not self.log_sheet:
            return
        
        try:
            timestamp = get_pkt_time().strftime("%d-%b-%y %I:%M %p")
            fields_text = ", ".join(changed_fields) if changed_fields else "-"
            before_text = json.dumps(before or {}, ensure_ascii=False)[:500]  # Limit length
            after_text = json.dumps(after or {}, ensure_ascii=False)[:500]   # Limit length
            
            self.safe_update(
                self.log_sheet.append_row,
                [timestamp, nickname, change_type, fields_text, before_text, after_text]
            )
        except Exception as e:
            log_msg(f"⚠️ Logging failed: {e}")
    
    def update_dashboard(self, metrics):
        """Update dashboard with run metrics"""
        if not self.dashboard_sheet:
            return
        
        try:
            existing_data = self.dashboard_sheet.get_all_values()
            expected_headers = ["Run#", "Timestamp", "Profiles", "Success", "Failed", "New", "Updated", "Unchanged"]
            
            if not existing_data or existing_data[0] != expected_headers:
                self.safe_update(self.dashboard_sheet.clear)
                self.safe_update(self.dashboard_sheet.append_row, expected_headers)
                self.safe_update(
                    self.dashboard_sheet.format,
                    "A1:H1",
                    {
                        "textFormat": {"bold": True, "fontSize": 9, "fontFamily": "Bona Nova SC"},
                        "horizontalAlignment": "CENTER"
                    }
                )
            
            row_data = [
                metrics.get("Run Number", ""),
                metrics.get("Last Run", ""),
                metrics.get("Profiles Processed", 0),
                metrics.get("Success", 0),
                metrics.get("Failed", 0),
                metrics.get("New Profiles", 0),
                metrics.get("Updated Profiles", 0),
                metrics.get("Unchanged Profiles", 0)
            ]
            self.safe_update(self.dashboard_sheet.append_row, row_data)
        except Exception as e:
            log_msg(f"⚠️ Dashboard update failed: {e}")
    
    def write_profile(self, data):
        """Write profile data - APPEND ONLY, no row 2 insertion"""
        nickname = data.get("NICK NAME", "").strip()
        if not nickname:
            return {"status": "error", "error": "Missing nickname", "changed_fields": []}
        
        # Add tags
        data['TAGS'] = self.get_tags_for_nickname(nickname)
        
        # Prepare row values
        row_values = []
        for col in COLUMN_ORDER:
            if col == "IMAGE":
                cell_value = ""  # Will be filled by formula
            elif col == "PROFILE LINK":
                cell_value = "Profile" if data.get(col) else ""
            elif col == "LAST POST":
                cell_value = "Post" if data.get(col) else ""
            else:
                cell_value = clean_data(data.get(col, ""))
            row_values.append(cell_value)
        
        nickname_lower = nickname.lower()
        existing = self.existing_profiles.get(nickname_lower)
        
        if existing:
            # Check for changes
            before_snapshot = {
                COLUMN_ORDER[idx]: (existing['data'][idx] if idx < len(existing['data']) else "")
                for idx in range(len(COLUMN_ORDER))
            }
            
            changed_indices = []
            for idx, col in enumerate(COLUMN_ORDER):
                old_val = before_snapshot.get(col, "") or ""
                new_val = row_values[idx] or ""
                if old_val != new_val:
                    changed_indices.append(idx)
            
            if not changed_indices:
                self.log_change(nickname, "UNCHANGED", [], before_snapshot, {col: data.get(col, "") for col in COLUMN_ORDER})
                return {"status": "unchanged", "changed_fields": []}
            
            # Update existing profile by appending new data
            self.safe_update(self.profiles_sheet.append_row, row_values)
            new_row = len(self.profiles_sheet.get_all_values())
            self.apply_link_formulas(new_row, data)
            
            changed_fields = [COLUMN_ORDER[idx] for idx in changed_indices]
            self.log_change(nickname, "UPDATED", changed_fields, before_snapshot, {col: data.get(col, "") for col in COLUMN_ORDER})
            
            # Update our cache
            self.existing_profiles[nickname_lower] = {'row': new_row, 'data': row_values}
            
            return {"status": "updated", "changed_fields": changed_fields}
        else:
            # New profile - append to end
            self.safe_update(self.profiles_sheet.append_row, row_values)
            new_row = len(self.profiles_sheet.get_all_values())
            self.apply_link_formulas(new_row, data)
            
            # Add to cache
            self.existing_profiles[nickname_lower] = {'row': new_row, 'data': row_values}
            
            changed_fields = list(COLUMN_ORDER)
            self.log_change(nickname, "NEW", changed_fields, None, {col: data.get(col, "") for col in COLUMN_ORDER})
            
            return {"status": "new", "changed_fields": changed_fields}
