#!/usr/bin/env python3
"""
DamaDam Online Profile Scraper v3.1
Scrapes all online users with no limit
Runs every 15 minutes via GitHub Actions
"""

import sys
import time
import random
from collections import defaultdict
from core_scraper import *
from browser_auth import setup_browser, restart_browser, login_to_damadam
from profile_scraper import scrape_profile
from sheets_manager import SheetsManager

def main():
    """Main execution for Online Scraper"""
    print("\n" + "="*60)
    print("🌐 DamaDam Online Profile Scraper v3.1")
    print("🎯 Mode: Online Users Only (No Limit)")
    print("⏰ Scheduled: Every 15 minutes")
    print("="*60)
    
    driver = setup_browser()
    if not driver:
        print("\n❌ Browser setup failed")
        sys.exit(1)
    
    try:
        if not login_to_damadam(driver):
            print("\n❌ Login failed")
            driver.quit()
            sys.exit(1)
        
        sheets = SheetsManager()
        if not sheets.setup():
            print("\n❌ Sheets setup failed")
            driver.quit()
            sys.exit(1)
        
        # Get online users only
        targets = sheets.get_online_nicknames(driver)
        
        if not targets:
            print("\n⚠️ No online users found")
            driver.quit()
            return
        
        print(f"\n🚀 Processing {len(targets)} online profiles...")
        print("-"*60)
        
        success = failed = 0
        run_stats = defaultdict(int)
        status_counts = defaultdict(int)
        start_time = time.time()
        
        for i, target in enumerate(targets, 1):
            nickname = target['nickname']
            source = target.get('source', 'Online')
            
            eta = calculate_eta(i-1, len(targets), start_time)
            print(f"\n[{i}/{len(targets)}] {nickname} (Online) | ETA: {eta}")
            
            profile = None
            for attempt in range(2):
                profile = scrape_profile(driver, nickname)
                if profile is None and attempt == 0:
                    driver = restart_browser(driver)
                    if driver and login_to_damadam(driver):
                        continue
                    else:
                        break
                break
            
            if profile:
                profile['SOURCE'] = source
                write_result = sheets.write_profile(profile)
                
                if write_result.get("status") in {"new", "updated", "unchanged"}:
                    success += 1
                    run_stats[write_result["status"]] += 1
                    status_counts[profile.get("STATUS", "Unknown")] += 1
                    
                    change_summary = write_result.get("changed_fields", [])
                    cleaned_summary = [field for field in change_summary if field not in HIGHLIGHT_EXCLUDE_COLUMNS]
                    
                    if write_result["status"] == "new":
                        remark_detail = "New online profile"
                    elif write_result["status"] == "updated":
                        if cleaned_summary:
                            trimmed = cleaned_summary[:5]
                            if len(cleaned_summary) > 5:
                                trimmed.append("…")
                            remark_detail = f"Updated: {', '.join(trimmed)}"
                        else:
                            remark_detail = "Updated (no key changes)"
                    else:
                        remark_detail = "No data changes"
                    
                    log_msg(f"✅ {nickname} {write_result['status']}")
                else:
                    failed += 1
                    error_msg = write_result.get("error", "Write failed")
                    log_msg(f"❌ {nickname} failed: {error_msg}")
            else:
                failed += 1
                log_msg(f"❌ {nickname} scraping failed")
            
            # Batch delay
            if BATCH_SIZE > 0 and i % BATCH_SIZE == 0 and i < len(targets):
                log_msg(f"⏸️ Batch pause (processed {i}/{len(targets)})")
                time.sleep(5)
            
            # Random delay between profiles
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        
        # Final summary
        print("\n" + "="*60)
        print(f"✅ Online Scraping Complete:")
        print(f"   ✓ Success: {success}")
        print(f"   ✗ Failed: {failed}")
        if len(targets) > 0:
            print(f"   📊 Success Rate: {(success/len(targets)*100):.1f}%")
        if run_stats:
            print(f"   ➕ New: {run_stats['new']}")
            print(f"   🔁 Updated: {run_stats['updated']}")
            print(f"   📎 Unchanged: {run_stats['unchanged']}")
        
        # Status breakdown
        if status_counts:
            print(f"\n📊 Status Breakdown:")
            for status, count in status_counts.items():
                print(f"   {status}: {count}")
        
        print("="*60)
        
        # Update dashboard
        metrics = {
            "Run Number": 1,
            "Last Run": get_pkt_time().strftime("%d-%b-%y %I:%M %p"),
            "Profiles Processed": len(targets),
            "Success": success,
            "Failed": failed,
            "New Profiles": run_stats.get('new', 0),
            "Updated Profiles": run_stats.get('updated', 0),
            "Unchanged Profiles": run_stats.get('unchanged', 0)
        }
        sheets.update_dashboard(metrics)
        
        print(f"🎯 Next run scheduled in 15 minutes")
    
    except KeyboardInterrupt:
        print("\n\n⚠️ INTERRUPTED BY USER")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            driver.quit()
            print("🔒 Browser closed")
        except:
            pass

if __name__ == "__main__":
    main()
