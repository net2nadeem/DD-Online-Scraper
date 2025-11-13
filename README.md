# 🌐 DamaDam Online Profile Scraper

Automated scraper that monitors and scrapes all online users from DamaDam every 15 minutes.

## Features

✅ **Scrapes all online users** (no limit)  
✅ **Runs every 15 minutes** via GitHub Actions  
✅ **Updates only if new data exists**  
✅ **Shared Google Sheet** with Target Scraper  
✅ **Append-only updates** (no row 2 insertion)  
✅ **Fixed gspread deprecation warnings**  
✅ **Modern Google Auth** (google-auth instead of oauth2client)  

## Setup

### 1. Repository Secrets

Add these secrets to your GitHub repository:

```
DAMADAM_USERNAME=your_username
DAMADAM_PASSWORD=your_password
DAMADAM_USERNAME_2=backup_username (optional)
DAMADAM_PASSWORD_2=backup_password (optional)
GOOGLE_SHEET_URL=https://docs.google.com/spreadsheets/d/your_sheet_id
GOOGLE_CREDENTIALS_JSON={"type":"service_account",...}
```

### 2. Google Sheets Setup

1. Create a Google Sheet with these tabs:
   - **Profiles** - Main data storage
   - **Target** - For target scraper coordination
   - **Tags** - Optional tag mapping
   - **Logs** - Change tracking
   - **Dashboard** - Run statistics

2. Share the sheet with your service account email

### 3. Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing
3. Enable Google Sheets API and Google Drive API
4. Create a Service Account
5. Download the JSON key file
6. Copy the entire JSON content to `GOOGLE_CREDENTIALS_JSON` secret

## How It Works

1. **Every 15 minutes**, GitHub Actions triggers the scraper
2. Scraper logs into DamaDam and fetches online users list
3. For each online user, scrapes their complete profile
4. **Appends new data** to Google Sheets (never overwrites)
5. Updates existing profiles only if data has changed
6. Logs all changes and maintains dashboard statistics

## Data Structure

The scraper collects:
- Profile image, nickname, tags
- Last post URL and timestamp
- Friend status, city, gender, marital status
- Age, join date, followers, posts count
- Profile link, intro, verification status
- Source (Online) and scraping timestamp

## Scheduling

- **Primary**: Every 15 minutes via cron schedule
- **Manual**: Can be triggered manually with custom parameters
- **Timeout**: 14 minutes max (to avoid overlap with next run)

## Monitoring

- Check the **Dashboard** sheet for run statistics
- Check the **Logs** sheet for detailed change tracking
- GitHub Actions logs show real-time progress
- Failed runs upload debug artifacts

## Coordination with Target Scraper

Both scrapers use the same Google Sheet but:
- Online Scraper: Focuses on currently online users
- Target Scraper: Processes specific targets from Target sheet
- Both append data (no conflicts)
- Shared formatting and structure
