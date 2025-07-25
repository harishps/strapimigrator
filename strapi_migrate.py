import os
import argparse
import csv
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone
from pathlib import Path

# Load environment variables
env_path = Path(__file__).resolve().parent / '.strapi.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    print(f"✅ Loaded environment variables from {env_path}")
else:
    print(f"❌ .env file not found at {env_path}")
    exit(1)

SOURCE_API = os.getenv("SOURCE_API")
SOURCE_TOKEN = os.getenv("SOURCE_TOKEN")
DEST_API = os.getenv("DEST_API")
DEST_TOKEN = os.getenv("DEST_TOKEN")

if not SOURCE_API or not SOURCE_TOKEN or not DEST_API or not DEST_TOKEN:
    print("❌ One or more required environment variables are missing. Please check your .strapi.env file.")
    exit(1)

HEADERS_SOURCE = {"Authorization": f"Bearer {SOURCE_TOKEN}"}
HEADERS_DEST = {"Authorization": f"Bearer {DEST_TOKEN}"}
REPORT_FILE = "migration_report.csv"

def fetch_entries(collection, match_field):
    entries = []
    page = 1
    while True:
        url = f"{SOURCE_API}/api/{collection}?pagination[pageSize]=100&pagination[page]={page}&filters[publishedAt][$notNull]=true&populate=*"
        print(f"🔄 Fetching page {page} from {url}")
        res = requests.get(url, headers=HEADERS_SOURCE)
        res.raise_for_status()
        data = res.json().get("data", [])
        if data:
            print(f"🔍 Sample entry structure: {data[0]}")
        if not data:
            break
        for entry in data:
            attributes = entry.get("attributes") if "attributes" in entry else entry
            if not isinstance(attributes, dict) or not attributes:
                print(f"⚠️ Entry ID {entry.get('id')} – invalid or empty attributes, skipping.")
                continue
            attributes["id"] = entry["id"]
            match_value = attributes.get(match_field)
            attributes["match_field"] = match_value
            if not match_value:
                print(f"⚠️ Entry ID {entry.get('id')} – missing or null '{match_field}', skipping.")
                continue
            print(f"🔍 Entry ID {entry.get('id')} – Match Field '{match_field}': {match_value}")
            entries.append(attributes)
        page += 1
    return entries

def sanitize_payload(entry):
    print(f"   🔧 Raw entry before sanitize: {entry}")
    payload = {
        "agendaFormatName": entry["agendaFormatName"],
        "agendaFormatOrder": entry["agendaFormatOrder"]
    }
    if not payload:
        print("   ⚠️ No payload to send, skipping.")
    print(f"   🔍 Payload: {payload}")
    return payload

def find_existing_entry(collection, match_field, match_value):
    url = f"{DEST_API}/api/{collection}?filters[{match_field}][$eq]={match_value}"
    print(f"🔍 Checking existing entry with URL: {url}")
    res = requests.get(url, headers=HEADERS_DEST)
    res.raise_for_status()
    data = res.json().get("data", [])
    print(f"🔍 Existing entry data: {data}")
    # Only return documentId (do not use outer id for update)
    if data and isinstance(data[0], dict):
        return {
            "documentId": data[0].get("attributes", {}).get("documentId")
        }
    return None

def migrate_collection(collection, match_field, dry_run=False):
    entries = fetch_entries(collection, match_field)
    print(f"📦 Total published entries fetched: {len(entries)}")
    report_rows = []

    for entry in entries:
        match_value = entry.get(match_field)
        print(f"🔄 Processing entry: {match_value}")
        payload = sanitize_payload(entry)
        post_payload = {"data": payload}
        existing_entry = find_existing_entry(collection, match_field, match_value)
        update_doc_id = existing_entry.get("documentId") if existing_entry else None

        try:
            if existing_entry:
                print(f"♻️ Updating existing entry: {match_value}")
                print(f"   🧪 Dry run: {dry_run}")
                if not dry_run:
                    url = f"{DEST_API}/api/{collection}/{update_doc_id}"
                    res = requests.put(url, headers=HEADERS_DEST, json=post_payload)
                    if res.status_code >= 400:
                        print(f"❌ Update error {res.status_code} – {res.text}")
                    res.raise_for_status()
                action = "updated"
                error = ""
            else:
                print(f"➕ Creating new entry: {match_value}")
                print(f"   🧪 Dry run: {dry_run}")
                if not dry_run:
                    url = f"{DEST_API}/api/{collection}"
                    res = requests.post(url, headers=HEADERS_DEST, json=post_payload)
                    if res.status_code >= 400:
                        print(f"❌ Create error {res.status_code} – {res.text}")
                    res.raise_for_status()
                action = "created"
                error = ""
            report_rows.append({
                "entry": entry.get(match_field),
                "action": action,
                "error": error,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "dry_run": dry_run
            })
        except Exception as e:
            print(f"❌ Failed to {('update' if update_doc_id else 'create')}: {entry.get(match_field)} – {e}")
            report_rows.append({
                "entry": entry.get(match_field),
                "action": "failed",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "dry_run": dry_run
            })
        else:
            # If response object exists and status is not successful, print the content for debugging
            if 'res' in locals() and res.status_code >= 400:
                print(f"❌ API Error ({res.status_code}): {res.text}")

    with open(REPORT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["entry", "action", "timestamp", "error", "dry_run"])
        writer.writeheader()
        for row in report_rows:
            writer.writerow(row)
    print(f"📄 Migration report saved to {REPORT_FILE}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--collection", required=True, help="Collection name to migrate")
    parser.add_argument("--match-field", required=True, help="Field used to match existing records")
    parser.add_argument("--dry-run", action="store_true", help="Perform dry run without data creation")
    args = parser.parse_args()


    migrate_collection(args.collection, match_field=args.match_field, dry_run=args.dry_run)

# Confirmation print at the end of the script
print("✅ Script completed. Migration report generated.")
