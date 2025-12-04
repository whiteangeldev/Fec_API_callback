import functions_framework
import os
import requests
import time
from datetime import date, timedelta
from google.cloud import bigquery
from dotenv import load_dotenv
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError

# ---------------------------------------------------
# Load environment variables
# ---------------------------------------------------
load_dotenv()

FEC_API_KEY = os.environ.get("FEC_API_KEY")
if not FEC_API_KEY:
    raise ValueError("FEC_API_KEY environment variable is required.")

BQ_PROJECT = os.environ.get("BQ_PROJECT")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

# ---------------------------------------------------
# BigQuery client
# ---------------------------------------------------
if GOOGLE_APPLICATION_CREDENTIALS:
    client = bigquery.Client.from_service_account_json(
        GOOGLE_APPLICATION_CREDENTIALS, project=BQ_PROJECT
    )
else:
    client = bigquery.Client(project=BQ_PROJECT)

if not BQ_PROJECT:
    BQ_PROJECT = client.project
    print(f"Using BigQuery project: {BQ_PROJECT}")

BQ_DATASET = "reporting"
STAGING_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.combined_report_staging"
FINAL_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.combined_report_all_years_new"

PER_PAGE = 100
YEARS_BACK = 6
BQ_BATCH_SIZE = 8000

# ---------------------------------------------------
# Select good cycles (no 2026 mass-data cycle)
# ---------------------------------------------------
def get_cycles():
    today = date.today()

    # pick the most recent *completed* cycle (even year <= today)
    if today.year % 2 == 0:
        latest_cycle = today.year
    else:
        latest_cycle = today.year - 1

    # cycles: latest_cycle, -2, -4
    return [latest_cycle - 2*i for i in range(3)]


# ---------------------------------------------------
# Retry-enabled FEC Fetch with 429 safe handling
# ---------------------------------------------------
def fetch_fec_page(period, page, min_date=None, retries=10):
    params = {
        "api_key": FEC_API_KEY,
        "two_year_transaction_period": period,
        "sort": "-contribution_receipt_date",
        "per_page": PER_PAGE,
        "page": page,
        "is_individual": True,
        # IMPORTANT: filter at server → reduces data massively
        "min_amount": 125,
    }
    
    # Add date filtering at API level to reduce data transfer
    if min_date:
        params["min_date"] = min_date.strftime("%Y-%m-%d")

    rate_limit_wait = 30  # Start with 30 seconds for rate limits
    
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(
                "https://api.open.fec.gov/v1/schedules/schedule_a/",
                params=params,
                timeout=60,
            )
            resp.raise_for_status()
            return resp.json()

        except HTTPError as e:
            if e.response.status_code == 429:
                print(f"🔒 Rate limit (attempt {attempt}/{retries}) — waiting {rate_limit_wait}s...")
                time.sleep(rate_limit_wait)
                rate_limit_wait = min(rate_limit_wait * 1.5, 300)  # Max 5 minutes
                continue
            
            # Retry 500/502/503 errors (server issues are often temporary)
            if e.response.status_code in [500, 502, 503]:
                print(f"🔧 Server error {e.response.status_code} (attempt {attempt}/{retries})")
                if attempt < retries:
                    wait = 5 * attempt  # 5s, 10s, 15s, 20s...
                    print(f"   🔁 Retrying in {wait}s...")
                    time.sleep(wait)
                    continue
            
            # Other HTTP errors: fail immediately
            print(f"❌ HTTP Error {e.response.status_code}: {e}")
            raise

        except (ReadTimeout, ConnectionError) as e:
            print(f"⏳ Timeout/connection error (attempt {attempt}/{retries})")
            if attempt < retries:
                wait = 2 ** attempt  # Exponential backoff
                print(f"   🔁 Waiting {wait}s before retry...")
                time.sleep(wait)
            continue
        
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise

    raise RuntimeError(f"Failed to fetch period {period}, page {page} after {retries} retries")


# ---------------------------------------------------
# Insert with batching
# ---------------------------------------------------
def insert_bq_rows(table, rows):
    total = len(rows)
    print(f"📤 Inserting {total} rows into {table}")

    for i in range(0, total, BQ_BATCH_SIZE):
        batch = rows[i : i + BQ_BATCH_SIZE]
        errors = client.insert_rows_json(table, batch)

        if errors:
            print(f"❌ BigQuery insert error: {errors}")
            raise RuntimeError(errors)

        print(f"   ✔ Inserted batch of {len(batch)} rows")


# ---------------------------------------------------
# MAIN FUNCTION
# ---------------------------------------------------
@functions_framework.http
def refresh_fec_data(request):
    print("🚀 Starting optimized FEC ETL")

    # Clear both staging and final tables at start
    print("🧹 Clearing staging and final tables...")
    client.query(f"TRUNCATE TABLE `{STAGING_TABLE}`").result()
    client.query(f"TRUNCATE TABLE `{FINAL_TABLE}`").result()

    today = date.today()
    six_years_ago = today - timedelta(days=365 * YEARS_BACK)
    
    cycles = get_cycles()
    print(f"📅 Cycles to fetch: {cycles}")

    # Buffer for batch inserts
    batch_buffer = []
    BATCH_INSERT_SIZE = 1000  # Insert every 1000 rows
    total_rows_inserted = 0
    total_rows_collected = 0
    seen_transaction_ids = set()

    # ---------------------------------------------------
    # Fetch data cycle by cycle, inserting as we go
    # ---------------------------------------------------
    for period in cycles:
        print(f"\n=== 🔄 Period {period} ===")

        page = 1
        empty_pages = 0
        MAX_PAGES_PER_PERIOD = 2000  # Safety limit to prevent infinite loops

        while True:
            # Safety check: stop if too many pages
            if page > MAX_PAGES_PER_PERIOD:
                print(f"⚠️ Reached safety limit ({MAX_PAGES_PER_PERIOD} pages) - stopping period")
                break
            
            print(f"   📄 Fetching page {page}...")
            
            try:
                data = fetch_fec_page(period, page, min_date=six_years_ago)
            except RuntimeError as e:
                print(f"❌ Failed to fetch page {page} after retries: {e}")
                print(f"⏭️  Skipping page {page}, continuing with next page...")
                page += 1
                time.sleep(2)  # Brief pause before continuing
                continue  # Skip this page, continue period
            except Exception as e:
                print(f"❌ Unexpected error on page {page}: {e}")
                print(f"⏭️  Skipping page {page}, continuing with next page...")
                page += 1
                time.sleep(2)
                continue
            
            # Show total pages on first fetch
            if page == 1:
                pagination_info = data.get("pagination", {})
                total_pages = pagination_info.get("pages", "?")
                total_count = pagination_info.get("count", "?")
                print(f"   📊 API reports ~{total_pages} pages, ~{total_count:,} total records (before date filtering)")
                
            results = data.get("results", [])

            if not results:
                empty_pages += 1
                if empty_pages >= 2:
                    print("⚠️ Two empty pages → stop this cycle.")
                    break
            else:
                empty_pages = 0

            for r in results:
                dt_str = r.get("contribution_receipt_date")
                if not dt_str:
                    continue

                try:
                    dt = date.fromisoformat(dt_str)
                except:
                    continue

                # Early stop: Since data is sorted by date descending,
                # once we hit old data, all remaining pages will also be old
                if dt < six_years_ago:
                    print(f"⏹️  Reached data older than {six_years_ago.strftime('%Y-%m-%d')} - stopping period {period}")
                    empty_pages = 999  # Signal outer loop to stop
                    break  # Exit results loop

                amt = r.get("contribution_receipt_amount") or 0
                try:
                    amt = float(amt)
                except:
                    continue
                if amt <= 125:
                    continue

                street = " ".join([
                    r.get("contributor_street_1") or "",
                    r.get("contributor_street_2") or "",
                ]).strip()

                tran_id = r.get("transaction_id")
                
                # Track for duplicate detection
                if tran_id:
                    seen_transaction_ids.add(tran_id)
                
                total_rows_collected += 1

                # Show progress every 1000 rows
                if total_rows_collected % 1000 == 0:
                    name = r.get("contributor_name", "")[:25]
                    contrib_date = r.get("contribution_receipt_date", "")
                    print(f"   ✓ Collected {total_rows_collected:,} rows | Latest: {tran_id} | {contrib_date} | ${amt:>8} | {name}")

                batch_buffer.append({
                    "cmte_id": r.get("committee_id"),
                    "ind_rpt_tp": r.get("report_type"),
                    "ind_transaction_tp": r.get("line_number_label"),
                    "ind_transaction_pgi": r.get("contribution_receipt_step"),
                    "ind_entity_tp": r.get("entity_type"),
                    "ind_name": r.get("contributor_name"),
                    "ind_street_address": street,
                    "ind_city": r.get("contributor_city"),
                    "ind_state": r.get("contributor_state"),
                    "ind_zip_code": r.get("contributor_zip"),
                    "ind_employer": r.get("contributor_employer"),
                    "ind_occupation": r.get("contributor_occupation"),
                    "ind_transaction_dt": dt.isoformat(),  # Convert date to string (YYYY-MM-DD)
                    "ind_transaction_amt": amt,
                    "ind_tran_id": tran_id,
                })
                
                # Insert batch when buffer is full
                if len(batch_buffer) >= BATCH_INSERT_SIZE:
                    print(f"💾 Inserting batch of {len(batch_buffer)} rows...")
                    insert_bq_rows(STAGING_TABLE, batch_buffer)
                    total_rows_inserted += len(batch_buffer)
                    batch_buffer = []
                    print(f"   ✓ Total inserted so far: {total_rows_inserted:,}")

            # Check if we hit old data and should stop
            if empty_pages >= 999:
                break
            
            # Stop when last page reached
            pagination = data.get("pagination", {})
            if page >= pagination.get("pages", 0):
                break

            page += 1
            # Longer sleep to avoid rate limiting on large datasets
            time.sleep(0.5)
        
        # After each period, flush remaining buffer and update final table
        if batch_buffer:
            print(f"💾 Inserting final batch of {len(batch_buffer)} rows for period {period}...")
            insert_bq_rows(STAGING_TABLE, batch_buffer)
            total_rows_inserted += len(batch_buffer)
            batch_buffer = []
        
        # Copy staging to final table after each period
        print(f"📤 Updating final table with period {period} data...")
        client.query(f"TRUNCATE TABLE `{FINAL_TABLE}`").result()
        client.query(f"INSERT INTO `{FINAL_TABLE}` SELECT * FROM `{STAGING_TABLE}`").result()
        print(f"   ✓ Final table updated with {total_rows_inserted:,} rows so far")

    # ---------------------------------------------------
    # Insert any remaining rows in buffer (shouldn't be any)
    # ---------------------------------------------------
    if batch_buffer:
        print(f"\n💾 Inserting final batch of {len(batch_buffer)} rows...")
        insert_bq_rows(STAGING_TABLE, batch_buffer)
        total_rows_inserted += len(batch_buffer)
        batch_buffer = []

    # ---------------------------------------------------
    # Show final statistics
    # ---------------------------------------------------
    print(f"\n{'='*60}")
    print(f"📊 ETL Statistics:")
    print(f"   Total rows collected: {total_rows_collected:,}")
    print(f"   Total rows inserted to staging: {total_rows_inserted:,}")
    print(f"   Unique transaction IDs: {len(seen_transaction_ids):,}")
    
    # Check for duplicates
    if len(seen_transaction_ids) < total_rows_collected:
        duplicates = total_rows_collected - len(seen_transaction_ids)
        print(f"   ⚠️  WARNING: {duplicates:,} duplicate transaction IDs detected!")
    else:
        print(f"   ✅ No duplicates detected")
    print(f"{'='*60}\n")
    
    if total_rows_inserted == 0:
        print("⚠️ No data was inserted.")
        return "No data collected", 200

    # Final table already updated after each period
    print("🎉 ETL Complete - Final table updated after each period!")
    return "OK", 200


# ---------------------------------------------------
# LOCAL RUN SUPPORT
# ---------------------------------------------------
if __name__ == "__main__":
    print("⚙️ Local ETL run...")
    refresh_fec_data(None)
