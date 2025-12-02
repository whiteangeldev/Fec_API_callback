import functions_framework
import os
import requests
import time
from datetime import date, timedelta
from google.cloud import bigquery
from dotenv import load_dotenv
from requests.exceptions import ReadTimeout, ConnectionError

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
# BigQuery Client Initialization
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
BQ_BATCH_SIZE = 8000  # BigQuery limit is ~10k rows


# ---------------------------------------------------
# Retry-enabled, backoff-enabled FEC Fetch
# ---------------------------------------------------
def fetch_fec_page(period, page, retries=5):
    params = {
        "api_key": FEC_API_KEY,
        "two_year_transaction_period": period,
        "sort": "-contribution_receipt_date",
        "per_page": PER_PAGE,
        "page": page,
        "is_individual": True,
    }

    backoff = 2

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(
                "https://api.open.fec.gov/v1/schedules/schedule_a/",
                params=params,
                timeout=60,  # Increased timeout
            )
            resp.raise_for_status()
            return resp.json()

        except ReadTimeout:
            print(
                f"⏳ Timeout (period {period}, page {page}), attempt {attempt}/{retries}"
            )
        except ConnectionError:
            print(
                f"🔌 Connection error (period {period}, page {page}), attempt {attempt}/{retries}"
            )
        except Exception as e:
            print(f"❌ Unexpected fetch error: {e}")
            raise

        print(f"   🔁 Retrying in {backoff}s...")
        time.sleep(backoff)
        backoff *= 2

    raise RuntimeError(
        f"❌ Failed to fetch period {period}, page {page} after retries."
    )


# ---------------------------------------------------
# Batch insert into BigQuery
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
# MAIN ETL FUNCTION (Cloud Function handler)
# ---------------------------------------------------
@functions_framework.http
def refresh_fec_data(request):
    print("🚀 Starting FEC data refresh")

    # 1) Clear staging table
    print("🧹 Clearing staging table…")
    client.query(f"TRUNCATE TABLE `{STAGING_TABLE}`").result()

    today = date.today()
    six_years_ago = today - timedelta(days=365 * YEARS_BACK)

    # 2-year cycles
    current_cycle = today.year if today.year % 2 == 0 else today.year + 1
    periods = [current_cycle - 2 * i for i in range(0, 3)]
    print(f"📅 Fetching cycles: {periods}")

    all_rows = []

    # ---------------------------------------------------
    # Fetch data from FEC
    # ---------------------------------------------------
    for period in periods:
        print(f"\n=== 🔄 Period: {period} ===")

        page = 1
        empty_pages = 0

        while True:
            print(f"   📄 Fetching page {page}...")

            try:
                data = fetch_fec_page(period, page)
            except Exception as e:
                print(f"❌ Fatal fetch error: {e}")
                break  # Move to next period

            results = data.get("results", [])

            # Early break if repeated empty pages
            if not results:
                empty_pages += 1
                if empty_pages >= 2:
                    print("⚠️ Two empty pages in a row → stopping early.")
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

                if dt < six_years_ago:
                    continue

                amt = r.get("contribution_receipt_amount") or 0
                try:
                    amt = float(amt)
                except:
                    continue
                if amt <= 125:
                    continue

                street1 = r.get("contributor_street_1") or ""
                street2 = r.get("contributor_street_2") or ""
                street_addr = f"{street1} {street2}".strip()

                row = {
                    "cmte_id": r.get("committee_id"),
                    "ind_rpt_tp": r.get("report_type"),
                    "ind_transaction_tp": r.get("line_number_label"),
                    "ind_transaction_pgi": r.get("contribution_receipt_step"),
                    "ind_entity_tp": r.get("entity_type"),
                    "ind_name": r.get("contributor_name"),
                    "ind_street_address": street_addr,
                    "ind_city": r.get("contributor_city"),
                    "ind_state": r.get("contributor_state"),
                    "ind_zip_code": r.get("contributor_zip"),
                    "ind_employer": r.get("contributor_employer"),
                    "ind_occupation": r.get("contributor_occupation"),
                    "ind_transaction_dt": dt,
                    "ind_transaction_amt": amt,
                    "ind_tran_id": r.get("transaction_id"),
                }

                all_rows.append(row)

            # Pagination
            pagination = data.get("pagination", {})
            if page >= pagination.get("pages", 0):
                break

            page += 1
            time.sleep(0.3)  # Avoid rate limiting

    print(f"\n📦 Total collected rows: {len(all_rows)}")

    # ---------------------------------------------------
    # Insert into staging
    # ---------------------------------------------------
    if all_rows:
        insert_bq_rows(STAGING_TABLE, all_rows)
    else:
        print("⚠️ No rows to insert.")

    # ---------------------------------------------------
    # Replace final table
    # ---------------------------------------------------
    print("📤 Staging → Final table")
    client.query(f"TRUNCATE TABLE `{FINAL_TABLE}`").result()
    client.query(
        f"INSERT INTO `{FINAL_TABLE}` SELECT * FROM `{STAGING_TABLE}`"
    ).result()

    print("🎉 Finished ETL refresh successfully!")
    return "Refresh completed", 200


# ---------------------------------------------------
# LOCAL TEST ENTRYPOINT
# ---------------------------------------------------
if __name__ == "__main__":
    print("⚙️ Running ETL locally...")
    refresh_fec_data(None)
