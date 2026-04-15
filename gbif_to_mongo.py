import datetime, logging, time, argparse
import requests
import pymongo

# ── FILL IN YOUR REAL MONGO URI HERE ──────────────────────────────────────────
MONGO_URI = "mongodb+srv://pragnaya:REDACTED_PASSWORD@dwp-cluster.mhebkb7.mongodb.net/gbif_birds"
# ──────────────────────────────────────────────────────────────────────────────

GBIF_BASE  = "https://api.gbif.org/v1/occurrence/search"
BATCH_SIZE = 300
SLEEP_SEC  = 1.0     # wait between requests
MAX_RETRY  = 5       # retry up to 5 times on 503

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

KEEP_FIELDS = [
    "gbifID","species","genus","family","order","class","kingdom",
    "decimalLatitude","decimalLongitude","countryCode","stateProvince",
    "eventDate","year","month","day","recordedBy","institutionCode",
    "basisOfRecord","occurrenceStatus",
]

def fetch_batch(offset):
    """Fetch one page with automatic retry on 503."""
    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = requests.get(GBIF_BASE, params={
                "taxonKey":          212,
                "occurrenceStatus":  "PRESENT",
                "hasCoordinate":     "true",
                "hasGeospatialIssue":"false",
                "limit":             BATCH_SIZE,
                "offset":            offset,
            }, timeout=60)

            if resp.status_code == 503:
                wait = attempt * 30   # 30s, 60s, 90s, 120s, 150s
                log.warning(f"GBIF 503 (attempt {attempt}/{MAX_RETRY}) — waiting {wait}s then retrying...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.Timeout:
            wait = attempt * 20
            log.warning(f"Request timed out (attempt {attempt}/{MAX_RETRY}) — waiting {wait}s...")
            time.sleep(wait)

        except requests.exceptions.ConnectionError:
            wait = attempt * 15
            log.warning(f"Connection error (attempt {attempt}/{MAX_RETRY}) — waiting {wait}s...")
            time.sleep(wait)

    log.error(f"All {MAX_RETRY} retries failed at offset {offset}. Skipping batch.")
    return None

def clean(raw):
    doc = {k: raw.get(k) for k in KEEP_FIELDS}
    for f in ("decimalLatitude","decimalLongitude"):
        try: doc[f] = float(doc[f]) if doc[f] is not None else None
        except: doc[f] = None
    for f in ("year","month","day"):
        try: doc[f] = int(doc[f]) if doc[f] is not None else None
        except: doc[f] = None
    doc["_ingested_at"] = datetime.datetime.now(datetime.timezone.utc)
    return doc

def ingest(total=100000, resume_from=0):
    log.info("Testing MongoDB connection...")
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
        client.admin.command("ping")
        log.info("MongoDB connected OK")
    except Exception as e:
        log.error(f"Connection failed: {e}")
        log.error("  Fix: check MONGO_URI at top of this file")
        log.error("  Fix: add 0.0.0.0/0 to Atlas Network Access whitelist")
        return

    col = client["gbif_birds"]["raw_occurrences"]
    col.create_index("gbifID", unique=True)

    already = col.estimated_document_count()
    log.info(f"Records already in MongoDB: {already:,}")

    effective_total = min(total, 100000)
    log.info(f"Target: {effective_total:,} records total")

    inserted = skipped = failed = 0
    offset = resume_from
    start = time.time()

    while offset < effective_total:
        log.info(f"Fetching {offset:,} / {effective_total:,} ...")

        data = fetch_batch(offset)

        if data is None:
            # All retries failed — skip this batch and continue
            log.warning(f"Skipping offset {offset} after all retries failed")
            failed += BATCH_SIZE
            offset += BATCH_SIZE
            time.sleep(30)
            continue

        results = data.get("results", [])
        if not results:
            log.info("GBIF returned empty page — all available records fetched.")
            break

        for r in results:
            try:
                col.insert_one(clean(r))
                inserted += 1
            except pymongo.errors.DuplicateKeyError:
                skipped += 1
            except Exception as e:
                log.warning(f"Write error: {e}")
                failed += 1

        offset += BATCH_SIZE
        time.sleep(SLEEP_SEC)

        # Progress summary every 10 batches
        if (offset // BATCH_SIZE) % 10 == 0:
            elapsed = round(time.time() - start)
            rate = inserted / elapsed if elapsed > 0 else 0
            eta = int((effective_total - inserted) / rate) if rate > 0 else 0
            log.info(f"  → {inserted:,} inserted | {skipped:,} skipped | ETA ~{eta//60}m {eta%60}s")

    elapsed = round(time.time() - start)
    log.info("=" * 40)
    log.info(f"Done in {elapsed}s (~{elapsed//60}m {elapsed%60}s)")
    log.info(f"Inserted : {inserted:,}")
    log.info(f"Skipped  : {skipped:,}  (duplicates — safe)")
    log.info(f"Failed   : {failed:,}  (GBIF errors)")
    total_in_db = col.estimated_document_count()
    log.info(f"Total in MongoDB: {total_in_db:,}")

    if total_in_db < effective_total:
        log.info(f"Tip: run again with --resume-from {offset} to continue from where it stopped")

    client.close()

parser = argparse.ArgumentParser()
parser.add_argument("--total",       type=int, default=100000, help="Total records to fetch (max 100000)")
parser.add_argument("--resume-from", type=int, default=0,      help="Resume from this offset (use if script stopped mid-way)")
args = parser.parse_args()
ingest(args.total, args.resume_from)