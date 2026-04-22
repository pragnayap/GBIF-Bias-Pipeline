import datetime, logging, time, argparse
import requests
import pymongo

MONGO_URI  = "mongodb+srv://pragnaya:REDACTED_PASSWORD@dwp-cluster.mhebkb7.mongodb.net/gbif_birds"
GBIF_BASE  = "https://api.gbif.org/v1/occurrence/search"
BATCH_SIZE = 300
SLEEP_SEC  = 1.0
MAX_RETRY  = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

KEEP_FIELDS = [
    "gbifID","species","genus","family","order","class","kingdom",
    "decimalLatitude","decimalLongitude","countryCode","stateProvince",
    "eventDate","year","month","day","recordedBy","institutionCode",
    "basisOfRecord","occurrenceStatus",
]

def fetch_batch(offset):
    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = requests.get(GBIF_BASE, params={
                "taxonKey": 212, "occurrenceStatus": "PRESENT",
                "hasCoordinate": "true", "hasGeospatialIssue": "false",
                "limit": BATCH_SIZE, "offset": offset,
            }, timeout=60)
            if resp.status_code == 503:
                wait = attempt * 30
                log.warning(f"GBIF 503 (attempt {attempt}/{MAX_RETRY}) — waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            time.sleep(attempt * 20)
        except requests.exceptions.ConnectionError:
            time.sleep(attempt * 15)
    log.error(f"All {MAX_RETRY} retries failed at offset {offset}. Skipping batch.")
    return None

def clean(raw):
    doc = {k: raw.get(k) for k in KEEP_FIELDS}
    for f in ("decimalLatitude", "decimalLongitude"):
        try: doc[f] = float(doc[f]) if doc[f] is not None else None
        except: doc[f] = None
    for f in ("year", "month", "day"):
        try: doc[f] = int(doc[f]) if doc[f] is not None else None
        except: doc[f] = None
    doc["_ingested_at"] = datetime.datetime.now(datetime.timezone.utc)
    return doc

def ingest(total=100_000, resume_from=0):
    log.info("Testing MongoDB connection...")
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
        client.admin.command("ping")
        log.info("MongoDB connected OK")
    except Exception as e:
        log.error(f"Connection failed: {e}")
        return

    col = client["gbif_birds"]["raw_occurrences"]
    col.create_index("gbifID", unique=True)

    effective_total = min(total, 100_000)
    log.info(f"Records in MongoDB: {col.estimated_document_count():,} | Target: {effective_total:,}")

    inserted = skipped = failed = 0
    offset = resume_from
    start = time.time()

    while offset < effective_total:
        log.info(f"Fetching {offset:,} / {effective_total:,} ...")
        data = fetch_batch(offset)
        if data is None:
            failed += BATCH_SIZE
            offset += BATCH_SIZE
            time.sleep(30)
            continue

        results = data.get("results", [])
        if not results:
            log.info("No more results from GBIF.")
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

    elapsed = round(time.time() - start)
    log.info(f"Done in {elapsed}s — inserted: {inserted:,} | skipped: {skipped:,} | failed: {failed:,}")
    log.info(f"Total in MongoDB: {col.estimated_document_count():,}")
    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--total",       type=int, default=100_000)
    parser.add_argument("--resume-from", type=int, default=0)
    args = parser.parse_args()
    ingest(args.total, args.resume_from)