import json                  # used to convert Python data to JSON text and back
import os                    # used to read environment variables (like your API token)
import urllib.request        # used to make HTTP requests to the WAQI API
import urllib.error          # used to catch network errors
from datetime import datetime, timezone  # used to work with dates and times

import boto3                 # the AWS library, lets Python talk to S3, Lambda and others

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# These values are NOT hardcoded — they come from Lambda's environment variables.
# That way your API token is never visible in the code itself (security best practice).
# ──────────────────────────────────────────────────────────────────────────────

# your secret API token from aqicn.org
WAQI_TOKEN   = os.environ["WAQI_TOKEN"]

# the ID of the Zurich Kaserne air quality station
WAQI_STATION = "A127069"                   

# this builds the full URL we call to get air quality data
WAQI_URL     = f"https://api.waqi.info/feed/@{WAQI_STATION}/?token={WAQI_TOKEN}"             

# the name of your S3 bucket: "team-survival-datalake"
S3_BUCKET    = os.environ["S3_BUCKET"] 

# the folder inside the bucket where files go
S3_PREFIX    = "raw/waqi"                   

# opens a connection to S3 so we can write files to it
s3 = boto3.client("s3")     

# ──────────────────────────────────────────────────────────────────────────────
# FUNCTION 1: fetch_waqi()
# Job: call the WAQI API and get the raw response back
# ──────────────────────────────────────────────────────────────────────────────
def fetch_waqi() -> dict:

    # builds the HTTP request (like clicking a URL in a browser, but in code)
    req = urllib.request.Request(WAQI_URL, headers={"Accept": "application/json"})
    
    # sends the request and reads the response text (raw JSON string)
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = resp.read().decode("utf-8")
    
    # converts the JSON text into a Python dictionary we can work with
    return json.loads(raw)
    
# ──────────────────────────────────────────────────────────────────────────────
# FUNCTION 2: parse_waqi()
# Job: take the full messy API response and pull out only the fields we need
# ──────────────────────────────────────────────────────────────────────────────


def parse_waqi(response: dict) -> dict:
    
    # the actual data is nested inside a "data" key
    data = response["data"]

    # "iaqi" contains individual pollutant readings
    # .get() means "if this key doesn't exist, return empty {}"     
    iaqi = data.get("iaqi", {}) 
                                 

    def safe_get(field: str):
        # The WAQI API doesn't always return every field (e.g. sometimes pm10 is missing).
        # This helper function safely returns the value, or None if the field isn't there.
        # Without this, the code would crash with a KeyError on missing fields.
        return iaqi[field]["v"] if field in iaqi else None

    # Build a clean, flat dictionary with exactly the fields we want to store
    return {
        "station_id":       WAQI_STATION,
        "station_name":     data["city"]["name"],           # e.g. "Zürich"
        "station_lat":      data["city"]["geo"][0],         # latitude
        "station_lon":      data["city"]["geo"][1],         # longitude
        "timestamp_utc":    data["time"]["iso"],            # e.g. "2026-03-09T15:21:33Z"
        "timestamp_local":  data["time"]["s"],              # e.g. "2026-03-09 16:21:33"
        "timezone_offset":  data["time"]["tz"],             # e.g. "+01:00"
        "aqi":              data.get("aqi"),                # overall Air Quality Index
        "dominant_pol":     data.get("dominentpol"),        # which pollutant is highest
        "pm25":             safe_get("pm25"),               # fine particles pm25
        "pm10":             safe_get("pm10"),               # coarser particles pm10
        "humidity":         safe_get("h"),                  # relative humidity %
        "temperature":      safe_get("t"),                  # temperature in Celsius
        "pressure":         safe_get("p"),                  # atmospheric pressure hPa
        "uvi":              safe_get("uvi"),                # UV index
        "ingested_at":      datetime.now(timezone.utc).isoformat(),  # when we pulled this data
    }


# ──────────────────────────────────────────────────────────────────────────────
# FUNCTION 3: build_s3_key()
# Job: decide WHERE in S3 to save the file (the full file path)
# ──────────────────────────────────────────────────────────────────────────────
def build_s3_key(timestamp_utc: str) -> str:
    dt = datetime.fromisoformat(timestamp_utc.replace("Z", "+00:00"))
    # converts the timestamp string into a Python datetime object so we can
    # extract the year, month, day, hour separately

    partition = (
        f"year={dt.year:04d}/"    # e.g. year=2026/
        f"month={dt.month:02d}/"  # e.g. month=03/
        f"day={dt.day:02d}/"      # e.g. day=09/
        f"hour={dt.hour:02d}"     # e.g. hour=15
    )
    # this "Hive partitioning" format means AWS Glue and Athena can
    # automatically filter by date without scanning all files

    filename = f"waqi_{timestamp_utc.replace(':', '-')}.json"
    # e.g. waqi_2026-03-09T15-21-33Z.json
    # (colons replaced with dashes because colons are invalid in file names)

    return f"{S3_PREFIX}/{partition}/{filename}"
    # full path: raw/waqi/year=2026/month=03/day=09/hour=15/waqi_2026-03-09T15-21-33Z.json


# ──────────────────────────────────────────────────────────────────────────────
# FUNCTION 4: save_to_s3()
# Job: write the data as a JSON file into S3
# ──────────────────────────────────────────────────────────────────────────────
def save_to_s3(payload: dict, s3_key: str) -> None:
    s3.put_object(
        Bucket=S3_BUCKET,                                    # which bucket
        Key=s3_key,                                          # which file path inside the bucket
        Body=json.dumps(payload, ensure_ascii=False, indent=2),  # the file content (pretty JSON)
        ContentType="application/json",                      # tells S3 what kind of file this is
    )


# ──────────────────────────────────────────────────────────────────────────────
# MAIN HANDLER: lambda_handler()
# This is the entry point, AWS calls this function every time the Lambda runs
# Think of it as the "main()" of your Lambda.
# ──────────────────────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    # "event" = any input passed to the Lambda (from EventBridge schedule, we ignore it here)
    # "context" = AWS runtime info (memory, timeout remaining, etc., we ignore it here too)

    print(f"[WAQI] Starting ingestion for station {WAQI_STATION}")

    # STEP 1: Call the API
    try:
        response = fetch_waqi()
    except urllib.error.URLError as e:
        # If the network call fails (API down, timeout, etc.) log it and stop
        print(f"[WAQI] Network error: {e}")
        return {"statusCode": 502, "body": f"Network error: {e}"}

    # STEP 2: Check the API actually returned good data
    # The WAQI API always returns {"status": "ok"} on success
    # If it returns anything else, something went wrong
    if response.get("status") != "ok":
        print(f"[WAQI] Unexpected API status: {response}")
        return {"statusCode": 502, "body": "WAQI API returned non-ok status"}

    # STEP 3: Extract only the fields we need
    payload = parse_waqi(response)
    print(f"[WAQI] Parsed measurement at {payload['timestamp_utc']} "
          f"| AQI={payload['aqi']} PM2.5={payload['pm25']} PM10={payload['pm10']}")

    # STEP 4: Figure out the S3 file path and save it
    s3_key = build_s3_key(payload["timestamp_utc"])
    save_to_s3(payload, s3_key)
    print(f"[WAQI] Saved to s3://{S3_BUCKET}/{s3_key}")

    # STEP 5: Return a success response
    # Lambda always returns a status code and body
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message":       "WAQI ingestion successful",
            "s3_key":        s3_key,
            "timestamp_utc": payload["timestamp_utc"],
            "aqi":           payload["aqi"],
            "pm25":          payload["pm25"],
            "pm10":          payload["pm10"],
        })
    }
