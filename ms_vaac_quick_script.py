import os
import json
import gzip
import base64
import urllib.parse
import datetime as dt
from zoneinfo import ZoneInfo
from collections import defaultdict
from statistics import mean
import requests
from dotenv import load_dotenv
 
load_dotenv()
 
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = "a672d62c-fc7b-4e81-a576-e60dc46e951d"
USERNAME = os.getenv("APP_USERNAME", "")
PASSWORD = os.getenv("APP_PASSWORD", "")
 
def build_query_payload():
    """
    Build the VAAC query payload to return only calls started in the last 1 hour.
    Uses `UserStartTimeUTC >= <now-1h>` for precise time filtering.
    """
    # Compute "now minus 1 hour" in UTC and format as ISO 8601 ending with Z
    from_utc = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
 
    json_object = {
        "Filters": [
            {"DataModelName": "UserStartTimeUTC", "Value": "2025-12-15T23:58:30", "Operand": 5},  # equal
            {"DataModelName": "Date", "Value": "2025-12-15", "Operand": 0}  # equal
 
            # equals True
        ],
        "Dimensions": [
            {"DataModelName": "UserStartTimeUTC"},
            {"DataModelName": "HasCQ"},
            {"DataModelName": "CallQueueCallResult"},
            {"DataModelName": "CallQueueIdentity"},
        ],
        "Measurements": [
            {"DataModelName": "TotalCallCount"},
            {"DataModelName": "AvgCallDuration"},
            {"DataModelName": "TotalAudioStreamDuration"},
        ],
        "Parameters": {"UserAgent": "Power BI Desktop V3.1.8"},
        "LimitResultRowsCount": 90000,
    }
    return json_object
 
def compress_encode_url_param(obj: dict) -> str:
    """
    Convert the JSON object to a compact JSON string, gzip compress, base64 encode, then URL-encode.
    Mirrors the PowerShell pipeline: ConvertTo-Json -> GZip -> Base64 -> UrlEncode.
    """
    json_str = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    gz_bytes = gzip.compress(json_str.encode("utf-8"))
    b64 = base64.b64encode(gz_bytes).decode("ascii")
    return urllib.parse.quote(b64)
 
def get_oauth_token():
    # For Microsoft Graph (v2 endpoint uses 'scope' with .default)
    TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "scope": "https://api.interfaces.records.teams.microsoft.com/.default",
        "userName": USERNAME,
        "password": PASSWORD,
        "grant_type": "password"
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    # Consider removing prints of sensitive data in production
    response = requests.post(TOKEN_URL, headers=headers, data=data)
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        print("Token request failed:", e)
        print("Details:", response.text)
        raise
    token = response.json()["access_token"]
    print("Token received")
    return token
 
def query_vaac_api(token):
    print("Querying VAAC API")
    VAAC_URL = "https://api.interfaces.records.teams.microsoft.com/Teams.VoiceAnalytics/getanalytics?query="
    headers = {
        'Authorization': f"Bearer {token}"
    }
    payload = build_query_payload()
    header_titles = [d["DataModelName"] for d in payload["Dimensions"]] + \
                    [m["DataModelName"] for m in payload["Measurements"]]
    encoded_param = compress_encode_url_param(payload)
    url_with_query = VAAC_URL + encoded_param
 
    response = requests.get(url_with_query, headers=headers, timeout=60)
    try:
        response.raise_for_status()
        data = response.json()
 
        # Optional: still print to console for quick inspection
        print(data)
 
        # Write the full JSON response to response.json
        with open("response.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
 
        print("Saved API response to response.json")
    except requests.HTTPError as e:
        print("VAAC request failed:", e)
        print("Details:", response.text)
        raise
 
def main():
    token = get_oauth_token()
    query_vaac_api(token)
 
if __name__ == "__main__":
    main()