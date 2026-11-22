import os
import json
import datetime
import logging
import math
import re
import sys
import time
import uuid
from contextlib import contextmanager
from urllib.parse import parse_qs, urlsplit

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==========================================
# 設定：取得ルールの定義
# ==========================================
REPORT_RULES = {
    # ① キャンペーン別
    "CPN": {
        "level": "campaign",
        "increment": "monthly",
        "fields": "campaign_name,impressions,clicks,spend,cpc,ctr,reach,frequency",
        "header": ["年月", "キャンペーン名", "インプレッション", "クリック数", "消化金額", "CTR", "リーチ", "フリークエンシー"],
        "sort_desc": True
    },
    # ② 広告別
    "ADS": {
        "level": "ad",
        "increment": "monthly",
        "fields": "campaign_name,ad_name,impressions,clicks,spend,cpc,ctr,reach,frequency",
        "header": ["年月", "キャンペーン名", "広告名", "インプレッション", "クリック数", "消化金額", "CTR", "リーチ", "フリークエンシー"],
        "sort_desc": True
    },
    # ③ 日別 (実績0の日も埋める)
    "DAILY": {
        "level": "account",
        "increment": "1",
        "fields": "impressions,clicks,spend,cpc,ctr,reach,frequency",
        "header": ["日付", "インプレッション", "クリック数", "消化金額", "CTR", "リーチ", "フリークエンシー"],
        "sort_desc": True,
        "fill_zero": True  # 【追加】0埋めフラグ
    },
    # ④ 月別
    "MONTHLY": {
        "level": "account",
        "increment": "monthly",
        "fields": "impressions,clicks,spend,cpc,ctr,reach,frequency",
        "header": ["年月", "インプレッション", "クリック数", "消化金額", "CTR", "リーチ", "フリークエンシー"],
        "sort_desc": True
    }
}


def next_cursor(endpoint, paging, params):
    if not isinstance(paging["next"], str):
        raise ExportError("Meta API returned invalid paging.")
    target = urlsplit(paging["next"])
    expected = urlsplit(endpoint)
    if (target.scheme, target.netloc, target.path) != (expected.scheme, expected.netloc, expected.path):
        raise ExportError("Meta API returned an unexpected paging destination.")
    query = parse_qs(target.query, keep_blank_values=True)
    if target.fragment or set(query) - (set(params) | {"after", "access_token"}):
        raise ExportError("Meta API returned unsupported paging.")
    cursors = paging.get("cursors", {})
    if not isinstance(cursors, dict):
        raise ExportError("Meta API returned invalid cursors.")
    candidates = query.get("after", [])
    if len(candidates) > 1:
        raise ExportError("Meta API returned ambiguous paging.")
    cursor = cursors.get("after", candidates[0] if candidates else None)
    if not isinstance(cursor, str) or not cursor or (candidates and candidates != [cursor]):
        raise ExportError("Meta API returned unsupported paging.")
    return cursor


class ExportError(Exception):
    """Contains only fixed operation labels and validated numeric diagnostics."""


def request_json(label, method, url, **kwargs):
    try:
        response = requests.request(method, url, allow_redirects=False, **kwargs)
    except requests.RequestException:
        raise ExportError(f"{label} connection failed.") from None
    status = response.status_code
    try:
        payload = response.json()
    except ValueError:
        raise ExportError(f"{label} returned invalid JSON (HTTP {status}).") from None
    if not 200 <= status < 300 or (isinstance(payload, dict) and "error" in payload):
        details = []
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            for key in ("code", "error_subcode"):
                if type(error.get(key)) is int:
                    details.append(f"{key}={error[key]}")
        reason = ", ".join([f"HTTP {status}", *details])
        raise ExportError(f"{label} failed ({reason}).")
    return payload


LOCK_NAME = "hills_export_lock_v1"
LOCK_PREFIX = "_hills_export_"
LEASE_SECONDS = 3 * 60 * 60


def active_lock(book):
    metadata = book.fetch_sheet_metadata(params={
        "fields": "sheets(properties(sheetId,title,hidden)),namedRanges(namedRangeId,name,range(sheetId))"
    })
    for item in metadata.get("namedRanges", []):
        if item["name"].casefold() != LOCK_NAME:
            continue
        guard_id = item["namedRangeId"]
        sheet_id = item["range"]["sheetId"]
        for sheet in metadata["sheets"]:
            props = sheet["properties"]
            if props["sheetId"] != sheet_id:
                continue
            title = props["title"]
            match = re.fullmatch(LOCK_PREFIX + r"([0-9]+)_([a-f0-9]{32})", title)
            if match and props.get("hidden") and guard_id == "hills_" + match[2]:
                return (guard_id, sheet_id, title, int(match[1]))
        raise ExportError("Sheets lock metadata is invalid.")
    return None


def remove_lock(owner):
    # Both operations are atomic. A retired owner cannot delete a successor's tab.
    return [
        {"deleteNamedRange": {"namedRangeId": owner[0]}},
        {"deleteSheet": {"sheetId": owner[1]}},
    ]


@contextmanager
def export_lock(book):
    previous = active_lock(book)
    now = int(time.time())
    if previous and previous[3] > now:
        raise ExportError("Another export holds the Sheets lock.")
    nonce = uuid.uuid4().hex
    sheet_id = 1 + uuid.uuid4().int % 2147483646
    expires = now + LEASE_SECONDS
    title = f"{LOCK_PREFIX}{expires}_{nonce}"
    guard_id = "hills_" + nonce
    owner = (guard_id, sheet_id, title, expires)
    commands = remove_lock(previous) if previous else []
    commands.extend([
        {"addSheet": {"properties": {
            "sheetId": sheet_id, "title": title, "hidden": True,
            "gridProperties": {"rowCount": 1, "columnCount": 1},
        }}},
        {"addNamedRange": {"namedRange": {
            "namedRangeId": guard_id, "name": LOCK_NAME,
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                      "startColumnIndex": 0, "endColumnIndex": 1},
        }}},
    ])
    try:
        book.batch_update({"requests": commands})
    except Exception:
        try:
            acquired = active_lock(book) == owner
        except Exception:
            acquired = False
        if not acquired:
            raise ExportError("Sheets lock acquisition was not confirmed.") from None
    try:
        yield owner
    finally:
        try:
            book.batch_update({"requests": remove_lock(owner)})
        except Exception:
            try:
                released = active_lock(book) != owner
            except Exception:
                released = False
            if not released:
                print("Sheets lock release unconfirmed. The lease expires automatically.", file=sys.stderr)


def raw_cell(value):
    if value == "":
        return {}
    if isinstance(value, str):
        return {"userEnteredValue": {"stringValue": value}}
    return {"userEnteredValue": {"numberValue": value}}


def publication_requests(book, owner, reports):
    metadata = book.fetch_sheet_metadata(params={"fields": "sheets(properties(sheetId,title,gridProperties))"})
    sheets = {item["properties"]["title"]: item["properties"] for item in metadata["sheets"]}
    # Validate the unique lease in the very same atomic batch as the report writes.
    commands = [{"updateNamedRange": {"namedRange": {"namedRangeId": owner[0], "name": LOCK_NAME},
                                      "fields": "name"}}]
    for title, header, rows in reports:
        width, height = len(header), 1 + len(rows)
        props = sheets.get(title)
        if props is None:
            raise ExportError("A configured report tab does not exist.")
        sheet_id = props["sheetId"]
        grid = props["gridProperties"]
        for dimension, need, current in (("ROWS", height, grid["rowCount"]),
                                          ("COLUMNS", width, grid["columnCount"])):
            if need > current:
                commands.append({"appendDimension": {"sheetId": sheet_id, "dimension": dimension,
                                                     "length": need - current}})
        # An open-ended row range clears previous values below the new output.
        # Updating userEnteredValue alone preserves formatting and the other columns.
        commands.append({"updateCells": {
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "startColumnIndex": 0,
                      "endColumnIndex": width},
            "fields": "userEnteredValue",
            "rows": [{"values": [raw_cell(value) for value in row]} for row in [header, *rows]],
        }})
    commands.append({"updateCells": {
        "start": {"sheetId": owner[1], "rowIndex": 0, "columnIndex": 0},
        "fields": "userEnteredValue", "rows": [{"values": [raw_cell("PUBLISHED")]}],
    }})
    return commands


def publish(book, owner, commands):
    try:
        book.batch_update({"requests": commands})
    except Exception:
        try:
            result = book.values_get(f"'{owner[2]}'!A1")
        except Exception:
            raise ExportError("Publication outcome unknown.") from None
        if result.get("values") != [["PUBLISHED"]]:
            raise ExportError("Publication not confirmed.") from None


def fetch_report(act_id, token, rule, start, end):
    endpoint = f"https://graph.facebook.com/v27.0/{act_id}/insights"
    params = {
        "level": rule["level"], "fields": rule["fields"], "time_increment": rule["increment"],
        "time_range": json.dumps({"since": start.isoformat(), "until": end.isoformat()}), "limit": 5000,
    }
    rows = []
    seen_cursors = set()
    while True:
        payload = request_json("Meta API", "GET", endpoint, headers={"Authorization": f"Bearer {token}"},
                               params=params, timeout=120)
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            raise ExportError("Meta API returned invalid data.")
        for item in payload["data"]:
            rows.append(report_row(item, rule, start, end))
        paging = payload.get("paging", {})
        if not isinstance(paging, dict):
            raise ExportError("Meta API returned invalid paging.")
        if "next" not in paging:
            break
        cursor = next_cursor(endpoint, paging, params)
        if cursor in seen_cursors or token in cursor:
            raise ExportError("Meta API returned an invalid or repeated cursor.")
        seen_cursors.add(cursor)
        params["after"] = cursor

    if rule.get("fill_zero"):
        days = {r[0] for r in rows}
        if len(days) != len(rows):
            raise ExportError("Daily report returned a duplicate date.")
        for offset in range((end - start).days + 1):
            day = (start + datetime.timedelta(days=offset)).isoformat()
            if day not in days:
                rows.append([day, 0, 0, 0.0, 0.0, 0, 0.0])
    return sorted(rows, key=lambda row: row[0], reverse=rule["sort_desc"])


def metric(item, field, integer=False):
    value = item.get(field)
    if integer:
        if type(value) is int and value >= 0:
            return value
        if isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
            return int(value)
    elif type(value) in (int, float, str):
        try:
            result = float(value)
        except (ValueError, OverflowError):
            pass
        else:
            if math.isfinite(result) and result >= 0:
                return result
    raise ExportError(f"Meta API returned an invalid or missing {field}.")


def report_row(item, rule, start, end):
    if not isinstance(item, dict):
        raise ExportError("Meta API returned an invalid row.")
    text = item.get("date_start")
    if not isinstance(text, str):
        raise ExportError("Meta API returned a missing date.")
    try:
        day = datetime.date.fromisoformat(text)
    except ValueError:
        raise ExportError("Meta API returned an invalid date.") from None
    # A monthly bucket can use the first day of the first partial month.
    if text != day.isoformat() or day > end or (
        day < start and not (rule["increment"] == "monthly" and day == start.replace(day=1))
    ):
        raise ExportError("Meta API returned an out-of-range date.")
    row = [text]
    fields = rule["fields"].split(",")
    for key in ("campaign_name", "ad_name"):
        if key in fields:
            if not isinstance(item.get(key), str) or not item[key]:
                raise ExportError("Meta API returned a missing report dimension.")
            row.append(item[key])
    impressions = metric(item, "impressions", integer=True)
    clicks = metric(item, "clicks", integer=True)
    spend = metric(item, "spend")
    reach = metric(item, "reach", integer=True)
    ctr = 0.0 if "ctr" not in item and (impressions == 0 or clicks == 0) else metric(item, "ctr")
    frequency = 0.0 if "frequency" not in item and reach == 0 else metric(item, "frequency")
    return [*row, impressions, clicks, spend, ctr, reach, frequency]


def main():
    raw = os.environ.get("APP_SECRET_JSON")
    if not raw:
        raise ExportError("APP_SECRET_JSON is missing.")
    try:
        config = json.loads(raw)
    except ValueError:
        raise ExportError("APP_SECRET_JSON contains invalid JSON.") from None
    if not isinstance(config, dict):
        raise ExportError("APP_SECRET_JSON must contain a configuration object.")
    names = config.get("sheets")
    token = config.get("m_token")
    if not isinstance(names, dict) or not isinstance(token, str) or not token.strip():
        raise ExportError("Invalid report configuration.")
    targets = {key: names[key] for key in REPORT_RULES if names.get(key)}
    if not targets or not all(isinstance(title, str) for title in targets.values()):
        raise ExportError("No valid report destinations configured.")
    if len({title.casefold() for title in targets.values()}) != len(targets):
        raise ExportError("Report destinations must be distinct.")
    account = str(config.get("m_act_id", "")).strip()
    match = re.fullmatch(r"(?:act[=_]?\s*)?([0-9]+)", account)
    if not match:
        raise ExportError("Invalid Meta account ID.")
    act_id = "act_" + match[1]
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(config["g_creds"], scopes)
    client = gspread.authorize(creds)
    client.set_timeout((15, 210))
    book = client.open_by_key(config["s_id"])

    with export_lock(book) as owner:
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        last_month_end = yesterday.replace(day=1) - datetime.timedelta(days=1)
        reports = []
        for key, title in targets.items():
            rule = REPORT_RULES[key]
            start = today - datetime.timedelta(days=180) if key == "MONTHLY" else last_month_end.replace(day=1)
            print(f"Fetching {key}: {start} to {yesterday}.", flush=True)
            rows = fetch_report(act_id, token, rule, start, yesterday)
            reports.append((title, rule["header"], rows))
        publish(book, owner, publication_requests(book, owner, reports))
    print("All configured reports published.")


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    try:
        main()
    except ExportError as error:
        sys.exit(str(error))
    except Exception:
        sys.exit("Export failed. Check configuration, response format or Sheets access.")
