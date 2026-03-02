import os
import json
import datetime
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

def main():
    print("Starting secure process (Zero-fill Mode)...")

    # --- 1. Load Secrets ---
    secret_env = os.environ.get("APP_SECRET_JSON")
    if not secret_env:
        print("Error: Secrets not loaded.")
        return

    try:
        config = json.loads(secret_env)
    except json.JSONDecodeError:
        print("Error: Invalid JSON format.")
        return

    # Secretsから各情報を取得
    meta_token = config.get("m_token")
    raw_act_id = str(config.get("m_act_id", "")).strip()
    sheet_id = config.get("s_id")
    sheet_names_map = config.get("sheets")
    google_creds_dict = config.get("g_creds")

    if not sheet_names_map:
        print("Error: 'sheets' config is missing in Secrets.")
        return

    # IDのクリーニング
    clean_act_num = raw_act_id.replace("act=", "").replace("act_", "").replace("act", "").strip()
    target_act = f"act_{clean_act_num}"
    masked_id = clean_act_num[-4:] if len(clean_act_num) > 4 else clean_act_num
    print(f"Target Account: ******{masked_id}")

    if not all([meta_token, sheet_id, google_creds_dict]):
        print("Error: Missing base configuration.")
        return

    # --- 2. Google Sheets Connection ---
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(google_creds_dict, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
    except Exception as e:
        print(f"Auth Error: {str(e)}")
        return

    # --- 3. Date Calculation ---
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    
    # A. 通常範囲 (先月1日 〜 昨日)
    this_month_start = datetime.date(yesterday.year, yesterday.month, 1)
    last_month_end = this_month_start - datetime.timedelta(days=1)
    last_month_start = datetime.date(last_month_end.year, last_month_end.month, 1)

    # B. 月別用範囲 (過去6ヶ月)
    m6_start = (today - datetime.timedelta(days=180))

    print(f"Standard Range: {last_month_start} to {yesterday}")

    # --- 4. Execute Tasks ---
    # 引数として「日付オブジェクト」を渡すように変更
    run_task(client, spreadsheet, target_act, meta_token, last_month_start, yesterday, "CPN", sheet_names_map)
    run_task(client, spreadsheet, target_act, meta_token, last_month_start, yesterday, "ADS", sheet_names_map)
    run_task(client, spreadsheet, target_act, meta_token, last_month_start, yesterday, "DAILY", sheet_names_map)
    
    run_task(client, spreadsheet, target_act, meta_token, m6_start, yesterday, "MONTHLY", sheet_names_map)

    print("All tasks completed.")

def run_task(client, spreadsheet, act_id, token, start_date, end_date, rule_key, name_map):
    target_sheet_name = name_map.get(rule_key)
    if not target_sheet_name:
        return

    rule = REPORT_RULES[rule_key]
    print(f"\nProcessing Task: {rule_key}")

    # 日付オブジェクトからAPI用のJSON文字列を作成
    time_range_str = f'{{"since":"{start_date.strftime("%Y-%m-%d")}","until":"{end_date.strftime("%Y-%m-%d")}"}}'

    url = f"https://graph.facebook.com/v19.0/{act_id}/insights"
    params = {
        'access_token': token,
        'level': rule["level"],
        'time_range': time_range_str,
        'fields': rule["fields"],
        'limit': 5000
    }
    if rule["increment"]:
        params['time_increment'] = rule["increment"]

    # APIリクエスト
    try:
        res = requests.get(url, params=params)
        data_json = res.json()
    except Exception as e:
        print(f"API Request Failed: {str(e)}")
        return

    if 'error' in data_json:
        print(f"API Error: {data_json['error'].get('message', 'Unknown')}")
        return

    raw_data = data_json.get('data', [])
    rows = []

    # === データ処理ロジックの分岐 ===

    # A. DAILYかつ「0埋めフラグ」がある場合
    if rule_key == "DAILY" and rule.get("fill_zero"):
        # 1. APIデータを「日付」をキーにした辞書に変換 (検索用)
        data_map = {item['date_start']: item for item in raw_data}

        # 2. 開始日から終了日までループして全日程を作成
        current_date = start_date
        while current_date <= end_date:
            d_str = current_date.strftime('%Y-%m-%d')
            
            # APIにデータがあればそれを使う、なければ空の辞書(getで0になる)を使う
            item = data_map.get(d_str, {})
            
            # データの抽出と数値変換
            imp = int(item.get('impressions', 0))
            click = int(item.get('clicks', 0))
            spend = float(item.get('spend', 0))
            ctr = float(item.get('ctr', 0))
            reach = int(item.get('reach', 0))
            freq = float(item.get('frequency', 0))
            
            # 日付をセットして追加 (APIデータがない場合、d_strを使う)
            rows.append([d_str, imp, click, spend, ctr, reach, freq])
            
            # 1日進める
            current_date += datetime.timedelta(days=1)

    # B. それ以外 (CPN, ADS, MONTHLY) - 従来通り
    else:
        for item in raw_data:
            date_val = item.get('date_start')
            # 数値変換
            imp = int(item.get('impressions', 0))
            click = int(item.get('clicks', 0))
            spend = float(item.get('spend', 0))
            ctr = float(item.get('ctr', 0))
            reach = int(item.get('reach', 0))
            freq = float(item.get('frequency', 0))

            if rule_key == "CPN":
                rows.append([date_val, item.get('campaign_name'), imp, click, spend, ctr, reach, freq])
            elif rule_key == "ADS":
                rows.append([date_val, item.get('campaign_name'), item.get('ad_name'), imp, click, spend, ctr, reach, freq])
            else:
                rows.append([date_val, imp, click, spend, ctr, reach, freq])

    # ソート処理 (DAILYもここで降順になる)
    if rule["increment"]:
        rows.sort(key=lambda x: x[0], reverse=rule["sort_desc"])

    output_data = [rule["header"]] + rows

    try:
        worksheet = spreadsheet.worksheet(target_sheet_name)
        worksheet.clear()
        worksheet.update(output_data)
        print(f"Write success. ({len(rows)} rows)")
    except Exception as e:
        print(f"Write Error: {str(e)}")

if __name__ == "__main__":
    main()
