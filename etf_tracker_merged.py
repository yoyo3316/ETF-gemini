# -*- coding: utf-8 -*-
"""
ETF 持股追蹤器（整合版）
執行順序：
【1】爬取各 ETF 最新持股（CMoney），存入 {code}_holdings.json
【2】產生 processed_etf_data.json（異動明細）
【3】產生 stock_history_data.json（股票歷史）
【3.5】爬取 MoneyDJ 主動ETF 三個月報酬率排行，存成 active_etf_ranking.json
【3.6】合併持倉區間 → 併發抓 FinMind 股價 → stock_price_cache.json（已有今日資料者跳過）
【4】自動 Git commit & push 到 GitHub
"""

import json
import os
import sys
import re
import glob
import time
import shutil
import tempfile
import subprocess
import logging
import requests
import urllib3
from bs4 import BeautifulSoup
from typing import Dict, List, Set
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from data_contract import write_homepage_index, write_data_manifest, validate_public_data
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    WebDriverException, TimeoutException,
    NoSuchElementException, UnexpectedAlertPresentException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ETF_CODE_NAME_MAP = {
    "00981A": "統一台股增長",
    "00980A": "野村臺灣智慧優選主動式ETF",
    "00982A": "群益台灣精選強棒主動式ETF基金",
    "00992A": "群益台灣科技創新主動式ETF",
    "00994A": "第一金台股優選主動ETF",
    "00995A": "中信台灣卓越主動ETF",
    "00987A": "台新優勢成長主動ETF",
    "00991A": "復華未來50主動ETF",
    "00403A": "統一升級50主動ETF",
    "00407A": "凱基主動ETF"
}

# 預設使用本程式所在資料夾：本機排程與 GitHub Actions 都能直接執行。
DATA_DIR = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
GIT_ENABLED = os.environ.get("GIT_ENABLED", "true").lower() == "true"
GIT_REMOTE_URL = os.environ.get("GIT_REMOTE_URL", "https://github.com/yoyo3316/ETF-gemini.git")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "")

WEIGHT_MAJOR_CHANGE_THRESHOLD = 0.25
COUNT_MAJOR_CHANGE_THRESHOLD = 50 * 1000
COUNT_DETAILED_CHANGE_THRESHOLD = 30 * 1000
WEIGHT_DISPLAY_THRESHOLD = 0.15

ETF_TARGETS = {
    code: {"name": name, "url": f"https://www.cmoney.tw/etf/tw/{code}/fundholding", "parser": "parse_cmoney"}
    for code, name in ETF_CODE_NAME_MAP.items()
}

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
NON_EQUITY_CODES_PRICE = {"C_NTD", "RDI_NTD", "M_NTD", "PFUR_NTD"}

# ── Chrome ──────────────────────────────────────────────────
def get_chrome_version():
    try:
        if os.name == 'nt':
            import winreg
            for hive, path in [
                (winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon"),
                (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\Google Chrome"),
            ]:
                try:
                    key = winreg.OpenKey(hive, path)
                    version, _ = winreg.QueryValueEx(key, "version" if "BLBeacon" in path else "DisplayVersion")
                    return int(version.split('.')[0])
                except Exception:
                    continue
        else:
            import subprocess as sp
            for cmd in [['google-chrome','--version'],['chromium','--version']]:
                try:
                    r = sp.run(cmd, capture_output=True, text=True, timeout=5)
                    if r.returncode == 0:
                        return int(r.stdout.strip().split()[-1].split('.')[0])
                except Exception:
                    continue
    except Exception:
        pass
    return None

def find_chrome_binary():
    for c in [r"C:\Program Files\Google\Chrome\Application\chrome.exe",
               r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"]:
        if os.path.exists(c):
            return c
    for name in ("chrome","chrome.exe","msedge","msedge.exe"):
        p = shutil.which(name)
        if p:
            return p
    return None

def clear_undetected_cache():
    try:
        d = os.path.join(os.path.expanduser("~"), ".undetected_chromedriver")
        if os.path.exists(d):
            shutil.rmtree(d)
    except Exception:
        pass

def _create_chrome_options(headless_mode):
    options = uc.ChromeOptions()
    options.add_argument("--headless=new" if headless_mode == "new" else "--headless")
    for arg in ["--disable-gpu","--no-sandbox","--disable-dev-shm-usage",
                "--disable-extensions","--window-size=1200,800",
                "--disable-blink-features=AutomationControlled"]:
        options.add_argument(arg)
    cb = find_chrome_binary()
    if cb:
        options.binary_location = cb
    return options

def setup_webdriver():
    version_main = get_chrome_version()
    for mode in ("new", "legacy"):
        try:
            driver = uc.Chrome(options=_create_chrome_options(mode),
                               use_subprocess=True, version_main=version_main)
            logging.info(f"✓ Chrome 啟動成功（version_main={version_main}）")
            return driver
        except Exception as e:
            logging.warning(f"Chrome {mode} 啟動失敗: {e}")
    return None

def setup_webdriver_with_retry(max_retries=2):
    for attempt in range(max_retries):
        driver = setup_webdriver()
        if driver:
            return driver
        if attempt < max_retries - 1:
            clear_undetected_cache()
            time.sleep(3)
    return None

# ── 工具函式 ─────────────────────────────────────────────────
def escape_markdown_v2(text):
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", text)

def clean_stock_name(name):
    return name.replace('*', '')

def split_and_send_message(message, chat_id, bot_token):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    parts, cur = [], ""
    for line in message.split('\n'):
        if len(cur) + len(line) + 1 > 4096:
            if cur: parts.append(cur)
            cur = line
        else:
            cur += "\n" + line
    if cur: parts.append(cur)
    for part in parts:
        try:
            requests.post(url, json={"chat_id": chat_id, "text": part}, timeout=10).raise_for_status()
            time.sleep(1)
        except Exception as e:
            logging.error(f"Telegram 分段發送失敗: {e}")

def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.info("Telegram 未設定，略過通知")
        return
    if len(message.encode('utf-8')) > 4000:
        split_and_send_message(message, TELEGRAM_CHAT_ID, TELEGRAM_BOT_TOKEN)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID,
                                  "text": escape_markdown_v2(message),
                                  "parse_mode": "MarkdownV2"}, timeout=10).raise_for_status()
    except Exception:
        try:
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
        except Exception as e:
            logging.error(f"Telegram 發送失敗: {e}")

CMONEY_JS = """
var holdings = [];
var rows = document.querySelectorAll('tbody tr');
for (var i = 0; i < rows.length; i++) {
    var cols = rows[i].querySelectorAll('td');
    if (cols.length >= 4 && cols[0].innerText.trim() !== '') {
        var stockCode = cols[0].innerText.trim();
        var h2_name = cols[1].querySelector('h2');
        var stockName = h2_name ? (h2_name.getAttribute('title') || h2_name.innerText.trim()) : '';
        var h2_weight = cols[2].querySelector('h2');
        var weightStr = h2_weight ? h2_weight.innerText.trim().replace('%', '') : '';
        var stockCount = cols[3].innerText.trim().replace(/,/g, '');
        if (stockCode && stockName && weightStr && stockCount) {
            holdings.push({code: stockCode, name: stockName,
                           count: parseInt(stockCount), weight: parseFloat(weightStr)});
        }
    }
}
return holdings;
"""

def parse_cmoney(driver, etf_code, url):
    holdings = {}
    data_date = "未知"
    is_latest_data = False
    price_info = {"price": "未知", "change_value": "未知", "change_percent": "未知"}
    try:
        driver.get(url)
        try:
            WebDriverWait(driver, 5).until(EC.alert_is_present())
            driver.switch_to.alert.accept()
        except TimeoutException:
            pass
        wait = WebDriverWait(driver, 20)
        try:
            main_info_div = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.stockMainInfo__mainNum')))
            price_container = None
            for cls in ('.up', '.down'):
                try:
                    price_container = main_info_div.find_element(By.CSS_SELECTOR, cls)
                    break
                except NoSuchElementException:
                    continue
            if price_container:
                price_info["price"] = price_container.find_element(By.CSS_SELECTOR, '.stockMainInfo__price').text.strip()
                change_text = price_container.find_element(By.CSS_SELECTOR, '.stockMainInfo__change').text.strip()
                price_info["change_value"] = change_text.replace('▼', '-').replace('▲', '+')
                price_info["change_percent"] = price_container.find_element(By.CSS_SELECTOR, '.stockMainInfo__changePercentage').text.strip()
            else:
                price_info["price"] = main_info_div.find_element(By.CSS_SELECTOR, '.stockMainInfo__price').text.strip()
            date_text = main_info_div.find_element(By.CSS_SELECTOR, '.stockMainInfo__update').text.strip()
            date_str = date_text.replace('更新時間：', '').strip()
            if date_str:
                try:
                    data_date = datetime.strptime(date_str, '%Y/%m/%d')
                    is_latest_data = (data_date.date() == datetime.now().date())
                except Exception:
                    data_date = date_str
        except (NoSuchElementException, TimeoutException) as e:
            logging.warning(f"無法取得收盤價或日期: {e}")
        try:
            wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, 'tbody tr')))
        except Exception as e:
            logging.error(f"等待資料表格超時: {e}")
            return {"holdings": None, "date": data_date, "is_latest": is_latest_data, "price_info": price_info}
        holdings_data = driver.execute_script(CMONEY_JS)
        for item in holdings_data:
            item['name'] = clean_stock_name(item['name'])
            holdings[item['code']] = {"name": item['name'], "count": item['count'], "weight": item['weight']}
        return {"holdings": holdings, "date": data_date, "is_latest": is_latest_data, "price_info": price_info}
    except Exception as e:
        logging.error(f"解析 {etf_code} 時發生錯誤: {e}")
        return {"holdings": None, "date": data_date, "is_latest": is_latest_data, "price_info": price_info}

def load_all_holdings(etf_code):
    filepath = os.path.join(DATA_DIR, f"{etf_code}_holdings.json")
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        logging.error(f"讀取 {filepath} 失敗: {e}")
        return []

def save_current_holdings(etf_code, new_data):
    os.makedirs(DATA_DIR, exist_ok=True)
    filepath = os.path.join(DATA_DIR, f"{etf_code}_holdings.json")
    history_data = load_all_holdings(etf_code)
    if history_data and history_data[-1].get("data_date") == new_data["data_date"]:
        history_data[-1] = new_data
    else:
        history_data.append(new_data)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=4)

def compare_holdings(etf_code, etf_name, today_data, historical_data):
    today_holdings = today_data.get("holdings", {})
    today_stocks = {k: v for k, v in today_holdings.items() if k.isdigit()}
    data_date_str = today_data.get("data_date", "未知")
    is_latest = today_data.get("is_latest", False)
    price_info = today_data.get("price_info", {})
    latest_status = "" if is_latest else " (資料非最新)"
    major_lines = [
        f"【{etf_name} ({etf_code}) 持股異動報告 {data_date_str}{latest_status}】",
        "\n【收盤資訊】:",
        f"  價格: {price_info.get('price','未知')}",
        f"  漲跌價: {price_info.get('change_value','未知')}",
        f"  漲跌幅: {price_info.get('change_percent','未知')}",
    ]
    if not historical_data:
        major_lines.append("\n找不到足夠的歷史資料。")
        return "\n".join(major_lines)
    prev_data = historical_data[-1]
    if today_data['data_date'] == prev_data['data_date']:
        if len(historical_data) >= 2:
            prev_data = historical_data[-2]
        else:
            major_lines.append("\n缺乏昨日資料可供比較。")
            return "\n".join(major_lines)
    prev_stocks = {k: v for k, v in prev_data.get("holdings", {}).items() if k.isdigit()}
    today_keys = set(today_stocks.keys())
    prev_keys = set(prev_stocks.keys())
    new_stocks = today_keys - prev_keys
    removed_stocks = prev_keys - today_keys
    increased_stocks, decreased_stocks = [], []
    for code in today_keys & prev_keys:
        w_diff = today_stocks[code]['weight'] - prev_stocks[code]['weight']
        c_diff = today_stocks[code]['count'] - prev_stocks[code]['count']
        if abs(c_diff) == 0 and abs(w_diff) < WEIGHT_MAJOR_CHANGE_THRESHOLD:
            continue
        w_display = f"\n  (權重變動: {w_diff:+.2f}%)" if abs(w_diff) >= WEIGHT_DISPLAY_THRESHOLD else ""
        entry = {"code": code, "name": today_stocks[code]['name'],
                 "count_change": f"變動: {int(c_diff/1000):+d}張", "weight_change": w_display}
        if c_diff > 0 and (w_diff >= WEIGHT_MAJOR_CHANGE_THRESHOLD or c_diff >= COUNT_MAJOR_CHANGE_THRESHOLD):
            increased_stocks.append(entry)
        elif c_diff < 0 and (w_diff <= -WEIGHT_MAJOR_CHANGE_THRESHOLD or c_diff <= -COUNT_MAJOR_CHANGE_THRESHOLD):
            decreased_stocks.append(entry)
    if not any([new_stocks, removed_stocks, increased_stocks, decreased_stocks]):
        major_lines.append("\n今日持股與前次相比無重大變化。")
    else:
        if new_stocks:
            major_lines.append("\n【新增持股】:")
            for code in new_stocks:
                s = today_stocks[code]
                major_lines.append(f"\n  {s['name']} ({code}), 增加: {int(s['count']/1000)}張, 權重: {s['weight']}%")
        if removed_stocks:
            major_lines.append("\n【刪除持股】:")
            for code in removed_stocks:
                s = prev_stocks[code]
                major_lines.append(f"\n  {s['name']} ({code}), 減少: {int(s['count']/1000)}張")
        if increased_stocks:
            major_lines.append(f"\n【大幅增持】:")
            for s in increased_stocks:
                major_lines.append(f"\n  {s['name']} ({s['code']}) {s['count_change']}{s['weight_change']}")
        if decreased_stocks:
            major_lines.append(f"\n【大幅減持】:")
            for s in decreased_stocks:
                major_lines.append(f"\n  {s['name']} ({s['code']}) {s['count_change']}{s['weight_change']}")
    return "\n".join(major_lines)


class ETFDataProcessor:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.etf_files = {code: f"{code}_holdings.json" for code in ETF_CODE_NAME_MAP}
        self.etf_names = dict(ETF_CODE_NAME_MAP)
        self.raw_data = {}
        self.stock_name_map = {
            "1210":"大成光","1303":"南亞","1319":"東陽","1326":"磨石","1560":"中砂",
            "2317":"鴻海","2330":"台積電","2345":"智邦","2354":"鴻準","2357":"華碩",
            "2368":"金像電","2383":"台光電","2454":"聯發科","2618":"長榮航","2808":"豐祥",
            "3017":"奇鋐","3037":"欣興","3264":"欣銓","3293":"鈺漲","3376":"新日興",
            "3529":"新美亞","3583":"辛耘","3665":"貿聯","3711":"日月光","5347":"世界",
            "5434":"崇義","6121":"新巨","6223":"旺矽","6257":"宏科","6274":"台燿",
            "6515":"力晶","6670":"宏達","8046":"南電","8069":"瑞銀","8114":"振樺",
            "2884":"玉山金","2308":"台達電","2344":"華邦電","2449":"京元電","2027":"大成鋼",
            "6669":"緯穎","1476":"儒鴻","3034":"聯詠",
        }
        self.NON_EQUITY_CODES = {
            "C_NTD","RDI_NTD","M_NTD","PFUR_NTD",
            "202508TX","202509TX","202510TX","202511TX","202512TX",
            "202601TX","202602TX","202603TX","202604TX","202605TX",
            "202606TX","202607TX","202608TX","202609TX","202610TX","202611TX","202612TX",
        }
        self.MIN_PRESENCE_SHARES = 5 * 1000
        self.MIN_COUNT_DELTA_SHARES = 1 * 1000
        self.MIN_WEIGHT_DELTA_PERCENT = 0.25

    def _atomic_write_json(self, path, data):
        dirpath = os.path.dirname(path) or "."
        fd, tmp = tempfile.mkstemp(prefix="tmp_", suffix=".json", dir=dirpath)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush(); os.fsync(f.fileno())
            shutil.move(tmp, path)
        except Exception:
            try: os.remove(tmp)
            except: pass
            raise

    def load_raw_data(self):
        print(f"【2】載入原始數據... 目錄: {self.data_dir}")
        for code, fname in self.etf_files.items():
            filepath = os.path.join(self.data_dir, fname)
            if not os.path.exists(filepath):
                print(f"✗ 找不到: {filepath}")
                return False
            with open(filepath, "r", encoding="utf-8") as f:
                self.raw_data[code] = json.load(f)
            print(f"[OK] {code} ({self.etf_names[code]}) {len(self.raw_data[code])} 筆")
        return True

    def get_latest_two_dates(self, etf_code):
        data = sorted(self.raw_data[etf_code], key=lambda x: x["data_date"])
        return data[-1], (data[-2] if len(data) >= 2 else None)

    def get_stock_name(self, code, name_from_data=""):
        if name_from_data and len(name_from_data) > 1:
            return name_from_data
        return self.stock_name_map.get(code, f"({code})")

    def calculate_daily_changes(self, etf_code):
        latest, prev = self.get_latest_two_dates(etf_code)
        if prev is None:
            return []
        lh = latest.get("holdings", {})
        ph = prev.get("holdings", {})
        changes = []
        for code in set(lh) | set(ph):
            if code in self.NON_EQUITY_CODES:
                continue
            l = lh.get(code, {}); p = ph.get(code, {})
            lc, lw = l.get("count",0), l.get("weight",0.0)
            pc, pw = p.get("count",0), p.get("weight",0.0)
            lce = 0 if lc <= self.MIN_PRESENCE_SHARES else lc
            pce = 0 if pc <= self.MIN_PRESENCE_SHARES else pc
            cd = lc - pc; wd = round(lw - pw, 4)
            include = False; ct = None
            if pce == 0 and lce > 0: ct, include = "新增", True
            elif lce == 0 and pce > 0: ct, include = "刪除", True
            elif cd != 0:
                ct = "增持" if cd > 0 else "減持"
                include = (abs(cd) >= self.MIN_COUNT_DELTA_SHARES or abs(wd) >= self.MIN_WEIGHT_DELTA_PERCENT)
            if not include: continue
            name = self.get_stock_name(code, l.get("name") or p.get("name") or "")
            changes.append({"code":code,"name":name,"type":ct,
                            "count_change":cd//1000,"weight_change":wd,
                            "prev_count":pc//1000,"prev_weight":pw,
                            "current_count":lc//1000,"current_weight":lw})
        changes.sort(key=lambda x: abs(x["count_change"]), reverse=True)
        return changes

    def save_processed_data(self):
        print("【3】產生 processed_etf_data.json...")
        processed = {}
        for code in self.etf_files:
            if code not in self.raw_data: continue
            latest, prev = self.get_latest_two_dates(code)
            pi = latest.get("price_info", {})
            processed[code] = {
                "name": self.etf_names[code],
                "latest_date": latest["data_date"],
                "previous_date": prev["data_date"] if prev else None,
                "price": pi.get("price"), "change_value": pi.get("change_value"),
                "change_percent": pi.get("change_percent"),
                "daily_changes": self.calculate_daily_changes(code),
            }
        outpath = os.path.join(self.data_dir, "processed_etf_data.json")
        self._atomic_write_json(outpath, processed)
        print(f"[OK] {outpath}")

    def get_stock_full_history(self, etf_code, stock_code):
        if etf_code not in self.raw_data: return []
        sorted_data = sorted(self.raw_data[etf_code], key=lambda x: x["data_date"])
        history = []; prev_count = None; prev_weight = 0.0
        for record in sorted_data:
            info = record.get("holdings", {}).get(stock_code)
            cur_raw = info.get("count",0) if info else 0
            cur_w = info.get("weight",0.0) if info else 0.0
            cur = 0 if cur_raw <= self.MIN_PRESENCE_SHARES else cur_raw
            if prev_count is None:
                if cur > 0:
                    history.append({"date":record["data_date"],"count":cur//1000,"weight":cur_w,
                                    "count_change":0,"weight_change":0.0,"status":"首次出現"})
                    prev_count, prev_weight = cur, cur_w
            else:
                if cur == 0 and prev_count > 0:
                    history.append({"date":record["data_date"],"count":0,"weight":0.0,
                                    "count_change":(cur-prev_count)//1000,
                                    "weight_change":round(cur_w-prev_weight,4),"status":"出清"})
                    prev_count, prev_weight = None, 0.0
                elif cur > 0 and cur != prev_count:
                    st = "增持" if cur > prev_count else "減持"
                    history.append({"date":record["data_date"],"count":cur//1000,"weight":cur_w,
                                    "count_change":(cur-prev_count)//1000,
                                    "weight_change":round(cur_w-prev_weight,4),"status":st})
                    prev_count, prev_weight = cur, cur_w
        return history

    def build_all_stock_history(self):
        all_stocks = {}
        for etf_code in self.etf_files:
            codes_raw = set()
            for rec in self.raw_data.get(etf_code, []):
                codes_raw.update(rec.get("holdings",{}).keys())
            for sc in [c for c in codes_raw if c not in self.NON_EQUITY_CODES]:
                if sc not in all_stocks:
                    all_stocks[sc] = {"code":sc,"name":"","etf_holdings":{}}
                history = self.get_stock_full_history(etf_code, sc)
                if not history: continue
                if not all_stocks[sc]["name"]:
                    for rec in reversed(self.raw_data[etf_code]):
                        nm = rec.get("holdings",{}).get(sc,{}).get("name","")
                        if nm and len(nm) > 1:
                            all_stocks[sc]["name"] = nm; break
                if not all_stocks[sc]["name"]:
                    all_stocks[sc]["name"] = self.stock_name_map.get(sc,"")
                max_r = max(history, key=lambda x: x["count"])
                min_r = min(history, key=lambda x: x["count"])
                cur_r = history[-1]
                all_stocks[sc]["etf_holdings"][etf_code] = {
                    "etf_name": self.etf_names[etf_code],
                    "current_count": cur_r["count"], "current_weight": cur_r["weight"],
                    "max_count": max_r["count"], "max_count_date": max_r["date"],
                    "min_count": min_r["count"], "min_count_date": min_r["date"],
                    "history": history,
                }
        return all_stocks

    def save_stock_history_data(self, filename="stock_history_data.json"):
        print("【4】產生 stock_history_data.json...")
        filepath = os.path.join(self.data_dir, filename)
        self._atomic_write_json(filepath, self.build_all_stock_history())
        print(f"[OK] {filepath}")


def fetch_and_save_active_etf_ranking(data_dir):
    URL = "https://www.moneydj.com/etf/x/rank/rank0013-1.xdjhtm?erank=click"
    TABLE_ID = "ctl00_ctl00_MainContent_MainContent_gvTbl"
    HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
               "Accept-Language": "zh-TW,zh;q=0.9", "Referer": "https://www.moneydj.com/"}
    def _f(v):
        try: return float(v)
        except: return None
    try:
        resp = requests.get(URL, headers=HEADERS, timeout=15)
        resp.encoding = "utf-8"; resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", id=TABLE_ID)
        if not table:
            logging.error("找不到 MoneyDJ 排行表格"); return False
        all_etfs = []
        for row in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 14: continue
            code = cells[3]
            if not code.endswith("A"): continue
            all_etfs.append({"code":code,"name":cells[4],"date":cells[5],"currency":cells[6],
                              "return_1d":_f(cells[7]),"return_1w":_f(cells[8]),
                              "return_ytd":_f(cells[9]),"return_1m":_f(cells[10]),
                              "return_3m":_f(cells[11]),"return_6m":_f(cells[12]),"return_1y":_f(cells[13])})
        has_data = sorted([e for e in all_etfs if e["return_3m"] is not None],
                          key=lambda x: x["return_3m"], reverse=True)
        no_data = sorted([e for e in all_etfs if e["return_3m"] is None], key=lambda x: x["name"])
        ranked = []
        for i, e in enumerate(has_data, 1): e["rank"] = i; ranked.append(e)
        for e in no_data: e["rank"] = None; ranked.append(e)
        output = {"updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                  "source": URL, "total": len(ranked), "ranked": len(has_data),
                  "no_data": len(no_data), "ranking": ranked}
        outpath = os.path.join(data_dir, "active_etf_ranking.json")
        fd, tmp = tempfile.mkstemp(prefix="tmp_", suffix=".json", dir=os.path.dirname(outpath) or ".")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2); f.flush(); os.fsync(f.fileno())
        shutil.move(tmp, outpath)
        logging.info(f"[OK] active_etf_ranking.json 已產生（{len(ranked)} 檔）")
        return True
    except Exception as e:
        logging.error(f"MoneyDJ 爬取失敗: {e}"); return False


# ── ★ 【3.6】股價快取：合併區間 + 併發（含今日快取保護）─────

def _fetch_single_price(code, buy_date, end_date, token=""):
    """日期統一轉 YYYY-MM-DD，修正斜線格式 400 錯誤"""
    buy_date = buy_date.replace("/", "-")
    end_date = end_date.replace("/", "-")
    try:
        params = {"dataset": "TaiwanStockPrice", "data_id": code,
                  "start_date": buy_date, "end_date": end_date}
        if token: params["token"] = token
        r = requests.get(FINMIND_URL, params=params, timeout=20)
        r.raise_for_status()
        result = r.json()
        if result.get("status") == 200:
            return [{"date":row["date"],"open":row["open"],"close":row["close"],
                     "max":row["max"],"min":row["min"]} for row in result.get("data",[])]
        logging.warning(f"FinMind 非200 [{code}]: {result.get('msg','')}")
        return None
    except Exception as e:
        logging.error(f"_fetch_single_price 例外 {code}: {e}")
        return None


def _load_existing_price_cache(outpath):
    """
    載入現有的 stock_price_cache.json。
    回傳 dict（code -> {name, prices}），讀取失敗時回傳空 dict 而不中斷流程。
    """
    if not os.path.exists(outpath):
        return {}
    try:
        with open(outpath, "r", encoding="utf-8") as f:
            existing = json.load(f)
        return existing.get("data", {})
    except Exception as e:
        logging.warning(f"讀取既有快取失敗，將視為空快取: {e}")
        return {}


def _cache_has_today_data(prices: list) -> bool:
    """
    判斷快取是否已包含今天的股價。
    採保守策略：prices 中最新一筆日期 >= 今天才算「今天已抓過」。
    若盤後 FinMind 尚未入庫導致重複觸發，可改為 >= yesterday。
    """
    if not prices:
        return False
    today = datetime.now().strftime("%Y-%m-%d")
    latest = max(p["date"] for p in prices)
    return latest >= today


def extract_stock_date_ranges(stock_data):
    """每檔股票只取最早日期，合併所有 ETF 持倉區間"""
    today = datetime.now().strftime('%Y-%m-%d')
    ranges = {}
    for code, info in stock_data.items():
        if not code.isdigit() or code in NON_EQUITY_CODES_PRICE:
            continue
        etf_holdings = info.get("etf_holdings") or info.get("etfholdings") or {}
        all_dates = []
        for holding in etf_holdings.values():
            for h in holding.get("history", []):
                d = h.get("date", "")
                if d: all_dates.append(d.replace("/", "-"))
        if not all_dates: continue
        ranges[code] = {"name": info.get("name",""), "earliest_date": min(all_dates), "end_date": today}
    logging.info(f"合併後共 {len(ranges)} 檔股票")
    return ranges


def fetch_price_cache_merged(stock_ranges, existing_cache, finmind_token="", max_workers=6):
    """
    ★ 核心保護邏輯：
      1. 今日已有資料 -> 直接沿用 existing_cache，不發 API
      2. 第一輪無 Token 抓取失敗 -> 沿用舊快取後列入補抓清單
      3. 第二輪 Token 也失敗 -> 仍沿用舊快取，不寫入空陣列
    """
    cache = {}
    skip_count = 0
    tasks = []

    # ── 分流：今日已有資料 vs 需要抓取 ──────────────────────
    for code, info in stock_ranges.items():
        existing = existing_cache.get(code)
        if existing and _cache_has_today_data(existing.get("prices", [])):
            cache[code] = existing
            skip_count += 1
        else:
            tasks.append((code, info))

    logging.info(
        f"【3.6】今日已有快取: {skip_count} 檔（跳過）；"
        f"需抓取: {len(tasks)} 檔"
    )

    if not tasks:
        logging.info("所有股票今日資料均已存在，跳過全部 API 呼叫。")
        return cache

    # ── 第一輪：無 Token 併發抓取 ────────────────────────────
    missing = []
    total = len(tasks)

    logging.info(f"【3.6 第一輪】無 Token 併發，{total} 檔，workers={max_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_single_price, code, info["earliest_date"], info["end_date"]): (code, info)
            for code, info in tasks
        }
        done = 0
        for future in as_completed(future_map):
            code, info = future_map[future]
            done += 1
            result = future.result()
            if result:
                cache[code] = {"name": info["name"], "prices": result}
            else:
                old = existing_cache.get(code)
                if old and old.get("prices"):
                    cache[code] = old
                    logging.info(f"  [{code}] 抓取失敗，沿用既有快取（{len(old['prices'])} 筆）")
                else:
                    missing.append((code, info))
            if done % 20 == 0 or done == total:
                logging.info(
                    f"  進度: {done}/{total}，"
                    f"成功: {len(cache) - skip_count}，"
                    f"待補: {len(missing)}"
                )

    # ── 第二輪：有 Token 補抓 ─────────────────────────────────
    if missing and finmind_token:
        logging.info(f"【3.6 第二輪】Token 補抓 {len(missing)} 檔...")
        for i, (code, info) in enumerate(missing):
            result = _fetch_single_price(
                code, info["earliest_date"], info["end_date"], token=finmind_token
            )
            if result:
                cache[code] = {"name": info["name"], "prices": result}
            else:
                old = existing_cache.get(code)
                if old and old.get("prices"):
                    cache[code] = old
                    logging.info(f"  [{code}] Token 抓取亦失敗，沿用既有快取")
                else:
                    cache[code] = {"name": info["name"], "prices": []}
            logging.info(
                f"  [{i+1}/{len(missing)}] {code} "
                f"{'✓' if result else '✗（沿用舊資料）'}"
            )
            time.sleep(0.3)
    elif missing:
        for code, info in missing:
            old = existing_cache.get(code)
            cache[code] = (
                old if (old and old.get("prices"))
                else {"name": info["name"], "prices": []}
            )
        logging.warning(
            f"有 {len(missing)} 檔遺漏，未設定 FINMIND_TOKEN；"
            f"已沿用既有快取（如有）"
        )

    ok = sum(1 for v in cache.values() if v.get("prices"))
    logging.info(f"完成：有資料 {ok} 檔，空值 {len(cache) - ok} 檔")
    return cache


def step_fetch_price_cache():
    print("\n" + "="*60)
    print("【3.6】批次抓取股票歷史股價（合併區間 + 併發）...")
    print("="*60)

    path = os.path.join(DATA_DIR, "stock_history_data.json")
    if not os.path.exists(path):
        logging.error(f"找不到 {path}"); return False

    with open(path, "r", encoding="utf-8") as f:
        stock_data = json.load(f)

    stock_ranges = extract_stock_date_ranges(stock_data)
    if not stock_ranges:
        logging.warning("沒有股票資料"); return False

    outpath = os.path.join(DATA_DIR, "stock_price_cache.json")

    # ★ 載入既有快取，保護已有的今日資料不被洗掉
    existing_cache = _load_existing_price_cache(outpath)
    logging.info(f"既有快取載入完成，共 {len(existing_cache)} 檔")

    price_cache = fetch_price_cache_merged(
        stock_ranges, existing_cache, finmind_token=FINMIND_TOKEN
    )

    output = {
        "updated_at": datetime.now().isoformat(),
        "note": "每檔股票完整歷史，前端依持倉日期 filter",
        "data": price_cache,
    }

    fd, tmp = tempfile.mkstemp(
        prefix="tmp_", suffix=".json",
        dir=os.path.dirname(outpath) or "."
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
            f.flush(); os.fsync(f.fileno())
        shutil.move(tmp, outpath)
        print(f"[OK] stock_price_cache.json 已產生，共 {len(price_cache)} 檔")
        return True
    except Exception as e:
        try: os.remove(tmp)
        except: pass
        logging.error(f"寫入失敗: {e}"); return False


# ── Git 上傳 ─────────────────────────────────────────────────
def git_commit_and_push(data_dir):
    if not GIT_ENABLED:
        logging.info("Git 已停用"); return True
    orig = os.getcwd()
    try:
        os.chdir(data_dir)
        if subprocess.run(['git','rev-parse','--git-dir'],
                          capture_output=True, timeout=10).returncode != 0:
            logging.error("不是 Git 倉庫"); os.chdir(orig); return False
        if GIT_REMOTE_URL:
            subprocess.run(['git','remote','set-url','origin',GIT_REMOTE_URL],
                           capture_output=True, timeout=10)
        files = ['processed_etf_data.json','stock_history_data.json','homepage_index.json',
                 'active_etf_ranking.json','stock_price_cache.json','data_manifest.json']
        added = False
        for fn in files:
            if os.path.exists(os.path.join(data_dir, fn)):
                subprocess.run(['git','add','-f',fn], check=True, timeout=10)
                logging.info(f"✓ git add {fn}"); added = True
            else:
                logging.warning(f"找不到 {fn}")
        if not added:
            logging.warning("沒有檔案可上傳"); os.chdir(orig); return False
        msg = f"Auto update ETF data - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        status = subprocess.run(['git','status','--porcelain'],
                                capture_output=True, text=True, timeout=10)
        if status.stdout.strip():
            subprocess.run(['git','commit','-m',msg], capture_output=True, check=True, timeout=30)
        else:
            subprocess.run(['git','commit','--allow-empty','-m',msg],
                           capture_output=True, check=True, timeout=30)
        result = subprocess.run(['git','push'], capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            logging.info("✅ 推送成功！"); os.chdir(orig); return True
        else:
            logging.error(f"git push 失敗: {result.stderr}"); os.chdir(orig); return False
    except Exception as e:
        logging.error(f"Git 錯誤: {e}"); os.chdir(orig); return False


# ── 主程式 ────────────────────────────────────────────────────
def main():
    logging.info("="*60)
    logging.info("ETF 持股追蹤器（整合版）啟動")
    logging.info("="*60)

    # 【1】爬蟲
    print("\n" + "="*60 + "\n【1】開始爬取 ETF 持股資料...\n" + "="*60)
    driver = setup_webdriver_with_retry(max_retries=2)
    if not driver:
        send_telegram_message("❌ WebDriver 初始化失敗")
        sys.exit(1)
    try:
        for code, info in ETF_TARGETS.items():
            historical_data = load_all_holdings(code)
            result = parse_cmoney(driver, code, info['url'])
            if result.get("holdings"):
                dd = result["date"]
                today_entry = {
                    "data_date": dd.strftime('%Y/%m/%d') if isinstance(dd, datetime) else str(dd),
                    "is_latest": result.get("is_latest"),
                    "price_info": result.get("price_info"),
                    "holdings": result["holdings"],
                }
                report = compare_holdings(code, info['name'], today_entry, historical_data)
                print(report); send_telegram_message(report)
                save_current_holdings(code, today_entry)
            else:
                msg = f"無法獲取 {info['name']} 的持股資料"
                logging.error(msg); send_telegram_message(msg)
    finally:
        driver.quit()

    # 【2~3】歷史資料
    print("\n" + "="*60 + "\n【2】處理歷史資料...\n" + "="*60)
    processor = ETFDataProcessor(DATA_DIR)
    if not processor.load_raw_data():
        sys.exit(1)
    processor.save_processed_data()
    processor.save_stock_history_data()
    homepage_index = write_homepage_index(DATA_DIR)

    # 【3.5】MoneyDJ
    print("\n" + "="*60 + "\n【3.5】MoneyDJ 排行...\n" + "="*60)
    fetch_and_save_active_etf_ranking(DATA_DIR)

    # 【3.6】股價快取（重複執行安全）
    step_fetch_price_cache()

    # 發布前建立資料契約並驗證，避免網站讀到半成品或欄位不相容的 JSON。
    write_data_manifest(DATA_DIR, homepage_index["latest_data_date"])
    try:
        validate_public_data(DATA_DIR)
        logging.info("[OK] 公開資料契約驗證通過")
    except ValueError as exc:
        logging.error(f"公開資料驗證失敗：{exc}")
        sys.exit(1)

    # 【4】Git
    print("\n" + "="*60 + "\n【4】上傳到 GitHub...\n" + "="*60)
    if git_commit_and_push(DATA_DIR):
        print("\n✅ 所有任務完成！")
    else:
        print("\n⚠️ Git 上傳失敗")
        sys.exit(1)

if __name__ == "__main__":
    main()
