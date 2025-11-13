import logging
import json
import os
import time
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException, TimeoutException, NoSuchElementException, UnexpectedAlertPresentException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import re
import glob

# --- 使用者自訂設定 ---
ETF_TARGETS = {
    "00981A": {
        "name": "統一台股增長",
        "url": "https://www.cmoney.tw/etf/tw/00981A/fundholding",
        "parser": "parse_cmoney"
    },
    "00980A": {
        "name": "野村臺灣智慧優選主動式ETF",
        "url": "https://www.cmoney.tw/etf/tw/00980A/fundholding",
        "parser": "parse_cmoney"
    },
    "00982A": {
        "name": "群益台灣精選強棒主動式ETF基金",
        "url": "https://www.cmoney.tw/etf/tw/00982A/fundholding",
        "parser": "parse_cmoney"
    }
}

# 設定持股變動的篩選門檻
# 重大變動門檻
WEIGHT_MAJOR_CHANGE_THRESHOLD = 0.25
COUNT_MAJOR_CHANGE_THRESHOLD = 50 * 1000

# 詳細變動列表門檻
COUNT_DETAILED_CHANGE_THRESHOLD = 30 * 1000

# 重大變動中，是否顯示權重變動的門檻
WEIGHT_DISPLAY_THRESHOLD = 0.15

# Telegram 設定（優先讀環境變數，無則回退到硬編值）
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "5364947038:AAFk-1_H7kAbb9D6nTYzxwi8SDXCQ3Scthk")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "-1002497578534")

# --- 常數設定 ---
# 支援環境變數 DATA_DIR（優先），否則使用你提供的預設路徑
DATA_DIR = os.environ.get("DATA_DIR", r"C:\Users\j3210\OneDrive\桌面\ETF網站執行檔")

# --- 設定日誌 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_webdriver():
    """使用 undetected-chromedriver 設定 WebDriver"""
    try:
        logging.info("正在啟動 undetected-chromedriver...")
        options = uc.ChromeOptions()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1200,800')
        
        # 讓 uc 自動處理 driver 版本
        driver = uc.Chrome(options=options, use_subprocess=True)
        return driver
    except Exception as e:
        logging.error(f"初始化 undetected-chromedriver 時發生錯誤: {e}")
        return None

def escape_markdown_v2(text: str) -> str:
    # 官方規範需要跳脫的符號
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", text)


def clean_stock_name(name):
    """移除股票名稱中會造成問題的特殊字元"""
    return name.replace('*', '')

def split_and_send_message(message, chat_id, bot_token):
    """將長訊息拆分成多條訊息發送"""
    max_length = 4096
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    parts = []
    current_part = ""
    # 按照換行符號來拆分原始訊息
    for line in message.split('\n'):
        # 如果目前部分加上新的一行超過了長度限制
        if len(current_part) + len(line) + 1 > max_length:
            # 如果目前部分有內容，就將其加入列表
            if current_part:
                parts.append(current_part)
            current_part = line
        else:
            # 否則，將新的一行加到目前部分
            current_part += "\n" + line
    
    # 加入最後剩餘的部分
    if current_part:
        parts.append(current_part)

    logging.info(f"訊息過長，將拆分成 {len(parts)} 則訊息發送。")

    for part in parts:
        payload = {
            "chat_id": chat_id,
            "text": part
            # 注意：拆分後的訊息以純文字發送，避免部分訊息的 Markdown 格式不完整
        }
        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            logging.info("部分訊息已成功發送。")
            time.sleep(1) # 每次發送後暫停1秒，避免觸發API速率限制
        except requests.exceptions.RequestException as e:
            logging.error(f"發送部分訊息失敗: {e}")
            # 即使部分失敗，也繼續嘗試發送下一部分

def send_telegram_message(message):
    """
    發送訊息到 Telegram。
    1. 檢查訊息長度，如果過長則調用拆分函式。
    2. 嘗試用 MarkdownV2 發送。
    3. 如果 MarkdownV2 失敗，則降級為純文字發送。
    """
    # --- 步驟 1: 檢查訊息長度 ---
    if len(message.encode('utf-8')) > 4000: # 使用 utf-8 字節長度判斷並保留一些餘裕
        logging.warning(f"訊息長度 ({len(message)}) 可能超過上限，啟用自動拆分模式。")
        split_and_send_message(message, TELEGRAM_CHAT_ID, TELEGRAM_BOT_TOKEN)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    # --- 步驟 2: 嘗試使用 MarkdownV2 發送 ---
    payload_markdown = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": escape_markdown_v2(message),
        "parse_mode": "MarkdownV2"
    }

    try:
        logging.info("正在使用 MarkdownV2 格式發送 Telegram 訊息...")
        response = requests.post(url, json=payload_markdown, timeout=10)
        response.raise_for_status()
        logging.info("Telegram 訊息 (MarkdownV2) 發送成功。")
        return
    except requests.exceptions.RequestException as e:
        # --- 步驟 3: 降級為純文字模式 ---
        if hasattr(e, 'response') and e.response is not None and e.response.status_code == 400:
            logging.warning(f"MarkdownV2 發送失敗，錯誤: {e}。訊息可能包含無效的 Markdown 語法。")
            logging.info("正在嘗試使用純文字格式重新發送...")
            
            payload_text = { "chat_id": TELEGRAM_CHAT_ID, "text": message }
            
            try:
                response_text = requests.post(url, json=payload_text, timeout=10)
                response_text.raise_for_status()
                logging.info("Telegram 訊息 (純文字) 已成功重新發送。")
            except requests.exceptions.RequestException as e_text:
                # 如果純文字也失敗，記錄詳細錯誤
                logging.error(f"純文字格式重新發送失敗: {e_text}")
                logging.error(f"失敗的訊息內容 (前200字): {message[:200]}")
        else:
            logging.error(f"發送 Telegram 訊息失敗: {e}")

def parse_cmoney(driver, etf_code, url):
    """從 CMoney 網站抓取ETF持股明細"""
    holdings = {}
    data_date = "未知"
    is_latest_data = False
    price_info = {
        "price": "未知",
        "change_value": "未知",
        "change_percent": "未知"
    }

    try:
        driver.get(url)
        logging.info(f"正在載入 CMoney 頁面: {url}")
        
        try:
            wait = WebDriverWait(driver, 5)
            wait.until(EC.alert_is_present())
            alert = driver.switch_to.alert
            logging.warning(f"偵測到彈窗: {alert.text}")
            alert.accept()
        except TimeoutException:
            pass
        
        wait = WebDriverWait(driver, 20)
        
        try:
            main_info_div = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.stockMainInfo__mainNum')))
            
            price_container = None
            try:
                price_container = main_info_div.find_element(By.CSS_SELECTOR, '.up')
            except NoSuchElementException:
                try:
                    price_container = main_info_div.find_element(By.CSS_SELECTOR, '.down')
                except NoSuchElementException:
                    pass

            if price_container:
                price_info["price"] = price_container.find_element(By.CSS_SELECTOR, '.stockMainInfo__price').text.strip()
                change_text = price_container.find_element(By.CSS_SELECTOR, '.stockMainInfo__change').text.strip().replace('▼', '-').replace('▲', '+')
                price_info["change_value"] = change_text
                price_info["change_percent"] = price_container.find_element(By.CSS_SELECTOR, '.stockMainInfo__changePercentage').text.strip()
            else:
                price_info["price"] = main_info_div.find_element(By.CSS_SELECTOR, '.stockMainInfo__price').text.strip()
                logging.warning("未能從 up/down div 中提取漲跌資訊，價格資訊可能不完整。")

            date_element = main_info_div.find_element(By.CSS_SELECTOR, '.stockMainInfo__update')
            date_text = date_element.text.strip()
            date_str = date_text.replace('更新時間：', '').strip()
            
            if date_str:
                try:
                    data_date = datetime.strptime(date_str, '%Y/%m/%d')
                    today = datetime.now()
                    if data_date.date() == today.date():
                        is_latest_data = True
                    logging.info(f"成功取得收盤資訊與資料日期: {data_date.strftime('%Y/%m/%d')}, 是否為最新: {is_latest_data}")
                except Exception:
                    logging.warning("解析日期格式失敗，保留原始日期字串。")
                    data_date = date_str
            else:
                logging.warning("找到日期元素，但內容為空。")

        except (NoSuchElementException, TimeoutException) as e:
            logging.warning(f"無法找到收盤價或日期元素，可能網頁結構已改變。錯誤: {e}")
        except Exception as e:
            logging.warning(f"解析收盤價或日期時發生錯誤: {e}")

        try:
            wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, 'tbody tr')))
            logging.info("成功找到持股表格及所有資料列。")
        except Exception as e:
            logging.error(f"在 CMoney 頁面等待資料超時: {e}")
            return {"holdings": None, "date": data_date, "is_latest": is_latest_data, "price_info": price_info}
        
        js_code = """
        var holdings = [];
        var rows = document.querySelectorAll('tbody tr');
        for (var i = 0; i < rows.length; i++) {
            var cols = rows[i].querySelectorAll('td');
            if (cols.length >= 4 && cols[0].innerText.trim() !== '') {
                var stockCode = cols[0].innerText.trim();
                
                var h2_name_element = cols[1].querySelector('h2');
                var stockName = '';
                if (h2_name_element) {
                    stockName = h2_name_element.getAttribute('title') || h2_name_element.innerText.trim();
                }

                var h2_weight = cols[2].querySelector('h2');
                var weightStr = h2_weight ? h2_weight.innerText.trim().replace('%', '') : '';

                var stockCount = cols[3].innerText.trim().replace(/,/g, '');

                if (stockCode && stockName && weightStr && stockCount) {
                    holdings.push({
                        code: stockCode,
                        name: stockName,
                        count: parseInt(stockCount),
                        weight: parseFloat(weightStr)
                    });
                }
            }
        }
        return holdings;
        """
        
        try:
            holdings_data = driver.execute_script(js_code)
            logging.info(f"成功使用 JavaScript 抓取到 {len(holdings_data)} 筆持股資料。")
            
            for item in holdings_data:
                # 在此處清理股票名稱
                item['name'] = clean_stock_name(item['name'])
                holdings[item['code']] = {"name": item['name'], "count": item['count'], "weight": item['weight']}
        except Exception as e:
            logging.error(f"執行 JavaScript 或解析資料時發生錯誤: {e}")
            return {"holdings": None, "date": data_date, "is_latest": is_latest_data, "price_info": price_info}
        
        if not holdings:
            logging.warning("抓取到的持股列表為空，請檢查網頁內容或解析邏輯。")
        
        return {"holdings": holdings, "date": data_date, "is_latest": is_latest_data, "price_info": price_info}
        
    except Exception as e:
        logging.error(f"解析 {etf_code} 資料時發生未知錯誤: {e}")
        return {"holdings": None, "date": data_date, "is_latest": is_latest_data, "price_info": price_info}

def load_all_holdings(etf_code):
    """
    載入單一 JSON 檔案中所有歷史持股資料。
    """
    filepath = os.path.join(DATA_DIR, f"{etf_code}_holdings.json")
    if not os.path.exists(filepath):
        logging.info(f"找不到 {etf_code} 的歷史資料檔案。")
        return []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if not isinstance(data, list):
                logging.warning(f"檔案 {filepath} 格式不正確，預期為列表。")
                return []
            logging.info(f"成功載入 {len(data)} 筆 {etf_code} 的歷史資料。")
            return data
    except (json.JSONDecodeError, IOError) as e:
        logging.error(f"讀取 {filepath} 舊持股資料失敗: {e}")
        return []

def save_current_holdings(etf_code, new_data):
    """將當天的持股資料附加到歷史檔案中，或更新當天的資料"""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    filepath = os.path.join(DATA_DIR, f"{etf_code}_holdings.json")
    
    history_data = load_all_holdings(etf_code)
    
    if history_data and history_data[-1].get("data_date") == new_data["data_date"]:
        history_data[-1] = new_data
        logging.info(f"檔案 {filepath} 已更新今天的資料。")
    else:
        history_data.append(new_data)
        logging.info(f"檔案 {filepath} 已新增一筆今天的資料。")
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(history_data, f, ensure_ascii=False, indent=4)
        logging.info(f"已成功儲存 {filepath} 的最新資料。")
    except IOError as e:
        logging.error(f"儲存 {etf_code} 持股資料失敗: {e}")

def compare_holdings(etf_code, etf_name, today_data, historical_data):
    """
    比對新舊持股，並回傳差異報告字串。
    """
    today_holdings = today_data.get("holdings", {})
    today_stocks_clean = {k: v for k, v in today_holdings.items() if k.isdigit()}
    
    report_parts = []
    major_report_lines = []
    
    data_date_str = today_data.get("data_date", "未知")
    is_latest_data = today_data.get("is_latest", False)
    price_info = today_data.get("price_info", {})
    
    latest_status = "" if is_latest_data else " (資料非最新，請注意)"
    major_report_lines.append(f"【{etf_name} ({etf_code}) 持股異動報告 {data_date_str}{latest_status}】")
    
    major_report_lines.append("\n【收盤資訊】:")
    major_report_lines.append(f"  價格: {price_info.get('price', '未知')}")
    major_report_lines.append(f"  漲跌價: {price_info.get('change_value', '未知')}")
    major_report_lines.append(f"  漲跌幅: {price_info.get('change_percent', '未知')}")

    if not historical_data:
        major_report_lines.append(f"\n找不到足夠的歷史資料，本日抓取結果已存為基準。")
        report_parts.append("\n".join(major_report_lines))
        return "\n\n".join(report_parts)

    # 修正後的邏輯：根據是否為同一天來選擇比較基準
    yesterday_holdings_data = historical_data[-1]
    if today_data['data_date'] == yesterday_holdings_data['data_date']:
        if len(historical_data) >= 2:
            yesterday_holdings_data = historical_data[-2]
        else:
            major_report_lines.append(f"\n當天多次執行，但缺乏昨日資料可供比較。")
            report_parts.append("\n".join(major_report_lines))
            return "\n\n".join(report_parts)
            
    yesterday_holdings = yesterday_holdings_data.get("holdings", {})
    yesterday_stocks_clean = {k: v for k, v in yesterday_holdings.items() if k.isdigit()}

    today_stock_keys = set(today_stocks_clean.keys())
    yesterday_stock_keys = set(yesterday_stocks_clean.keys())

    new_stocks = today_stock_keys - yesterday_stock_keys
    removed_stocks = yesterday_stock_keys - today_stock_keys
    increased_stocks = []
    decreased_stocks = []
    
    common_stocks = today_stock_keys & yesterday_stock_keys
    for stock_code in common_stocks:
        weight_diff = today_stocks_clean[stock_code]['weight'] - yesterday_stocks_clean[stock_code]['weight']
        count_diff = today_stocks_clean[stock_code]['count'] - yesterday_stocks_clean[stock_code]['count']

        is_major_increase = count_diff > 0 and (weight_diff >= WEIGHT_MAJOR_CHANGE_THRESHOLD or count_diff >= COUNT_MAJOR_CHANGE_THRESHOLD)
        is_major_decrease = count_diff < 0 and (weight_diff <= -WEIGHT_MAJOR_CHANGE_THRESHOLD or count_diff <= -COUNT_MAJOR_CHANGE_THRESHOLD)

        if abs(count_diff) == 0 and abs(weight_diff) < WEIGHT_MAJOR_CHANGE_THRESHOLD:
            continue

        if is_major_increase:
            # 修正：將權重變動拆成下一行顯示
            weight_change_display = f"\n  (權重變動: {weight_diff:+.2f}%)" if abs(weight_diff) >= WEIGHT_DISPLAY_THRESHOLD else ""
            increased_stocks.append({
                "code": stock_code,
                "name": today_stocks_clean[stock_code]['name'],
                "count_change": f"變動: {int(count_diff / 1000):+d}張",
                "weight_change": weight_change_display
            })
        elif is_major_decrease:
            # 修正：將權重變動拆成下一行顯示
            weight_change_display = f"\n  (權重變動: {weight_diff:+.2f}%)" if abs(weight_diff) >= WEIGHT_DISPLAY_THRESHOLD else ""
            decreased_stocks.append({
                "code": stock_code,
                "name": today_stocks_clean[stock_code]['name'],
                "count_change": f"變動: {int(count_diff / 1000):+d}張",
                "weight_change": weight_change_display
            })
    
    has_major_changes = any([new_stocks, removed_stocks, increased_stocks, decreased_stocks])
    if not has_major_changes:
        major_report_lines.append("\n今日持股與前次相比無重大變化。")
    else:
        if new_stocks:
            major_report_lines.append("\n【新增持股】:")
            for stock_code in new_stocks:
                stock = today_stocks_clean[stock_code]
                major_report_lines.append(f"\n  {stock['name']} ({stock_code}), 增加: {int(stock['count']/1000)}張, 權重: {stock['weight']}%")

        if removed_stocks:
            major_report_lines.append("\n【刪除持股】:")
            for stock_code in removed_stocks:
                stock = yesterday_stocks_clean[stock_code]
                major_report_lines.append(f"\n  {stock['name']} ({stock_code}), 減少: {int(stock['count']/1000)}張, 原有權重: {stock['weight']}%")
                
        if increased_stocks:
            major_report_lines.append(f"\n【權重增加達 {WEIGHT_MAJOR_CHANGE_THRESHOLD}% 或張數增加達 {int(COUNT_MAJOR_CHANGE_THRESHOLD/1000)}張 以上】:")
            for stock in increased_stocks:
                major_report_lines.append(f"\n  {stock['name']} ({stock['code']}) {stock['count_change']}{stock['weight_change']}")
        
        if decreased_stocks:
            major_report_lines.append(f"\n【權重減少達 {WEIGHT_MAJOR_CHANGE_THRESHOLD}% 或張數減少達 {int(COUNT_MAJOR_CHANGE_THRESHOLD/1000)}張 以上】:")
            for stock in decreased_stocks:
                major_report_lines.append(f"\n  {stock['name']} ({stock['code']}) {stock['count_change']}{stock['weight_change']}")
    
    report_parts.append("\n".join(major_report_lines))

    detailed_report_lines = []
    all_stock_keys = today_stock_keys | yesterday_stock_keys
    
    if all_stock_keys:
        detailed_report_lines.append("\n---")
        detailed_report_lines.append("【詳細變動列表】")
        
        for stock_code in sorted(list(all_stock_keys)):
            today = today_stocks_clean.get(stock_code)
            yesterday = yesterday_stocks_clean.get(stock_code)
            
            has_changed = False
            if today and not yesterday:
                has_changed = True
            elif not today and yesterday:
                has_changed = True
            elif today and yesterday:
                count_diff = today['count'] - yesterday['count']
                if abs(count_diff) > COUNT_DETAILED_CHANGE_THRESHOLD:
                    has_changed = True
            
            if has_changed:
                if today and not yesterday:
                    detailed_report_lines.append(f"\n新增: {today['name']} ({stock_code})")
                    detailed_report_lines.append(f"  張數: {int(today['count'] / 1000)}, 權重: {today['weight']}%")
                elif not today and yesterday:
                    detailed_report_lines.append(f"\n刪除: {yesterday['name']} ({stock_code})")
                    detailed_report_lines.append(f"  原有張數: {int(yesterday['count'] / 1000)}, 原有權重: {yesterday['weight']}%")
                elif today and yesterday:
                    count_diff = today['count'] - yesterday['count']
                    weight_diff = today['weight'] - yesterday['weight']
                    detailed_report_lines.append(f"\n{today['name']} ({stock_code})")
                    detailed_report_lines.append(f"  張數變動: {int(yesterday['count'] / 1000)} -> {int(today['count'] / 1000)} ({int(count_diff / 1000):+d}張)")
                    detailed_report_lines.append(f"  權重變動: {yesterday['weight']}% -> {today['weight']}% ({weight_diff:+.2f}%)")

        if len(detailed_report_lines) > 1:
            report_parts.append("\n".join(detailed_report_lines))

    return "\n\n".join(report_parts)

def job():
    """執行的主要任務"""
    logging.info("開始執行每日ETF持股變動分析...")
    driver = setup_webdriver()
    if not driver:
        logging.error("WebDriver 初始化失敗，終止本次任務。")
        return
        
    try:
        for code, info in ETF_TARGETS.items():
            logging.info(f"正在處理 {info['name']} ({code})...")
            
            historical_data = load_all_holdings(code)
            
            parser_function_name = info.get("parser")
            if parser_function_name == "parse_cmoney":
                scrape_result = parse_cmoney(driver, code, info['url'])
            else:
                logging.error(f"找不到 {code} 對應的解析器: {parser_function_name}")
                continue

            current_holdings = scrape_result.get("holdings")
            data_date = scrape_result.get("date")
            is_latest_data = scrape_result.get("is_latest")
            price_info = scrape_result.get("price_info")
            
            if current_holdings:
                today_data_entry = {
                    "data_date": data_date.strftime('%Y/%m/%d') if isinstance(data_date, datetime) else str(data_date),
                    "is_latest": is_latest_data,
                    "price_info": price_info,
                    "holdings": current_holdings
                }
                
                report_message = compare_holdings(code, info['name'], today_data_entry, historical_data)
                print(report_message)
                
                send_telegram_message(report_message)
                
                save_current_holdings(code, today_data_entry)
            else:
                logging.error(f"無法獲取 {info['name']} 的持股資料，跳過本次分析。")
                send_telegram_message(f"無法獲取 {info['name']} 的持股資料，跳過本次分析。")
    finally:
        if driver:
            driver.quit()
        logging.info("每日分析任務執行完畢。")

def main():
    """主執行函式"""
    logging.info("主動型ETF持股追蹤器已啟動。")
    job()
    logging.info("所有任務已執行完畢，程式即將關閉。")

if __name__ == "__main__":
    main()
