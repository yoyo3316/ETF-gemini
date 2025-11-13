# -*- coding: utf-8 -*-
"""
後端處理：產生 processed_etf_data.json 與 stock_history_data.json
調整項目：
  1) 異動只在「張數真的有變」或「新增／刪除」時才列出；張數持平但權重變動一律忽略
  2) 歷史明細不輸出「持平」的紀錄
  3) 把「前一日張數 <= MIN_PRESENCE_SHARES（5 張）」視為不存在 -> 若今天有持股則視為新增
  4) 與現有前端相容：數量仍以「張」為單位（shares // 1000）
  5) 支援環境變數 DATA_DIR 或使用預設路徑
  6) 原子性寫檔（先寫 tmp 再 replace）
"""

import json
import os
import tempfile
import shutil
from typing import Dict, List

class ETFDataProcessor:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir

        # 原始每日檔的檔名（請確認資料夾內有這三個檔）
        self.etf_files = {
            "00980A": "00980A_holdings.json",
            "00981A": "00981A_holdings.json",
            "00982A": "00982A_holdings.json",
        }

        # ETF 顯示名稱
        self.etf_names = {
            "00980A": "野村臺灣智慧優選主動式ETF",
            "00981A": "統一台股增長",
            "00982A": "群益台灣精選強棒主動式ETF基金",
        }

        self.raw_data: Dict[str, list] = {}

        # 常見股票名稱補完（資料缺名時使用）
        self.stock_name_map = {
            "1210": "大立光", "1303": "南亞", "1319": "東陽", "1326": "磨石", "1560": "中砂",
            "2317": "鴻海", "2330": "台積電", "2345": "智邦", "2354": "鴻準", "2357": "華碩",
            "2368": "金像電", "2383": "台光電", "2454": "聯發科", "2618": "長榮",
            "2808": "豐祥", "3017": "奇鋐", "3037": "欣興", "3264": "欣銓", "3293": "鈺漲",
            "3376": "新日興", "3529": "新美亞", "3583": "辛耘", "3665": "貿聯", "3711": "日月光",
            "5347": "世界", "5434": "崇義", "6121": "新巨", "6223": "旺矽", "6257": "宏科",
            "6274": "台燿", "6515": "力晶", "6670": "宏達", "8046": "南電", "8069": "瑞銀",
            "8114": "振樺", "2884": "玉山金", "2308": "台達電", "2344": "華邦電", "2449": "京元電",
            "2027": "大成鋼", "6669": "緯穎", "2024": "鴨肉王", "1476": "儒鴻", "3034": "聯詠"
        }

        # ---- 參數 ----
        # 把「存在」的判定門檻：MIN_PRESENCE_SHARES 表示少於等於多少 shares 視為不存在（預設 5 張）
        # （注意：資料用 shares 為單位；1 張 = 1000 shares）
        self.MIN_PRESENCE_SHARES = 5 * 1000  # 5 張 -> 5000 shares

        # 異動要納入每日變動的門檻（當張數確實變動時才套用）
        # 這個門檻僅在張數變動不為 0 時用來去雜訊：如果張數差異很小且權重差異也很小就忽略
        self.MIN_COUNT_DELTA_SHARES = 1 * 1000   # 1 張 = 1000 shares (可視情況提高)
        self.MIN_WEIGHT_DELTA_PERCENT = 0.25     # 權重變動門檻 0.25%（僅在張數變動有意義時作為放寬條件）

    # ---------- atomic write helper ----------
    def _atomic_write_json(self, path: str, data) -> None:
        """先寫入臨時檔，再移動覆蓋（避免半成品）"""
        dirpath = os.path.dirname(path) or "."
        fd, tmp_path = tempfile.mkstemp(prefix="tmp_", suffix=".json", dir=dirpath)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            # 使用 move/replace，Windows 下會覆蓋
            shutil.move(tmp_path, path)
        except Exception:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            raise

    # ---------- 基本載入 ----------
    def load_raw_data(self) -> bool:
        print(f"【1】載入原始數據...\n   目錄: {self.data_dir}\n")
        for code, fname in self.etf_files.items():
            filepath = os.path.join(self.data_dir, fname)
            if not os.path.exists(filepath):
                print(f"✗ 檔案不存在：{filepath}")
                return False
            with open(filepath, "r", encoding="utf-8") as f:
                self.raw_data[code] = json.load(f)
            print(f"✓ 已載入 {code} ({self.etf_names[code]}) 共 {len(self.raw_data[code])} 筆日期資料")
        return True

    def get_latest_two_dates(self, etf_code: str):
        data = self.raw_data[etf_code]
        sorted_data = sorted(data, key=lambda x: x["data_date"])
        latest = sorted_data[-1]
        prev = sorted_data[-2]
        return latest, prev

    def get_stock_name(self, code: str, name_from_data: str = "") -> str:
        """若資料無名稱，從對應表補充"""
        if name_from_data and len(name_from_data) > 1:
            return name_from_data
        return self.stock_name_map.get(code, f"({code})")

    # ---------- 異動明細（給 processed_etf_data.json） ----------
    def calculate_daily_changes(self, etf_code: str) -> List[Dict]:
        """
        只在「張數有變動」或「新增／刪除」時輸出；
        張數持平（count_delta == 0）則忽略（不論權重）
        對於出清/新增的判定：使用 MIN_PRESENCE_SHARES 作為存在門檻
        """
        latest, prev = self.get_latest_two_dates(etf_code)
        latest_holdings = latest.get("holdings", {})
        prev_holdings = prev.get("holdings", {})

        all_codes = set(latest_holdings) | set(prev_holdings)
        changes: List[Dict] = []

        for code in all_codes:
            l = latest_holdings.get(code, {})
            p = prev_holdings.get(code, {})

            lc = l.get("count", 0)     # shares
            lw = l.get("weight", 0.0)  # %
            pc = p.get("count", 0)
            pw = p.get("weight", 0.0)

            # 將很小的存在視為 0（門檻：MIN_PRESENCE_SHARES）
            pc_effective = 0 if pc <= self.MIN_PRESENCE_SHARES else pc
            lc_effective = 0 if lc <= self.MIN_PRESENCE_SHARES else lc

            count_delta = lc - pc
            weight_delta = round(lw - pw, 4)

            # 判斷類型：先看 presence，再看張數變化
            include = False
            change_type = None

            if pc_effective == 0 and lc_effective > 0:
                change_type = "新增"
                include = True
            elif lc_effective == 0 and pc_effective > 0:
                change_type = "刪除"
                include = True
            else:
                # 兩天都存在（或都視為存在） -> 只在張數真正改變時才考慮
                if count_delta == 0:
                    include = False  # 張數沒變，不列（即使權重有差也不列）
                else:
                    change_type = "增持" if count_delta > 0 else "減持"
                    # 張數有變才用門檻：若張數差異超過 MIN_COUNT_DELTA_SHARES 或權重差超過 MIN_WEIGHT_DELTA_PERCENT 才列
                    include = (abs(count_delta) >= self.MIN_COUNT_DELTA_SHARES) or (abs(weight_delta) >= self.MIN_WEIGHT_DELTA_PERCENT)

            if not include:
                continue

            # 名稱補齊
            name = l.get("name") or p.get("name") or ""
            name = self.get_stock_name(code, name)

            changes.append({
                "code": code,
                "name": name,
                "type": change_type,
                "count_change": count_delta // 1000,        # 給前端顯示「張」
                "weight_change": weight_delta,
                "prev_count": pc // 1000,
                "prev_weight": pw,
                "current_count": lc // 1000,
                "current_weight": lw
            })

        # 依張數變動量排序（大到小）
        changes.sort(key=lambda x: abs(x["count_change"]), reverse=True)
        return changes

    def save_processed_data(self) -> None:
        processed: Dict[str, Dict] = {}

        for etf_code in self.etf_files:
            if etf_code not in self.raw_data:
                continue

            latest, prev = self.get_latest_two_dates(etf_code)
            price_info = latest.get("price_info", {})

            processed[etf_code] = {
                "name": self.etf_names[etf_code],
                "latest_date": latest["data_date"],
                "previous_date": prev["data_date"],
                "price": price_info.get("price"),
                "change_value": price_info.get("change_value"),
                "change_percent": price_info.get("change_percent"),
                "daily_changes": self.calculate_daily_changes(etf_code),
            }

        outpath = os.path.join(self.data_dir, "processed_etf_data.json")
        # 原子性寫入
        self._atomic_write_json(outpath, processed)
        print(f"✓ 已保存 {outpath}")

    # ---------- 股票單檔歷史（給 stock_history_data.json） ----------
    def get_stock_full_history(self, etf_code: str, stock_code: str) -> List[Dict]:
        """
        只輸出：首次出現 / 增持 / 減持 / 出清
        「持平」不輸出（可避免權重波動造成的視覺干擾）
        判斷存在與否採用 MIN_PRESENCE_SHARES
        """
        if etf_code not in self.raw_data:
            return []

        data = self.raw_data[etf_code]
        sorted_data = sorted(data, key=lambda x: x["data_date"])

        history: List[Dict] = []
        prev_count = None
        prev_weight = 0.0

        for record in sorted_data:
            holdings = record.get("holdings", {})
            info = holdings.get(stock_code)

            if info:
                cur_count_raw = info.get("count", 0)
                cur_weight = info.get("weight", 0.0)
            else:
                cur_count_raw = 0
                cur_weight = 0.0

            # 把非常小的持股視為 0（門檻）
            cur_count = 0 if cur_count_raw <= self.MIN_PRESENCE_SHARES else cur_count_raw

            # 首次出現（prev_count 為 None） -> 只在實際有持股時記錄
            if prev_count is None:
                if cur_count > 0:
                    history.append({
                        "date": record["data_date"],
                        "count": cur_count // 1000,
                        "weight": cur_weight,
                        "count_change": 0,
                        "weight_change": 0.0,
                        "status": "首次出現",
                    })
                    prev_count = cur_count
                    prev_weight = cur_weight
                else:
                    # prev_count None 且 cur_count == 0 -> 都沒持股，跳過
                    continue
            else:
                # prev_count 有值（先前存在）
                if cur_count == 0 and prev_count > 0:
                    # 出清
                    history.append({
                        "date": record["data_date"],
                        "count": 0,
                        "weight": 0.0,
                        "count_change": (cur_count - prev_count) // 1000,
                        "weight_change": round(cur_weight - prev_weight, 4),
                        "status": "出清",
                    })
                    prev_count = None
                    prev_weight = 0.0
                elif cur_count > 0 and cur_count != prev_count:
                    # 張數改變（增/減持） -> 記錄
                    status = "增持" if cur_count > prev_count else "減持"
                    history.append({
                        "date": record["data_date"],
                        "count": cur_count // 1000,
                        "weight": cur_weight,
                        "count_change": (cur_count - prev_count) // 1000,
                        "weight_change": round(cur_weight - prev_weight, 4),
                        "status": status,
                    })
                    prev_count = cur_count
                    prev_weight = cur_weight
                else:
                    # 張數持平（或都為 0） -> 不輸出
                    pass

        return history

    def build_all_stock_history(self) -> Dict:
        all_stocks: Dict[str, Dict] = {}

        for etf_code in self.etf_files.keys():
            data = self.raw_data.get(etf_code, [])
            stock_codes = set()
            for rec in data:
                stock_codes.update(rec.get("holdings", {}).keys())

            for sc in stock_codes:
                if sc not in all_stocks:
                    all_stocks[sc] = {
                        "code": sc,
                        "name": "",
                        "etf_holdings": {}
                    }

                history = self.get_stock_full_history(etf_code, sc)
                if not history:
                    continue

                # 補股票名稱（先從資料找，找不到再用對照表）
                if not all_stocks[sc]["name"]:
                    try:
                        for rec in reversed(self.raw_data[etf_code]):
                            if sc in rec.get("holdings", {}):
                                nm = rec["holdings"][sc].get("name", "")
                                if nm and len(nm) > 1:
                                    all_stocks[sc]["name"] = nm
                                    break
                    except Exception:
                        pass
                if not all_stocks[sc]["name"]:
                    all_stocks[sc]["name"] = self.stock_name_map.get(sc, "")

                # 取 max/min/current（以張為單位）
                max_record = max(history, key=lambda x: x["count"])
                min_record = min(history, key=lambda x: x["count"])
                current_record = history[-1]

                all_stocks[sc]["etf_holdings"][etf_code] = {
                    "etf_name": self.etf_names[etf_code],
                    "current_count": current_record["count"],
                    "current_weight": current_record["weight"],
                    "max_count": max_record["count"],
                    "max_count_date": max_record["date"],
                    "min_count": min_record["count"],
                    "min_count_date": min_record["date"],
                    "history": history
                }

        return all_stocks

    def save_stock_history_data(self, filename: str = "stock_history_data.json") -> None:
        history = self.build_all_stock_history()
        filepath = os.path.join(self.data_dir, filename)
        # 原子性寫入
        self._atomic_write_json(filepath, history)
        print(f"✓ 已保存精簡版股票歷史資料至: {filepath}")


# -------------------- 執行區 --------------------
if __name__ == "__main__":
    # 讀取環境變數 DATA_DIR（若有）
    env_dir = os.environ.get("DATA_DIR")
    if env_dir and os.path.isdir(env_dir):
        data_dir = env_dir
    else:
        # 你說檔案已移到這裡，我把預設值改為你指定的路徑
        data_dir = r"C:\Users\j3210\OneDrive\桌面\ETF網站執行檔"

    print(f"執行 data_dir = {data_dir}")
    processor = ETFDataProcessor(data_dir)
    if processor.load_raw_data():
        processor.save_processed_data()
        processor.save_stock_history_data()
    else:
        print("無法載入數據，請確認目錄與 JSON 檔案名稱是否正確")
