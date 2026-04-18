
# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_trans.py  (V5.8 變速箱_面板統御防崩潰版)
# [節拍] 狀態機邏輯：透過 MAX_TICKS 控制循環。若主將設為 3 拍，則依序執行 [1:下載, 2:摘要, 3:轉譯]。
# [節拍] 判斷公式：利用除以 2 的餘數 (current_tick % 2 != 0) 來動態交替分配任務型態。
# [節拍] 任務分配：單數拍 (1, 3, 5...) 執行轉譯 (STT)；雙數拍 (2, 4, 6...) 執行摘要 (Summary)。
# [變速箱] IDLE_GEARBOX: 隱蔽變速箱。控制非值勤機甲的降速齒輪比。預設 3.0 代表巡邏週期拉長 3 倍 

# [主將範例] FLY 為主將 (MAX=12)：僅在「第 1 拍」出門抓音檔，第 2~12 拍交替做摘要與轉譯 (低頻進貨)。
# [主將範例] RENDER 為主將 (MAX=6)：同樣在「第 1 拍」抓音檔，第 2~6 拍做摘要與轉譯 (高頻進貨)。
# [後勤範例] 若身分為「後勤兵」：完全不管 MAX 是多少，【永遠不出門抓檔】，只專心交替做轉譯與摘要。
# [節拍總結] MAX_TICKS 的大小，實質上決定了「主將多久出門進貨一次」的冷卻週期。
# [防禦] 穩健進貨：放寬至 limit(2)，並配備雙重 Jitter 擬人化延遲。 搭配修正5.
# [隱蔽] 導入 camouflage 千面人模組，透過機甲基因種子達成每日一致性偽裝。

# 修正：1. 徹底拔除 audio_officers 與冗餘的傳入參數，避免呼叫崩潰。
# 2. 將 max_ticks 交由 src.pod_scra_intel_control 面板動態管理，落實低耦合。
# 3. [T2 敗戰轉移] 遭遇 403/401 封鎖時，自動將任務降級為 pending (冰封10天)並標記 T1_RESCUE。
# 4. [黃金救援期] 推遲 troop2_start_at 7 天，完美錯開 T2 雷達，精準移交 T1 數位人格處理。
# 5. 下載檔案放大至50M，相關設定:timeout=180s, 切片 3MB, 喘息 0.5s
# # ---------------------------------------------------------
# [隱蔽] 導入 camouflage 千面人模組，透過身分旗標精準配發迷彩。
# ---------------------------------------------------------

import os, time, random, gc, json
from curl_cffi import requests # 🚀 換裝！
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from src.pod_scra_intel_r2 import get_s3_client 
from src.pod_scra_intel_control import get_tactical_panel # 🚀 引入控制面板
from src.pod_scra_intel_camouflage import get_camouflage_headers # 🚀 引入千面人偽裝模組

def execute_fortress_stages(sb, config, s_log_func):
    now_iso = datetime.now(timezone.utc).isoformat()
    worker_id = config.get("WORKER_ID", "UNKNOWN_NODE")
    
    # 🚀 取得面板裝備 (包含 MAX_TICKS 與 IDLE_GEARBOX)
    panel = get_tactical_panel(worker_id)
    
    time.sleep(random.uniform(3.0, 8.0))
    t_res = sb.table("pod_scra_tactics").select("*").eq("id", 1).single().execute()
    if not t_res.data: return
    tactic = t_res.data
    
    is_duty_officer = (tactic.get("active_worker", "") == worker_id)
    w_status = tactic.get("worker_status", {})
    tick_key = f"{worker_id}_tick"
    current_tick = w_status.get(tick_key, 0) + 1
    
    # ⚙️ 啟動變速箱邏輯：非值勤兵套用怠速齒輪比
    max_ticks = panel.get("MAX_TICKS", 2) 
    if not is_duty_officer:
        gear_ratio = panel.get("IDLE_GEARBOX", 4.0) # 預設容錯 4.0
        max_ticks = int(max_ticks * gear_ratio)  # 確保計算結果為完美的整數節拍
        
    if current_tick > max_ticks: current_tick = 1
        
    role_name = "👑 值勤官" if is_duty_officer else "🛠️ 後勤兵"
    s_log_func(sb, "STATE_M", "INFO", f"⚙️ [戰略狀態機] 身分: {role_name} | 階段節拍: {current_tick} / {max_ticks}")

    from src.pod_scra_intel_core import run_audio_to_stt_mission, run_stt_to_summary_mission

    # 🛡️ 只要是「第 1 拍」，全軍皆可出門！但依據身分給予不同載重量。
    if current_tick == 1:
        dl_limit = 2 if is_duty_officer else 1  # 👈 主將拿 2 個，後勤兵低調只拿 1 個
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 執行階段 1/{max_ticks}: 外部走私下載 (上限 {dl_limit} 筆)")
        
        rule_res = sb.table("pod_scra_rules").select("domain").in_("worker_id", [worker_id, "ALL"]).gte("expired_at", now_iso).execute()
        my_blacklist = [r['domain'] for r in rule_res.data] if rule_res.data else []
        
        # 🚀 修正：將上限 dl_limit 以及 身分旗標(is_duty_officer) 傳入物流引擎
        run_logistics_engine(sb, config, now_iso, s_log_func, my_blacklist, dl_limit, is_duty_officer) 
    
    # 轉譯與摘要交替執行 (單數拍 STT, 雙數拍 Summary)
    elif current_tick % 2 != 0:
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 啟動轉譯產線 (由面板接管)")
        run_audio_to_stt_mission(sb) 
    else:
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 啟動摘要發報 (由面板接管)")
        run_stt_to_summary_mission(sb) 

    w_status[tick_key] = current_tick
    health = tactic.get('workers_health', {})
    health[worker_id] = now_iso
    sb.table("pod_scra_tactics").update({"last_heartbeat_at": now_iso, "workers_health": health, "worker_status": w_status}).eq("id", 1).execute()

# 🚀 修正：接收 is_duty_officer 參數
def run_logistics_engine(sb, config, now_iso, s_log_func, my_blacklist, dl_limit=2, is_duty_officer=True):
    # 🛡️ 為了尋找「不同網域」的目標，我們先拿多一點候選清單 (limit 10)
    query = sb.table("mission_queue").select("*, mission_program_master(*)").eq("scrape_status", "success").is_("r2_url", "null").lte("troop2_start_at", now_iso).order("created_at", desc=True)\
        .limit(10)  
    tasks = query.execute().data or []
    if not tasks: return
    
    s3 = get_s3_client()
    bucket = os.environ.get("R2_BUCKET_NAME")
    worker_id = config.get('WORKER_ID', 'UNKNOWN')
    
    time.sleep(random.uniform(2.0, 5.0))
    
    visited_domains = set() # 🚀 已造訪網域紀錄簿，確保「打完就跑」不重複
    downloaded_count = 0    # 🚀 實際下載計數器
    
    for m in tasks:
        # 🛡️ 控制單次最高產能：最多抓 dl_limit 個就收隊
        if downloaded_count >= dl_limit:
            break
            
        f_url = m.get('audio_url')
        if not f_url: continue
        target_domain = urlparse(f_url).netloc
        
        if any(b in target_domain for b in my_blacklist): continue
        
        # 🚀 核心低調邏輯：如果這個網域剛剛才抓過，直接跳過
        if target_domain in visited_domains:
            s_log_func(sb, "DOWNLOAD", "INFO", f"🕵️ [低調迴避] 剛才已打擊過 {target_domain}，分散火力，跳過此筆。")
            continue

        if downloaded_count > 0:
            time.sleep(random.uniform(5.0, 12.0))

        ext = os.path.splitext(urlparse(f_url).path)[1] or ".mp3"
        tmp_path = f"/tmp/dl_{m['id'][:8]}{ext}"
        

        try:
            dynamic_headers = get_camouflage_headers(worker_id, is_duty_officer)

            # 🚀 戰術升級：使用 Session 處理多次跳轉，並加上 safari15_3 完美指紋
            with requests.Session(impersonate="safari15_3") as session:
                # 💡 黃金比例：timeout=180s, 切片 3MB, 喘息 0.5s
                with session.get(f_url, stream=True, timeout=180, headers=dynamic_headers) as r:
                    r.raise_for_status()
                    with open(tmp_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=3 * 1024 * 1024): 
                            if chunk: 
                                f.write(chunk)
                                time.sleep(0.5)
                    
            s3.upload_file(tmp_path, bucket, os.path.basename(tmp_path))
            
            sb.table("mission_queue").update({
                "scrape_status": "completed", 
                "r2_url": os.path.basename(tmp_path)
            }).eq("id", m['id']).execute()
            
            s_log_func(sb, "DOWNLOAD", "SUCCESS", f"✅ 物資入庫 (安全擴容至 50MB): {m['id'][:8]}")

            visited_domains.add(target_domain) 
            downloaded_count += 1              
            
        except requests.exceptions.HTTPError as he:
            status_code = he.response.status_code
            if status_code in [403, 401, 429]:
                s_log_func(sb, "DOWNLOAD", "ERROR", f"🚫 [{worker_id}] 遭封鎖 ({status_code})，呼叫 T1 特種救援！")
                
                # 🛡️ 遭受403狀況，以最嚴重事件處理，改為10天 (240小時) 冰封
                victim_freeze = (datetime.now(timezone.utc) + timedelta(hours=240)).isoformat()
                ally_freeze = (datetime.now(timezone.utc) + timedelta(hours=240)).isoformat()
                sb.table("pod_scra_rules").insert([
                    {"worker_id": worker_id, "domain": target_domain, "rule_type": "AUTO_COOLDOWN", "expired_at": victim_freeze},
                    {"worker_id": "ALL", "domain": target_domain, "rule_type": "VIGILANCE", "expired_at": ally_freeze}
                ]).execute()

                rescue_time = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
                sb.table("mission_queue").update({
                    "assigned_troop": "T1_RESCUE", 
                    "troop2_start_at": rescue_time, 
                    "scrape_status": "pending" 
                }).eq("id", m['id']).execute()
                
            else:
                s_log_func(sb, "DOWNLOAD", "ERROR", f"❌ 搬運異常: {status_code}")
        except Exception as e: 
            s_log_func(sb, "DOWNLOAD", "ERROR", f"❌ 搬運失敗: {str(e)}")
        finally:
            if os.path.exists(tmp_path): os.remove(tmp_path)
            gc.collect()
