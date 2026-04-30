# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_trans.py  (V6 變速箱_面板統御_裝甲防線版)
# [節拍] 狀態機邏輯：透過 MAX_TICKS 控制循環。若主將設為 3 拍，則依序執行 [1:下載, 2:摘要, 3:轉譯]。
# [節拍] 判斷公式：利用除以 2 的餘數 (current_tick % 2 != 0) 來動態交替分配任務型態。
# [節拍] 任務分配：單數拍 (1, 3, 5...) 執行轉譯 (STT)；雙數拍 (2, 4, 6...) 執行摘要 (Summary)。
# [變速箱] IDLE_GEARBOX: 隱蔽變速箱。控制非值勤機甲的降速齒輪比。預設 3.0 代表巡邏週期拉長 3 倍 

# [主將範例] FLY 為主將 (MAX=12)：僅在「第 1 拍」出門抓音檔，第 2~12 拍交替做摘要與轉譯 (低頻進貨)。
# [後勤範例] 若身分為「後勤兵」：完全不管 MAX 是多少，【永遠不出門抓檔】，只專心交替做轉譯與摘要。
# [隱蔽] 導入 camouflage 千面人模組，透過機甲基因種子達成每日一致性偽裝。

# [V5.9 裝甲] 打卡機制前移：在執行重型任務前，先將 current_tick 寫入 DB，防止 OOM 導致無限輪迴。
# [V5.9.1 裝甲] 導入下載軟失敗 (dl_soft_failure_count) 與 AppleCoreMedia 擬真探測協定。
# [V5.9.2 編裝] 將 GITHUB 晉升為重裝兵，與 HUGGINGFACE 共同承接 dl_heavy_only 任務。
# [V6] 全面移除切片休息
# ---------------------------------------------------------

import os, time, random, gc, json
from curl_cffi import requests # 🚀 換裝！
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from src.pod_scra_intel_r2 import get_s3_client 
from src.pod_scra_intel_camouflage import get_tactical_camouflage
from src.pod_scra_intel_control import get_tactical_panel

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

    # =====================================================================
    # V5.9 關鍵防禦升級：將打卡動作「提前」到執行重型任務之前
    # =====================================================================
    w_status[tick_key] = current_tick
    health = tactic.get('workers_health', {})
    health[worker_id] = now_iso
    sb.table("pod_scra_tactics").update({
        "last_heartbeat_at": now_iso, 
        "workers_health": health, 
        "worker_status": w_status
    }).eq("id", 1).execute()

    from src.pod_scra_intel_core import run_audio_to_stt_mission, run_stt_to_summary_mission

    # 🛡️ 接下來再開始執行高風險的耗時任務
    # 只要是「第 1 拍」，全軍皆可出門！但依據身分給予不同載重量。
    if current_tick == 1:
        dl_limit = 2 if is_duty_officer else 1  # 主將拿 2 個，後勤兵低調只拿 1 個
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 執行階段 1/{max_ticks}: 外部走私下載 (上限 {dl_limit} 筆)")
        
        rule_res = sb.table("pod_scra_rules").select("domain").in_("worker_id", [worker_id, "ALL"]).gte("expired_at", now_iso).execute()
        my_blacklist = [r['domain'] for r in rule_res.data] if rule_res.data else []
        
        # 🚀 傳入上限 dl_limit 以及 身分旗標(is_duty_officer) 供物流引擎調度
        run_logistics_engine(sb, config, now_iso, s_log_func, my_blacklist, dl_limit, is_duty_officer) 
    
    # 轉譯與摘要交替執行 (單數拍 STT, 雙數拍 Summary)
    elif current_tick % 2 != 0:
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 啟動轉譯產線 (由面板接管)")
        run_audio_to_stt_mission(sb) 
    else:
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 啟動摘要發報 (由面板接管)")
        run_stt_to_summary_mission(sb) 

def run_logistics_engine(sb, config, now_iso, s_log_func, my_blacklist, dl_limit=2, is_duty_officer=True):
    worker_id = config.get('WORKER_ID', 'UNKNOWN')
    
    # 🚀 [V5.9.2 編裝升級] 將 GITHUB 納入重裝部隊
    HEAVY_ARMORS = ["HUGGINGFACE", "GITHUB"]
    allowed_statuses = ["success", "dl_heavy_only"] if worker_id in HEAVY_ARMORS else ["success"]

    # 🛡️ 雷達分流：輕裝兵只看 success，重裝部隊兼看 dl_heavy_only
    query = sb.table("mission_queue").select("*, mission_program_master(*)").in_("scrape_status", allowed_statuses).is_("r2_url", "null").lte("troop2_start_at", now_iso).order("created_at", desc=True)\
        .limit(10)  
    tasks = query.execute().data or []
    if not tasks: return
    
    s3 = get_s3_client()
    bucket = os.environ.get("R2_BUCKET_NAME")
    
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
        
        # 🚀 讀取當前下載軟失敗次數
        current_dl_fails = m.get('dl_soft_failure_count', 0)
        
        # 提取節目資訊以供情報分析
        prog_info = f"{m.get('source_name', '未知')} - {m.get('episode_title', '未知')[:15]}..."


        try:
            camo_gear = get_tactical_camouflage(worker_id, is_duty_officer)
            dynamic_headers = camo_gear["headers"]
            tls_fingerprint = camo_gear["impersonate"]
            
            with requests.Session(impersonate=tls_fingerprint) as session:
                
                # 🍎 針對曾被拖延的目標，啟動 AppleCoreMedia 擬真探測
                if current_dl_fails == 1:
                    s_log_func(sb, "DOWNLOAD", "INFO", f"🍎 [{worker_id}] 對目標 [{target_domain}] 啟動媒體播放器擬真協定 (Range Probe)...")
                    import uuid
                    
                    dynamic_headers["X-Playback-Session-Id"] = str(uuid.uuid4()).upper()
                    dynamic_headers["Icy-MetaData"] = "1"
                    
                    probe_headers = dynamic_headers.copy()
                    probe_headers["Range"] = "bytes=0-1" 
                    
                    try:
                        probe_r = session.get(f_url, timeout=15, headers=probe_headers)
                        probe_r.close()
                        time.sleep(random.uniform(0.8, 2.0)) 
                    except Exception as probe_err:
                        s_log_func(sb, "DOWNLOAD", "WARNING", f"⚠️ 探測階段遇阻: {probe_err}，繼續強行突破...")

                # 🚀 V5.9.4 絕對防線：鎖死超時極限、絕對碼表與高階擬真緩衝區
                final_timeout = 300 if worker_id in HEAVY_ARMORS else 120
                dl_start_time = time.time()
                
                # 💡 模擬真實播放器的 Buffer Size (16KB ~ 64KB)
                # 這能讓底層的 TCP 傳輸曲線看起來極度自然，完全不需要 time.sleep 這種人工破綻！
                realistic_chunk_size = random.choice([16384, 32768, 65536]) 
                
                r = session.get(f_url, stream=True, timeout=final_timeout, headers=dynamic_headers)
                
                try:
                    r.raise_for_status()
                    with open(tmp_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=realistic_chunk_size): 
                            
                            # 🛡️ 絕對時間防線：只要總耗時超標，立刻斬斷，拒絕惡意拖延！
                            if time.time() - dl_start_time > final_timeout:
                                raise TimeoutError(f"Absolute download timeout ({final_timeout}s) exceeded.")
                                
                            if chunk: 
                                f.write(chunk)
                                # 💥 已拔除 time.sleep(0.5)，解除人工封印，讓 TCP 自然流動！
                finally:
                    r.close()
                    
            # 👇 往左退一格縮排，讓 Session 提早關閉釋放記憶體
            s3.upload_file(tmp_path, bucket, os.path.basename(tmp_path))
            
            # 下載成功歸零計數
            sb.table("mission_queue").update({"scrape_status": "completed", "r2_url": os.path.basename(tmp_path), "dl_soft_failure_count": 0}).eq("id", m['id']).execute()
            s_log_func(sb, "DOWNLOAD", "SUCCESS", f"✅ 物資入庫: {m['id'][:8]}")
            
            downloaded_count += 1 


        except requests.exceptions.HTTPError as he:
            status_code = getattr(he.response, 'status_code', 0)
            if status_code in [403, 401, 429]:
                s_log_func(sb, "DOWNLOAD", "ERROR", f"🚫 [{worker_id}] 遭封鎖 ({status_code})")
                victim_freeze = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
                ally_freeze = (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat()
                sb.table("pod_scra_rules").insert([
                    {"worker_id": worker_id, "domain": target_domain, "rule_type": "AUTO_COOLDOWN", "expired_at": victim_freeze},
                    {"worker_id": "ALL", "domain": target_domain, "rule_type": "VIGILANCE", "expired_at": ally_freeze}
                ]).execute()
            else:
                s_log_func(sb, "DOWNLOAD", "ERROR", f"❌ 搬運異常: {status_code}")
                
        except Exception as e: 
            err_str = str(e).lower()
            # 🚀 專屬下載超時的「軟失敗」防禦網
            if 'timeout' in err_str or 'timed out' in err_str:
                if current_dl_fails < 1:
                    s_log_func(sb, "DOWNLOAD", "WARNING", f"⚠️ [{worker_id}] 抓取超時(>120s)，計數+1。嫌疑犯: {prog_info}")
                    sb.table("mission_queue").update({"dl_soft_failure_count": current_dl_fails + 1}).eq("id", m['id']).execute()
                else:
                    s_log_func(sb, "DOWNLOAD", "WARNING", f"⚠️ [{worker_id}] 抓取再次超時，標記為 dl_heavy_only 移交重裝。死硬派: {prog_info}")
                    sb.table("mission_queue").update({"scrape_status": "dl_heavy_only"}).eq("id", m['id']).execute()
            else:
                s_log_func(sb, "DOWNLOAD", "ERROR", f"❌ 搬運失敗: {str(e)}")
        finally:
            if os.path.exists(tmp_path): os.remove(tmp_path)
            gc.collect()
            visited_domains.add(target_domain) # 將網域加入已造訪清單
