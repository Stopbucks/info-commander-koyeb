# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_trans.py  (V6.14 網域感知與離線擬真 終極合併版)
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
# [V6.11 升級] 實裝 DOWNLOAD_LIMIT 與 MAX_SAME_DOMAIN 網域感知限流機制。
# [V6.12 升級] 實裝 AppleCoreMedia 的動態 Session UUID 與 Range 偽裝，
#              並加入局中即時黑名單與泥沼戰術 (Tarpit) 預警系統。
# [V6.13 升級] 實裝網域分散度動態偵測 (Dynamic Dispersion)，智能切換游擊/併發模式。
# [V6.14 升級] 戰術校準：全面轉向「App 離線下載」行為擬真，消除 Range 矛盾。
#              擴大泥沼戰術防禦圈，將 curl 56 等連線斷裂錯誤納入軟失敗重試機制。
# ---------------------------------------------------------

import os, time, random, gc, json
from curl_cffi import requests 
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from src.pod_scra_intel_r2 import get_s3_client 
from src.pod_scra_intel_camouflage import get_tactical_camouflage
from src.pod_scra_intel_control import get_tactical_panel

def execute_fortress_stages(sb, config, s_log_func):
    now_iso = datetime.now(timezone.utc).isoformat()
    worker_id = config.get("WORKER_ID", "UNKNOWN_NODE")
    
    # 🚀 取得面板裝備
    panel = get_tactical_panel(worker_id)
    
    time.sleep(random.uniform(3.0, 8.0))
    t_res = sb.table("pod_scra_tactics").select("*").eq("id", 1).single().execute()
    if not t_res.data: return
    tactic = t_res.data
    
    is_duty_officer = (tactic.get("active_worker", "") == worker_id)
    w_status = tactic.get("worker_status", {})
    tick_key = f"{worker_id}_tick"
    current_tick = w_status.get(tick_key, 0) + 1
    
    max_ticks = panel.get("MAX_TICKS", 2) 
    if not is_duty_officer:
        gear_ratio = panel.get("IDLE_GEARBOX", 4.0) 
        max_ticks = int(max_ticks * gear_ratio)  
        
    if current_tick > max_ticks: current_tick = 1
        
    role_name = "👑 值勤官" if is_duty_officer else "🛠️ 後勤兵"
    s_log_func(sb, "STATE_M", "INFO", f"⚙️ [戰略狀態機] 身分: {role_name} | 階段節拍: {current_tick} / {max_ticks}")

    w_status[tick_key] = current_tick
    health = tactic.get('workers_health', {})
    health[worker_id] = now_iso
    sb.table("pod_scra_tactics").update({
        "last_heartbeat_at": now_iso, 
        "workers_health": health, 
        "worker_status": w_status
    }).eq("id", 1).execute()

    from src.pod_scra_intel_core import run_audio_to_stt_mission, run_stt_to_summary_mission

    if current_tick == 1:
        # 🚀 [V6.11] 從面板讀取下載配額，特許 AUDIO_EAT 也能火力全開
        base_dl_limit = panel.get("DOWNLOAD_LIMIT", 2)
        if is_duty_officer or worker_id == "AUDIO_EAT":
            dl_limit = base_dl_limit
        else:
            dl_limit = 1 
            
        max_same_domain = panel.get("MAX_SAME_DOMAIN", 1)
        
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 執行階段 1/{max_ticks}: 外部下載 (目標總量 {dl_limit}, 同網域上限 {max_same_domain})")
        
        rule_res = sb.table("pod_scra_rules").select("domain").in_("worker_id", [worker_id, "ALL"]).gte("expired_at", now_iso).execute()
        db_blacklist = [r['domain'] for r in rule_res.data] if rule_res.data else []
        
        panel_blacklist = panel.get("GLOBAL_DOMAIN_BLACKLIST", [])
        combined_blacklist = list(set(db_blacklist + panel_blacklist))
        
        run_logistics_engine(sb, config, now_iso, s_log_func, combined_blacklist, dl_limit, max_same_domain, is_duty_officer) 
    
    elif current_tick % 2 != 0:
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 啟動轉譯產線 (由面板接管)")
        run_audio_to_stt_mission(sb) 
    else:
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 啟動摘要發報 (由面板接管)")
        run_stt_to_summary_mission(sb) 

def run_logistics_engine(sb, config, now_iso, s_log_func, my_blacklist, dl_limit=2, max_same_domain=1, is_duty_officer=True):
    worker_id = config.get('WORKER_ID', 'UNKNOWN')
    
    HEAVY_ARMORS = ["HUGGINGFACE", "GITHUB"]
    allowed_statuses = ["success", "dl_heavy_only"] if worker_id in HEAVY_ARMORS else ["success"]

    # 🚀 將抽取樣本數擴大至 50 筆，確保有足夠樣本進行網域篩選
    query = sb.table("mission_queue").select("*, mission_program_master(*)").in_("scrape_status", allowed_statuses).is_("r2_url", "null").lte("troop2_start_at", now_iso).order("created_at", desc=True)\
        .limit(50)  
    
    tasks = query.execute().data or []
    if not tasks: return
    
    s3 = get_s3_client()
    bucket = os.environ.get("R2_BUCKET_NAME")
    
    time.sleep(random.uniform(2.0, 5.0))
    
    # =========================================================
    # 🚀 [V6.13] 網域分散度動態偵測 (Dynamic Dispersion)
    # =========================================================
    available_domains = set([urlparse(t['audio_url']).netloc for t in tasks if t.get('audio_url')])
    
    if len(available_domains) >= dl_limit:
        dynamic_max_domain = 1
        s_log_func(sb, "DOWNLOAD", "INFO", f"🌐 貨源極度分散 (獨立網域: {len(available_domains)} 個 >= 目標 {dl_limit})。動態併發降為 1。")
    else:
        dynamic_max_domain = max_same_domain  
        s_log_func(sb, "DOWNLOAD", "INFO", f"🌐 貨源相對集中 (獨立網域: {len(available_domains)} 個 < 目標 {dl_limit})。動態併發維持 {dynamic_max_domain}。")

    domain_counts = {} 
    downloaded_count = 0    
    
    for m in tasks:
        if downloaded_count >= dl_limit: break
            
        f_url = m.get('audio_url')
        if not f_url: continue
        target_domain = urlparse(f_url).netloc
        
        # 🛡️ 局中即時黑名單過濾
        if any(b in target_domain for b in my_blacklist): 
            continue
        
        # 🛡️ 同網域動態併發控制
        current_domain_usage = domain_counts.get(target_domain, 0)
        if current_domain_usage >= dynamic_max_domain:
            if current_domain_usage == dynamic_max_domain:
                s_log_func(sb, "DOWNLOAD", "INFO", f"🕵️ [{target_domain}] 已達動態上限 ({dynamic_max_domain})，跳過。")
            continue

        if downloaded_count > 0:
            time.sleep(random.uniform(5.0, 12.0))

        ext = os.path.splitext(urlparse(f_url).path)[1] or ".mp3"
        tmp_path = f"/tmp/dl_{m['id'][:8]}{ext}"
        
        current_dl_fails = m.get('dl_soft_failure_count', 0)
        prog_info = f"{m.get('source_name', '未知')} - {m.get('episode_title', '未知')[:15]}..."

        try:
            camo_gear = get_tactical_camouflage(worker_id, is_duty_officer)
            dynamic_headers = camo_gear["headers"]
            tls_fingerprint = camo_gear["impersonate"]
            
            # 🚀 [V6.14] 蘋果離線下載擬真：動態掛載單次行動特徵，拔除 Range
            is_apple = "AppleCoreMedia" in dynamic_headers.get("User-Agent", "")
            if is_apple:
                import uuid
                dynamic_headers["X-Playback-Session-Id"] = str(uuid.uuid4()).upper()
            
            with requests.Session(impersonate=tls_fingerprint) as session:
                
                # 🍎 探測階段維持原樣 (針對 current_dl_fails == 1)
                if current_dl_fails == 1:
                    s_log_func(sb, "DOWNLOAD", "INFO", f"🍎 [{worker_id}] 對目標 [{target_domain}] 啟動媒體連線預熱 (Probe)...")
                    probe_headers = dynamic_headers.copy()
                    if "X-Playback-Session-Id" not in probe_headers:
                        import uuid
                        probe_headers["X-Playback-Session-Id"] = str(uuid.uuid4()).upper()
                    probe_headers["Icy-MetaData"] = "1"
                    probe_headers["Range"] = "bytes=0-100" 
                    try:
                        probe_r = session.get(f_url, timeout=15, headers=probe_headers)
                        probe_r.close()
                        time.sleep(random.uniform(0.8, 2.0)) 
                    except Exception as probe_err:
                        s_log_func(sb, "DOWNLOAD", "WARNING", f"⚠️ 探測階段遇阻: {probe_err}，繼續強行突破...")

                final_timeout = 300 if worker_id in HEAVY_ARMORS else 120
                dl_start_time = time.time()
                realistic_chunk_size = random.choice([16384, 32768, 65536]) 
                
                # 🚀 執行真實下載
                r = session.get(f_url, stream=True, timeout=final_timeout, headers=dynamic_headers)
                
                try:
                    r.raise_for_status()
                    with open(tmp_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=realistic_chunk_size): 
                            if time.time() - dl_start_time > final_timeout:
                                raise TimeoutError(f"Absolute download timeout ({final_timeout}s) exceeded.")
                            if chunk: f.write(chunk)
                finally:
                    r.close()
                    
            s3.upload_file(tmp_path, bucket, os.path.basename(tmp_path))
            
            sb.table("mission_queue").update({"scrape_status": "completed", "r2_url": os.path.basename(tmp_path), "dl_soft_failure_count": 0}).eq("id", m['id']).execute()
            s_log_func(sb, "DOWNLOAD", "SUCCESS", f"✅ 物資入庫: {m['id'][:8]}")
            
            downloaded_count += 1 
            domain_counts[target_domain] = domain_counts.get(target_domain, 0) + 1

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
                # 🚀 局中防禦：立刻將該網域加入本次迴圈的黑名單，保護後續任務
                my_blacklist.append(target_domain)
            else:
                s_log_func(sb, "DOWNLOAD", "ERROR", f"❌ 搬運異常: {status_code}")
                
        except Exception as e: 
            err_str = str(e).lower()
            # 🚀 [V6.14 修補] 將連線中斷 (connection closed/reset) 也納入泥沼戰術防禦網！
            is_tarpit = any(kw in err_str for kw in ['timeout', 'timed out', 'connection closed', 'connection reset'])
            
            if is_tarpit:
                if current_dl_fails < 1:
                    warning_msg = f"⚠️ [{worker_id}] 遭遇泥沼戰術 (超時或斷線)，強制斬斷。嫌疑犯: {prog_info}"
                    s_log_func(sb, "DOWNLOAD", "WARNING", warning_msg)
                    sb.table("mission_queue").update({"dl_soft_failure_count": current_dl_fails + 1}).eq("id", m['id']).execute()
                    try:
                        sb.table("pod_scra_log").insert({
                            "worker_id": worker_id, "task_type": "TARPIT_WARNING", 
                            "status": "WARNING", "message": warning_msg
                        }).execute()
                    except: pass
                else:
                    s_log_func(sb, "DOWNLOAD", "WARNING", f"⚠️ [{worker_id}] 抓取再次超時或斷線，標記為 dl_heavy_only 移交重裝。死硬派: {prog_info}")
                    sb.table("mission_queue").update({"scrape_status": "dl_heavy_only"}).eq("id", m['id']).execute()
            else:
                s_log_func(sb, "DOWNLOAD", "ERROR", f"❌ 搬運失敗: {str(e)}")
        finally:
            if os.path.exists(tmp_path): os.remove(tmp_path)
            gc.collect()
