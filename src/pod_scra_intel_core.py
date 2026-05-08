# ---------------------------------------------------------
# src/pod_scra_intel_core.py v6.5 (Router 掛載版)
#  適用部隊：RENDER, KOYEB, ZEABUR
# 任務：專注於 STT 與 Summary 的核心戰鬥流程。
# [V6.4 重大升級] 
# 1. 移除計分解析：拔除 parse_intel_metrics，徹底消滅 Regex 崩潰地雷。
# 2. 絕對解耦防禦：嚴格執行「先完成入庫、後發送TG」。若TG當機，不影響任務完結，杜絕幽靈迴圈。
# 3. 歸檔標記：TG 戰報標題強制鑲嵌 [任務ID前8碼]，精準對位 HuggingFace 歸檔庫。
# [V6.5 重大升級] 全面接軌 stt_router.py 聯合火力網。
# 徹底移除原有的 STT 決策叢林 (NVIDIA/GROQ/GEMINI)，改為單一呼叫 execute_stt_routing。256MB小機器 攜帶網址聽打。
# ---------------------------------------------------------

import os, time, random, gc, base64, re 
from datetime import datetime, timezone          
from curl_cffi import requests 
from src.pod_scra_intel_control import get_tactical_panel, get_sb, get_secrets 
from src.pod_scra_intel_groqcore import GroqFallbackAgent
from src.pod_scra_intel_nvidiacore import NvidiaAgent  

# 🚀 匯入全新的 STT 火力協調中心
from src.pod_scra_intel_stt_router import execute_stt_routing

try:
    from src.pod_scra_intel_r2 import compress_task_to_opus  
except ImportError:
    def compress_task_to_opus(task_id, r2_url):
        print("⚠️ [系統] 本機甲未配備 R2 壓縮模組，強制略過壓縮作業。")
        return False, r2_url

from src.pod_scra_intel_techcore import (
    fetch_stt_tasks, fetch_summary_tasks, upsert_intel_status, 
    update_intel_success, delete_intel_task, 
    call_gemini_summary, send_tg_report, increment_soft_failure
)

# 🎤 第一棒：Audio to STT (Router 接管版)
# =========================================================
def run_audio_to_stt_mission(sb=None):
    start_time = time.time()
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    panel = get_tactical_panel(worker_id)
    if panel["STT_LIMIT"] <= 0: return

    time.sleep(random.uniform(3.0, 8.0))
    if not sb: sb = get_sb()
    s = get_secrets()
    
    print(f"🔍 [{worker_id}] 啟動 STT 決策雷達 (戰力: {panel['MEM_TIER']}MB | 掃描: {panel['RADAR_FETCH_LIMIT']}筆)...")
    tasks = fetch_stt_tasks(sb, panel["MEM_TIER"], worker_id, fetch_limit=panel["RADAR_FETCH_LIMIT"])
    if not tasks: 
        print(f"🛌 [{worker_id}] 目前無適合體量之任務。")
        return

    actual_processed = 0 
    
    for task in tasks:
        if actual_processed >= panel["STT_LIMIT"]: break 
        if time.time() - start_time > panel["SAFE_DURATION_SECONDS"]: break

        if actual_processed > 0:
            delay = random.uniform(2.0, 5.0)
            print(f"⏳ [{worker_id}] 戰術冷卻 {delay:.1f} 秒...")
            time.sleep(delay)

        task_id = task['id']
        r2_url = str(task.get('r2_url') or '').lower()
        current_size = task.get('audio_size_mb') or 0
        current_fails = task.get('soft_failure_count') or 0
        
        if r2_url.endswith('.opus') and current_size > 30.0: 
            if worker_id not in ["HUGGINGFACE", "DBOS", "AUDIO_EAT", "RAILWAY"]: continue
            
        if not r2_url.endswith('.opus') and current_size > 85.0:
            if worker_id not in ["HUGGINGFACE", "DBOS", "AUDIO_EAT", "RAILWAY"]: continue

        print(f"🎯 [{worker_id}] 鎖定目標: {task.get('source_name')} (大小: {current_size}MB)")

        try:
            is_compressed_now = False 
            if not panel["CAN_COMPRESS"] and (r2_url.endswith('.mp3') or r2_url.endswith('.m4a')): continue
            if panel["CAN_COMPRESS"] and (r2_url.endswith('.mp3') or r2_url.endswith('.m4a')):
                success, new_url = compress_task_to_opus(task_id, task['r2_url'])
                if success:
                    is_compressed_now = True
                    r2_url = new_url.lower() 
                    compressed_size_mb = 5 
                    try:
                        head_req = requests.head(f"{s['R2_URL']}/{new_url}", timeout=10)
                        if head_req.status_code == 200 and 'Content-Length' in head_req.headers:
                            compressed_size_mb = int(head_req.headers['Content-Length']) / (1024 * 1024)
                    except: pass

                    update_payload = {"r2_url": new_url, "audio_ext": ".opus", "audio_size_mb": round(compressed_size_mb, 1)}
                    if compressed_size_mb < 50.0:
                        update_payload["assigned_troop"] = "T2"
                        update_payload["troop2_start_at"] = datetime.now(timezone.utc).isoformat()
                        update_payload["scrape_status"] = "completed"

                    sb.table("mission_queue").update(update_payload).eq("id", task_id).execute()
                    task['r2_url'] = new_url
                    
                    if compressed_size_mb > 14.0 and worker_id not in ["HUGGINGFACE", "DBOS", "AUDIO_EAT", "RAILWAY"]:
                        actual_processed += 1
                        continue 
                    if panel.get("COMPRESS_ONLY"):
                        actual_processed += 1
                        continue 
                else:
                    increment_soft_failure(sb, task_id)
                    continue 

            if is_compressed_now:
                actual_processed += 1
                continue 

            if not r2_url.endswith('.opus'): continue

            # 🚀 [V6.5 核心換裝] 呼叫 STT 火控中心
            print(f"🔒 [{worker_id}] 執行第一棒預佔，移交 STT Router 聯合火力網...")
            upsert_intel_status(sb, task_id, "Sum.-proc", "STT_ROUTER")
            sb.table("mission_queue").update({"soft_failure_count": current_fails + 1}).eq("id", task_id).execute()

            # 🎲 Router 會自動處理 Groq -> Gladia -> Speechmatics 的輪詢
            # 💡 傳入 current_size，啟動資源感知防護網
            stt_text, chosen_provider, errors = execute_stt_routing(sb, r2_url, current_size)

            upsert_intel_status(sb, task_id, "Sum.-pre", provider=chosen_provider, stt_text=stt_text)
            print(f"✅ [{worker_id}] STT 轉譯成功，由 {chosen_provider} 完成任務！")

            sb.table("mission_queue").update({"soft_failure_count": 0}).eq("id", task_id).execute()
            actual_processed += 1 

        except Exception as e:
            err_str = str(e)
            if '23505' in err_str or 'duplicate key' in err_str.lower():
                print(f"🤝 [{worker_id}] 競態攔截：任務已被友軍接管。")
            else:
                print(f"💥 [{worker_id}] Router 打擊失敗: {err_str}")
                delete_intel_task(sb, task_id)
                sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
        
        finally:
            gc.collect()

# ✍️ 第二棒：STT to Summary 
# =========================================================
def run_stt_to_summary_mission(sb=None):
    start_time = time.time()
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    panel = get_tactical_panel(worker_id)
    if panel["SUMMARY_LIMIT"] <= 0: return

    time.sleep(random.uniform(3.0, 8.0))
    if not sb: sb = get_sb()
    s = get_secrets()
    
    tasks = fetch_summary_tasks(sb, fetch_limit=panel["RADAR_FETCH_LIMIT"])
    actual_processed = 0
    
    for intel in tasks:
        if actual_processed >= panel["SUMMARY_LIMIT"]: break
        if time.time() - start_time > panel["SAFE_DURATION_SECONDS"]: break
            
        if actual_processed > 0:
            delay = random.uniform(8.0, 15.0)
            time.sleep(delay)

        task_id = intel['task_id']
        provider = intel['ai_provider']
        q_data = intel.get('mission_queue') or {}
        r2_file = str(q_data.get('r2_url') or '').lower()
        current_fails = q_data.get('soft_failure_count') or 0
        
        if not r2_file or r2_file == 'null': continue 

        print(f"✍️ [{worker_id}] 啟動摘要產線: {provider} | 任務: {q_data.get('episode_title', '')[:15]}...")
        
        p_res = sb.table("pod_scra_metadata").select("key_name, content").in_("key_name", ["PROMPT_FALLBACK", "PROMPT_ANTI_AD"]).execute()
        prompts = {item['key_name']: item['content'] for item in p_res.data} if p_res.data else {}
        sys_prompt = prompts.get("PROMPT_FALLBACK", "請分析情報。")
        anti_ad_prompt = prompts.get("PROMPT_ANTI_AD", "請過濾廣告。")

        try:
            summary = ""
            print(f"🔒 [{worker_id}] 執行第二棒預佔...")
            upsert_intel_status(sb, task_id, "Sum.-proc", provider)
            sb.table("mission_queue").update({"soft_failure_count": current_fails + 1}).eq("id", task_id).execute()

            is_text_transcript = (provider in ["GROQ", "NVIDIA", "GLADIA", "SPEECHMATICS"]) # 💡 加入新供應商
            target_r2_url = q_data.get('r2_url')
            
            if is_text_transcript:
                gemini_prompt = sys_prompt + f"\n\n{anti_ad_prompt}\n\n【純文字逐字稿】\n{intel.get('stt_text', '')}"
                target_r2_url = None 
            else:
                gemini_prompt = sys_prompt + "\n\n【系統提示】以下提供的是原始音檔。請仔細聆聽並過濾廣告，根據指示提取摘要。"

            stt_content = intel.get('stt_text', '')
            stt_len = len(stt_content) if stt_content else 0
            current_active_provider = "" 

            if current_fails >= 2 or stt_len > 30000:
                print(f"🚀 [{worker_id}] [C 方案] 觸發重裝條件，啟動 NVIDIA...")
                nv_agent = NvidiaAgent()
                summary = nv_agent.call_nvidia_summary(stt_content, sys_prompt)
                current_active_provider = "NVIDIA" 
            else:
                try:
                    print(f"🚀 [{worker_id}] [A 方案] 優先呼叫 GEMINI (字數: {stt_len})...")
                    summary = call_gemini_summary(s, target_r2_url, gemini_prompt)
                    current_active_provider = "GEMINI" 
                except Exception as gemini_err:
                    if is_text_transcript:
                        print(f"🛡️ [{worker_id}] [B 方案] 啟動 GROQ 備援摘要產線...")
                        groq_agent = GroqFallbackAgent()
                        summary = groq_agent.generate_summary(stt_content, sys_prompt)
                        current_active_provider = "GROQ" 
                    else:
                        raise gemini_err
            
            if summary:
                print(f"💾 [{worker_id}] 執行入庫作業...")
                update_intel_success(sb, task_id, summary, 0)
                sb.table("mission_queue").update({"soft_failure_count": 0, "scrape_status": "completed"}).eq("id", task_id).execute()

                try:
                    display_title = q_data.get('episode_title', '未知')
                    print(f"📨 [{worker_id}] 準備空投戰報至 Telegram...")
                    send_tg_report(s, q_data.get('source_name', '未知'), display_title, summary, task_id, sb, worker_id, provider=current_active_provider)
                    print(f"🎉 [{worker_id}] Telegram 戰報空投成功！")
                except Exception as tg_e:
                    print(f"⚠️ [{worker_id}] Telegram 戰報空投失敗，但資料庫已安全結案。錯誤: {tg_e}")
                
                actual_processed += 1 

        except Exception as e:
            err_str = str(e)
            print(f"❌ [{worker_id}] 第二棒崩潰: {err_str}")
            if '429' in err_str or 'quota' in err_str.lower() or 'rate_limit' in err_str.lower(): 
                if provider == "GEMINI" and not is_text_transcript:
                    delete_intel_task(sb, task_id)
                    sb.table("mission_queue").update({"scrape_status": "pending", "soft_failure_count": current_fails }).eq("id", task_id).execute()
                else:
                    upsert_intel_status(sb, task_id, "Sum.-pre", provider)
                    sb.table("mission_queue").update({"soft_failure_count": current_fails}).eq("id", task_id).execute()

                penalty_delay = random.uniform(180.0, 300.0)
                print(f"⚠️ [{worker_id}] 摘要 API 枯竭！強制深潛 {penalty_delay:.1f} 秒！")
                time.sleep(penalty_delay)
                break 
            elif '404' in err_str and 'Not Found' in err_str:
                delete_intel_task(sb, task_id)
                sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
            else:
                upsert_intel_status(sb, task_id, "Sum.-pre", provider)
                sb.table("mission_queue").update({"soft_failure_count": current_fails}).eq("id", task_id).execute()
        
        finally:
            gc.collect()
