# ---------------------------------------------------------
# src/pod_scra_intel_core.py v5.9.2 (中型部隊專用：大腦閘門與絕對深潛防禦)
# 適用部隊：RENDER, KOYEB, ZEABUR
# 任務：專注於 STT 與 Summary 的核心戰鬥流程。
# [V5.9.2 更新] 1. 移除冗餘重複檢查，全面交由 Supabase VIEW (vw_safe_mission_queue) 過濾。
# [V5.9.2 更新] 2. 雙產線全面實裝 429 絕對防禦：深潛 180~300 秒並強制斷尾。
# [V5.9.2 更新] 3. 第二棒套用全新 TG 靜默防禦網，保護主線結案。
# [保留] 物理級斷開 (壓完即收隊)、14MB 智能交接防線 (移交重裝)。
# ---------------------------------------------------------
import os, time, random, gc, traceback, requests 
from datetime import datetime, timezone          
from src.pod_scra_intel_control import get_tactical_panel, get_sb, get_secrets 
from src.pod_scra_intel_r2 import compress_task_to_opus  
from src.pod_scra_intel_groqcore import GroqFallbackAgent
from src.pod_scra_intel_techcore import (
    fetch_stt_tasks, fetch_summary_tasks, upsert_intel_status, 
    update_intel_success, delete_intel_task, call_groq_stt, 
    call_gemini_summary, parse_intel_metrics, send_tg_report,
    increment_soft_failure
)

# =========================================================
# 🎤 第一棒：Audio to STT 
# =========================================================
def run_audio_to_stt_mission(sb=None):
    start_time = time.time()
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    panel = get_tactical_panel(worker_id)
    
    if panel["STT_LIMIT"] <= 0:
        print(f"⏸️ [{worker_id}] 面板指示：不參與 STT 轉譯產線。")
        return

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
        if actual_processed >= panel["STT_LIMIT"]: 
            print(f"🏁 [{worker_id}] 第一棒已達目標產能 ({panel['STT_LIMIT']} 件)，準備交接。")
            break 
        if time.time() - start_time > panel["SAFE_DURATION_SECONDS"]: 
            print(f"⏱️ [{worker_id}] 巡邏逼近安全極限 ({panel['SAFE_DURATION_SECONDS']}s)，強制撤退！")
            break

        if actual_processed > 0:
            delay = random.uniform(2.0, 5.0)
            print(f"⏳ [{worker_id}] 戰術冷卻 {delay:.1f} 秒...")
            time.sleep(delay)

        task_id = task['id']
        r2_url = str(task.get('r2_url') or '').lower()
        
        # 💡 [防禦升級 1] 遇到已經壓縮好，但大於 14MB 的 Opus，中型機甲不准碰！留給重裝部隊
        current_size = task.get('audio_size_mb') or 0
        if r2_url.endswith('.opus') and current_size > 14.0:
            print(f"⛔ [{worker_id}] 偵測到 {current_size}MB 的超大 Opus，超越 Base64 極限，保留給重裝部隊！")
            continue

        if panel.get("COMPRESS_ONLY") and r2_url.endswith('.opus'):
            print(f"🔄 [{worker_id}] 兵工廠產能閒置：自動轉職為【主力兵】支援 AI 轉譯！")

        print(f"🎯 [{worker_id}] 鎖定目標: {task.get('source_name')} (大小: {task.get('audio_size_mb')}MB)")

        try:
            is_compressed_now = False 

            if not panel["CAN_COMPRESS"] and (r2_url.endswith('.mp3') or r2_url.endswith('.m4a')):
                print(f"⛔ [{worker_id}] 權限不足：禁止執行壓縮。跳過此大檔案。")
                try:
                    sb.table("pod_scra_log").insert({
                        "worker_id": worker_id, "task_type": "CORE_STT", "status": "WARNING",
                        "message": f"⛔ 權限不足，跳過大怪獸 ({task.get('audio_size_mb')}MB) | Task: {task_id[:8]}"
                    }).execute()
                except: pass
                continue

            if panel["CAN_COMPRESS"] and (r2_url.endswith('.mp3') or r2_url.endswith('.m4a')):
                print(f"⚙️ [{worker_id}] 面板授權壓縮！啟動 FFmpeg 引擎...")
                success, new_url = compress_task_to_opus(task_id, task['r2_url'])
                if success:
                    print(f"✅ [{worker_id}] 壓縮成功: {new_url}！")
                    is_compressed_now = True
                    r2_url = new_url.lower() 
                    
                    compressed_size_mb = 5 
                    try:
                        head_req = requests.head(f"{s['R2_URL']}/{new_url}", timeout=10)
                        if head_req.status_code == 200 and 'Content-Length' in head_req.headers:
                            compressed_size_mb = int(head_req.headers['Content-Length']) / (1024 * 1024)
                            print(f"📏 [{worker_id}] 壓縮後檔案大小偵測: {compressed_size_mb:.2f} MB")
                    except Exception as e:
                        print(f"⚠️ [{worker_id}] 無法偵測壓縮後大小，使用預設值 5MB。原因: {e}")

                    update_payload = {
                        "r2_url": new_url, 
                        "audio_ext": ".opus", 
                        "audio_size_mb": round(compressed_size_mb, 1)
                    }

                    if compressed_size_mb < 50.0:
                        print(f"🚀 [{worker_id}] 壓縮完畢，確保 T2 兵牌與解凍狀態！")
                        update_payload["assigned_troop"] = "T2"
                        update_payload["troop2_start_at"] = datetime.now(timezone.utc).isoformat()
                        update_payload["scrape_status"] = "completed"

                    sb.table("mission_queue").update(update_payload).eq("id", task_id).execute()
                    task['r2_url'] = new_url
                    
                    # 💡 [防禦升級 2] 壓縮完畢後，如果體積大於 14MB，中型部隊立刻收隊，保留給重裝！
                    if compressed_size_mb > 14.0:
                        msg = f"⚠️ 壓縮後仍達 {compressed_size_mb:.1f}MB！超越中型機甲極限，已入庫並移交重裝部隊！"
                        print(f"[{worker_id}] {msg}")
                        try:
                            sb.table("pod_scra_log").insert({
                                "worker_id": worker_id, "task_type": "CORE_STT", "status": "WARNING",
                                "message": f"{msg} | Task: {task_id[:8]}"
                            }).execute()
                        except: pass
                        actual_processed += 1
                        continue 
                    
                    if panel.get("COMPRESS_ONLY"):
                        print(f"🏭 [{worker_id}] 兵工廠任務完成！檔案已入庫，交由輕裝部隊轉譯。")
                        actual_processed += 1
                        continue 
                        
                else:
                    print(f"❌ [{worker_id}] 壓縮失敗，觸發容錯推進！")
                    increment_soft_failure(sb, task_id)
                    continue 

            if is_compressed_now:
                print(f"🏭 [{worker_id}] 壓縮任務完成。釋放記憶體，將轉譯任務留給下一個節拍或其他友軍！")
                actual_processed += 1
                continue 

            if not r2_url.endswith('.opus'):
                print(f"🛡️ [{worker_id}] 記憶體保護機制觸發：檔案非 opus 格式 ({r2_url})，嚴禁轉譯！跳過。")
                continue

            chosen_provider = "GROQ" if panel.get("SCOUT_MODE") else "GEMINI"

            print(f"🎲 [{worker_id}] 戰術分流 -> [{chosen_provider}] (檔案已確認為輕量 Opus)")
            upsert_intel_status(sb, task_id, "Sum.-proc", chosen_provider)

            if chosen_provider == "GROQ":
                stt_text = call_groq_stt(s, r2_url)
                upsert_intel_status(sb, task_id, "Sum.-pre", stt_text=stt_text)
                print(f"✅ [{worker_id}] GROQ 轉譯成功")
            else:
                upsert_intel_status(sb, task_id, "Sum.-pre", stt_text="[GEMINI_2.5_NATIVE_STREAM]")
                print(f"✅ [{worker_id}] GEMINI 鎖定原生流")

            actual_processed += 1 

        except Exception as e:
            err_str = str(e)
            if '23505' in err_str or 'duplicate key' in err_str.lower():
                print(f"🤝 [{worker_id}] 競態攔截：任務已被友軍接管。")
                
            # 🚀 第一棒 429 絕對深潛防禦
            elif '429' in err_str or 'quota' in err_str.lower():
                penalty_delay = random.uniform(180.0, 300.0)
                print(f"⚠️ [{worker_id}] 第一棒 API 限流！強制深潛 {penalty_delay:.1f} 秒，放棄本輪剩餘任務！")
                time.sleep(penalty_delay)
                break 
                
            else:
                print(f"💥 [{worker_id}] 第一棒打擊失敗: {err_str}")
                delete_intel_task(sb, task_id)
                if '404' in err_str and 'Not Found' in err_str:
                    print(f"🕳️ [{worker_id}] 踩到 404 炸彈！退回物流佇列！")
                    sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
                else:
                    increment_soft_failure(sb, task_id)
            
        finally:
            gc.collect()

# =========================================================
# ✍️ 第二棒：STT to Summary 
# =========================================================
def run_stt_to_summary_mission(sb=None):
    start_time = time.time()
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    panel = get_tactical_panel(worker_id)
    
    if panel["SUMMARY_LIMIT"] <= 0:
        print(f"⏸️ [{worker_id}] 面板指示：不參與摘要產線。")
        return

    time.sleep(random.uniform(3.0, 8.0))
    if not sb: sb = get_sb()
    s = get_secrets()
    
    tasks = fetch_summary_tasks(sb, fetch_limit=panel["RADAR_FETCH_LIMIT"])
    actual_processed = 0
    
    for intel in tasks:
        if actual_processed >= panel["SUMMARY_LIMIT"]: 
            print(f"🏁 [{worker_id}] 第二棒已達目標產能 ({panel['SUMMARY_LIMIT']} 件)，交接。")
            break
        if time.time() - start_time > panel["SAFE_DURATION_SECONDS"]:
            print(f"⏱️ [{worker_id}] 摘要產線逼近安全極限 ({panel['SAFE_DURATION_SECONDS']}s)，強制撤退！")
            break
            
        # 🚀 [Jitter 雜訊升級] 
        if actual_processed > 0:
            delay = random.uniform(8.0, 15.0)
            print(f"⏳ [{worker_id}] 戰術冷卻 {delay:.1f} 秒，等待 API 恢復 Token 池...")
            time.sleep(delay)

        task_id = intel['task_id']
        provider = intel['ai_provider']
        q_data = intel.get('mission_queue') or {}
        r2_file = str(q_data.get('r2_url') or '').lower()
        
        if not r2_file or r2_file == 'null': continue 

        print(f"✍️ [{worker_id}] 啟動摘要產線: {provider} | 任務: {q_data.get('episode_title', '')[:15]}...")
        p_res = sb.table("pod_scra_metadata").select("content").eq("key_name", "PROMPT_FALLBACK").single().execute()
        sys_prompt = p_res.data['content'] if p_res.data else "請分析情報。"

        try:
            summary = ""
            if provider == "GROQ":
                groq_agent = GroqFallbackAgent()
                summary = groq_agent.generate_summary(intel['stt_text'], sys_prompt)
            elif provider == "GEMINI":
                summary = call_gemini_summary(s, q_data['r2_url'], sys_prompt)

            if summary:
                metrics = parse_intel_metrics(summary)
                # 🚀 引入 V5.9.2 安全傳參：啟動 TG 靜默防禦網
                send_tg_report(s, q_data.get('source_name', '未知'), q_data.get('episode_title', '未知'), summary, sb, worker_id)
                
                update_intel_success(sb, task_id, summary, metrics["score"])
                print(f"🎉 [{worker_id}] 戰報發送成功，摘要已安全結案！")
                actual_processed += 1 

        except Exception as e:
            err_str = str(e)
            print(f"❌ [{worker_id}] 第二棒崩潰: {err_str}")
            
            # 🚀 第二棒 429 絕對深潛防禦
            if '429' in err_str or 'quota' in err_str.lower(): 
                penalty_delay = random.uniform(180.0, 300.0)
                print(f"⚠️ [{worker_id}] 摘要 API 枯竭！強制深潛 {penalty_delay:.1f} 秒，放棄後續任務以保護 Token 池！")
                time.sleep(penalty_delay)
                break 
                
            elif '404' in err_str and 'Not Found' in err_str:
                delete_intel_task(sb, task_id)
                sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
        
        finally:
            gc.collect()
