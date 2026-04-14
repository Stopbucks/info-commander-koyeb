# ---------------------------------------------------------
# src/pod_scra_intel_core.py v5.7 (兵工廠與 Jitter 防踩踏版)
# 適用部隊：ALL (FLY, RENDER, KOYEB, ZEABUR, DBOS, HF)
# 任務：專注於 STT 與 Summary 的核心戰鬥流程。
# [新增] 1. COMPRESS_ONLY 兵工廠：壓縮完直接結案。若遇已壓縮檔則自動化身轉譯兵。
# [新增] 2. 迴圈 Jitter 雜訊：在多檔案處理間加入隨機延遲，防止 API 封鎖與併發衝突。
# [修改] RENDER 壓縮後，如果檔案低於25MB 把原本只能T1 部隊處理的檔案 ，也交付給 T2
# ---------------------------------------------------------
import os, time, random, gc, requests  # 💡 核心手術：補上 requests 模組！
from datetime import datetime, timezone  # 💡 補上這一行，確保時間計算不會報錯
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
    
    # 向指揮所申請專屬戰術面板
    panel = get_tactical_panel(worker_id)
    
    if panel["STT_LIMIT"] <= 0:
        print(f"⏸️ [{worker_id}] 面板指示：不參與 STT 轉譯產線。")
        return

    time.sleep(random.uniform(3.0, 8.0))
    if not sb: sb = get_sb()
    s = get_secrets()
    
    print(f"🔍 [{worker_id}] 啟動 STT 雷達 (戰力: {panel['MEM_TIER']}MB | 掃描: {panel['RADAR_FETCH_LIMIT']}筆)...")
    
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

        # 🚀 [Jitter 雜訊] 迴圈內第二筆開始，加入擬人化隨機延遲
        if actual_processed > 0:
            delay = random.uniform(2.0, 5.0)
            print(f"⏳ [{worker_id}] 戰術冷卻 {delay:.1f} 秒...")
            time.sleep(delay)

        task_id = task['id']
        r2_url = str(task.get('r2_url') or '').lower()
        
        # 🔄 [動態兵工廠] 如果這台是專職壓縮，但遇到已經是 opus 的檔案，代表無檔可壓！自動放行往下轉譯
        if panel.get("COMPRESS_ONLY") and r2_url.endswith('.opus'):
            print(f"🔄 [{worker_id}] 兵工廠產能閒置：自動轉職為【中型主力】支援 AI 轉譯！")

        check = sb.table("mission_intel").select("intel_status").eq("task_id", task_id).execute()
        if check.data:
            print(f"⏩ 任務 {task.get('source_name')} 已存在，尋找下一筆...")
            continue 

        print(f"🎯 [{worker_id}] 鎖定目標: {task.get('source_name')} (大小: {task.get('audio_size_mb')}MB)")



        try:
            # 💡 手術一：新增壓縮標記，落實「壓縮與轉譯」的物理級斷開
            is_compressed_now = False 

            # 根據面板權限決定是否壓縮
            if panel["CAN_COMPRESS"] and (r2_url.endswith('.mp3') or r2_url.endswith('.m4a')):
                print(f"⚙️ [{worker_id}] 面板授權壓縮！啟動 FFmpeg 引擎...")
                success, new_url = compress_task_to_opus(task_id, task['r2_url'])
                if success:
                    print(f"✅ [{worker_id}] 壓縮成功: {new_url}！")
                    is_compressed_now = True
                    r2_url = new_url.lower() # 更新目前迴圈的 URL 變數為 .opus
                    
                    # 💡 核心偵測：取得壓縮後的檔案大小 (透過 requests)
                    compressed_size_mb = 5 # 預設安全值
                    try:
                        s = get_secrets()
                        head_req = requests.head(f"{s['R2_URL']}/{new_url}", timeout=10)
                        if head_req.status_code == 200 and 'Content-Length' in head_req.headers:
                            compressed_size_mb = int(head_req.headers['Content-Length']) / (1024 * 1024)
                            print(f"📏 [{worker_id}] 壓縮後檔案大小偵測: {compressed_size_mb:.2f} MB")
                    except Exception as e:
                        print(f"⚠️ [{worker_id}] 無法偵測壓縮後大小，使用預設值 5MB。原因: {e}")

                    # 💡 核心更新：將新規格寫入資料庫
                    update_payload = {
                        "r2_url": new_url, 
                        "audio_ext": ".opus", 
                        "audio_size_mb": round(compressed_size_mb, 1)
                    }

                    # 若壓縮後小於 50MB，確保其具備 T2 兵牌與解凍狀態
                    if compressed_size_mb < 50.0:
                        print(f"🚀 [{worker_id}] 壓縮完畢，確保 T2 兵牌與解凍狀態！")
                        update_payload["assigned_troop"] = "T2"
                        update_payload["troop2_start_at"] = datetime.now(timezone.utc).isoformat()
                        update_payload["scrape_status"] = "completed"

                    sb.table("mission_queue").update(update_payload).eq("id", task_id).execute()
                    task['r2_url'] = new_url
                    
                else:
                    print(f"❌ [{worker_id}] 壓縮失敗，觸發容錯推進！")
                    increment_soft_failure(sb, task_id)
                    continue 
            elif not panel["CAN_COMPRESS"] and (r2_url.endswith('.mp3') or r2_url.endswith('.m4a')):
                print(f"⛔ [{worker_id}] 權限不足：禁止執行壓縮。跳過此大檔案。")
                continue

            # 💡 手術二：壓縮完畢後，無論是誰 (兵工廠或重裝兵)，都強制收隊休息！
            if is_compressed_now:
                print(f"🏭 [{worker_id}] 壓縮任務完成。釋放記憶體，將轉譯任務留給下一個節拍或其他友軍！")
                actual_processed += 1
                continue # 🚀 強制跳出！絕不貪刀！

            # 💡 手術三：絕對物理防線 (非 Opus 禁入 API 轉譯區)
            if not r2_url.endswith('.opus'):
                print(f"🛡️ [{worker_id}] 記憶體保護機制觸發：檔案非 opus 格式 ({r2_url})，嚴禁轉譯！跳過。")
                continue

            # ==========================================
            # ⚔️ 以下為 API 呼叫區塊 (保證只有輕量 Opus 能進入)
            # ==========================================
            chosen_provider = "GROQ" if panel["SCOUT_MODE"] else "GEMINI"

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
            
        # 🚀 [Jitter 雜訊] 摘要產線加入隨機延遲，降低 429 Rate Limit 風險
        if actual_processed > 0:
            delay = random.uniform(3.0, 6.0)
            print(f"⏳ [{worker_id}] 戰術冷卻 {delay:.1f} 秒...")
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
                send_tg_report(s, q_data.get('source_name', '未知'), q_data.get('episode_title', '未知'), summary)
                update_intel_success(sb, task_id, summary, metrics["score"])
                print(f"🎉 [{worker_id}] 戰報發送成功，摘要已安全結案！")
                actual_processed += 1 

        except Exception as e:
            err_str = str(e)
            print(f"❌ [{worker_id}] 第二棒崩潰: {err_str}")
            if '429' in err_str: print(f"⚠️ [{worker_id}] API Rate Limit，任務退回。")
            elif '404' in err_str and 'Not Found' in err_str:
                delete_intel_task(sb, task_id)
                sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
        
        finally:
            gc.collect()
