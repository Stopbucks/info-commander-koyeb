# ---------------------------------------------------------
# src/pod_scra_intel_core.py v6.3 (大部隊 KOYEB 終極版：A/B/C 三段變速防禦系統)
# 適用部隊：RENDER, KOYEB, ZEABUR
# 任務：專注於 STT 與 Summary 的核心戰鬥流程。
# [V5.9.2 更新] 1. 交由 Supabase VIEW (vw_safe_mission_queue) 過濾。 429 絕對防禦：深潛 180~300 秒並強制斷尾。
# [V5.9.3 補齊] 第一棒與第二棒全面實裝「預佔鎖 (Pessimistic Locking)」。
# [V5.9.5 更新] 核心連線套件全面升級為 curl_cffi，統一全軍 HTTP 引擎。
# [V5.9.6 更新] 升級智能大腦閘門：放寬中型機甲壓縮極限至 85MB。
# [V5.9.7 零信任] 修復軟失敗歸零邏輯，確保成功任務重獲新生，補齊缺失依賴。
# [V6.0 重大升級] 導入 GROQ 超長訪談聽寫 (>=3MB)，並實裝 429 降級重鑄斷尾策略。
# [V6.1] 新增去廣告濾鏡 (in_ 雙取機制) 與 FLY 防呆掛載。
# [V6.2] 實裝 NVIDIA Plan C 終極接管，根據 soft_failure_count 自動換裝。
# [V6.3] 超過3萬字的任務，直接找NVDIA進行摘要
# ---------------------------------------------------------

import os, time, random, gc, traceback, base64, re 
from datetime import datetime, timezone          
from curl_cffi import requests 
from src.pod_scra_intel_control import get_tactical_panel, get_sb, get_secrets 
from src.pod_scra_intel_groqcore import GroqFallbackAgent
from src.pod_scra_intel_nvidiacore import NvidiaAgent  

# 🚀 [防禦升級：動態裝備掛載] 處理 FLY 等輕裝部隊未配備 R2 壓縮模組的狀況
try:
    from src.pod_scra_intel_r2 import compress_task_to_opus  
except ImportError:
    # 宣告空函式防呆替身
    def compress_task_to_opus(task_id, r2_url):
        print("⚠️ [系統] 本機甲未配備 R2 壓縮模組，強制略過壓縮作業。")
        return False, r2_url

from src.pod_scra_intel_techcore import (
    fetch_stt_tasks, fetch_summary_tasks, upsert_intel_status, 
    update_intel_success, delete_intel_task, call_groq_stt, 
    call_gemini_summary, parse_intel_metrics, send_tg_report,
    increment_soft_failure
)


# 🎤 第一棒：Audio to STT (三段變速版)
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
        current_size = task.get('audio_size_mb') or 0
        current_fails = task.get('soft_failure_count') or 0
        
        if r2_url.endswith('.opus') and current_size > 30.0: 
            if worker_id not in ["HUGGINGFACE", "DBOS", "AUDIO_EAT", "RAILWAY"]:
                print(f"⛔ [{worker_id}] 偵測到 {current_size}MB 超大 Opus，保留給重裝部隊！")
                continue
            
        if not r2_url.endswith('.opus') and current_size > 85.0:
            if worker_id not in ["HUGGINGFACE", "DBOS", "AUDIO_EAT", "RAILWAY"]:
                print(f"⛔ [{worker_id}] 偵測到 {current_size}MB 巨型原檔，交給重裝部隊壓縮！")
                continue

        if panel.get("COMPRESS_ONLY") and r2_url.endswith('.opus'):
            print(f"🔄 [{worker_id}] 兵工廠產能閒置：自動轉職為【主力兵】支援 AI 轉譯！")

        print(f"🎯 [{worker_id}] 鎖定目標: {task.get('source_name')} (大小: {current_size}MB)")

        try:
            is_compressed_now = False 

            if not panel["CAN_COMPRESS"] and (r2_url.endswith('.mp3') or r2_url.endswith('.m4a')):
                print(f"⛔ [{worker_id}] 權限不足：禁止執行壓縮。跳過此大檔案。")
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
                        update_payload["assigned_troop"] = "T2"
                        update_payload["troop2_start_at"] = datetime.now(timezone.utc).isoformat()
                        update_payload["scrape_status"] = "completed"

                    sb.table("mission_queue").update(update_payload).eq("id", task_id).execute()
                    task['r2_url'] = new_url
                    
                    if compressed_size_mb > 14.0 and worker_id not in ["HUGGINGFACE", "DBOS", "AUDIO_EAT", "RAILWAY"]:
                        print(f"[{worker_id}] ⚠️ 壓縮後仍達 {compressed_size_mb:.1f}MB！已入庫並移交重裝部隊！")
                        actual_processed += 1
                        continue 
                    
                    if panel.get("COMPRESS_ONLY"):
                        print(f"🏭 [{worker_id}] 兵工廠任務完成！交由輕裝部隊轉譯。")
                        actual_processed += 1
                        continue 
                        
                else:
                    print(f"❌ [{worker_id}] 壓縮失敗，觸發容錯推進！")
                    increment_soft_failure(sb, task_id)
                    continue 

            if is_compressed_now:
                print(f"🏭 [{worker_id}] 壓縮任務完成。將轉譯任務留給下一個節拍或其他友軍！")
                actual_processed += 1
                continue 

            if not r2_url.endswith('.opus'):
                print(f"🛡️ [{worker_id}] 檔案非 opus 格式 ({r2_url})，嚴禁轉譯！跳過。")
                continue

            # 🎯 [核心決策] 根據失敗次數決定聽寫引擎 (三段變速)
            if current_fails >= 2:
                chosen_provider = "NVIDIA"
            elif current_size >= 3.0 or current_fails == 1:
                chosen_provider = "GROQ"
            else:
                chosen_provider = "GEMINI"
                
            print(f"🎲 [{worker_id}] 戰術分流 -> [{chosen_provider}] (檔案大小 {current_size}MB，失敗次數 {current_fails})")
            
            # 預佔鎖與失敗次數 +1
            print(f"🔒 [{worker_id}] 執行第一棒狀態預佔：標記為 Sum.-proc 並預先增加失敗計數...")
            upsert_intel_status(sb, task_id, "Sum.-proc", chosen_provider)
            sb.table("mission_queue").update({"soft_failure_count": current_fails + 1}).eq("id", task_id).execute()

            # 💡 依照決策執行不同聽寫方案
            if chosen_provider == "NVIDIA":
                nv_agent = NvidiaAgent()
                stt_text = nv_agent.call_nvidia_stt(r2_url)
                upsert_intel_status(sb, task_id, "Sum.-pre", stt_text=stt_text)
                print(f"✅ [{worker_id}] NVIDIA STT 轉譯成功")
            elif chosen_provider == "GROQ":
                stt_text = call_groq_stt(s, r2_url)
                upsert_intel_status(sb, task_id, "Sum.-pre", stt_text=stt_text)
                print(f"✅ [{worker_id}] GROQ 轉譯成功")
            else:
                upsert_intel_status(sb, task_id, "Sum.-pre", stt_text="[GEMINI_2.5_NATIVE_STREAM]")
                print(f"✅ [{worker_id}] GEMINI 鎖定原生流")

            # 任務成功歸 0
            sb.table("mission_queue").update({"soft_failure_count": 0}).eq("id", task_id).execute()
            actual_processed += 1 

        except Exception as e:
            err_str = str(e)
            if '23505' in err_str or 'duplicate key' in err_str.lower():
                print(f"🤝 [{worker_id}] 競態攔截：任務已被友軍接管。")
            elif '429' in err_str or 'quota' in err_str.lower():
                print(f"🔄 [{worker_id}] 遭遇限流，退回任務狀態，解除預佔鎖...")
                delete_intel_task(sb, task_id)
                sb.table("mission_queue").update({"soft_failure_count": current_fails, "r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
                
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
        
        finally:
            gc.collect()

# =========================================================
# ✍️ 第二棒：STT to Summary (三段變速版)
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
            
        if actual_processed > 0:
            delay = random.uniform(8.0, 15.0)
            print(f"⏳ [{worker_id}] 戰術冷卻 {delay:.1f} 秒，等待 API 恢復 Token 池...")
            time.sleep(delay)

        task_id = intel['task_id']
        provider = intel['ai_provider']
        q_data = intel.get('mission_queue') or {}
        r2_file = str(q_data.get('r2_url') or '').lower()
        current_fails = q_data.get('soft_failure_count') or 0
        
        if not r2_file or r2_file == 'null': continue 

        print(f"✍️ [{worker_id}] 啟動摘要產線: {provider} | 任務: {q_data.get('episode_title', '')[:15]}...")
        
        # 🚀 [效能優化] 透過 in_ 語法，單次連線同時取回「主提示詞」與「去廣告濾鏡」
        p_res = sb.table("pod_scra_metadata").select("key_name, content").in_("key_name", ["PROMPT_FALLBACK", "PROMPT_ANTI_AD"]).execute()
        prompts = {item['key_name']: item['content'] for item in p_res.data} if p_res.data else {}
        
        sys_prompt = prompts.get("PROMPT_FALLBACK", "請分析情報。")
        anti_ad_prompt = prompts.get("PROMPT_ANTI_AD", "請過濾廣告。")

        try:
            summary = ""
            
            # 🚀 👇 [第二棒：預佔鎖] 呼叫 API 前，先預佔狀態並 +1 失敗次數
            print(f"🔒 [{worker_id}] 執行第二棒狀態預佔：標記為 Sum.-proc 並預先增加失敗計數...")
            upsert_intel_status(sb, task_id, "Sum.-proc", provider)
            sb.table("mission_queue").update({"soft_failure_count": current_fails + 1}).eq("id", task_id).execute()

            # 🎯 狀態判定：只要是 GROQ 或 NVIDIA 產生，都是「純文字逐字稿」
            is_text_transcript = (provider in ["GROQ", "NVIDIA"])
            target_r2_url = q_data.get('r2_url')
            
            if is_text_transcript:
                # 🛡️ 啟োট純文字防禦網：掛載資料庫抓取的「去廣告與來賓標示濾鏡」
                gemini_prompt = sys_prompt + f"\n\n{anti_ad_prompt}\n\n【純文字逐字稿】\n{intel.get('stt_text', '')}"
                target_r2_url = None # 阻斷音檔傳遞
            else:
                # 🎙️ 原生音訊流：以簡潔指令引導原生多模態過濾廣告
                gemini_prompt = sys_prompt + "\n\n【系統提示】以下提供的是原始音檔。請仔細聆聽並運用邏輯判斷力，自動將「贊助商廣告、產品推銷」等干擾資訊過濾掉，根據指示提取摘要。"

            # ---------------------------------------------------------
            # 🛡️ [核心決策] 根據失敗次數與「文本長度」決定摘要執行方案
            # ---------------------------------------------------------
            stt_content = intel.get('stt_text', '')
            stt_len = len(stt_content) if stt_content else 0

            # 🚀 條件：失敗 2 次以上，或者字數超過 3 萬字 (直接規避 GROQ 死亡輪迴與 GEMINI 429)
            if current_fails >= 2 or stt_len > 30000:
                print(f"🚀 [{worker_id}] [C 方案] 觸發重裝條件 (失敗: {current_fails}, 字數: {stt_len})，啟動 NVIDIA 終極摘要產線...")
                nv_agent = NvidiaAgent()
                summary = nv_agent.call_nvidia_summary(stt_content, sys_prompt)
            else:
                try:
                    print(f"🚀 [{worker_id}] [A 方案] 優先呼叫 GEMINI 執行摘要 (字數: {stt_len})...")
                    summary = call_gemini_summary(s, target_r2_url, gemini_prompt)
                    
                except Exception as gemini_err:
                    print(f"⚠️ [{worker_id}] GEMINI 摘要遭遇阻礙 ({str(gemini_err)[:50]})...")
                    
                    # 🛡️ 啟動 B 方案備援
                    if is_text_transcript:
                        print(f"🛡️ [{worker_id}] [B 方案] 啟動 GROQ 備援摘要產線 (字數: {stt_len})...")
                        groq_agent = GroqFallbackAgent()
                        summary = groq_agent.generate_summary(stt_content, sys_prompt)
                    else:
                        raise gemini_err
            
            if summary:
                metrics = parse_intel_metrics(summary)
                send_tg_report(s, q_data.get('source_name', '未知'), q_data.get('episode_title', '未知'), summary, sb, worker_id)
                
                # 任務成功歸 0
                sb.table("mission_queue").update({"soft_failure_count": 0}).eq("id", task_id).execute()
                update_intel_success(sb, task_id, summary, metrics["score"])
                print(f"🎉 [{worker_id}] 戰報發送成功，摘要已安全結案！")
                actual_processed += 1 

        except Exception as e:
            err_str = str(e)
            print(f"❌ [{worker_id}] 第二棒崩潰: {err_str}")
            
            if '429' in err_str or 'quota' in err_str.lower() or 'rate_limit' in err_str.lower(): 
                print(f"🔄 [{worker_id}] 遭遇限流，執行斷尾防禦...")
                
                # 🚀 [降級重鑄戰術] 如果是舊版 GEMINI 原生音訊任務導致 429，將其退回第一棒 (pending) 重新交由 GROQ 聽寫
                if provider == "GEMINI" and not is_text_transcript:
                    print(f"⚔️ [{worker_id}] 偵測到舊版高負載任務，啟動降級重鑄 ➡️ 退回第一棒重新由 GROQ 聽寫...")
                    delete_intel_task(sb, task_id)
                    sb.table("mission_queue").update({
                        "scrape_status": "pending", 
                        "soft_failure_count": current_fails # 💡 保持當前失敗次數，讓系統下次能觸發備援
                    }).eq("id", task_id).execute()
                else:
                    # 純文字任務被限流，保留狀態等下次
                    upsert_intel_status(sb, task_id, "Sum.-pre", provider)
                    sb.table("mission_queue").update({"soft_failure_count": current_fails}).eq("id", task_id).execute()

                penalty_delay = random.uniform(180.0, 300.0)
                print(f"⚠️ [{worker_id}] 摘要 API 枯竭！強制深潛 {penalty_delay:.1f} 秒，本輪強制收隊！")
                time.sleep(penalty_delay)
                break # 🛑 斷尾求生
            
            elif '404' in err_str and 'Not Found' in err_str:
                print(f"🕳️ [{worker_id}] 踩到 404 炸彈！抹除紀錄退回物流。")
                delete_intel_task(sb, task_id)
                sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
            
            else:
                print(f"🔄 [{worker_id}] 任務異常，保留失敗標記，等待下次重試。")
        
        finally:
            gc.collect()
