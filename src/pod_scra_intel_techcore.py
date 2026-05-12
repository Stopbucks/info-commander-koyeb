# ---------------------------------------------------------
# src/pod_scra_intel_techcore.py v6.8 (關注點分離淨化版)
# 職責：1. [雷達] fetch_stt_tasks：對接 Supabase 智能檢視表，進行三級分流與兵牌隔離。
#       2. [容錯] increment_soft_failure：處理失敗不墜機，打上標記交接重裝。
#       3. [火力] 封裝 Supabase 讀寫、REST API 呼叫與 TG 戰報。
# [V5.9.2 保留] Gemini 手刻 API 加裝起飛前安檢與錯誤黑盒子 (無 SDK 依賴)。
# [V5.9.5 更新] 核心連線套件全面升級為 curl_cffi，提升 HTTP/2 連線穩定度。
# [V6.0   更新] 採用GROQ執行長訪談逐字稿，交GEMINI摘要。輕裝游擊隊(FLY)加裝防禦網。
# 適用：RENDER, KOYEB, ZEABUR (純 REST 輕快版，無 SDK 依賴)
# ---------------------------------------------------------
# [V6.1] 新增def send_tg_report(secrets,..)顯示 AI 提供者
# [V6.8 重大更新] 已將所有聽寫 (STT) API 呼叫剝離，全數移交 stt_router.py 接管。
# ---------------------------------------------------------
import base64, re, gc, os
from datetime import datetime, timezone, timedelta
from curl_cffi import requests 

# =========================================================
# 📡 戰略雷達 (Strategic Radar)
# =========================================================

def fetch_stt_tasks(sb, mem_tier, worker_id="UNKNOWN", fetch_limit=50):
    query = sb.table("vw_safe_mission_queue").select("*")
    query = query.or_("assigned_troop.neq.AUDIO_EAT,assigned_troop.is.null,assigned_troop.eq.T2")

    if mem_tier < 512:
        query = query.gte("audio_size_mb", 0).ilike("r2_url", "%.opus") \
                     .lt("audio_size_mb", 15).eq("soft_failure_count", 0) \
                     .order("audio_size_mb", desc=False)
    elif worker_id in ["HUGGINGFACE", "AUDIO_EAT", "RAILWAY"]:
        query = query.order("audio_size_mb", desc=True, nullsfirst=True)
    else:
        query = query.order("soft_failure_count", desc=False, nullsfirst=True) \
                     .order("audio_size_mb", desc=True, nullsfirst=True)
        
    return query.limit(fetch_limit).execute().data or []

def increment_soft_failure(sb, task_id):
    try:
        res = sb.table("mission_queue").select("soft_failure_count").eq("id", task_id).single().execute()
        current_count = res.data.get("soft_failure_count") or 0
        sb.table("mission_queue").update({
            "soft_failure_count": current_count + 1,
            "scrape_status": "success", 
            "r2_url": None 
        }).eq("id", task_id).execute()
        print(f"🚩 [容錯推進] 任務 {task_id[:8]} 失敗次數 +1 (目前: {current_count + 1}/6)")
    except Exception as e: 
        print(f"⚠️ 容錯推進紀錄失敗: {e}")

# =========================================================
# 📊 資料庫軍械庫 (Database Armory)
# =========================================================

def fetch_summary_tasks(sb, fetch_limit=50):
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN")
    dead_line = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()
    query = sb.table("mission_intel").select("*, mission_queue(episode_title, source_name, r2_url, audio_size_mb, soft_failure_count)")\
              .or_(f"intel_status.eq.Sum.-pre,and(intel_status.eq.Sum.-proc,updated_at.lt.{dead_line})")
    
    if worker_id not in ["HUGGINGFACE", "DBOS", "AUDIO_EAT", "RAILWAY"]:
        query = query.lte("mission_queue.audio_size_mb", 30)
        if worker_id == "FLY_LAX" or int(os.environ.get("MEM_TIER", 1024)) < 512:
            query = query.eq("mission_queue.soft_failure_count", 0)

    return query.order("created_at").limit(fetch_limit).execute().data or []

def upsert_intel_status(sb, task_id, status, provider=None, stt_text=None):
    payload = {"task_id": task_id, "intel_status": status}
    if provider: payload["ai_provider"] = provider
    if stt_text: payload["stt_text"] = stt_text
    sb.table("mission_intel").upsert(payload, on_conflict="task_id").execute()

def update_intel_success(sb, task_id, summary, score):
    sb.table("mission_intel").update({
        "summary_text": summary, 
        "intel_status": "Sum.-sent",
        "report_date": datetime.now().strftime("%Y-%m-%d"), 
        "total_score": score
    }).eq("task_id", task_id).execute()
    try: 
        sb.table("mission_queue").update({"scrape_status": "completed"}).eq("id", task_id).execute()
    except: pass

def delete_intel_task(sb, task_id):
    try: sb.table("mission_intel").delete().eq("task_id", task_id).execute()
    except: pass

def parse_intel_metrics(text):
    return {"score": 0, "evidence": 0}

# =========================================================
# 🧠 AI 火控與通訊 (AI & Comms)
# =========================================================

def call_gemini_summary(secrets, r2_url_path, sys_prompt):
    gem_api_key = secrets.get('GEMINI_API_KEY', secrets.get('GEMINI_KEY'))
    # 🚀 修正 404 錯誤：更新為正確的 Gemini API 模型名稱
    gemini_models = ["gemini-2.5-flash", "gemini-1.5-flash-latest", "gemini-1.5-pro-latest"]    
    payload_rest = None; uploaded_file = None; tmp_path = None; use_sdk = False
    
    if not r2_url_path or r2_url_path.lower() == 'null':
        payload_rest = {"contents": [{"parts": [{"text": sys_prompt}]}]}
    else:
        url = f"{secrets['R2_URL']}/{r2_url_path}"
        m_type = "audio/ogg" if ".opus" in url.lower() or ".ogg" in url.lower() else "audio/mpeg"
        resp = requests.get(url, timeout=120); resp.raise_for_status()
        raw_bytes = resp.content
        file_size_mb = len(raw_bytes) / (1024 * 1024)

        if file_size_mb <= 14.0:
            b64_audio = base64.b64encode(raw_bytes).decode('utf-8')
            del raw_bytes; gc.collect()
            payload_rest = {"contents": [{"parts": [{"text": sys_prompt}, {"inline_data": {"mime_type": m_type, "data": b64_audio}}]}]}
        else:
            use_sdk = True
            if os.environ.get("WORKER_ID") not in ["HUGGINGFACE", "DBOS", "AUDIO_EAT", "RAILWAY"]:
                del raw_bytes; raise Exception(f"越權攔截：檔案達 {file_size_mb:.1f}MB，中型機甲無重裝權限。")
            import tempfile, google.generativeai as genai
            genai.configure(api_key=gem_api_key)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".opus") as tmp: 
                tmp.write(raw_bytes); tmp_path = tmp.name
            del raw_bytes; gc.collect()
            uploaded_file = genai.upload_file(path=tmp_path, mime_type=m_type)

    last_error = ""; result_text = ""
    for model_name in gemini_models:
        print(f"🎯 [Gemini 輪詢] 嘗試呼叫模型: {model_name}...")
        try:
            if not use_sdk:
                g_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={gem_api_key}"
                ai_resp = requests.post(g_url, json=payload_rest, timeout=180)
                if ai_resp.status_code == 200:
                    cands = ai_resp.json().get('candidates', [])
                    result_text = cands[0]['content']['parts'][0].get('text', "") if cands else ""
                    break
                else: raise Exception(f"HTTP {ai_resp.status_code}: {ai_resp.text[:150]}")
            else:
                import google.generativeai as genai
                model = genai.GenerativeModel(model_name)
                response = model.generate_content([sys_prompt, uploaded_file])
                result_text = response.text; break
        except Exception as e:
            last_error = str(e); print(f"⚠️ [Gemini 戰損] 模型 {model_name} 遭遇阻礙: {last_error}"); continue 

    if payload_rest: del payload_rest; gc.collect()
    if use_sdk and uploaded_file:
        import google.generativeai as genai
        try: genai.delete_file(uploaded_file.name); os.remove(tmp_path)
        except: pass

    if result_text: return result_text
    else: raise Exception(f"所有 Gemini 梯隊均已陣亡。最後錯誤: {last_error}")

def send_tg_report(secrets, source, title, summary, task_id, sb=None, worker_id="UNKNOWN", provider="AUTO"):
    safe_summary = summary[:3800] + ("...\n(因字數限制截斷)" if len(summary) > 3800 else "")
    f_source = str(source).replace("_", "＿").replace("*", "＊").replace("[", "〔").replace("]", "〕")
    f_title = str(title).replace("_", "＿").replace("*", "＊").replace("[", "〔").replace("]", "〕")
    
    short_id = str(task_id)[:8]
    report_msg = f"🎙️ *{f_source}*\n📌 *[{short_id}] {f_title}*\n🧠 *戰術核心*: {provider}\n\n{safe_summary}"
    
    url = f"https://api.telegram.org/bot{secrets['TG_TOKEN']}/sendMessage"
    payload = {"chat_id": secrets["TG_CHAT"], "text": report_msg, "parse_mode": "Markdown"}

    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code != 200:
            payload["parse_mode"] = None
            resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code == 200: return True
        else: raise Exception(f"Telegram 終極發送失敗: {resp.text}")
    except Exception as e: 
        print(f"[{worker_id}] TG 發報失敗: {str(e)[:100]}")
        if sb:
            try: sb.table("pod_scra_log").insert({"worker_id": worker_id, "task_type": "TG_REPORT", "status": "ERROR", "message": f"TG 發報失敗 | ID: {short_id} | Err: {str(e)[:50]}"}).execute()
            except: pass 
        return False


def log_system_error(sb, worker_id, source, action, err_msg):
    # 專責將系統異常寫入 Supabase，僅捕捉嚴重錯誤以保護資料庫負載
    if not sb: return
    try:
        # 強制截斷錯誤訊息長度，避免超大 Payload 塞爆資料庫欄位
        safe_err_msg = str(err_msg)[:250]
        sb.table("pod_scra_log").insert({
            "worker_id": worker_id,
            "task_type": source,
            "status": "ERROR",
            "message": f"[{action}] 異常: {safe_err_msg}"
        }).execute()
        print(f"📝 [{worker_id}] [S_LOG 紀錄成功] 致命錯誤已同步至 Supabase")
    except Exception as e:
        print(f"⚠️ [{worker_id}] [S_LOG 寫入失敗] 無法連線資料庫: {str(e)[:50]}")

