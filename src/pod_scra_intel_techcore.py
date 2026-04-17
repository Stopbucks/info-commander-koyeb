# ---------------------------------------------------------
# src/pod_scra_intel_techcore.py v5.9.4 (中型部隊專用：純 REST 與核彈隔離版)
# 職責：1. [雷達] fetch_stt_tasks：對接 Supabase 智能檢視表，進行三級分流與兵牌隔離。
#       2. [容錯] increment_soft_failure：處理失敗不墜機，打上標記交接重裝。
#       3. [火力] 封裝 Supabase 讀寫、REST API 呼叫與 TG 戰報。
# [V5.9.2 保留] Gemini 手刻 API 加裝 14MB 起飛前安檢與錯誤黑盒子 (無 SDK 依賴)。
# [V5.9.4 更新] 實裝「核彈隔離區」：中型部隊嚴禁觸碰指派給 AUDIO_EAT 等專屬大檔。
# 適用：RENDER, KOYEB, ZEABUR (純 REST 輕快版，無 SDK 依賴)
# ---------------------------------------------------------
import requests, base64, re, gc
from datetime import datetime

# =========================================================
# 📡 戰略雷達 (Strategic Radar)
# =========================================================
def fetch_stt_tasks(sb, mem_tier, worker_id="UNKNOWN", fetch_limit=50):
    """【低耦合戰略閘道】依據 mem_tier 進行動態分流，並實裝兵牌隔離"""
    
    query = sb.table("vw_safe_mission_queue").select("*")

    # 🛡️ 核彈隔離防線：其他所有機甲，絕對不准碰 AUDIO_EAT 的專屬檔案！
    # Supabase 的 neq 會過濾掉 NULL，必須用 or_ 把 NULL 加回來，加上 T2 標籤
    query = query.or_("assigned_troop.neq.AUDIO_EAT,assigned_troop.is.null,assigned_troop.eq.T2")

    # 🚀 動態分流 (中型部隊只會走到 else 區塊，但保留完整邏輯以備不時之需)
    if mem_tier < 512:
        # 🏹 輕裝游擊隊 (FLY): 安全第一
        query = query.gte("audio_size_mb", 0).ilike("r2_url", "%.opus").lt("audio_size_mb", 15) \
                     .order("audio_size_mb", desc=False)
                     
    elif worker_id in ["HUGGINGFACE", "AUDIO_EAT", "RAILWAY"]:
        # 🚜 重裝巨獸：無差別碾壓
        query = query.order("audio_size_mb", desc=True, nullsfirst=True)
                     
    else:
        # 🛡️ 中型部隊 (RENDER / KOYEB / ZEABUR)：穩健推進
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
            "r2_url": None  # 🚀 使用 Python 的 None，對應資料庫的 SQL NULL
        }).eq("id", task_id).execute()
        print(f"🚩 [容錯推進] 任務 {task_id[:8]} 失敗次數 +1 (目前: {current_count + 1}/6)")
    except Exception as e: 
        print(f"⚠️ 容錯推進紀錄失敗: {e}")

# =========================================================
# 📊 資料庫軍械庫 (Database Armory)
# =========================================================
def fetch_summary_tasks(sb, fetch_limit=50):
    import os 
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN")
    
    # 💡 擴充查詢：在 mission_queue 的關聯中加入 audio_size_mb 欄位以供過濾
    query = sb.table("mission_intel").select("*, mission_queue(episode_title, source_name, r2_url, audio_size_mb)").eq("intel_status", "Sum.-pre")
    
    # 🚀 絕對物理防線：中/輕型機甲，在雷達階段徹底無視 14MB 以上的巨怪！
    if worker_id not in ["HUGGINGFACE", "DBOS", "AUDIO_EAT", "RAILWAY"]:
        # 透過外鍵關聯 (Foreign Key) 直接在資料庫底層進行數值過濾
        query = query.lte("mission_queue.audio_size_mb", 14)

    # 💡 中型機甲不負責撿死檔，所以維持單純的 Sum.-pre 查詢即可
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
    try: sb.table("mission_queue").update({"scrape_status": "completed"}).eq("id", task_id).execute()
    except: pass

def delete_intel_task(sb, task_id):
    try: sb.table("mission_intel").delete().eq("task_id", task_id).execute()
    except: pass

def parse_intel_metrics(text):
    metrics = {"score": 0, "evidence": 0}
    try:
        s_match = re.search(r"綜合情報分.*?(\d+)", text)
        if s_match: metrics["score"] = int(s_match.group(1))
    except: pass
    return metrics

# =========================================================
# 🧠 AI 火控與通訊 (AI & Comms)
# =========================================================

def call_groq_stt(secrets, r2_url_path):
    url = f"{secrets['R2_URL']}/{r2_url_path}"
    m_type = "audio/ogg" if ".opus" in url else "audio/mpeg"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    audio_data = resp.content
    headers = {"Authorization": f"Bearer {secrets['GROQ_KEY']}"}
    files = {'file': (r2_url_path, audio_data, m_type)}
    data = {'model': 'whisper-large-v3', 'response_format': 'text', 'language': 'en'}
    stt_resp = requests.post("https://api.groq.com/openai/v1/audio/transcriptions", headers=headers, files=files, data=data, timeout=120)
    del audio_data, files, resp; gc.collect()
    if stt_resp.status_code == 200: return stt_resp.text
    else: raise Exception(f"Groq API Error: HTTP {stt_resp.status_code} - {stt_resp.text}")

def call_gemini_summary(secrets, r2_url_path, sys_prompt):
    url = f"{secrets['R2_URL']}/{r2_url_path}"
    m_type = "audio/ogg" if ".opus" in url.lower() or ".ogg" in url.lower() else "audio/mpeg"
    
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    raw_bytes = resp.content
    
    # 💡 防護：起飛前安檢 (14MB 硬上限，確保手刻 REST 不會 OOM 或超載)
    file_size_mb = len(raw_bytes) / (1024 * 1024)
    if file_size_mb > 14.0:
        del raw_bytes; gc.collect() 
        raise Exception(f"越權攔截：壓縮後檔案仍達 {file_size_mb:.1f}MB，中型機甲無重裝權限，退回交接給重裝部隊。")

    b64_audio = base64.b64encode(raw_bytes).decode('utf-8')
    del raw_bytes; gc.collect() 
    
    gemini_model = "gemini-2.5-flash"
    g_url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent?key={secrets['GEMINI_KEY']}"
    payload = {"contents": [{"parts": [{"text": sys_prompt}, {"inline_data": {"mime_type": m_type, "data": b64_audio}}]}]}
    
    ai_resp = requests.post(g_url, json=payload, timeout=180)
    del b64_audio, payload; gc.collect() 
    
    if ai_resp.status_code == 200:
        cands = ai_resp.json().get('candidates', [])
        if cands and cands[0].get('content'): return cands[0]['content']['parts'][0].get('text', "")
        return ""
    else: 
        err_msg = ai_resp.text[:200] 
        raise Exception(f"Gemini API 拒絕存取 (HTTP {ai_resp.status_code}): {err_msg}")

def send_tg_report(secrets, source, title, summary, sb=None, worker_id="UNKNOWN"):
    """【TG 防彈版】靜默處理 TG 崩潰，保全主線。"""
    safe_summary = summary[:3800] + ("...\n(因字數限制截斷)" if len(summary) > 3800 else "")
    safe_source = str(source).replace("_", "＿").replace("*", "＊").replace("[", "〔").replace("]", "〕").replace("`", "‵")
    safe_title = str(title).replace("_", "＿").replace("*", "＊").replace("[", "〔").replace("]", "〕").replace("`", "‵")
    report_msg = f"🎙️ *{safe_source}*\n📌 *{safe_title}*\n\n{safe_summary}"
    
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
        err_msg = f"⚠️ TG 戰報發送失敗: {str(e)[:150]}"
        print(f"[{worker_id}] {err_msg} (已轉紀錄至 S_LOG，主線任務繼續)")
        if sb:
            try:
                sb.table("pod_scra_log").insert({
                    "worker_id": worker_id, "task_type": "TG_REPORT", "status": "ERROR",
                    "message": f"TG 發報失敗 | Title: {safe_title[:30]} | Err: {str(e)[:100]}"
                }).execute()
            except: pass 
        return False
