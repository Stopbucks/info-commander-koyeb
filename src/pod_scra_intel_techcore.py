# ---------------------------------------------------------
# src/pod_scra_intel_techcore.py v6.0 (中型部隊專用：GROQ升級長訪談_純 REST + curl_cffi 升級版)
# 職責：1. [雷達] fetch_stt_tasks：對接 Supabase 智能檢視表，進行三級分流與兵牌隔離。
#       2. [容錯] increment_soft_failure：處理失敗不墜機，打上標記交接重裝。
#       3. [火力] 封裝 Supabase 讀寫、REST API 呼叫與 TG 戰報。
# [V5.9.2 保留] Gemini 手刻 API 加裝起飛前安檢與錯誤黑盒子 (無 SDK 依賴)。
# [V5.9.5 更新] 核心連線套件全面升級為 curl_cffi，提升 HTTP/2 連線穩定度。
# [V6.0   更新] 採用GROQ執行長訪談逐字稿，交GEMINI摘要 
# 適用：RENDER, KOYEB, ZEABUR (純 REST 輕快版，無 SDK 依賴)
# ---------------------------------------------------------
import base64, re, gc
from datetime import datetime
from curl_cffi import requests # 🚀 換裝：使用 curl_cffi 替換原生 requests，規避 Cloudflare 阻擋

# =========================================================
# 📡 戰略雷達 (Strategic Radar)
# 職責：負責向情報中心 (Supabase) 索取任務，並執行兵牌與體量隔離
# =========================================================

def fetch_stt_tasks(sb, mem_tier, worker_id="UNKNOWN", fetch_limit=50):
    """
    【低耦合戰略閘道】依據機甲記憶體 (mem_tier) 進行動態分流，並實裝兵牌隔離。
    第一棒 STT 任務專用雷達。
    """
    # 查詢已排除危險任務的安全視圖
    query = sb.table("vw_safe_mission_queue").select("*")

    # 🛡️ 核彈隔離防線：中輕型機甲，絕對不准碰 AUDIO_EAT (重裝部隊) 的專屬檔案！
    # 技術說明：Supabase 的 neq (不等於) 會濾掉 NULL，所以必須用 or_ 把 NULL 與 T2 標籤加回來
    query = query.or_("assigned_troop.neq.AUDIO_EAT,assigned_troop.is.null,assigned_troop.eq.T2")

    # 🚀 動態分流：根據硬體配置決定拿取任務的策略
    if mem_tier < 512:
        # 🏹 輕裝游擊隊 (如 FLY)：安全第一，只拿 15MB 以下的 Opus，從最小的開始拿
        query = query.gte("audio_size_mb", 0).ilike("r2_url", "%.opus").lt("audio_size_mb", 15) \
                     .order("audio_size_mb", desc=False)
                     
    elif worker_id in ["HUGGINGFACE", "AUDIO_EAT", "RAILWAY"]:
        # 🚜 重裝巨獸：無差別碾壓，從最大的檔案開始吃
        query = query.order("audio_size_mb", desc=True, nullsfirst=True)
                     
    else:
        # 🛡️ 中型部隊 (RENDER / KOYEB / ZEABUR)：穩健推進
        # 優先處理失敗次數少的任務，同等失敗次數下優先處理大檔
        query = query.order("soft_failure_count", desc=False, nullsfirst=True) \
                     .order("audio_size_mb", desc=True, nullsfirst=True)
        
    return query.limit(fetch_limit).execute().data or []

def increment_soft_failure(sb, task_id):
    """
    【容錯推進機制】任務失敗時不讓機甲墜機，而是增加失敗計數並退回佇列。
    累積達一定次數後，主系統會判定為死檔並放棄。
    """
    try:
        res = sb.table("mission_queue").select("soft_failure_count").eq("id", task_id).single().execute()
        current_count = res.data.get("soft_failure_count") or 0
        sb.table("mission_queue").update({
            "soft_failure_count": current_count + 1,
            "scrape_status": "success", 
            "r2_url": None  # 🚀 使用 Python 的 None，對應資料庫會轉為 SQL NULL，強制重新下載
        }).eq("id", task_id).execute()
        print(f"🚩 [容錯推進] 任務 {task_id[:8]} 失敗次數 +1 (目前: {current_count + 1}/6)")
    except Exception as e: 
        print(f"⚠️ 容錯推進紀錄失敗: {e}")

# =========================================================
# 📊 資料庫軍械庫 (Database Armory)
# 職責：與 Supabase 進行狀態同步、寫入情報成果與解析數據
# =========================================================

def fetch_summary_tasks(sb, fetch_limit=50):
    """
    【第二棒雷達】尋找已經完成轉譯 (Sum.-pre)，等待寫摘要的任務。
    """
    import os 
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN")
    
    # 💡 擴充查詢：利用 Foreign Key 從 mission_queue 關聯帶出標題、來源與檔案大小
    query = sb.table("mission_intel").select("*, mission_queue(episode_title, source_name, r2_url, audio_size_mb)").eq("intel_status", "Sum.-pre")
    
    # 🚀 絕對物理防線：中/輕型機甲，配合 GROQ 升級，放寬至撿取 30MB 以下的大檔！
    if worker_id not in ["HUGGINGFACE", "DBOS", "AUDIO_EAT", "RAILWAY"]:
        # 透過外鍵關聯 (mission_queue.audio_size_mb) 直接在資料庫底層進行數值過濾
        query = query.lte("mission_queue.audio_size_mb", 30)

    return query.order("created_at").limit(fetch_limit).execute().data or []

def upsert_intel_status(sb, task_id, status, provider=None, stt_text=None):
    """【狀態更新】更新情報狀態 (如 Sum.-proc, Sum.-pre)，並支援選填供應商或逐字稿"""
    payload = {"task_id": task_id, "intel_status": status}
    if provider: payload["ai_provider"] = provider
    if stt_text: payload["stt_text"] = stt_text
    sb.table("mission_intel").upsert(payload, on_conflict="task_id").execute()

def update_intel_success(sb, task_id, summary, score):
    """【任務結案】第二棒摘要完成後，寫入最終成果並推進狀態至 Sum.-sent"""
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
    """【退匣機制】遇到致命錯誤時，抹除情報暫存紀錄，讓任務退回起點"""
    try: sb.table("mission_intel").delete().eq("task_id", task_id).execute()
    except: pass

def parse_intel_metrics(text):
    """【情報解析】使用正則表達式 (RegEx) 從 AI 生成的摘要中萃取「綜合情報分」"""
    metrics = {"score": 0, "evidence": 0}
    try:
        s_match = re.search(r"綜合情報分.*?(\d+)", text)
        if s_match: metrics["score"] = int(s_match.group(1))
    except: pass
    return metrics

# =========================================================
# 🧠 AI 火控與通訊 (AI & Comms)
# 職責：直接呼叫 Groq/Gemini API，並負責與 Telegram 基地台通訊
# =========================================================

def call_groq_stt(secrets, r2_url_path):
    """
    【GROQ STT 發報機】
    下載 R2 音檔後，傳送給 Groq Whisper 模型進行極速轉譯。
    """
    url = f"{secrets['R2_URL']}/{r2_url_path}"
    m_type = "audio/ogg" if ".opus" in url else "audio/mpeg"
    
    # 🚀 使用 curl_cffi 下載音檔至記憶體
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    audio_data = resp.content
    
    headers = {"Authorization": f"Bearer {secrets['GROQ_KEY']}"}
    files = {'file': (r2_url_path, audio_data, m_type)}
    data = {'model': 'whisper-large-v3', 'response_format': 'text', 'language': 'en'}
    
    # 發送給 API
    stt_resp = requests.post("https://api.groq.com/openai/v1/audio/transcriptions", headers=headers, files=files, data=data, timeout=120)
    
    # 🧹 記憶體防護：確保釋放二進位龐大資源，防止 OOM
    del audio_data, files, resp; gc.collect()
    
    if stt_resp.status_code == 200: 
        return stt_resp.text
    else: 
        raise Exception(f"Groq API Error: HTTP {stt_resp.status_code} - {stt_resp.text}")

def call_gemini_summary(secrets, r2_url_path, sys_prompt):
    """
    【GEMINI 摘要發報機 (支援 A/B 備援雙模式)】
    模式 A (無音檔)：當 r2_url_path 為空，代表手邊已有 GROQ 逐字稿，只傳遞純文字進行摘要。
    模式 B (原生流)：當有網址時，下載音檔並夾帶傳給 GEMINI 原生多模態聽取。
    """
    gemini_model = "gemini-2.5-flash"
    g_url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent?key={secrets['GEMINI_KEY']}"
    
    # 🛡️ 智能備援邏輯：如果 r2_url_path 為空，啟動「純文字傳輸模式」
    if not r2_url_path or r2_url_path.lower() == 'null':
        payload = {"contents": [{"parts": [{"text": sys_prompt}]}]}
    else:
        # 🎙️ 原生流模式：下載音檔並轉為 Base64
        url = f"{secrets['R2_URL']}/{r2_url_path}"
        m_type = "audio/ogg" if ".opus" in url.lower() or ".ogg" in url.lower() else "audio/mpeg"
        
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        raw_bytes = resp.content
        
        # 🧮 物理防護：配合 GROQ 長訪談，放寬中型機甲載重極限至 30.0MB
        file_size_mb = len(raw_bytes) / (1024 * 1024)
        if file_size_mb > 30.0: 
            del raw_bytes; gc.collect() 
            raise Exception(f"越權攔截：檔案達 {file_size_mb:.1f}MB，中型機甲無重裝權限，退回交接給重裝部隊。")

        b64_audio = base64.b64encode(raw_bytes).decode('utf-8')
        del raw_bytes; gc.collect() 
        
        payload = {"contents": [{"parts": [{"text": sys_prompt}, {"inline_data": {"mime_type": m_type, "data": b64_audio}}]}]}
    
    # 發送給 Gemini
    ai_resp = requests.post(g_url, json=payload, timeout=180)
    
    # 🧹 釋放 Payload 記憶體
    if 'b64_audio' in locals(): del b64_audio
    del payload; gc.collect() 
    
    if ai_resp.status_code == 200:
        cands = ai_resp.json().get('candidates', [])
        if cands and cands[0].get('content'): 
            return cands[0]['content']['parts'][0].get('text', "")
        return ""
    else: 
        err_msg = ai_resp.text[:200] 
        raise Exception(f"Gemini API 拒絕存取 (HTTP {ai_resp.status_code}): {err_msg}")

def send_tg_report(secrets, source, title, summary, sb=None, worker_id="UNKNOWN"):
    """
    【TG 防彈發報系統】
    靜默處理 Telegram 的崩潰狀況。如果 Markdown 解析失敗，會自動降級為純文字重試。
    """
    # ✂️ 字數防護：TG 單則訊息上限約 4096 字元
    safe_summary = summary[:3800] + ("...\n(因字數限制截斷)" if len(summary) > 3800 else "")
    
    # 🛡️ 特殊字元防護：避免 Markdown 格式因特殊符號而解析失敗
    safe_source = str(source).replace("_", "＿").replace("*", "＊").replace("[", "〔").replace("]", "〕").replace("`", "‵")
    safe_title = str(title).replace("_", "＿").replace("*", "＊").replace("[", "〔").replace("]", "〕").replace("`", "‵")
    report_msg = f"🎙️ *{safe_source}*\n📌 *{safe_title}*\n\n{safe_summary}"
    
    url = f"https://api.telegram.org/bot{secrets['TG_TOKEN']}/sendMessage"
    payload = {"chat_id": secrets["TG_CHAT"], "text": report_msg, "parse_mode": "Markdown"}
    
    try:
        # 第一次嘗試：帶有 Markdown 格式
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code != 200:
            # 第二次嘗試 (降級防護)：拔除 parse_mode 改用純文字發送
            payload["parse_mode"] = None
            resp = requests.post(url, json=payload, timeout=15)
            
        if resp.status_code == 200: 
            return True
        else: 
            raise Exception(f"Telegram 終極發送失敗: {resp.text}")
            
    except Exception as e: 
        err_msg = f"⚠️ TG 戰報發送失敗: {str(e)[:150]}"
        print(f"[{worker_id}] {err_msg} (已轉紀錄至 S_LOG，主線任務繼續)")
        # 發生錯誤不阻斷主線，將錯誤寫回 Supabase 日誌以供後續查修
        if sb:
            try:
                sb.table("pod_scra_log").insert({
                    "worker_id": worker_id, "task_type": "TG_REPORT", "status": "ERROR",
                    "message": f"TG 發報失敗 | Title: {safe_title[:30]} | Err: {str(e)[:100]}"
                }).execute()
            except: pass 
        return False
