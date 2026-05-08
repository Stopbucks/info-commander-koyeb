# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_stt_router.py (V6.10 資源感知路由版)
# 職責：專職處理 STT 聽寫任務的 5 階段輪詢與 API 呼叫。
# 戰術順序：Groq -> Gladia -> Speechmatics -> AssemblyAI -> Deepgram
# 記憶體防護：實作「閱後即焚」，單次下載後上傳即釋放二進位記憶體。
# [V6.10 重大突破] 實裝微型機甲防護網：FLY_LAX / ALWAYSDATA 遇到 >= 8MB 檔案，
# 自動跳過需消耗記憶體的 Groq，直接進入零記憶體消耗的 URL 輪詢！
# ---------------------------------------------------------
import os, gc, time, json
import httpx 
from curl_cffi import requests 
from datetime import datetime, timezone

# =========================================================
# 🛡️ 戰術控制與基礎模組
# =========================================================
def get_stt_secrets():
    return {
        "GROQ_KEY": os.environ.get("GROQ_API_KEY", os.environ.get("GROQ_KEY")),
        "GLADIA_KEY": os.environ.get("GLADIA_API_KEY"),
        "SPEECHMATICS_KEY": os.environ.get("SPEECHMATICS_API_KEY"),
        "ASSEMBLYAI_KEY": os.environ.get("ASSEMBLYAI_API_KEY"),
        "DEEPGRAM_KEY": os.environ.get("DEEPGRAM_API_KEY"),
        "R2_URL": (os.environ.get("R2_PUBLIC_URL") or "").rstrip('/')
    }

def get_current_week():
    return datetime.now(timezone.utc).isocalendar()[1]

def log_quota_exhaustion(sb, provider, status_code, message):
    if not sb: return
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN")
    try:
        sb.table("pod_scra_log").insert({
            "worker_id": worker_id, 
            "task_type": "STT_QUOTA_ALERT", 
            "status": "WARNING",
            "message": f"[{provider}] 額度疑似耗盡 (HTTP {status_code}): {message[:100]}"
        }).execute()
    except Exception as e:
        print(f"⚠️ 無法寫入額度日誌: {e}")

# =========================================================
# 🚰 滴流管控 (Quota Pacing)
# =========================================================
def check_and_update_quota(sb, provider_name):
    if not sb: return False
    try:
        res = sb.table("pod_scra_metadata").select("dictionary").eq("key_name", "STT_QUOTA_PACING").single().execute()
        if not res.data or not res.data.get("dictionary"): return False
        
        quota_dict = res.data["dictionary"]
        if provider_name not in quota_dict: return False
        
        current_week = get_current_week()
        provider_data = quota_dict[provider_name]
        
        if provider_data.get("week") != current_week:
            provider_data["week"] = current_week
            provider_data["count"] = 0
            
        limit = provider_data.get("limit_per_week", 2)
        if provider_data["count"] >= limit:
            print(f"⚠️ [{provider_name}] 本週額度已達上限 ({limit}次)，拒絕開火。")
            return False
            
        provider_data["count"] += 1
        quota_dict[provider_name] = provider_data
        
        sb.table("pod_scra_metadata").update({"dictionary": quota_dict}).eq("key_name", "STT_QUOTA_PACING").execute()
        print(f"🔓 [{provider_name}] 額度放行 (目前本週使用: {provider_data['count']}/{limit})")
        return True
    except Exception as e:
        print(f"⚠️ 配額檢查失敗: {e}")
        return False

# =========================================================
# 🎤 獨立 STT API 呼叫模組
# =========================================================

def _call_groq(api_key, audio_data, filename, mime_type):
    if not api_key: return None, "NO_API_KEY"
    print("🎯 [Plan B] 呼叫 Groq 聽寫...")
    try:
        headers = {"Authorization": f"Bearer {api_key}"}
        files = {'file': (filename, audio_data, mime_type)}
        data = {'model': 'whisper-large-v3', 'response_format': 'text', 'language': 'en'}
        
        with httpx.Client(timeout=180.0) as client:
            resp = client.post("https://api.groq.com/openai/v1/audio/transcriptions", headers=headers, files=files, data=data)
        
        if resp.status_code == 200: return resp.text, "SUCCESS"
        return None, f"GROQ_HTTP_{resp.status_code}_{resp.text[:50]}"
    except Exception as e:
        return None, f"GROQ_EXCEPTION_{str(e)[:50]}"

def _call_gladia(api_key, audio_url, sb=None):
    if not api_key: return None, "NO_API_KEY"
    print("🎯 [Plan C] 呼叫 Gladia 聽寫 (URL 模式)...")
    try:
        headers = {"x-gladia-key": api_key, "Content-Type": "application/json"}
        resp = requests.post("https://api.gladia.io/v2/transcription", headers=headers, json={"audio_url": audio_url}, timeout=30)
        
        if resp.status_code in [401, 402, 403, 429]:
            log_quota_exhaustion(sb, "Gladia", resp.status_code, resp.text)
            return None, f"GLADIA_QUOTA_HIT_{resp.status_code}"
            
        if resp.status_code not in [200, 201, 202]:
            return None, f"GLADIA_INIT_FAIL_{resp.status_code}"
            
        result_url = resp.json().get("result_url")
        if not result_url: return None, "GLADIA_NO_RESULT_URL"
        
        print("⏳ [Gladia] 等待遠端處理...")
        for _ in range(40):
            time.sleep(10)
            try:
                poll_resp = requests.get(result_url, headers=headers, timeout=15)
                if poll_resp.status_code == 200:
                    status = poll_resp.json().get("status")
                    if status == "done":
                        result = poll_resp.json().get("result", {})
                        full_text = " ".join([utt.get("text", "") for utt in result.get("transcription", {}).get("utterances", [])])
                        return full_text if full_text else "GLADIA_EMPTY_TEXT", "SUCCESS"
                    elif status in ["error", "aborted"]:
                        return None, f"GLADIA_PROCESS_ERROR_{status}"
            except: pass
        return None, "GLADIA_TIMEOUT"
    except Exception as e:
        return None, f"GLADIA_EXCEPTION_{str(e)[:50]}"

def _call_speechmatics(api_key, audio_url, sb=None):
    if not api_key: return None, "NO_API_KEY"
    print("🎯 [Plan D] 呼叫 Speechmatics 聽寫 (URL 模式)...")
    try:
        url = "https://asr.api.speechmatics.com/v2/jobs"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {"type": "transcription", "fetch_data": {"url": audio_url}, "transcription_config": {"operating_point": "enhanced", "language": "en"}}
        
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code in [401, 402, 403, 429]:
            log_quota_exhaustion(sb, "Speechmatics", resp.status_code, resp.text)
            return None, f"SPEECHMATICS_QUOTA_HIT_{resp.status_code}"
            
        if resp.status_code not in [200, 201]:
            return None, f"SPEECHMATICS_INIT_FAIL_{resp.status_code}"
            
        job_id = resp.json().get("id")
        if not job_id: return None, "SPEECHMATICS_NO_JOB_ID"
        
        print("⏳ [Speechmatics] 等待遠端處理...")
        status_url = f"{url}/{job_id}"
        for _ in range(40):
            time.sleep(10)
            try:
                poll_resp = requests.get(status_url, headers=headers, timeout=15)
                if poll_resp.status_code == 200:
                    status = poll_resp.json().get("job", {}).get("status")
                    if status == "done":
                        transcript_resp = requests.get(f"{status_url}/transcript?format=txt", headers=headers, timeout=15)
                        if transcript_resp.status_code == 200: return transcript_resp.text, "SUCCESS"
                        return None, f"SPEECHMATICS_DL_FAIL_{transcript_resp.status_code}"
                    elif status in ["rejected", "deleted"]:
                        return None, f"SPEECHMATICS_REJECTED_{status}"
            except: pass
        return None, "SPEECHMATICS_TIMEOUT"
    except Exception as e:
        return None, f"SPEECHMATICS_EXCEPTION_{str(e)[:50]}"

def _call_assemblyai(api_key, audio_url):
    if not api_key: return None, "NO_API_KEY"
    print("🎯 [Plan E] 呼叫 AssemblyAI 聽寫 (URL 模式)...")
    try:
        headers = {"authorization": api_key, "content-type": "application/json"}
        resp = requests.post("https://api.assemblyai.com/v2/transcript", json={"audio_url": audio_url, "language_code": "en_us"}, headers=headers, timeout=15)
        if resp.status_code != 200: return None, f"ASSEMBLYAI_INIT_FAIL_{resp.status_code}"
        transcript_id = resp.json().get("id")
        if not transcript_id: return None, "ASSEMBLYAI_NO_ID"

        print("⏳ [AssemblyAI] 等待遠端處理...")
        poll_url = f"https://api.assemblyai.com/v2/transcript/{transcript_id}"
        for _ in range(30):
            time.sleep(10)
            try:
                poll_resp = requests.get(poll_url, headers=headers, timeout=15)
                if poll_resp.status_code == 200:
                    data = poll_resp.json()
                    if data["status"] == "completed": return data["text"], "SUCCESS"
                    elif data["status"] == "error": return None, f"ASSEMBLYAI_PROCESS_ERROR_{data.get('error')}"
            except: pass
        return None, "ASSEMBLYAI_TIMEOUT"
    except Exception as e:
        return None, f"ASSEMBLYAI_EXCEPTION_{str(e)[:50]}"

def _call_deepgram(api_key, audio_url):
    if not api_key: return None, "NO_API_KEY"
    print("🎯 [Plan F] 呼叫 Deepgram 聽寫 (URL 模式)...")
    try:
        url = "https://api.deepgram.com/v1/listen?model=nova-2&smart_format=true"
        headers = {"Authorization": f"Token {api_key}", "Content-Type": "application/json"}
        with httpx.Client(timeout=180.0) as client:
            resp = client.post(url, headers=headers, json={"url": audio_url})
        if resp.status_code == 200:
            result = resp.json()
            transcript = result.get("results", {}).get("channels", [{}])[0].get("alternatives", [{}])[0].get("transcript", "")
            return transcript if transcript else "DEEPGRAM_EMPTY_TEXT", "SUCCESS"
        else:
            return None, f"DEEPGRAM_HTTP_{resp.status_code}_{resp.text[:50]}"
    except Exception as e:
        return None, f"DEEPGRAM_EXCEPTION_{str(e)[:50]}"

# =========================================================
# ⚙️ STT 火力協調中心主入口 (The Router)
# =========================================================
# 💡 注意：這裡新增了 file_size_mb 參數，用於資源感知路由
def execute_stt_routing(sb, r2_url_path, file_size_mb=0):
    s = get_stt_secrets()
    url = f"{s['R2_URL']}/{r2_url_path}"
    m_type = "audio/ogg" if ".opus" in url.lower() else "audio/mpeg"
    filename = os.path.basename(r2_url_path)
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN")
    
    stt_text = None
    all_errors = []
    
    # -----------------------------------------------------
    # 🚀 資源感知路由：決定是否跳過需要記憶體的 Groq
    # -----------------------------------------------------
    skip_groq = False
    if worker_id in ["FLY_LAX", "ALWAYSDATA"] and file_size_mb >= 8.0:
        skip_groq = True
        print(f"🛡️ [STT Router] 微型機甲記憶體防護啟動！檔案達 {file_size_mb}MB，跳過 Groq 避免 OOM，直接進入 URL 輪詢。")
        all_errors.append("Groq:SKIPPED_DUE_TO_OOM_RISK")
        
    if not skip_groq:
        print(f"📥 [STT Router] 下載物資供 Groq 使用: {filename}...")
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            audio_data = resp.content
            
            stt_text, status = _call_groq(s['GROQ_KEY'], audio_data, filename, m_type)
            
            # 💥 不管成功或失敗，立刻銷毀二進位檔案
            del audio_data, resp
            gc.collect()
            print("🧹 [STT Router] 本地音檔已焚毀，釋放記憶體。")
            
            if status == "SUCCESS" and stt_text:
                return stt_text, "GROQ", all_errors
            all_errors.append(f"Groq:{status}")
            
        except Exception as e:
            all_errors.append(f"Groq_R2_DL_FAIL:{str(e)[:30]}")
    
    # -----------------------------------------------------
    # 🛡️ URL 降維輪詢區塊 (零記憶體消耗)
    # -----------------------------------------------------
    
    stt_text, status = _call_gladia(s['GLADIA_KEY'], url, sb)
    if status == "SUCCESS" and stt_text: return stt_text, "GLADIA", all_errors
    all_errors.append(f"Gladia:{status}")
    
    stt_text, status = _call_speechmatics(s['SPEECHMATICS_KEY'], url, sb)
    if status == "SUCCESS" and stt_text: return stt_text, "SPEECHMATICS", all_errors
    all_errors.append(f"Speechmatics:{status}")
    
    if check_and_update_quota(sb, "assemblyai"):
        stt_text, status = _call_assemblyai(s['ASSEMBLYAI_KEY'], url)
        if status == "SUCCESS" and stt_text: return stt_text, "ASSEMBLYAI", all_errors
        all_errors.append(f"AssemblyAI:{status}")
    else:
        all_errors.append("AssemblyAI:QUOTA_REACHED")
        
    if check_and_update_quota(sb, "deepgram"):
        stt_text, status = _call_deepgram(s['DEEPGRAM_KEY'], url)
        if status == "SUCCESS" and stt_text: return stt_text, "DEEPGRAM", all_errors
        all_errors.append(f"Deepgram:{status}")
    else:
        all_errors.append("Deepgram:QUOTA_REACHED")
    
    raise Exception(f"STT 聯合火力網全軍覆沒: {' | '.join(all_errors)}")
