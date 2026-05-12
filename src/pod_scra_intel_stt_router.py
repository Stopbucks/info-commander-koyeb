# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_stt_router.py (V6.12  感知路由_CF 版)
# 職責：專職處理 STT 聽寫任務的 5 階段輪詢與 API 呼叫。
# 戰術順序：Groq -> CF --> Gladia -> Speechmatics -> AssemblyAI -> Deepgram
# 記憶體防護：實作「閱後即焚」，單次下載後上傳即釋放二進位記憶體。
# [V6.10 重大突破] 實裝微型機甲防護網：FLY_LAX / ALWAYSDATA 遇到 >= 8MB 檔案，
# 自動跳過需消耗記憶體的 Groq，直接進入零記憶體消耗的 URL 輪詢！
# [V6.12] 新增 CF 聽寫功能 每日單檔消耗：60 秒 × 14 神經元/秒 = 840 神經元 
#               每日總消耗：4 檔 × 840 神經元 = 3,360 神經元 / 天(約一半額度，觀察一個月至0612)
# --------------------------------------------------------
# [S_LOG 守則] 未來若新增 log_system_error，請務必放置於「最外層的 except 區塊」。
# [防禦機制] 嚴禁置於 Retry 迴圈或高頻輪詢內，以防 API 崩潰時無限觸發寫入，導致資料庫超載。
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
        
        # 🚀 [V6.12 新增] Cloudflare 算力金鑰與帳戶 ID
        "CLOUDFLARE_API_TOKEN": os.environ.get("CLOUDFLARE_API_TOKEN"),
        "CLOUDFLARE_ACCOUNT_ID": os.environ.get("CLOUDFLARE_ACCOUNT_ID"),
        
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
# 🚰 滴流管控 (Quota Pacing - 支援週/日雙軌制)
# =========================================================
def check_and_update_quota(sb, provider_name):
    if not sb: return False
    try:
        res = sb.table("pod_scra_metadata").select("dictionary").eq("key_name", "STT_QUOTA_PACING").single().execute()
        if not res.data or not res.data.get("dictionary"): return False
        
        quota_dict = res.data["dictionary"]
        if provider_name not in quota_dict: return False
        
        provider_data = quota_dict[provider_name]
        
        # 🚀 [新增] Cloudflare 專用：每日重置邏輯
        if provider_name == "cloudflare_ai":
            current_date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            
            # 如果日期不同，代表跨日了，重置計數器
            if provider_data.get("date") != current_date_str:
                provider_data["date"] = current_date_str
                provider_data["count"] = 0
                
            limit = provider_data.get("limit_per_day", 5) # 預設每天 5 次
            
            if provider_data["count"] >= limit:
                print(f"⚠️ [{provider_name}] 今日額度已達安全上限 ({limit}次)，鎖定開火。")
                return False
                
            provider_data["count"] += 1
            print(f"🔓 [{provider_name}] 額度放行 (今日使用: {provider_data['count']}/{limit})")
            
        # 🛡️ 其他服務 (AssemblyAI, Deepgram)：每週重置邏輯
        else:
            current_week = get_current_week()
            
            # 如果週數不同，代表跨週了，重置計數器
            if provider_data.get("week") != current_week:
                provider_data["week"] = current_week
                provider_data["count"] = 0
                
            limit = provider_data.get("limit_per_week", 2)
            
            if provider_data["count"] >= limit:
                print(f"⚠️ [{provider_name}] 本週額度已達上限 ({limit}次)，拒絕開火。")
                return False
                
            provider_data["count"] += 1
            print(f"🔓 [{provider_name}] 額度放行 (本週使用: {provider_data['count']}/{limit})")
            
        # 💾 更新資料庫
        quota_dict[provider_name] = provider_data
        sb.table("pod_scra_metadata").update({"dictionary": quota_dict}).eq("key_name", "STT_QUOTA_PACING").execute()
        
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



def _call_cloudflare_stt(secrets, audio_data):
    """
    🎯 [Plan E] 呼叫 Cloudflare Workers AI (Whisper)
    使用 Account ID 與 API Token 直接對接 Cloudflare 算力池。
    """
    account_id = secrets.get("CLOUDFLARE_ACCOUNT_ID")
    api_token = secrets.get("CLOUDFLARE_API_TOKEN")
    
    if not account_id or not api_token: return None, "NO_CF_CREDENTIALS"
    
    # Cloudflare Whisper 官方模型端點
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/@cf/openai/whisper"
    headers = {"Authorization": f"Bearer {api_token}"}
    
    print("🎯 [Plan E] 呼叫 Cloudflare Workers AI 聽寫...")
    try:
        # Cloudflare 接受二進位音檔直接 Post
        resp = requests.post(url, headers=headers, data=audio_data, timeout=300)
        
        if resp.status_code == 200:
            result = resp.json()
            if result.get("success"):
                return result["result"].get("text", "CF_EMPTY_TEXT"), "SUCCESS"
            else:
                return None, f"CF_API_ERROR: {result.get('errors')}"
        else:
            return None, f"CF_HTTP_{resp.status_code}"
    except Exception as e:
        return None, f"CF_EXCEPTION_{str(e)[:50]}"

# =========================================================
# ⚙️ STT 火力協調中心主入口 (The Router V6.13)
# =========================================================
def execute_stt_routing(sb, r2_url_path, file_size_mb=0):
    s = get_stt_secrets()
    url = f"{s['R2_URL']}/{r2_url_path}"
    m_type = "audio/ogg" if ".opus" in url.lower() else "audio/mpeg"
    filename = os.path.basename(r2_url_path)
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN")
    
    stt_text = None
    all_errors = []
    
    # -----------------------------------------------------
    # 🚀 階段一：輕型任務區 (處理 24.5MB 以下，追求免費與極速)
    # -----------------------------------------------------
    if file_size_mb < 24.5:
        # 1. Groq (首選主力 - 絕對記憶體保護)
        skip_groq = (worker_id in ["FLY_LAX", "ALWAYSDATA"] and file_size_mb >= 8.0)
        if not skip_groq:
            print(f"📥 [STT Router] 下載物資供 Groq/CF 使用: {filename}...")
            try:
                resp = requests.get(url, timeout=60)
                resp.raise_for_status()
                audio_data = resp.content
                
                # 第一順位：Groq
                stt_text, status = _call_groq(s['GROQ_KEY'], audio_data, filename, m_type)
                if status == "SUCCESS" and stt_text:
                    del audio_data; gc.collect()
                    return stt_text, "GROQ", all_errors
                all_errors.append(f"Groq:{status}")
                
                # 第二順位：Cloudflare (僅在輕型任務且 Groq 失敗時，受額度安全鎖保護)
                if check_and_update_quota(sb, "cloudflare_ai"):
                    stt_text, status = _call_cloudflare_stt(s, audio_data)
                    if status == "SUCCESS" and stt_text:
                        del audio_data; gc.collect()
                        return stt_text, "CLOUDFLARE", all_errors
                    all_errors.append(f"Cloudflare:{status}")
                else:
                    all_errors.append("Cloudflare:SAFE_LOCK_ACTIVATED")
                
                del audio_data; gc.collect()
                print("🧹 [STT Router] 本地音檔已焚毀，釋放記憶體。")
            except Exception as e:
                all_errors.append(f"Light_Zone_DL_FAIL:{str(e)[:30]}")

    # -----------------------------------------------------
    # 🛡️ 階段二：重型任務區 (處理 >24.5MB 或輕型區掉棒任務)
    # -----------------------------------------------------
    if not stt_text:
        print(f"🛡️ [STT Router] 進入 URL 降維輪詢區 (重型/備援)...")
        
        # Plan C: Gladia
        stt_text, status = _call_gladia(s['GLADIA_KEY'], url, sb)
        if status == "SUCCESS" and stt_text: return stt_text, "GLADIA", all_errors
        all_errors.append(f"Gladia:{status}")
        
        # Plan D: Speechmatics
        stt_text, status = _call_speechmatics(s['SPEECHMATICS_KEY'], url, sb)
        if status == "SUCCESS" and stt_text: return stt_text, "SPEECHMATICS", all_errors
        all_errors.append(f"Speechmatics:{status}")
        
        # Plan F/G: 滴流管制最終防線
        for provider in ["assemblyai", "deepgram"]:
            if check_and_update_quota(sb, provider):
                if provider == "assemblyai": stt_text, status = _call_assemblyai(s['ASSEMBLYAI_KEY'], url)
                else: stt_text, status = _call_deepgram(s['DEEPGRAM_KEY'], url)
                
                if status == "SUCCESS" and stt_text: return stt_text, provider.upper(), all_errors
                all_errors.append(f"{provider}:{status}")

    raise Exception(f"STT 聯合火力網全軍覆沒: {' | '.join(all_errors)}")
