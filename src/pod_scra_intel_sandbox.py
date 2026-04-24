# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_sandbox.py (V5.8.13 實彈寫入結案版)
# 任務：Groq API 金絲雀影子測試 (Canary Release)
# 特色：正式開啟 Supabase Upsert，將戰利品存回情報資料庫。
# ---------------------------------------------------------
import os, time
from src.pod_scra_intel_control import get_secrets

def run_groq_sandbox_test(sb, s_log_func):
    """【沙盒演習】防彈級 API 接收器測試 + 實彈寫入"""
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    try:
        s_log_func(sb, "SANDBOX", "INFO", f"🧪 [{worker_id}] 【沙盒 V5.8.13】執行實彈寫入任務...")
        import requests 
        
        s = get_secrets()
        groq_key = s.get("GROQ_KEY")
        if not groq_key: return

        TARGET_LIST = [
            {"file_name": "opt_95b032f9.opus", "url": "https://pub-a17c3e04067c4370a5778189ab64618e.r2.dev/opt_95b032f9.opus"},
            {"file_name": "opt_91fc4d08.opus", "url": "https://pub-a17c3e04067c4370a5778189ab64618e.r2.dev/opt_91fc4d08.opus"}
        ]
        
        test_completed = False

        for target in TARGET_LIST:
            if test_completed: break 
            
            file_name = target["file_name"]
            target_url = target["url"]
            
            try:
                #---獲取檔案與呼叫 API 邏輯相同---#
                audio_resp = requests.get(target_url, timeout=60)
                audio_resp.raise_for_status()
                audio_data = audio_resp.content

                headers = {"Authorization": f"Bearer {groq_key}"}
                files = {'file': (file_name, audio_data, "audio/ogg")} 
                data = {'model': 'whisper-large-v3', 'response_format': 'json', 'language': 'en'}

                start_time = time.time()
                stt_resp = requests.post("https://api.groq.com/openai/v1/audio/transcriptions", headers=headers, files=files, data=data, timeout=180)
                
                # 🛡️ 絕對防禦拾取區塊
                stt_text = ""
                if stt_resp.status_code != 200:
                    stt_text = f"[GROQ_API_ERROR] {stt_resp.status_code}: {stt_resp.text}"
                else:
                    try:
                        stt_text = stt_resp.json().get('text', '') # 優先嘗試 JSON 解析
                    except:
                        stt_text = stt_resp.text # 備援：直接擷取原始文字
                
                stt_text = stt_text.strip() if stt_text else "[STT_EMPTY]"
                elapsed = time.time() - start_time

                # 🚨 【實彈寫入】找到該檔案在資料庫中的 ID 並更新
                task_res = sb.table("mission_queue").select("id").eq("r2_url", file_name).limit(1).execute()
                if task_res.data:
                    target_task_id = task_res.data[0]['id']
                    # 🚀 執行 Upsert：寫入逐字稿並標記沙盒狀態
                    sb.table("mission_intel").upsert({
                        "task_id": target_task_id, 
                        "stt_text": stt_text,
                        "intel_status": "Sandbox-Test",
                        "ai_provider": "GROQ"
                    }, on_conflict="task_id").execute() # 確保相同任務不重複產生多筆紀錄
                    
                    s_log_func(sb, "SANDBOX", "SUCCESS", f"✅ 拾取成功且已寫入資料庫！耗時: {elapsed:.1f}s | 字數: {len(stt_text)}")
                else:
                    s_log_func(sb, "SANDBOX", "WARNING", f"⚠️ 轉譯成功但資料庫找不到對應檔名: {file_name}")
                
                test_completed = True 
                
            except Exception as target_err:
                s_log_func(sb, "SANDBOX", "WARNING", f"⚠️ 靶材 {file_name} 異常: {str(target_err)}")
                continue

    except Exception as e:
        s_log_func(sb, "SANDBOX", "ERROR", f"❌ 沙盒系統異常: {str(e)}")
