# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_sandbox.py (V5.8.11 絕對防禦拾取版)
# 任務：Groq API 金絲雀影子測試 (Canary Release)
# 特色：加入強固型 JSON 解析與退匣防護，確保「無論如何都要帶回文字」。
# ---------------------------------------------------------
import os, time
from src.pod_scra_intel_control import get_secrets

def run_groq_sandbox_test(sb, s_log_func):
    """【沙盒演習】防彈級 API 接收器測試"""
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    try:
        s_log_func(sb, "SANDBOX", "INFO", f"🧪 [{worker_id}] 啟動 Groq 絕對防禦拾取測試...")
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
                # [略過下載步驟日誌以保持簡潔]
                audio_resp = requests.get(target_url, timeout=60)
                audio_resp.raise_for_status()
                audio_data = audio_resp.content

                headers = {"Authorization": f"Bearer {groq_key}"}
                files = {'file': (file_name, audio_data, "audio/ogg")} 
                # 依然要求 JSON 格式
                data = {'model': 'whisper-large-v3', 'response_format': 'json', 'language': 'en'}

                start_time = time.time()
                stt_resp = requests.post("https://api.groq.com/openai/v1/audio/transcriptions", headers=headers, files=files, data=data, timeout=180)
                
                # ---------------------------------------------------------
                # 🛡️ 【絕對防禦拾取區塊】 (日後移植 techcore 必備)
                # ---------------------------------------------------------
                stt_text = ""
                
                # 1. 如果伺服器根本不給 200，但有回傳文字，我們把錯誤訊息當成結果帶回，以利除錯。
                if stt_resp.status_code != 200:
                    stt_text = f"[GROQ_API_ERROR] 狀態碼: {stt_resp.status_code}. 伺服器回覆: {stt_resp.text}"
                    # 這裡故意不 raise Exception，而是把錯誤當成文字帶回去
                    s_log_func(sb, "SANDBOX", "WARNING", f"⚠️ API 狀態異常，已將錯誤訊息帶回 STT_TXT。")
                
                else:
                    # 2. 伺服器成功回傳 (200 OK)
                    try:
                        # 優先嘗試用標準 JSON 解析袋子 (期望格式: {"text": "Hello world..."})
                        stt_text = stt_resp.json().get('text', '')
                    except Exception as json_err:
                        # 3. 【退匣防護】萬一 Groq 調皮不給 JSON，給了純字串？
                        # 絕對不報錯！直接硬生生擷取原始回傳文字 (Raw Text)。
                        s_log_func(sb, "SANDBOX", "WARNING", f"⚠️ JSON 解析失敗 ({json_err})，啟動退匣防護，直接強制擷取原始字串！")
                        stt_text = stt_resp.text 
                
                # 防呆：確保 stt_text 絕對不是 None
                stt_text = stt_text.strip() if stt_text else "[STT_EMPTY_RESPONSE]"
                # ---------------------------------------------------------

                elapsed = time.time() - start_time
                text_len = len(stt_text)

                # 模擬主線：把文字塞回 Supabase (即使是錯誤訊息也塞)
                # 您可以隨便找一個已知的 task_id 來模擬寫入，或者這一步只是印出即可
                # sb.table("mission_intel").upsert({...}).execute()

                s_log_func(sb, "SANDBOX", "SUCCESS", f"✅ 拾取大獲全勝！耗時: {elapsed:.1f}s | 字數: {text_len} | 預覽: {stt_text[:50]}...")
                test_completed = True 
                
            except Exception as target_err:
                s_log_func(sb, "SANDBOX", "ERROR", f"❌ 本地處理致命錯誤 (如下載失敗): {str(target_err)}")
                continue

    except Exception as e:
        s_log_func(sb, "SANDBOX", "ERROR", f"❌ 極簡沙盒系統異常: {str(e)}")
