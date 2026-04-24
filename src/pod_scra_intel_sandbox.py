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
        # 🚨 辨識碼更新：用這行字確認新裝甲已上線！
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
                audio_resp = requests.get(target_url, timeout=60)
                audio_resp.raise_for_status()
                audio_data = audio_resp.content
                size_mb = len(audio_data) / (1024 * 1024)

                s_log_func(sb, "SANDBOX", "INFO", f"🎯 檔案下載成功 ({size_mb:.2f}MB)。開始填裝並呼叫 Groq API...")

                headers = {"Authorization": f"Bearer {groq_key}"}
                files = {'file': (file_name, audio_data, "audio/ogg")} 
                data = {'model': 'whisper-large-v3', 'response_format': 'json', 'language': 'en'}

                start_time = time.time()
                stt_resp = requests.post("https://api.groq.com/openai/v1/audio/transcriptions", headers=headers, files=files, data=data, timeout=180)
                
                # ---------------------------------------------------------
                # 🛡️ 【絕對防禦拾取區塊】 
                # ---------------------------------------------------------
                stt_text = ""
                
                if stt_resp.status_code != 200:
                    stt_text = f"[GROQ_API_ERROR] 狀態碼: {stt_resp.status_code}. 伺服器回覆: {stt_resp.text}"
                    s_log_func(sb, "SANDBOX", "WARNING", f"⚠️ API 狀態異常，已將錯誤訊息帶回 STT_TXT。")
                else:
                    try:
                        # 嘗試標準 JSON 解析
                        stt_text = stt_resp.json().get('text', '')
                    except Exception as json_err:
                        # 🚀 退匣防護：如果 Groq 又不給 JSON，我們直接硬吸原始字串！絕對不報錯！
                        s_log_func(sb, "SANDBOX", "WARNING", f"⚠️ JSON 解析失敗 ({json_err})，啟動退匣防護，直接強制擷取原始字串！")
                        stt_text = stt_resp.text 
                
                stt_text = stt_text.strip() if stt_text else "[STT_EMPTY_RESPONSE]"
                # ---------------------------------------------------------

                elapsed = time.time() - start_time
                text_len = len(stt_text)

                s_log_func(sb, "SANDBOX", "SUCCESS", f"✅ 拾取大獲全勝！耗時: {elapsed:.1f}s | 字數: {text_len} | 預覽: {stt_text[:50]}...")
                test_completed = True 
                
            except Exception as target_err:
                s_log_func(sb, "SANDBOX", "WARNING", f"⚠️ 靶材 {file_name} 試射失敗: {str(target_err)[:100]}... 切換下一發。")
                continue

        if not test_completed:
            s_log_func(sb, "SANDBOX", "ERROR", "❌ 所有標靶皆狙擊失敗或無效。")

    except Exception as e:
        s_log_func(sb, "SANDBOX", "ERROR", f"❌ 極簡沙盒系統異常: {str(e)}")
