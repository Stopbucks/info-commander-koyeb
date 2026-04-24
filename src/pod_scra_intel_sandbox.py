# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_sandbox.py (V5.8.9 純淨輪詢直連版)
# 任務：Groq API 金絲雀影子測試 (Canary Release)
# 策略：準備多個實體網址進行輪詢，完全使用標準 requests 硬派直連。
# ---------------------------------------------------------
import os, time
from src.pod_scra_intel_control import get_secrets

def run_groq_sandbox_test(sb, s_log_func):
    """【沙盒演習】多重靶材網址輪詢，純測 Groq API 火力"""
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    try:
        s_log_func(sb, "SANDBOX", "INFO", f"🧪 [{worker_id}] 啟動 Groq 純淨輪詢直連測試...")
        
        # 🚨 區域內強制載入「標準」的 requests，避開 curl_cffi
        import requests 
        
        s = get_secrets()
        groq_key = s.get("GROQ_KEY")
        if not groq_key:
            s_log_func(sb, "SANDBOX", "ERROR", "❌ 找不到 GROQ_KEY 金鑰！")
            return

        # 🎯 標靶清單：直接寫入實體 R2 網址 (包含主將與備援)
        TARGET_LIST = [
            {
                "file_name": "opt_95b032f9.opus", 
                "url": "https://pub-a17c3e04067c4370a5778189ab64618e.r2.dev/opt_95b032f9.opus"
            },
            {
                "file_name": "opt_91fc4d08.opus", 
                "url": "https://pub-a17c3e04067c4370a5778189ab64618e.r2.dev/opt_91fc4d08.opus"
            }
        ]
        
        test_completed = False

        # 🔄 開始輪詢標靶
        for target in TARGET_LIST:
            if test_completed: break 
            
            file_name = target["file_name"]
            target_url = target["url"]
            
            s_log_func(sb, "SANDBOX", "INFO", f"📥 準備從 R2 獲取實體檔案: {file_name}")
            
            try:
                # 1. 下載音檔到記憶體
                audio_resp = requests.get(target_url, timeout=60)
                audio_resp.raise_for_status()
                audio_data = audio_resp.content
                size_mb = len(audio_data) / (1024 * 1024)
                
                s_log_func(sb, "SANDBOX", "INFO", f"🎯 檔案下載成功 ({size_mb:.2f}MB)。開始填裝並呼叫 Groq API...")

                # 2. 封裝並發送給 Groq
                headers = {"Authorization": f"Bearer {groq_key}"}
                files = {'file': (file_name, audio_data, "audio/ogg")} 
                data = {'model': 'whisper-large-v3', 'response_format': 'text', 'language': 'en'}

                start_time = time.time()
                
                stt_resp = requests.post(
                    "https://api.groq.com/openai/v1/audio/transcriptions",
                    headers=headers,
                    files=files,
                    data=data,
                    timeout=180 
                )

                if stt_resp.status_code != 200:
                    raise Exception(f"Groq 伺服器拒絕: {stt_resp.status_code} - {stt_resp.text}")

                # 3. 取得成果
                stt_text = stt_resp.json().get('text', '')
                elapsed = time.time() - start_time
                text_len = len(stt_text)

                s_log_func(sb, "SANDBOX", "SUCCESS", f"✅ Groq 狙擊大獲全勝！靶材: {file_name} | 耗時: {elapsed:.1f}s | 字數: {text_len}")
                test_completed = True # 標記成功，準備撤退
                
            except Exception as target_err:
                # 如果這發子彈卡彈，記錄錯誤並換下一發
                s_log_func(sb, "SANDBOX", "WARNING", f"⚠️ 靶材 {file_name} 試射失敗: {str(target_err)[:150]}... 切換下一發。")
                continue
                
        if not test_completed:
            s_log_func(sb, "SANDBOX", "ERROR", "❌ 所有標靶皆狙擊失敗或無效。")

    except Exception as e:
        s_log_func(sb, "SANDBOX", "ERROR", f"❌ 極簡沙盒系統異常: {str(e)}")
