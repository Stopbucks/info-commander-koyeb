#---------------------------------------------------
# src/pod_scra_intel_sandbox.py
# src/pod_scra_intel_sandbox.py
# 沙盒測試 V1.0 
# 任務：將音檔交GROQ 進行逐字稿翻譯
#---------------------------------------------------
import os, time
from src.pod_scra_intel_techcore import call_groq_stt

def run_groq_sandbox_test(sb, s_log_func):
    """【沙盒演習】專門測試 Groq API，絕不干擾主產線"""
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    try:
        s_log_func(sb, "SANDBOX", "INFO", f"🧪 [{worker_id}] 啟動 Groq 影子測試...")
        
        # 🎯 條件：尋找一個 > 7MB 且狀態還是 pending 的檔案來當靶子
        # 注意：我們加上 limit(1) 確保每次只抓一個靶子
        query = sb.table("vw_safe_mission_queue").select("id, r2_url, audio_size_mb, source_name, episode_title") \
                  .gte("audio_size_mb", 7.0).ilike("r2_url", "%.opus") \
                  .eq("scrape_status", "pending") \
                  .order("created_at", desc=True).limit(1)
        
        target = query.execute().data
        if not target:
            s_log_func(sb, "SANDBOX", "INFO", "🛌 沙盒無靶材 (無 >7MB 之 Opus 檔)。")
            return
            
        task = target[0]
        task_id = task['id']
        r2_url = task['r2_url']
        size = task['audio_size_mb']
        
        s_log_func(sb, "SANDBOX", "INFO", f"🎯 鎖定沙盒靶材: {task.get('source_name')} ({size}MB) - {task_id[:8]}")
        
        # 🚀 發起實彈射擊 (呼叫 Groq)
        start_time = time.time()
        stt_text = call_groq_stt(os.environ, r2_url) # 假設 secrets 已經存在 os.environ
        elapsed = time.time() - start_time
        
        text_len = len(stt_text)
        
        # 🚨 關鍵防禦：【絕對不要】把狀態改成 Sum.-pre！
        # 我們只把翻譯好的字，偷偷寫進 stt_text 欄位，或者寫進 log 裡。
        # 這樣這筆任務在主系統眼裡，依然是個 "pending" 的音檔。
        
        sb.table("mission_intel").upsert({
            "task_id": task_id, 
            "stt_text": stt_text,
            "intel_status": "Sandbox-Test" # 👈 用一個特殊的假狀態！
        }, on_conflict="task_id").execute()
        
        s_log_func(sb, "SANDBOX", "SUCCESS", f"✅ Groq 轉譯成功！耗時: {elapsed:.1f}s | 字數: {text_len} | 已存入沙盒狀態。")

    except Exception as e:
        s_log_func(sb, "SANDBOX", "ERROR", f"❌ Groq 沙盒測試失敗: {str(e)}")
