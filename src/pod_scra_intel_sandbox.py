# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_sandbox.py (V5.8.2 甜蜜點精準狙擊版)
# 任務：Groq API 金絲雀影子測試 (Canary Release)
# 範圍：精準鎖定 14.0MB ~ 24.0MB 區間的無主大檔。
# 目的：測試純 API 連線穩定度與耗時，不涉及切塊技術。
# ---------------------------------------------------------
import os, time
# 🚀 直接從軍械庫呼叫您已經寫好的完美 STT 函式 (內含 whisper-large-v3)
from src.pod_scra_intel_techcore import call_groq_stt

def run_groq_sandbox_test(sb, s_log_func):
    """【沙盒演習】尋找 14~24MB 的甜區靶材，不干擾主線"""
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    try:
        s_log_func(sb, "SANDBOX", "INFO", f"🧪 [{worker_id}] 啟動 Groq 甜蜜點 (14-24MB) 影子測試...")
        
        # 🎯 雷達校準：14.0 < 大小 <= 24.0
        query = sb.table("vw_safe_mission_queue").select("id, r2_url, audio_size_mb, source_name, episode_title") \
                  .gt("audio_size_mb", 14.0).lte("audio_size_mb", 24.0).ilike("r2_url", "%.opus") \
                  .eq("scrape_status", "completed") \
                  .order("created_at", desc=True).limit(10) 
        
        targets = query.execute().data
        if not targets:
            s_log_func(sb, "SANDBOX", "INFO", "🛌 沙盒無靶材 (無 14~24MB 區間之 Opus 檔)。")
            return
            
        task = None
        for t in targets:
            # 🛡️ 防呆：檢查是否已經被沙盒打過了
            check_res = sb.table("mission_intel").select("intel_status").eq("task_id", t['id']).execute().data
            if check_res: continue 
            
            task = t
            break
            
        if not task:
            s_log_func(sb, "SANDBOX", "INFO", "🛌 目前符合 14~24MB 條件的靶材皆已完成測試。")
            return
            
        task_id = task['id']
        r2_url = task['r2_url']
        size = task['audio_size_mb']
        
        s_log_func(sb, "SANDBOX", "INFO", f"🎯 鎖定甜區靶材: {task.get('source_name')} ({size}MB) - {task_id[:8]}")
        
        # 🚀 發起實彈射擊 (單發呼叫 Groq，無切塊)
        start_time = time.time()
        stt_text = call_groq_stt(os.environ, r2_url) 
        elapsed = time.time() - start_time
        
        text_len = len(stt_text)
        
        # 🚨 寫入沙盒專屬狀態
        sb.table("mission_intel").upsert({
            "task_id": task_id, 
            "stt_text": stt_text,
            "intel_status": "Sandbox-Test" 
        }, on_conflict="task_id").execute()
        
        s_log_func(sb, "SANDBOX", "SUCCESS", f"✅ Groq 轉譯成功！耗時: {elapsed:.1f}s | 字數: {text_len} | 甜蜜點測試通關。")

    except Exception as e:
        s_log_func(sb, "SANDBOX", "ERROR", f"❌ Groq 沙盒測試失敗: {str(e)}")
