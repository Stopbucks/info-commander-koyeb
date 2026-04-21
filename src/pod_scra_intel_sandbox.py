#---------------------------------------------------
# 程式碼：src/pod_scra_intel_sandbox.py
# src/pod_scra_intel_sandbox.py
# 沙盒測試 V1.2
# 任務：Groq API 金絲雀影子測試 (Canary Release)將音檔交GROQ 進行逐字稿翻譯
# 特色：完美貼合正式流程，自動尋找無人處理的 >7MB Opus 檔進行試射
# ---------------------------------------------------------
import os, time
from src.pod_scra_intel_techcore import call_groq_stt

def run_groq_sandbox_test(sb, s_log_func):
    """【沙盒演習】專門測試 Groq API，絕不干擾主產線"""
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    try:
        s_log_func(sb, "SANDBOX", "INFO", f"🧪 [{worker_id}] 啟動 Groq 影子測試...")
        
        # 🎯 雷達校準：尋找正式流程中已經下載壓縮完畢 (completed) 的 >7MB Opus 檔
        query = sb.table("vw_safe_mission_queue").select("id, r2_url, audio_size_mb, source_name, episode_title") \
                  .gte("audio_size_mb", 7.0).ilike("r2_url", "%.opus") \
                  .eq("scrape_status", "completed") \
                  .order("created_at", desc=True).limit(5) # 多抓幾筆來篩選
        
        targets = query.execute().data
        if not targets:
            s_log_func(sb, "SANDBOX", "INFO", "🛌 沙盒無靶材 (無 >7MB 之 Opus 檔)。")
            return
            
        task = None
        for t in targets:
            # 🛡️ 防呆機制：檢查這筆任務是否已經在 intel 表格有了紀錄
            # 如果已經有紀錄 (代表被主線處理過，或之前沙盒打過)，就跳過換下一個
            check_res = sb.table("mission_intel").select("intel_status").eq("task_id", t['id']).execute().data
            if check_res: continue 
            
            task = t
            break
            
        if not task:
            s_log_func(sb, "SANDBOX", "INFO", "🛌 目前符合條件的靶材皆已完成測試或進入主線。")
            return
            
        task_id = task['id']
        r2_url = task['r2_url']
        size = task['audio_size_mb']
        
        s_log_func(sb, "SANDBOX", "INFO", f"🎯 鎖定沙盒靶材: {task.get('source_name')} ({size}MB) - {task_id[:8]}")
        
        # 🚀 發起實彈射擊 (呼叫 Groq)
        start_time = time.time()
        stt_text = call_groq_stt(os.environ, r2_url) 
        elapsed = time.time() - start_time
        
        text_len = len(stt_text)
        
        # 🚨 寫入沙盒專屬狀態，與主線隔離
        sb.table("mission_intel").upsert({
            "task_id": task_id, 
            "stt_text": stt_text,
            "intel_status": "Sandbox-Test" 
        }, on_conflict="task_id").execute()
        
        s_log_func(sb, "SANDBOX", "SUCCESS", f"✅ Groq 轉譯成功！耗時: {elapsed:.1f}s | 字數: {text_len} | 已存入沙盒狀態。")

    except Exception as e:
        s_log_func(sb, "SANDBOX", "ERROR", f"❌ Groq 沙盒測試失敗: {str(e)}")
