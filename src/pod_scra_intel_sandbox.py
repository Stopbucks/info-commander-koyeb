# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_sandbox.py (V5.8.6 裸查底層版)
# 任務：Groq API 金絲雀影子測試 (Canary Release)
# 策略：直接查詢 mission_queue 底層資料表，無視安全視圖過濾，確保找出靶材。
# 繞過view資料表格，直取檔案
# ---------------------------------------------------------
import os, time
from src.pod_scra_intel_techcore import call_groq_stt

def run_groq_sandbox_test(sb, s_log_func):
    """【沙盒演習】多重靶材輪詢，裸查底層資料表"""
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    try:
        s_log_func(sb, "SANDBOX", "INFO", f"🧪 [{worker_id}] 啟動 Groq 多重定點狙擊測試 (底層裸查模式)...")
        
        # 🎯 標靶清單：依序排列優先權
        TARGET_LIST = [
            "opt_95b032f9.opus", 
            "opt_91fc4d08.opus"
        ]
        
        test_completed = False
        
        # 🔄 開始輪詢標靶
        for target_r2 in TARGET_LIST:
            if test_completed: break 
                
            # 🚨 關鍵突破：將 "vw_safe_mission_queue" 改為 "mission_queue"
            query = sb.table("mission_queue").select("id, r2_url, audio_size_mb, source_name, episode_title") \
                      .eq("r2_url", target_r2).limit(1) 
            
            targets = query.execute().data
            if not targets:
                s_log_func(sb, "SANDBOX", "INFO", f"⏭️ [跳過] 資料表中找不到靶材: {target_r2}，切換下一發。")
                continue 
                
            task = targets[0]
            task_id = task['id']
            r2_url = task['r2_url']
            size = task['audio_size_mb']
            
            s_log_func(sb, "SANDBOX", "INFO", f"🎯 鎖定狙擊靶材: {task.get('source_name')} ({size}MB) - {task_id[:8]}")
            
            try:
                # 🚀 發起實彈射擊
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
                
                s_log_func(sb, "SANDBOX", "SUCCESS", f"✅ Groq 狙擊成功！靶材: {target_r2} | 耗時: {elapsed:.1f}s | 字數: {text_len}")
                test_completed = True 
                
            except Exception as stt_err:
                s_log_func(sb, "SANDBOX", "WARNING", f"⚠️ 靶材 {target_r2} 試射失敗: {str(stt_err)[:100]}... 切換下一發。")
                continue
                
        if not test_completed:
            s_log_func(sb, "SANDBOX", "ERROR", "❌ 所有標靶皆狙擊失敗或無效。")

    except Exception as e:
        s_log_func(sb, "SANDBOX", "ERROR", f"❌ Groq 沙盒系統異常: {str(e)}")
