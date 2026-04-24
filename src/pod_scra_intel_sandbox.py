# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_sandbox.py (V5.8.7 金鑰修復版)
# 任務：Groq API 金絲雀影子測試 (Canary Release)
# 修正：匯入 get_secrets() 解決 'R2_URL' KeyError 錯誤。
# ---------------------------------------------------------
import os, time
from src.pod_scra_intel_techcore import call_groq_stt
from src.pod_scra_intel_control import get_secrets # 🚨 新增：匯入金鑰庫

def run_groq_sandbox_test(sb, s_log_func):
    """【沙盒演習】多重靶材輪詢，裸查底層資料表"""
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    try:
        s_log_func(sb, "SANDBOX", "INFO", f"🧪 [{worker_id}] 啟動 Groq 多重定點狙擊測試 (底層裸查模式)...")
        
        # 🚨 領取金鑰
        s = get_secrets()
        
        # 🎯 標靶清單：依序排列優先權
        TARGET_LIST = [
            "opt_95b032f9.opus", 
            "opt_91fc4d08.opus"
        ]
        
        test_completed = False
        
        # 🔄 開始輪詢標靶
        for target_r2 in TARGET_LIST:
            if test_completed: break 
                
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
                # 🚀 發起實彈射擊 (傳入正確的金鑰字典 s)
                start_time = time.time()
                stt_text = call_groq_stt(s, r2_url) # 🚨 修正：將 os.environ 改為 s
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
