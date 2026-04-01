# ---------------------------------------------------------
# app.py (V5.5 KOYEB_主力極簡淨化與非同步防禦版)
# 適用：RENDER, KOYEB, ZEABUR, FLY, HF, DBOS | 規格：全軍通用
# [工作流程] 每 2 小時執行一次任務，透過「初始隨機延遲 + 排程器 Jitter」錯開起跑線，避免羊群效應。
# [工作流程] 本程式負責排程喚醒與連線保活，其他交由src/pod_scra_intel_trans.py  與 control.py 面板動態統御。
# [修正] 1. 徹底拔除轉譯白名單與冗餘參數，對接 V5.5 狀態機。
# [新增] 2. 植入 db_jitter 微延遲避震器，並套用至日誌與錯誤回報，避免併發競合寫入 (Race Condition)。
# ---------------------------------------------------------
import os, time, gc, random, threading, traceback
from datetime import datetime, timezone, timedelta
from flask import Flask, request
from supabase import create_client
from apscheduler.schedulers.background import BackgroundScheduler

from src.pod_scra_intel_trans import execute_fortress_stages 

app = Flask(__name__)

CONFIG = {
    "WORKER_ID": os.environ.get("WORKER_ID", "UNKNOWN_NODE"),
    "INTERVAL_HOURS": 2, 
    "CRON_SECRET": os.environ.get("CRON_SECRET")
}

MISSION_LOCK = threading.Lock()
MISSION_STATE = {"is_running": False, "start_time": 0.0}
WATCHDOG_TIMEOUT = 3600  

def get_sb(): 
    return create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def db_jitter():
    """🛡️ 隨機微延遲避震：防止多台機甲同時寫入造成資料庫 Lock 或競合"""
    time.sleep(random.uniform(0.2, 1.0))

def s_log(sb, task_type, status, message, err_stack=None):
    try:
        print(f"[{task_type}][{status}] {message}", flush=True)
        if status in ["SUCCESS", "ERROR"] or "啟動" in message or "V5.5" in message:
            db_jitter()  # 👈 寫入前閃避
            sb.table("mission_logs").insert({
                "worker_id": CONFIG["WORKER_ID"], "task_type": task_type,
                "status": status, "message": message, "traceback": err_stack
            }).execute()
    except: pass

def report_soft_failure(sb, worker_id, error_msg):
    try:
        db_jitter()  # 👈 讀取前閃避
        res = sb.table("pod_scra_tactics").select("active_worker, consecutive_soft_failures, worker_status").eq("id", 1).single().execute()
        if not res.data: return
        tactic = res.data
        
        db_jitter()  # 👈 寫入前閃避
        if worker_id == tactic.get("active_worker"):
            sb.table("pod_scra_tactics").update({
                "consecutive_soft_failures": tactic.get("consecutive_soft_failures", 0) + 1,
                "last_error_type": f"🚨 [主將] {worker_id} 崩潰: {error_msg}"[:200]
            }).eq("id", 1).execute()
        else:
            w_status = tactic.get("worker_status", {})
            w_status[f"{worker_id}_last_err"] = str(error_msg)[:100]
            sb.table("pod_scra_tactics").update({
                "worker_status": w_status,
                "last_error_type": f"⚠️ [後勤] {worker_id} 局部異常: {error_msg}"[:200]
            }).eq("id", 1).execute()
    except: pass

def run_integrated_mission():
    global MISSION_STATE
    if not MISSION_LOCK.acquire(blocking=False): return

    sb = get_sb()
    MISSION_STATE["is_running"] = True
    MISSION_STATE["start_time"] = time.time()
    
    try:
        s_log(sb, "SYSTEM", "SUCCESS", f"🚀 [{CONFIG['WORKER_ID']} V5.5] 大部隊連線，低耦合容錯防禦就位！")
        # 🚀 致命錯誤已排除：對接 V5.5 狀態機，精準傳入 3 個參數
        execute_fortress_stages(sb, CONFIG, s_log)
        
    except Exception as e:
        report_soft_failure(sb, CONFIG["WORKER_ID"], str(e))
    finally:
        MISSION_STATE["is_running"] = False
        if MISSION_LOCK.locked():
            try: MISSION_LOCK.release()
            except: pass
        del sb; gc.collect()

@app.route('/')
def health(): return f"Fortress {CONFIG['WORKER_ID']} V5.5 Active", 200

@app.route('/ping')
def trigger():
    global MISSION_STATE
    token = request.args.get('token')
    if not token or token != CONFIG['CRON_SECRET']: return "Unauthorized", 401
    
    if MISSION_STATE["is_running"]:
        elapsed = time.time() - MISSION_STATE["start_time"]
        if elapsed > WATCHDOG_TIMEOUT:
            sb = get_sb()
            report_soft_failure(sb, CONFIG["WORKER_ID"], "Watchdog_Timeout_Deadlock")
            MISSION_STATE["is_running"] = False
            if MISSION_LOCK.locked():
                try: MISSION_LOCK.release()
                except: pass
        else:
            return f"Already running ({elapsed:.0f}s elapsed).", 202

    threading.Thread(target=run_integrated_mission, daemon=True).start()
    return "Mission Triggered (Async)", 202

scheduler = BackgroundScheduler()
# 🚀 【起跑線錯開】：每次重開機隨機等待 2 到 15 分鐘後才執行第一輪
startup_delay = random.randint(2, 15)
run_next = datetime.now() + timedelta(minutes=startup_delay)

# 🚀 【週期性防踩踏】：每次 2 小時排程觸發時，隨機提早或延遲 0~15 分鐘 (900秒)
scheduler.add_job(
    func=run_integrated_mission, 
    trigger="interval", 
    hours=CONFIG["INTERVAL_HOURS"], 
    jitter=900, 
    next_run_time=run_next
)
scheduler.start()

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000)) 
    app.run(host='0.0.0.0', port=port)
