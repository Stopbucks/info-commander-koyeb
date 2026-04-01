# ---------------------------------------------------------
# app.py (V5.5 KOYEB_主力極簡淨化與非同步防禦版)
# [工作流程] 2026_0401以前 是每 2 小時執行一次任務，透過「初始隨機延遲 + 排程器 Jitter」錯開起跑線，避免羊群效應。
# [工作流程] 本程式負責排程喚醒與連線保活，其他交由src/pod_scra_intel_trans.py  與 control.py 面板動態統御。

# ---------------------------------------------------------
# app.py (V5.8 KOYEB 零成本刺客專用版)
# 適用：僅限 KOYEB 獨立倉庫
# [工作流程]
# 1. 由外部 Cron-job A (XX:58) 打 API 喚醒 KOYEB 容器。
# 2. 由外部 Cron-job B (XX:00) 敲擊 /ping 路由，觸發 run_integrated_mission。
# 3. 任務執行完畢後，直接呼叫 Koyeb API 將自己強制 Pause (斷電)，達成 $0 元。
# [修正] 拔除 BackgroundScheduler，完全交由外部驅動與自我毀滅。
# [2026_0401] 觀察測試1個月，每5小時醒來1次
# ---------------------------------------------------------
import os, time, gc, random, threading, requests
from datetime import datetime, timezone
from flask import Flask, request
from supabase import create_client

from src.pod_scra_intel_trans import execute_fortress_stages 

app = Flask(__name__)

CONFIG = {
    "WORKER_ID": os.environ.get("WORKER_ID", "UNKNOWN_NODE"),
    "CRON_SECRET": os.environ.get("CRON_SECRET")
}

MISSION_LOCK = threading.Lock()
MISSION_STATE = {"is_running": False, "start_time": 0.0}

def get_sb(): 
    return create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def db_jitter():
    """🛡️ 隨機微延遲避震：防止多台機甲同時寫入造成資料庫 Lock"""
    time.sleep(random.uniform(5.2, 20.0))

def s_log(sb, task_type, status, message, err_stack=None):
    try:
        print(f"[{task_type}][{status}] {message}", flush=True)
        if status in ["SUCCESS", "ERROR"] or "啟動" in message or "V" in message:
            db_jitter() 
            sb.table("mission_logs").insert({
                "worker_id": CONFIG["WORKER_ID"], "task_type": task_type,
                "status": status, "message": message, "traceback": err_stack
            }).execute()
    except: pass

def report_soft_failure(sb, worker_id, error_msg):
    try:
        db_jitter() 
        res = sb.table("pod_scra_tactics").select("active_worker, consecutive_soft_failures, worker_status").eq("id", 1).single().execute()
        if not res.data: return
        tactic = res.data
        
        db_jitter() 
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

# =========================================================
# 🛑 終極武器：API 驅動自我毀滅協議
# =========================================================
def self_destruct_koyeb():
    """任務完成後，延遲 10 秒呼叫官方 API 暫停容器，停止計費"""
    def _shutdown():
        print("⏳ [KOYEB] 任務結束，10 秒後執行自我斷電協議...")
        time.sleep(10) # 讓 Flask 有時間回傳最後的 Log 與 HTTP 狀態
        
        token = os.environ.get("KOYEB_API_TOKEN")
        service_id = os.environ.get("KOYEB_SERVICE_ID")
        
        if token and service_id:
            url = f"https://app.koyeb.com/v1/services/{service_id}/pause"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            try:
                resp = requests.post(url, headers=headers)
                if resp.status_code == 200:
                    print("🛑 [KOYEB] 電源已拔除，機甲進入深度休眠！")
                else:
                    print(f"⚠️ [KOYEB] 自毀指令遭拒絕: HTTP {resp.status_code}")
            except Exception as e:
                print(f"⚠️ [KOYEB] 自毀連線失敗: {e}")
        else:
            print("⚠️ [KOYEB] 找不到 API_TOKEN 或 SERVICE_ID，無法執行自毀。將依賴官方 1 小時閒置休眠。")
            
    threading.Thread(target=_shutdown).start()

# =========================================================
# 🚀 核心執行緒
# =========================================================
def run_integrated_mission():
    global MISSION_STATE
    if not MISSION_LOCK.acquire(blocking=False): return

    sb = get_sb()
    MISSION_STATE["is_running"] = True
    MISSION_STATE["start_time"] = time.time()
    
    try:
        s_log(sb, "SYSTEM", "SUCCESS", f"🚀 [{CONFIG['WORKER_ID']} V5.7] 零成本刺客連線！準備執行單次突擊！")
        
        # 執行核心狀態機 (裡面會自動判斷 Ticks 去做下載/摘要/轉譯)
        execute_fortress_stages(sb, CONFIG, s_log)
        
    except Exception as e:
        report_soft_failure(sb, CONFIG["WORKER_ID"], str(e))
    finally:
        MISSION_STATE["is_running"] = False
        if MISSION_LOCK.locked():
            try: MISSION_LOCK.release()
            except: pass
        del sb; gc.collect()
        
        # 💥 關鍵行動：無論任務成功或失敗，最後一定要呼叫自毀！
        self_destruct_koyeb()

@app.route('/')
def health(): return f"Fortress {CONFIG['WORKER_ID']} V5.7 Assassin Active", 200

@app.route('/ping')
def trigger():
    global MISSION_STATE
    token = request.args.get('token')
    if not token or token != CONFIG['CRON_SECRET']: return "Unauthorized", 401
    
    if MISSION_STATE["is_running"]:
        return "Already running.", 202

    # 收到 Cron-job B 敲門後，在背景啟動任務，立刻回傳 202 讓 Cron-job 結案
    threading.Thread(target=run_integrated_mission, daemon=True).start()
    return "Assassin Mission Triggered", 202

# ⚠️ 注意：已全面拔除 BackgroundScheduler。此機甲不再擁有自我時間觀念。

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000)) 
    app.run(host='0.0.0.0', port=port)
