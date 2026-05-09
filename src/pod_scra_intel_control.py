# ---------------------------------------------------------
# src/pod_scra_intel_control.py (V5.9 面板統御_物流感知版)
# 職責：統御機甲權限、配額與全局排除規則。
# [雷達] techcore.fetch_stt_tasks: 於techcore檔案過濾任務，重裝挑大檔，輕裝僅撿壓縮(opt_)小檔。
# [核心] core.py: 戰鬥中心，執行 FFmpeg 壓縮，並呼叫 AI (Gemini/Groq) 進行轉譯與摘要產線。
# [面板] control.py: 統御機甲權限。CAN_COMPRESS=True 才能壓縮；STT_LIMIT 決定單輪轉譯總數。
# [節拍] MAX_TICKS: 控制狀態機(trans.py)循環長度。FLY預設12步(低頻打擊)，主力預設2步(雙產線交替)。
# [變速箱] IDLE_GEARBOX: 隱蔽變速箱。控制非值勤機甲的降速齒輪比。預設 3.0 代表巡邏週期拉長 3 倍 
#            (例如 MAX_TICKS 8 乘以 3 = 24，達成 1 天出門 1 次)。支援浮點數微調 (如 1.5 則約 12 小時出門一次)。

# [輕裝範例] FLY: STT_LIMIT=1, CAN_COMPRESS=False。雷達僅給已壓縮檔，拿 1 筆直接送 AI 轉譯。
# [中型範例] KOYEB: STT_LIMIT=2, CAN_COMPRESS=True。搭fetch_stt_tasks「軟失敗升冪」，選 2 筆
#            健康(失敗=0)的未壓縮檔，執行 FFmpeg 壓縮後轉譯；若遇軟失敗+1並優雅繞道。
# [重裝範例] DBOS: STT_LIMIT=5, CAN_COMPRESS=True。搭fetch_stt_tasks「軟失敗降冪與 null 優先」，
#            專撿取失敗任務或未知大檔，強勢輾壓 5 筆疑難雜症；達 6 次失敗則全軍隔離。
# [檔案館範例] HF: STT_LIMIT=5。擁有雙重身分，進入狀態機前必定先執行專屬的快照與情報歸檔任務；
#              進入狀態機後則化身重裝清道夫，無差別輾壓大檔案與軟失敗任務。
# [兵工廠範例] RENDER: 獨立為 COMPRESS_ONLY=True，專壓大檔。若全庫皆已壓縮(Opus)，自動轉為轉譯兵。
# [scout 預留擴充] 斥候模式開關。若開啟，此機甲專職爬取 RSS 情報，不執行下載與轉譯 (目前 Github 執行，預設休眠)。
#        "MAX_TICKS": 4,               # ⏱️ 主力：4 個檔次 (KOYEB 專屬 3 小時起床 1 次進貨與轉譯/摘要輪替)
#        "IDLE_GEARBOX": 2.0           # ⚙️ 隱蔽變速箱：非值勤時的巡邏降速齒輪_0507:4檔 -> 2檔
# ---------------------------------------------------------
# [scout 預留擴充] 斥候模式開關。若開啟，此機甲專職爬取 RSS 情報，不執行下載與轉譯 (目前 Github 執行，預設休眠)。
#        "MAX_TICKS": 8,               # ⏱️ 主力：8 個檔次 (2 小時起床 1 次進貨與轉譯/摘要輪替)
#        "IDLE_GEARBOX": 4.0           # ⚙️ 隱蔽變速箱：非值勤時的巡邏降速齒輪
# [V5.8 升級] 新增 GLOBAL_DOMAIN_BLACKLIST，集中管理下載伺服器黑名單。
# [V5.9 升級] 新增 DOWNLOAD_LIMIT (總下載量) 與 MAX_SAME_DOMAIN (同網域併發上限)。
# ---------------------------------------------------------
import os
from supabase import create_client

def get_tactical_panel(worker_id):
    """依據機甲代號 (WORKER_ID)，動態發放戰鬥裝備與產能配額"""
    
    # 🚫 全局網域黑名單 
    base_blacklist = [
        "example-malicious.com", 
        "broken-audio-server.net"
    ]
    
    # 🛡️ 1. 預設防線：最低規格 (FLY.io 輕裝模式)
    default_panel = {
        "MEM_TIER": 256,
        "RADAR_FETCH_LIMIT": 50,
        "DOWNLOAD_LIMIT": 1,           # 📥 總下載配額
        "MAX_SAME_DOMAIN": 1,          # 🛡️ 同網域安全併發數
        "STT_LIMIT": 1,
        "SUMMARY_LIMIT": 1,
        "SAFE_DURATION_SECONDS": 600,
        "CAN_COMPRESS": False,
        "COMPRESS_ONLY": False,
        "SCOUT_MODE": False,
        "MAX_TICKS": 24,              
        "IDLE_GEARBOX": 4.0,           
        "GLOBAL_DOMAIN_BLACKLIST": base_blacklist 
    }

    # 🛡️ 2. 中型主力模板  _  KOYEB
    medium_panel = {
        "MEM_TIER": 512,
        "RADAR_FETCH_LIMIT": 100,
        "DOWNLOAD_LIMIT": 3,           # 📥 總下載配額
        "MAX_SAME_DOMAIN": 1,          # 🛡️ 同網域安全併發數。例如總下載2個，每個網域最多1個
        "STT_LIMIT": 3,
        "SUMMARY_LIMIT": 2,
        "SAFE_DURATION_SECONDS": 1500,
        "CAN_COMPRESS": True,
        "COMPRESS_ONLY": False,
        "SCOUT_MODE": False,
        "MAX_TICKS": 4,               
        "IDLE_GEARBOX": 2.0,
        "GLOBAL_DOMAIN_BLACKLIST": base_blacklist 
    }

    # 🚜 3. 重裝巨獸模板
    heavy_panel = {
        "MEM_TIER": 512,
        "RADAR_FETCH_LIMIT": 100,
        "DOWNLOAD_LIMIT": 4,           # 📥 總下載配額 (重裝兵胃口較大)
        "MAX_SAME_DOMAIN": 2,          # 🛡️ 同網域安全併發數
        "STT_LIMIT": 5,
        "SUMMARY_LIMIT": 3,
        "SAFE_DURATION_SECONDS": 1500,
        "CAN_COMPRESS": True,
        "COMPRESS_ONLY": False,
        "SCOUT_MODE": False,
        "MAX_TICKS": 8,               
        "IDLE_GEARBOX": 4.0,
        "GLOBAL_DOMAIN_BLACKLIST": base_blacklist 
    }

    # 📚 4. 檔案館重裝模板
    archive_heavy_panel = {            
        "MEM_TIER": 512,
        "RADAR_FETCH_LIMIT": 100,
        "DOWNLOAD_LIMIT": 3,           # 📥 總下載配額
        "MAX_SAME_DOMAIN": 2,          # 🛡️ 同網域安全併發數
        "STT_LIMIT": 5,
        "SUMMARY_LIMIT": 0,            
        "SAFE_DURATION_SECONDS": 1500,
        "CAN_COMPRESS": True,
        "COMPRESS_ONLY": False,
        "SCOUT_MODE": False,
        "MAX_TICKS": 8,
        "IDLE_GEARBOX": 4.0,
        "GLOBAL_DOMAIN_BLACKLIST": base_blacklist 
    } 

    # 🏭 5. 兵工廠專屬模板
    factory_panel = {
        "MEM_TIER": 512,
        "RADAR_FETCH_LIMIT": 100,
        "DOWNLOAD_LIMIT": 5,           # 📥 總下載配額 (兵工廠專司下載與壓縮)
        "MAX_SAME_DOMAIN": 2,          # 🛡️ 同網域安全併發數
        "STT_LIMIT": 2,                 
        "SUMMARY_LIMIT": 2,            
        "SAFE_DURATION_SECONDS": 1500,
        "CAN_COMPRESS": True,          
        "COMPRESS_ONLY": True,         
        "SCOUT_MODE": False,
        "MAX_TICKS": 4,                 
        "IDLE_GEARBOX": 4.0,
        "GLOBAL_DOMAIN_BLACKLIST": base_blacklist 
    }

    panels = {
        "FLY_LAX": default_panel,
        "KOYEB": medium_panel,
        "ZEABUR": medium_panel,
        "DBOS": heavy_panel,
        "HUGGINGFACE": archive_heavy_panel,
        "RENDER": factory_panel
    }
    
    return panels.get(worker_id, default_panel)  

def get_secrets():
    return {
        "SB_URL": os.environ.get("SUPABASE_URL"), 
        "SB_KEY": os.environ.get("SUPABASE_KEY"),
        "GROQ_KEY": os.environ.get("GROQ_API_KEY"), 
        "GEMINI_KEY": os.environ.get("GEMINI_API_KEY"),
        "TG_TOKEN": os.environ.get("TELEGRAM_BOT_TOKEN"), 
        "TG_CHAT": os.environ.get("TELEGRAM_CHAT_ID"),
        "R2_URL": os.environ.get("R2_PUBLIC_URL")
    }

def get_sb():
    s = get_secrets()
    return create_client(s["SB_URL"], s["SB_KEY"])
