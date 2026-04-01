# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_camouflage.py (V5.7 KOYEB_基因種子偽裝模組 - 坦誠相對版)
# 職責：提供千面人級別的 HTTP Headers，規避反爬蟲雷達。
# 戰術：[主將] 每日換裝 / [後勤] 永遠固定專屬制服。
# 修正：捨棄不合理的高防護行動裝置偽裝。後勤大方承認開源聚合器身分，主將改用合法 Linux 特徵，完美融入 Podcast 生態圈。
# ---------------------------------------------------------
import random
from datetime import datetime, timezone

def get_camouflage_headers(worker_id: str, is_duty_officer: bool = True) -> dict:
    """
    根據機甲身分發放裝備：
    - 主將 (True): 機甲代號 + 日期 -> 每天換一套。
    - 後勤 (False): 機甲代號 + _IDLE -> 永遠穿同一套專屬制服。
    """
    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    # 🚀 固定種子：讓每台後勤機甲擁有不同、但各自固定的專屬制服
    seed = f"{worker_id}_{today_str}" if is_duty_officer else f"{worker_id}_IDLE"
    tactical_rng = random.Random(seed)

    # ==========================================
    # 🛡️ [後勤兵專屬] 極簡匿蹤套裝 (AntennaPod 變體)
    # ==========================================
    if not is_duty_officer:
        # 5 個版本號 x 4 種語言 = 20 種獨一無二的固定制服 (足夠 10 台以上機甲分配)
        versions = ["3.1.0", "3.1.1", "3.2.0", "3.3.0", "3.3.1"]
        langs = ["en-US,en;q=0.9", "en-GB,en;q=0.8", "zh-TW,zh;q=0.9", "es-US,es;q=0.9"]
        
        return {
            "User-Agent": f"AntennaPod/{tactical_rng.choice(versions)}",
            "Accept": "audio/webm,audio/ogg,audio/wav,audio/*;q=0.9,*/*;q=0.5",
            "Accept-Language": tactical_rng.choice(langs),
            "Connection": "keep-alive"
        }

    # ==========================================
    # ⚔️ [值勤官專屬] 坦誠相對的多變指紋 (物理相容)
    # ==========================================
    LANGUAGES = [
        "en-US,en;q=0.9",                                      
        "en-US,en;q=0.9,es-US;q=0.8,es;q=0.7",                 
        "en-US,en;q=0.9,zh-TW;q=0.8,zh-CN;q=0.7",              
        "en-GB,en-US;q=0.9,en;q=0.8",                          
    ]
    
    # 80% 無 Referer，背景排程下載器本來就不會有 Referer
    REFERERS = [None] * 16 + [
        "https://www.google.com/",                             
        "https://podcasts.apple.com/",                         
        "https://t.co/",                                       
        "https://www.bing.com/"                                
    ]

    ACCEPT_AUDIO = "audio/webm,audio/ogg,audio/wav,audio/*;q=0.9,application/ogg;q=0.7,video/*;q=0.6,*/*;q=0.5"

    PROFILES = [
        # 🎭 套裝 0: 標準 Linux 伺服器瀏覽器 (坦蕩蕩的資料中心爬蟲)
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Linux"'
        },
        # 🎭 套裝 1: 老式 Android 10 背景多媒體下載器 (Dalvik)
        {
            "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 10; SM-G960F Build/QP1A.190711.020)"
        },
        # 🎭 套裝 2: 開源 Podcast 播放器 (大方承認身分)
        {
            "User-Agent": "AntennaPod/3.2.0"
        },
        # 🎭 套裝 3: Android 11 上的舊版 ExoPlayer (音訊底層套件)
        {
            "User-Agent": "ExoPlayerDemo/2.15.1 (Linux; Android 11) ExoPlayerLib/2.15.1"
        }
    ]

    base_profile = tactical_rng.choice(PROFILES)
    headers = base_profile.copy()
    
    if "Accept" not in headers:
        headers["Accept"] = ACCEPT_AUDIO
        
    if "Mozilla" in headers["User-Agent"]:
        headers["Accept-Language"] = tactical_rng.choice(LANGUAGES)
        
    headers["Connection"] = "keep-alive"
    
    chosen_referer = tactical_rng.choice(REFERERS)
    if chosen_referer and "Mozilla" in headers["User-Agent"]:
        headers["Referer"] = chosen_referer
        
    if tactical_rng.choice([True, False]):
        headers["Cache-Control"] = "max-age=0"

    return headers
