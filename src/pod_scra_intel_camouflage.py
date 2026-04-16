# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_camouflage.py (V5.9.8 大道至簡：全軍 Tier 1 追溯版)
# 職責：提供千面人級別的 HTTP Headers，規避反爬蟲雷達。
# 戰術：全軍每日換裝，固定日期的 Seed 確保 24 小時內特徵一致，且 100% 可逆向追溯。
# [V5.9.8 升級] 戰略收斂：拔除次級迷彩，全軍強制穿著 Tier 1 (Apple/Spotify) 絕對白名單。
# ---------------------------------------------------------
import random
from datetime import datetime, timezone

def get_camouflage_headers(worker_id: str, is_duty_officer: bool = True) -> dict:
    """
    發放裝備邏輯：
    全軍皆使用 Tier 1 (極低風險) 裝備。
    Seed 綁定「機甲代號 + 任務狀態 + 系統日期」，確保一天內出勤 N 次皆穿同一套，降低風險積分。
    """
    
    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    # 🚀 精準溯源種子：區分主將與後勤，但兩者皆會每日換裝
    role_str = "DUTY" if is_duty_officer else "IDLE"
    seed = f"{worker_id}_{role_str}_{today_str}"
    tactical_rng = random.Random(seed)

    # ==========================================
    # 🛠️ 20% 小套件軍械庫 (動態變數池)
    # ==========================================
    # Apple 相關參數
    ios_versions = ["15_4_1", "15_5", "16_0_2", "16_1", "16_3_1", "17_0_1"]
    mac_versions = ["12_3_1", "12_4", "13_1", "13_2_1", "14_0"]
    core_media_ios = ["1.0.0.19E266", "1.0.0.19F77", "1.0.0.20A362", "1.0.0.20B92"]
    core_media_mac = ["1.0.0.21F79", "1.0.0.21G83", "1.0.0.22A400"]
    iphone_models = ["iPhone12,1", "iPhone13,2", "iPhone14,2", "iPhone14,5", "iPhone15,2"]
    
    # Spotify 相關參數
    spotify_ios_ver = ["8.8.12", "8.8.22", "8.8.30", "8.8.44"]
    spotify_and_ver = ["8.8.14", "8.8.26", "8.8.38", "8.8.50"]
    android_api = ["31", "32", "33", "34"]
    android_models = ["SM-G991B", "SM-S901B", "SM-S908B", "Pixel 6", "Pixel 7 Pro"]

    # ==========================================
    # 🟢 Tier 1: 絕對白名單軍械庫 (全軍配發)
    # ==========================================
    VIP_PROFILES = [
        # --- AppleCoreMedia (iOS) ---
        {"User-Agent": f"AppleCoreMedia/{tactical_rng.choice(core_media_ios)} (iPhone; U; CPU OS {tactical_rng.choice(ios_versions)} like Mac OS X; zh_tw)", "Accept": "*/*"},
        {"User-Agent": f"AppleCoreMedia/{tactical_rng.choice(core_media_ios)} (iPhone; U; CPU OS {tactical_rng.choice(ios_versions)} like Mac OS X; en_us)", "Accept": "*/*"},
        
        # --- AppleCoreMedia (macOS) ---
        {"User-Agent": f"AppleCoreMedia/{tactical_rng.choice(core_media_mac)} (Macintosh; U; Intel Mac OS X {tactical_rng.choice(mac_versions)}; en_us)", "Accept": "*/*"},
        {"User-Agent": f"AppleCoreMedia/{tactical_rng.choice(core_media_mac)} (Macintosh; U; Intel Mac OS X {tactical_rng.choice(mac_versions)}; zh_tw)", "Accept": "*/*"},

        # --- Spotify (iOS) ---
        {"User-Agent": f"Spotify/{tactical_rng.choice(spotify_ios_ver)} iOS/{tactical_rng.choice(ios_versions).replace('_', '.')} ({tactical_rng.choice(iphone_models)})", "Accept": "*/*"},

        # --- Spotify (Android) ---
        {"User-Agent": f"Spotify/{tactical_rng.choice(spotify_and_ver)} Android/{tactical_rng.choice(android_api)} ({tactical_rng.choice(android_models)})", "Accept": "*/*"}
    ]

    base_profile = tactical_rng.choice(VIP_PROFILES)
    headers = base_profile.copy()
    
    # 補齊 Podcast 常用的 Audio 請求特徵
    if "Accept" not in headers or headers["Accept"] == "*/*":
        headers["Accept"] = "audio/webm,audio/ogg,audio/wav,audio/*;q=0.9,application/ogg;q=0.7,*/*;q=0.5"
        
    headers["Connection"] = "keep-alive"
    
    # 加入隨機快取控制，模擬真實瀏覽行為 (24 小時內這個值也會因為 seed 固定而保持一致，非常安全)
    if tactical_rng.choice([True, False]):
        headers["Cache-Control"] = "no-cache"

    return headers
