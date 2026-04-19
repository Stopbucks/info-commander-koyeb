
# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_camouflage.py (V6.2 成套迷彩_動態底層對齊版)
# 職責：提供千面人級別的 HTTP Headers 與對應的 TLS 底層指紋。
# 戰術：全軍每日換裝，固定日期的 Seed 確保 24 小時內特徵一致，且 100% 可逆向追溯。
# [V5.9.8 升級] 戰略收斂：拔除次級迷彩，全軍強制穿著 Tier 1 (Apple/Spotify) 絕對白名單。
# [V6.2 升級] 戰略突破：恢復多樣性 (包含 Android/Spotify)，並將 User-Agent 與
#             curl_cffi 的 impersonate 參數「成套綁定」，達成「多樣性與真實性」的完美平衡！
# ---------------------------------------------------------
import random
from datetime import datetime, timezone

def get_tactical_camouflage(worker_id: str, is_duty_officer: bool = True) -> dict:
    """
    發放裝備邏輯：回傳包含 headers 與 impersonate 的成套裝備。
    """
    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    role_str = "DUTY" if is_duty_officer else "IDLE"
    seed = f"{worker_id}_{role_str}_{today_str}"
    tactical_rng = random.Random(seed)

    # --- 裝備元件庫 ---
    ios_versions = ["15_4_1", "15_5", "16_0_2", "16_1", "16_3_1", "17_0_1"]
    mac_versions = ["12_3_1", "12_4", "13_1", "13_2_1", "14_0"]
    core_media_ios = ["1.0.0.19E266", "1.0.0.19F77", "1.0.0.20A362", "1.0.0.20B92"]
    core_media_mac = ["1.0.0.21F79", "1.0.0.21G83", "1.0.0.22A400"]
    iphone_models = ["iPhone12,1", "iPhone13,2", "iPhone14,2", "iPhone14,5", "iPhone15,2"]
    
    spotify_ios_ver = ["8.8.12", "8.8.22", "8.8.30", "8.8.44"]
    spotify_and_ver = ["8.8.14", "8.8.26", "8.8.38", "8.8.50"]
    android_api = ["31", "32", "33", "34"]
    android_models = ["SM-G991B", "SM-S901B", "SM-S908B", "Pixel 6", "Pixel 7 Pro"]

    # ==========================================
    # 🟢 成套軍械庫 (Header + 底層指紋 完美對齊)
    # ==========================================
    ARMORY = [
        # 1. AppleCoreMedia (iOS) -> 骨架: safari_ios
        {
            "ua": f"AppleCoreMedia/{tactical_rng.choice(core_media_ios)} (iPhone; U; CPU OS {tactical_rng.choice(ios_versions)} like Mac OS X; zh_tw)",
            "impersonate": "safari_ios"
        },
        # 2. AppleCoreMedia (macOS) -> 骨架: safari15_3
        {
            "ua": f"AppleCoreMedia/{tactical_rng.choice(core_media_mac)} (Macintosh; U; Intel Mac OS X {tactical_rng.choice(mac_versions)}; en_us)",
            "impersonate": "safari15_3"
        },
        # 3. Spotify (iOS) -> 骨架: safari_ios
        {
            "ua": f"Spotify/{tactical_rng.choice(spotify_ios_ver)} iOS/{tactical_rng.choice(ios_versions).replace('_', '.')} ({tactical_rng.choice(iphone_models)})",
            "impersonate": "safari_ios"
        },
        # 4. Spotify (Android) -> 骨架: chrome110 (Android 流量特徵接近 Chrome)
        {
            "ua": f"Spotify/{tactical_rng.choice(spotify_and_ver)} Android/{tactical_rng.choice(android_api)} ({tactical_rng.choice(android_models)})",
            "impersonate": "chrome110"
        }
    ]

    selected_gear = tactical_rng.choice(ARMORY)

    headers = {
        "User-Agent": selected_gear["ua"],
        "Accept": "audio/webm,audio/ogg,audio/wav,audio/*;q=0.9,application/ogg;q=0.7,*/*;q=0.5",
        "Connection": "keep-alive"
    }
    
    if tactical_rng.choice([True, False]):
        headers["Cache-Control"] = "no-cache"

    # 🚀 回傳字典：包含 Header 與對應的 TLS 指紋
    return {
        "headers": headers,
        "impersonate": selected_gear["impersonate"]
    }
