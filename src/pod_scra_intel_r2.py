# ---------------------------------------------------------
# src/pod_scra_intel_r2.py (V5.6 KOYEB_兵工廠與倉儲模組 - 鈦合金防爆版)
# 適用：RENDER, KOYEB, ZEABUR, HF, DBOS, FLY | 規格：全軍通用
# [任務] 1. R2 儲存端對端連線 2. 檔案上下傳 3. FFmpeg 強化轉檔
# [機制] 實裝 600 秒硬性超時，防止損壞音檔導致機甲永久卡死。
# [升級] 1. 拔除無效參數 (-preset) 並加入 -nostdin 防止背景 I/O 死結。
# [升級] 2. 加入 -loglevel error 防止進度條洪流塞爆機甲記憶體 (OOM)。
# [升級] 3. 為 Boto3 S3 客戶端加入嚴格的連線 Timeout 與重試規則。
# [升級] 4. 智慧清洗 R2_PUBLIC_URL 尾部斜線，防止 404 找不到檔案。
# ---------------------------------------------------------

import os, requests, gc, subprocess, boto3
from botocore.config import Config
import imageio_ffmpeg   

def get_s3_client():
    """【基礎建設】建立並回傳 R2/S3 連線物件 (具備嚴格超時防護)"""
    # 🚀 防禦升級：限制連線時間(15s)與讀寫時間(60s)，最多重試 3 次
    boto_config = Config(connect_timeout=15, read_timeout=60, retries={'max_attempts': 3})
    return boto3.client('s3', endpoint_url=os.environ.get("R2_ENDPOINT_URL"),
                        aws_access_key_id=os.environ.get("R2_ACCESS_KEY_ID"),
                        aws_secret_access_key=os.environ.get("R2_SECRET_ACCESS_KEY"), 
                        region_name="auto", config=boto_config)

def upload_to_r2(local_path, filename):
    """【倉儲物流】將本機物資上傳至 R2"""
    s3 = get_s3_client()
    s3.upload_file(local_path, os.environ.get("R2_BUCKET_NAME"), filename)

def download_from_r2(filename, local_path):
    """【倉儲物流】提領 R2 物資至本機暫存區"""
    # 🚀 防禦升級：自動消除網址尾部多餘的斜線，避免產生 https://...//filename 導致 404
    base_url = str(os.environ.get('R2_PUBLIC_URL', '')).rstrip('/')
    url = f"{base_url}/{filename}"
    
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)

def compress_task_to_opus(task_id, raw_filename):
    """【兵工廠改裝】泛用轉檔步驟，具備超時、死結與記憶體防護"""
    ext = os.path.splitext(raw_filename)[1].lower() or '.mp3'
    tmp_in = f"/tmp/in_{task_id[:8]}{ext}"
    tmp_out = f"/tmp/out_{task_id[:8]}.opus"
    new_name = f"opt_{task_id[:8]}.opus"
    
    try:
        # 1. 提領物資
        download_from_r2(raw_filename, tmp_in)
        
        # 2. 獲取 FFmpeg 並配置指令
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
        
        # 🚀 絕對防爆指令編排：
        # -nostdin: 防止背景 I/O 死結
        # -loglevel error: 封印進度條輸出，保護 RAM 不被 stdout 塞爆
        cmd = [
            ffmpeg_exe, '-y', '-nostdin', '-loglevel', 'error', 
            '-i', tmp_in, '-ar', '16000', '-ac', '1', 
            '-c:a', 'libopus', '-b:a', '16k', '-vbr', 'off', 
            '-compression_level', '0', tmp_out
        ]
        
        # 🚀 強化防禦：加入 timeout=600 (10分鐘)，即便音檔損壞導致 FFmpeg 卡住也能強制斬斷
        subprocess.run(cmd, check=True, capture_output=True, timeout=600)
        
        # 3. 回傳新物資
        upload_to_r2(tmp_out, new_name)
        return True, new_name
        
    except subprocess.TimeoutExpired:
        print(f"❌ [改裝超時]: FFmpeg 轉檔耗時過長 (大於10分鐘)，疑似損壞檔案，強制終止任務。")
        return False, None
    except subprocess.CalledProcessError as e:
        # 🚀 強化除錯：加入了 -loglevel error，這裡的 stderr 只會包含真正的致命錯誤
        err_msg = e.stderr.decode('utf-8', errors='ignore') if e.stderr else str(e)
        print(f"❌ [FFmpeg 崩潰]: {err_msg[:200]}")
        return False, None
    except Exception as e:
        print(f"❌ [改裝失敗]: 系統異常 -> {e}")
        return False, None
    finally:
        # 🧹【內存守衛】強制清理暫存檔與回收記憶體
        for f in [tmp_in, tmp_out]:
            if os.path.exists(f): 
                try: os.remove(f)
                except: pass
        gc.collect()
