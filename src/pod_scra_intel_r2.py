# ---------------------------------------------------------
# src/pod_scra_intel_r2.py (V5.9.2 兵工廠與倉儲模組 - 拆除 __enter__ 炸彈版)
# 適用：RENDER, KOYEB, ZEABUR, HF, DBOS, FLY | 規格：全軍通用
# [任務] 1. R2 儲存端對端連線 2. 檔案上下傳 3. FFmpeg 強化轉檔
# [機制] 實裝 600 秒硬性超時，防止損壞音檔導致機甲永久卡死。
# [升級] 1. 拔除無效參數 (-preset) 並加入 -nostdin 防止背景 I/O 死結。
# [升級] 2. 加入 -loglevel error 防止進度條洪流塞爆機甲記憶體 (OOM)。
# [升級] 3. 為 Boto3 S3 客戶端加入嚴格的連線 Timeout 與重試規則。
# [V5.6.1 更新] 全面替換底層連線為 curl_cffi，統一全軍 HTTP 引擎。
# [V5.9.1 補齊] 補回缺失的 compress_task_to_opus 核心壓縮邏輯，解救 KOYEB。
# [V5.9.2 拆彈] 徹底移除 requests.get 的 with 語法，防止 curl_cffi 引發 __enter__ 崩潰。
# ---------------------------------------------------------

import os, gc, subprocess, boto3
from curl_cffi import requests # 🚀 換裝：統一使用 curl_cffi
from botocore.config import Config
import imageio_ffmpeg   

def get_s3_client():
    """【基礎建設】建立並回傳 R2/S3 連線物件 (具備嚴格超時防護)"""
    boto_config = Config(connect_timeout=15, read_timeout=60, retries={'max_attempts': 3})
    return boto3.client('s3', endpoint_url=os.environ.get("R2_ENDPOINT_URL"),
                        aws_access_key_id=os.environ.get("R2_ACCESS_KEY_ID"),
                        aws_secret_access_key=os.environ.get("R2_SECRET_ACCESS_KEY"), 
                        region_name="auto", config=boto_config)

def upload_to_r2(local_path, filename):
    """【倉儲物流】將本機物資上傳至 R2"""
    s3 = get_s3_client()
    s3.upload_file(local_path, os.environ.get("R2_BUCKET_NAME"), filename)

def compress_task_to_opus(task_id, original_r2_url):
    """
    【兵工廠】從 R2 下載原檔，使用 FFmpeg 極限壓縮為 Opus，再回傳 R2。
    使用 32k bitrate, 16k 採樣率，單聲道，追求極致的檔案縮小，專為 STT 打造。
    """
    tmp_dl = f"/tmp/dl_{task_id}.tmp"
    tmp_op = f"/tmp/opt_{task_id[:8]}.opus"
    
    pub_url = os.environ.get("R2_PUBLIC_URL", "").rstrip('/')
    s3 = get_s3_client()
    bucket = os.environ.get("R2_BUCKET_NAME")

    try:
        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        if not ffmpeg_path or not os.path.exists(ffmpeg_path):
            print(f"⚠️ [R2_COMPRESS] 找不到 imageio_ffmpeg，嘗試使用系統內建 ffmpeg...")
            ffmpeg_path = "ffmpeg"
            
        file_url = f"{pub_url}/{original_r2_url}"
        print(f"📥 [R2_COMPRESS] 開始下載物資: {file_url}")
        
        # 🚀 拆除 __enter__ 炸彈：不使用 with，直接賦值並使用 try...finally 關閉
        r = requests.get(file_url, stream=True, timeout=120)
        try:
            r.raise_for_status()
            with open(tmp_dl, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk: f.write(chunk)
        finally:
            r.close()
                    
        print(f"⚙️ [R2_COMPRESS] 啟動壓縮產線...")
        cmd = [
            ffmpeg_path, "-y", "-i", tmp_dl,
            "-c:a", "libopus", "-b:a", "32k", "-vbr", "on", 
            "-compression_level", "10", "-ac", "1", "-ar", "16000",
            "-loglevel", "error", "-nostdin", tmp_op
        ]
        
        subprocess.run(cmd, check=True, timeout=600)
        
        new_r2_name = os.path.basename(tmp_op)
        print(f"📤 [R2_COMPRESS] 壓縮完畢，上傳成品: {new_r2_name}")
        s3.upload_file(tmp_op, bucket, new_r2_name)
        
        return True, new_r2_name

    except requests.exceptions.HTTPError as he:
        print(f"❌ [R2_COMPRESS] 下載原檔 HTTP 失敗: {he}")
        return False, original_r2_url
    except subprocess.TimeoutExpired:
        print(f"❌ [R2_COMPRESS] 壓縮超時 (600s)，強制中斷任務！")
        return False, original_r2_url
    except subprocess.CalledProcessError as e:
        print(f"❌ [R2_COMPRESS] FFmpeg 壓縮失敗，可能檔案已損壞: {e}")
        return False, original_r2_url
    except Exception as e:
        print(f"❌ [R2_COMPRESS] 壓縮流程發生未預期錯誤: {e}")
        return False, original_r2_url
    finally:
        if os.path.exists(tmp_dl): os.remove(tmp_dl)
        if os.path.exists(tmp_op): os.remove(tmp_op)
        gc.collect()
