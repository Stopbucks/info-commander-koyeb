# ---------------------------------------------------------
# src/pod_scra_intel_r2.py (V5.6.1 兵工廠與倉儲模組 - curl_cffi 升級版)
# 適用：RENDER, KOYEB, ZEABUR, HF, DBOS, FLY | 規格：全軍通用
# [任務] 1. R2 儲存端對端連線 2. 檔案上下傳 3. FFmpeg 強化轉檔
# [機制] 實裝 600 秒硬性超時，防止損壞音檔導致機甲永久卡死。
# [升級] 1. 拔除無效參數 (-preset) 並加入 -nostdin 防止背景 I/O 死結。
# [升級] 2. 加入 -loglevel error 防止進度條洪流塞爆機甲記憶體 (OOM)。
# [升級] 3. 為 Boto3 S3 客戶端加入嚴格的連線 Timeout 與重試規則。
# [升級] 4. 智慧清洗 R2_PUBLIC_URL 尾部斜線，防止 404 找不到檔案。
# [V5.6.1 更新] 全面替換底層連線為 curl_cffi，統一全軍 HTTP 引擎。
# ---------------------------------------------------------

import os, gc, subprocess, boto3
from curl_cffi import requests # 🚀 換裝：統一使用 curl_cffi
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
