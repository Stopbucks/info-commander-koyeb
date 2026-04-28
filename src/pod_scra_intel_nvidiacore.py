# ---------------------------------------------------------
# src/pod_scra_intel_nvidiacore.py (NVIDIA Plan C 終極接管模組)
# 任務：1. [聽寫] call_nvidia_stt (Whisper-large-v3)
#       2. [摘要] call_nvidia_summary (Llama-3.3-70B 大胃口模式)
# 特色：128K 超大上下文，支援一次性處理 10 萬字逐字稿，無需切塊。
# ---------------------------------------------------------

import os
from curl_cffi import requests
from src.pod_scra_intel_control import get_secrets

class NvidiaAgent:
    def __init__(self):
        # 從中央金庫或環境變數領取 NVIDIA 金鑰
        s = get_secrets()
        self.api_key = os.environ.get("NVIDIA_API_KEY") or s.get("NVIDIA_API_KEY")
        self.base_url = "https://integrate.api.nvidia.com/v1"

    def call_nvidia_stt(self, r2_url_path):
        """🎤 NVIDIA Whisper-large-v3 聽寫 (備援方案)"""
        if not self.api_key: raise Exception("找不到 NVIDIA_API_KEY")
        
        s = get_secrets()
        audio_url = f"{s['R2_URL']}/{r2_url_path}"
        
        # 透過 curl_cffi 下載音檔至記憶體
        resp = requests.get(audio_url, timeout=120)
        resp.raise_for_status()
        
        # 發送到 NVIDIA 進行高精度聽寫
        files = {
            'file': ('audio.opus', resp.content, 'audio/ogg'),
            'model': (None, 'nvidia/whisper-large-v3'),
            'response_format': (None, 'text')
        }
        headers = {"Authorization": f"Bearer {self.api_key}"}
        
        nv_resp = requests.post(f"{self.base_url}/audio/transcriptions", headers=headers, files=files, timeout=300)
        
        if nv_resp.status_code == 200:
            return nv_resp.text
        else:
            raise Exception(f"NVIDIA STT 失敗 ({nv_resp.status_code}): {nv_resp.text}")

    def call_nvidia_summary(self, long_text, sys_prompt):
        """🧠 NVIDIA Llama-3.3-70B 摘要 (大胃口模式)"""
        if not self.api_key: raise Exception("找不到 NVIDIA_API_KEY")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # 將整份 10 萬字文本一次性塞入，交給 70B 模型處理
        payload = {
            "model": "meta/llama-3.3-70b-instruct",
            "messages": [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": f"請針對以下逐字稿進行深度摘要：\n\n{long_text}"}
            ],
            "temperature": 0.3,
            "max_tokens": 4096
        }

        nv_resp = requests.post(f"{self.base_url}/chat/completions", headers=headers, json=payload, timeout=240)
        
        if nv_resp.status_code == 200:
            return nv_resp.json()['choices'][0]['message']['content']
        else:
            raise Exception(f"NVIDIA Summary 失敗 ({nv_resp.status_code}): {nv_resp.text}")
