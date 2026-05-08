# ---------------------------------------------------------
# src/pod_scra_intel_nvidiacore.py (V 6.1 NVIDIA Plan C 終極接管模組)
# 任務：1. [聽寫] call_nvidia_stt (Whisper-large-v3)
#        2. [摘要] call_nvidia_summary (Llama 模型降級輪詢)

# 特色：128K 超大上下文，支援一次性處理 10 萬字逐字稿，無需切塊。
# [V6.10 升級] 實裝 Llama 3.3-70B -> Llama 3.1-8B 降級輪詢防護。
# ---------------------------------------------------------

import os
from curl_cffi import requests
from src.pod_scra_intel_control import get_secrets

class NvidiaAgent:
    def __init__(self):
        s = get_secrets()
        self.api_key = os.environ.get("NVIDIA_API_KEY") or s.get("NVIDIA_API_KEY")
        self.base_url = "https://integrate.api.nvidia.com/v1"
        
        # 🚀 [新增] NVIDIA 摘要模型降級梯隊
        self.summary_models = [
            "meta/llama-3.3-70b-instruct",
            "meta/llama-3.1-8b-instruct"
        ]

    def call_nvidia_stt(self, r2_url_path):
        """🎤 NVIDIA Whisper-large-v3 聽寫 (備援方案)"""
        if not self.api_key: raise Exception("找不到 NVIDIA_API_KEY")
        
        s = get_secrets()
        audio_url = f"{s['R2_URL']}/{r2_url_path}"
        
        resp = requests.get(audio_url, timeout=120)
        resp.raise_for_status()
        
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
        """🧠 NVIDIA 摘要 (降級輪詢大胃口模式)"""
        if not self.api_key: raise Exception("找不到 NVIDIA_API_KEY")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        last_error = ""
        
        # 🚀 執行 NVIDIA 模型輪詢
        for model_name in self.summary_models:
            print(f"🎯 [NVIDIA 輪詢] 嘗試呼叫模型: {model_name}...")
            
            payload = {
                "model": model_name,
                "messages": [
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": f"請針對以下逐字稿進行深度摘要：\n\n{long_text}"}
                ],
                "temperature": 0.3,
                "max_tokens": 4096
            }

            try:
                nv_resp = requests.post(f"{self.base_url}/chat/completions", headers=headers, json=payload, timeout=240)
                
                if nv_resp.status_code == 200:
                    print(f"✅ [{model_name}] NVIDIA 摘要生成成功！")
                    return nv_resp.json()['choices'][0]['message']['content']
                else:
                    err_msg = f"HTTP {nv_resp.status_code}: {nv_resp.text[:100]}"
                    print(f"⚠️ [NVIDIA 戰損] 模型 {model_name} 失敗: {err_msg}")
                    last_error = err_msg
                    continue # 失敗則嘗試下一個 8B 模型
            except Exception as e:
                print(f"⚠️ [NVIDIA 戰損] 網路或超時異常: {str(e)[:50]}")
                last_error = str(e)
                continue

        # 如果輪詢結束都失敗
        raise Exception(f"NVIDIA Summary 所有梯隊皆陣亡。最後錯誤: {last_error}")
