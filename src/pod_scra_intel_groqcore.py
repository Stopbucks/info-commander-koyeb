# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_groqcore.py (Groq B計畫防爆模組 V2.1)
# 任務：處理超長文本的滑動窗口切塊、重疊銜接、防爆休眠與摘要生成
# 修正：1. 同步使用 Supabase 中央金庫的 GROQ_KEY。
#       2. [V2.1] 拔除虛假勝利邏輯，遇錯立即拋出異常請求上級換裝。
# ---------------------------------------------------------

import os
import time
from groq import Groq
from src.pod_scra_intel_control import get_secrets

class GroqFallbackAgent:
    """
    🛡️ [B計畫特種兵] 專門處理 Groq 的長文本切塊與 API 呼叫
    """
    def __init__(self):
        # 🚀 從中央金庫領取 Groq 金鑰
        s = get_secrets()
        api_key = s.get("GROQ_KEY") 
        
        if not api_key:
            print("⚠️ [Groq 備援] 找不到 GROQ_KEY，備援系統處於休眠狀態。")
        self.client = Groq(api_key=api_key) if api_key else None
        
        # 設定模型與切塊參數，確保不超過 TPM 限制
        self.model_name = "llama-3.3-70b-versatile"
        self.chunk_size = 15000  
        self.overlap_size = 800  

    def _chunk_text_with_overlap(self, text: str):
        """✂️ 執行滑動窗口切塊，保留前後文重疊區間"""
        chunks = []
        start = 0
        text_length = len(text)
        
        if self.chunk_size <= self.overlap_size:
            raise ValueError("chunk_size 必須大於 overlap_size")

        while start < text_length:
            end = start + self.chunk_size
            chunks.append(text[start:end])
            start += (self.chunk_size - self.overlap_size) 
            
        return chunks

    def generate_summary(self, long_text: str, original_prompt: str):
        """🧠 分塊處理長文本，並組合最終摘要"""
        if not self.client:
            return "❌ [Groq 備援] 系統未初始化，無法執行 B 計畫。"

        chunks = self._chunk_text_with_overlap(long_text)
        total_chunks = len(chunks)
        final_summary = ""

        print(f"🛡️ [Groq 備援] 文本總長 {len(long_text)} 字元，已切分為 {total_chunks} 塊進行交火...")

        for idx, chunk_text in enumerate(chunks):
            print(f"🧩 正在呼叫 Groq 處理第 {idx + 1}/{total_chunks} 塊...")
            
            # 針對首塊與接續區塊給予不同的邏輯指引
            if idx == 0:
                system_instruction = (
                    f"以下是一份長篇音訊轉譯稿的「第一部分」。\n"
                    f"請根據以下核心規則進行摘要：\n{original_prompt}"
                )
            else:
                system_instruction = (
                    f"以下是同一份轉譯稿的「接續部分」。\n"
                    f"注意：為了保持上下文連貫，這段文字的最開頭可能與您剛才處理的結尾有『少部分重疊』。\n"
                    f"請自行判斷並忽略重複的資訊，然後緊接著您先前的邏輯繼續往下寫摘要。\n"
                    f"請持續遵守以下核心規則：\n{original_prompt}"
                )

            messages = [
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": chunk_text}
            ]

            try:
                response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=messages,
                    temperature=0.3,
                    max_tokens=2048
                )
                chunk_result = response.choices[0].message.content
                final_summary += chunk_result + "\n\n"
                print(f"✅ 第 {idx + 1} 塊處理成功。")
                
            except Exception as e:
                # 🚀 拋出異常，阻斷虛假勝利，交由 core.py 接手升級 C 方案
                print(f"❌ [Groq 戰損] 第 {idx + 1} 塊處理失敗，請求上級執行升級備援...")
                raise e

            # 若不是最後一塊，強制休眠清洗 Token 桶以防 429
            if idx < total_chunks - 1:
                print("⏳ [冷卻防禦] 進入 65 秒戰術休眠，規避 TPM 12000 限制...")
                time.sleep(65)

        return final_summary.strip()
