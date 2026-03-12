import os
import math
from typing import List
import ollama
from openai import OpenAI
import config

class NaiveRAGEngine:
    def __init__(self):
        self.provider = config.LLM_PROVIDER
        self.model = config.LLM_MODEL
        self.emb_model = config.EMBEDDING_MODEL
        self.chunks = []
        self.embeddings = []
        self._load_data()

    def _load_data(self):
        filepath = "sample_data.txt"
        if not os.path.exists(filepath):
            with open(filepath, "w") as f:
                f.write("The Matrix stars Keanu Reeves.\nTom Hanks acted in Cast Away.")
        
        with open(filepath, "r") as f:
            text = f.read()
        
        self.chunks = [line.strip() for line in text.split("\n") if line.strip()]
        self._create_embeddings()

    def _get_embedding(self, text: str) -> List[float]:
        try:
            if self.provider == "ollama":
                try:
                    response = ollama.embeddings(model=self.emb_model, prompt=text)
                except Exception:
                    print(f"Pulling embedding model {self.emb_model}...")
                    ollama.pull(self.emb_model)
                    response = ollama.embeddings(model=self.emb_model, prompt=text)
                return response['embedding']
            else:
                client = OpenAI()
                response = client.embeddings.create(input=[text], model=self.emb_model)
                return response.data[0].embedding
        except Exception as e:
            print(f"Warning: Embedding failed: {e}")
            return [] # Empty list to avoid math errors

    def _create_embeddings(self):
        print(f"Generating embeddings for {len(self.chunks)} chunks using {self.emb_model}...")
        for chunk in self.chunks:
            emb = self._get_embedding(chunk)
            if emb:
                self.embeddings.append(emb)
            else:
                # Even if one fails, we need a placeholder to keep list indices aligned
                self.embeddings.append([0.0] * 384) 

    def _cosine_similarity(self, v1, v2):
        dot = sum(a * b for a, b in zip(v1, v2))
        mag1 = math.sqrt(sum(a * a for a in v1))
        mag2 = math.sqrt(sum(b * b for b in v2))
        return dot / (mag1 * mag2) if mag1 * mag2 else 0

    def query(self, user_question: str) -> str:
        print(f"\n[DEBUG] Question: {user_question}")
        query_emb = self._get_embedding(user_question)
        
        # Calculate similarities and keep scores for debugging
        raw_sims = []
        for e, c in zip(self.embeddings, self.chunks):
            score = self._cosine_similarity(query_emb, e)
            raw_sims.append((score, c))
        
        # Sort and take top 3
        sims = sorted(raw_sims, key=lambda x: x[0], reverse=True)
        
        print("[DEBUG] Top 3 Retrieved Chunks & Scores:")
        for i, (score, chunk) in enumerate(sims[:3]):
            print(f"  {i+1}. Score: {score:.4f} | Content: {chunk[:100]}...")

        context = "\n".join([s[1] for s in sims[:3]])
        
        prompt = f"""Use the following context to answer the question.
Context:
{context}

Question: {user_question}

Answer strictly based on the context. If the information is not there, say "I don't know".
"""
        print("-" * 50)
        print("[DEBUG] Prompt being sent to LLM:")
        print(prompt)
        print("-" * 50)
        
        if self.provider == "ollama":
            response = ollama.chat(model=self.model, messages=[{'role': 'user', 'content': prompt}])
            return response['message']['content']
        else:
            client = OpenAI()
            response = client.chat.completions.create(model=self.model, messages=[{"role": "user", "content": prompt}])
            return response.choices[0].message.content

if __name__ == "__main__":
    engine = NaiveRAGEngine()
    print(engine.query("Who is Keanu Reeves?"))
