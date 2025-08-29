import os, json, textwrap, requests
from pathlib import Path

SAMPLE = Path(__file__).with_name('samples/q2_suggestions.json')
API_KEY = os.getenv('OPEN_API_KEY', '').strip()
PROMPT = lambda schema, slow: f"""
Given the schema and a slow query, try:
1) rewriting SQL
2) suggested indexes
Return JSON with keys: rewritten_sql, indexes.
Schema:\n{schema}\n\nSlow query:\n{slow}
"""

def call_llm(schema, slow):
  # replace with your preferred endpoint if needed
  r = requests.post(
     "https://api.openai.com/v1/chat/completions",
     headers={"Authorization": f"Bearer {API_KEY}"},
     json={
        "model":"gpt-5",
        "messages":[{"role": "user", "content": PROMPT(schema,slow)}],
        "temperature":0.2
      }
  )
  r.raise_for_status()
  text = r.json()["choices"][0]["message"]["content"]
  try:
    return json.loads(text)
  except:
    return {"rewritten_sql": slow, "indexes": [text]}

if __name__ == "__main__":
  schema = open('ingestion/postgres_init.sql').read_text(encoding='utf-8')
  slow = open('benchmarks/baseline_queries.sql').read_text(encoding='utf-8')
  out = call_llm(schema, slow)
  print(json.dumps(out, indent=2))