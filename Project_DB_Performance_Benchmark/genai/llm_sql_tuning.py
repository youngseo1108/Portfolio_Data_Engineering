import os, json, textwrap, requests
from pathlib import Path

SAMPLE = Path(__file__).with_name('samples/q2_suggestions.json')
API_KEY = os.getenv('OPEN_API_KEY', '').strip()

def prompt(schema, slow_sql):
  '''
  input: schema (string), slow_sql (string)
  output: dict
  '''
  # Offline
  if not API_KEY:
    if SAMPLE.exists():
      return json.loads(SAMPLE.read_text(encoding='utf-8'))
    return {
        'rewritten_sql': slow_sql.replace(
        "BETWEEN '2025-01-15' AND '2025-01-31'",
        ">= '2025-01-15 AND tpep_pickup_datetime < '2025-02-01'"
      ),
      'indexes': [
        "CREATE INDEX IF NOT EXISTS idx_taxi_pu_time ON raw.taxi (pu_location_id, tpep_pickup_datetime);"
      ]
    }

  # Online
  body = {
    'model': 'gpt-5',
    'messages': [{
      'role': 'user',
      'content': (
        'Given the schema and a slow query, return JSON with keys: '
        'rewritten_sql, indexes (array of DDL strings).\n\n'
        f'Schema:\n{schema}\n\nSlowquery:\n{slow_sql}'
      )
    }],
    'temperature': 0.2
  }
  r = requests.post(
    'https://api.openai.com/v1/chat/completions',
    headers={'Authorization': f'Bearer {API_KEY}'},
    json=body,
    timeout=60
  )
  r.raise_for_status()
  text = r.json()['choices'][0]['message']['content']
  try:
    return json.loads(text)
  except Exception:
    return {'rewritten_sql': slow_sql, 'indexes': [f'-- LLM non-JSON reply: {text[:120]}...']}


if __name__ == '__main__':
  schema = Path("ingestion/postgres_init.sql").read_text(encoding="utf-8")
  slow = Path("benchmarks/baseline_queries.sql").read_text(encoding="utf-8").split(";")[1]
  out = prompt(schema, slow)
  print(json.dumps(out, indent=2))