import time, csv, psycopg2, os
from pathlib import Path
import re

DB = dict(
  host='postgres', port=os.getenv('POSTGRES_PORT', '5432'),
  user=os.getenv('POSTGRES_USER', 'postgres'),
  password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
  dbname=os.getenv('POSTGRES_DB', 'perfdb')
)
COMMENT_BLOCK = re.compile(r'/\*.*?\*/', re.S)
COMMENT_LINE = re.compile(r'--.*?$', re.M)

def strip_sql_comments(sql):
  sql = COMMENT_BLOCK.sub('', sql)
  sql = COMMENT_LINE.sub('', sql)
  return sql


def split_statements(sql):
  stmts = [ s.strip() for s in sql.split(';') ]
  return [ s for s in stmts if s ]


def run_sql(cur, sql_text):
  t0 = time.perf_counter()
  try:
    cur.execute(sql_text)
    try: cur.fetchall()
    except: pass
    return (time.perf_counter() - t0) * 1000.0
  except Exception as e:
    print("\n--- ERROR executing SQL ---")
    print(sql_text)
    print("---------------------------")
    raise


def run_file(cur, path, label):
  text = Path(path).read_text(encoding='utf-8')
  text = strip_sql_comments(text)
  queries = split_statements(text)
  res = []
  for i, q in enumerate(queries):
    ms = run_sql(cur, q)
    res.append((Path(path).name, f'{label}_Q{i+1}', ms))
  return res


def main():
  out = Path(__file__).with_name('results.csv')
  conn = psycopg2.connect(**DB)
  cur = conn.cursor()
  rows = []
  rows += run_file(cur, Path(__file__).with_name('baseline_queries.sql'), 'baseline')
  rows += run_file(cur, Path(__file__).with_name('tuned_queries.sql'), 'tuned')
  conn.close()

  write_header = not out.exists()
  with open(out, 'a', newline='') as f:
    w = csv.writer(f)
    if write_header:
      w.writerow(['file', 'query', 'time_ms'])
    w.writerows(rows)
  print('wrote', out)

if __name__ == '__main__':
  main()