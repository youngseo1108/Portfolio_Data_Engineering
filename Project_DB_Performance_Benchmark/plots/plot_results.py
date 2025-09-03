import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

csv_path = Path(__file__).with_name('../benchmarks/results.csv')
df = pd.read_csv(csv_path)

# reshape: baseline vs tuned per query id (Q1/Q2/…)
df['qid'] = df['query'].str.extract(r'Q(\d+)').astype(int)
df['kind'] = df['query'].str.extract(r'^(baseline|tuned)')

pivot = df.pivot_table(index="qid", columns="kind", values="time_ms", aggfunc="mean").sort_index()
ax = pivot.plot(kind="bar", figsize=(8,5))
ax.set_ylabel("Execution Time (ms)")
ax.set_xlabel("Query")
ax.set_title("Baseline vs LLM-tuned Query Performance")
plt.tight_layout()

out = Path(__file__).with_name('example_plot.png')
plt.savefig(out)
print('saved', out)