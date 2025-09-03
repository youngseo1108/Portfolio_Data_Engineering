import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

csv_path = Path(__file__).resolve().parent.parent / 'benchmarks' / 'results.csv'
df = pd.read_csv(csv_path)

# reshape: baseline vs tuned per query id (Q1/Q2/…)
df['qid'] = df['query'].apply(lambda x: x.split('_')[1])
df['kind'] = df['query'].apply(lambda x: x.split('_')[0])

# plotting
pivot = df.pivot_table(index='qid', columns='kind', values='time_ms', aggfunc='mean').sort_index()
ax = pivot.plot(kind='bar')
ax.set_ylabel('Execution Time (ms)')
ax.set_title('Baseline vs LLM-tuned Query Performance')
ax.legend()
ax.set_xlabel('Query')
ax.legend()
ax.set_xticklabels(ax.get_xticklabels(), rotation=0)

for p in ax.patches:
  ax.annotate(
    f'{p.get_height():.1f}',               # text
      (p.get_x() + p.get_width() / 2, p.get_height()), # position
      ha='center', va='bottom', fontsize=8, rotation=0
  )
# plt.tight_layout()

out = Path(__file__).with_name('example_plot.png')
plt.savefig(out)
print('saved', out)