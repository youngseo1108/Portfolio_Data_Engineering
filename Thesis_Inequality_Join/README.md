## MSc Thesis Project – Inequality Join Optimisation

A research-driven project focused on improving the efficiency of inequality joins for interval-based data (e.g., time series or temporal ranges). Conducted as part of my Master's thesis.

### Overview
- Implemented and compared multiple join algorithms in Python:
  - **Brute-force baseline**: naive nested loop across all tuples  
  - **IEJoin**: index-based inequality join using sorted arrays and bitmap indexes
  - **RMJ (Range Merge Join)**: join based on scanning ranges for overlapping intervals  
  - **OMJ (Overlap Merge Join)**: symmetric extension of RMJ combining forward and backward scans  
- Built a benchmarking framework to evaluate runtime performance across varying dataset sizes (1k–10k rows) and overlap densities (10–90%+).  
- Analysed execution time, scalability, and trade-offs between filtering strategies.

### Outcomes
- Showed that **algorithm performance depends heavily on overlap density** in interval datasets  
- **IEJoin** achieved the best performance when overlap was low to moderate (e.g., 10–50%)  
- **Brute-force join** surprisingly outperformed other methods under very high overlap (>90%) due to reduced filtering overhead  
- **RMJ/OMJ**, while theoretically efficient, performed worse than expected and in some cases slower than the baseline, revealing gaps for further optimisation
- Overall, findings highlight the **need to adapt join strategy to dataset characteristics**, with implications for temporal and time-series query optimisation

#### Visual Comparison
![output](output.png)

### Folder structure
```sh
Thesis_Ineuqality_Join
├───Inequality_Join_Algo    # Core implementation of each join algorithm variant
├───Evaluation/             # Jupyter notebooks for performance experiments
│   └── src/                # Python scripts for data generation and benchmarking
└───Master_Thesis.pdf       # Full thesis with summary, processes and results
```