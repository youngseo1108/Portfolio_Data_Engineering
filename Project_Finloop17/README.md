# Project Work-Flow 17: An Automated GMM Valuation Engine for Long-Tail Liabilities (CAS Workers' Comp)

```powershell
finloop-17/
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_cas_workers_comp.sql    # Unpivots triangles to long-form [cite: 28]
│   │   │   └── stg_fx_rates.sql            # Ingests EUR/CHF/USD rates [cite: 40]
│   │   └── marts/
│   │       ├── fact_ifrs17_gmm_valuation.sql # Main GMM & RA logic [cite: 21]
│   │       └── dim_policy_cohorts.sql      # Groups data by Accident Year
│   ├── macros/
│   │   └── calculate_risk_adjustment.sql   # Actuarial logic
│   └── tests/
│       └── assert_positive_cash_flows.sql  # DQ check
└── src/
    └── ingestion/
        └── load_cas_to_snowflake.py
```


https://www.casact.org/publications-research/research/research-resources/loss-reserving-data-pulled-naic-schedule-p


