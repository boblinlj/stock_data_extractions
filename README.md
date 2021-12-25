# stock_data_extractions

```
project
│   README.md
│    
│
└───configs
│   │   database_configs.py
│   │   job_configs.py
│   │   prox_configs.py
│   │   yahoo_configs.py
│   
└───inputs
│   │   yahoo_financial_fundamental.csv
│      
└───modules
│   │   extract_yahoo_stats.py
│   │   extract_yahoo_price.py
│   │   extract_yahoo_financial.py
│   │   extract_finviz_data
│   │   extract_factors.py
│   │   
│
└───util
│   │   create_output_sqls.py
│   │   fundamental_factors.py
│   │   gcp_functions.py
│   │   get_stock_population.py
│   │   helper_functions.py
│   │   parallel_process.py
│   │   price_factors.py
│
└───jobs
│   │   daily_extractions.py
│   │   financial_statement.py
│   │  
│
```