# stock_data_extractions

```
project
│   │   README.md
│   │   daily_extractions.py    : daily extraction
│   │   financial_statement.py  : financial statement extraction - weekly
│   │   weekly_factor_job.py
|   |   weekly_yahoo_conesus.py
|   |   weekly_price_job.py
|   |
│
└───configs
│   │   database_configs.py : database configrations
│   │   job_configs.py      : job configrations
│   │   prox_configs.py     : proxy configrations (onion)
│   │   yahoo_configs.py    : yahoo API configrations
│   │
│   
└───inputs
│   │   yahoo_financial_fundamental.csv : input mapping to read Yahoo
│   │                                       API for financial statement
│   │                                       data
│   │
│      
└───modules
│   │   extract_yahoo_stats.py  : logics to extract yahoo fundamental data
│   │   extract_yahoo_price.py  : logics to price data from Yahoo API
│   │   extract_yahoo_financial.py  : logics to extract Yahoo financial statements
│   │   extract_finviz_data : logics to extract data from Finviz.com
│   │   extract_factors.py  : logics to calcaute factors
│   │   extract_analysis.py
│
└───util
│   │   create_output_sqls.py   : create sql files for GCP uploads
│   │   fundamental_factors.py  : calcaute fundamental factors
│   │   gcp_functions.py        : uplaod to GCP storage
│   │   get_stock_population.py : get stock universe
│   │   helper_functions.py     : supporting functions
│   │   parallel_process.py     : multiprocessing modules
│   │   price_factors.py        : calcualte price factor
│   │
│
└───logs
│   │   this folder will store job logs
│
└───sql_outputs
│   │   this foler will store .sql files
│
│
```