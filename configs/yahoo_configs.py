YAHOO_STATS_COLUMNS = ['sharesOutstanding',
                       'enterpriseToRevenue',
                       'enterpriseToEbitda',
                       'forwardPE',
                       'trailingPE',
                       'priceToBook',
                       'enterpriseValue',
                       'priceToSalesTrailing12Months',
                       'pegRatio',
                       'marketCap',
                       'shortName',
                       'longName',
                       'totalRevenue',
                       'revenueGrowth',
                       'revenueQuarterlyGrowth',
                       'ebitda',
                       'totalAssets',
                       'totalCash',
                       'totalDebt',
                       'operatingCashflow',
                       'freeCashflow',
                       'revenuePerShare',
                       'bookValue',
                       'forwardEps',
                       'trailingEps',
                       'netIncomeToCommon',
                       'profitMargins',
                       'ebitdaMargins',
                       'grossMargins',
                       'operatingMargins',
                       'grossProfits',
                       'currentRatio',
                       'returnOnAssets',
                       'totalCashPerShare',
                       'quickRatio',
                       'payoutRatio',
                       'debtToEquity',
                       'returnOnEquity',
                       'beta',
                       'beta3Year',
                       'floatShares',
                       'sharesShort',
                       '52WeekChange',
                       'sharesPercentSharesOut',
                       'heldPercentInsiders',
                       'heldPercentInstitutions',
                       'shortRatio',
                       'sharesShortPreviousMonthDate',
                       'shortPercentOfFloat',
                       'sharesShortPriorMonth',
                       'lastFiscalYearEnd',
                       'nextFiscalYearEnd',
                       'mostRecentQuarter',
                       'fiveYearAverageReturn',
                       'twoHundredDayAverage',
                       'volume24Hr',
                       'averageDailyVolume10Day',
                       'fiftyDayAverage',
                       'averageVolume10days',
                       'SandP52WeekChange',
                       'dateShortInterest',
                       'regularMarketVolume',
                       'averageVolume',
                       'averageDailyVolume3Month',
                       'volume',
                       'fiftyTwoWeekHigh',
                       'fiveYearAvgDividendYield',
                       'fiftyTwoWeekLow',
                       'currentPrice',
                       'previousClose',
                       'regularMarketOpen',
                       'regularMarketPreviousClose',
                       'open',
                       'dayLow',
                       'dayHigh',
                       'regularMarketDayHigh',
                       'postMarketChange',
                       'postMarketPrice',
                       'exchangeName',
                       'preMarketChange',
                       'regularMarketPrice',
                       'preMarketChangePercent',
                       'postMarketSource',
                       'postMarketChangePercent',
                       'preMarketSource',
                       'regularMarketChangePercent',
                       'preMarketPrice',
                       'targetLowPrice',
                       'targetMeanPrice',
                       'targetHighPrice'
                       ]

YAHOO_STATS_DROP_COLUMNS = [
                        'regularMarketDayLow',
                        'morningStarRiskRating',
                        'fundInceptionDate',
                        'annualReportExpenseRatio',
                        'fundFamily',
                        'lastDividendValue',
                        'yield',
                        'priceHint',
                        'threeYearAverageReturn',
                        'lastSplitDate',
                        'lastSplitFactor',
                        'legalType',
                        'lastDividendDate',
                        'morningStarOverallRating',
                        'earningsQuarterlyGrowth',
                        'ytdReturn',
                        'maxAge',
                        'lastCapGain',
                        'impliedSharesOutstanding',
                        'category',
                        'recommendationKey',
                        'targetMedianPrice',
                        'earningsGrowth',
                        'numberOfAnalystOpinions',
                        'financialCurrency',
                        'recommendationMean',
                        'trailingAnnualDividendYield',
                        'navPrice',
                        'trailingAnnualDividendRate',
                        'toCurrency',
                        'expireDate',
                        'algorithm',
                        'dividendRate',
                        'exDividendDate',
                        'circulatingSupply',
                        'startDate',
                        'currency',
                        'lastMarket',
                        'maxSupply',
                        'openInterest',
                        'volumeAllCurrencies',
                        'strikePrice',
                        'ask',
                        'askSize',
                        'fromCurrency',
                        'bid',
                        'tradeable',
                        'bidSize',
                        'quoteSourceName',
                        'exchange',
                        'regularMarketTime',
                        'regularMarketChange',
                        'currencySymbol',
                        'postMarketTime',
                        'preMarketTime',
                        'exchangeDataDelayedBy',
                        'regularMarketSource',
                        'marketState',
                        'underlyingSymbol',
                        'quoteType',
                        'exchangeName',
                        'preMarketSource',
                        'postMarketSource',
                        'annualHoldingsTurnover',
                        'longName'
                        ]

YAHOO_CONSENSUS_COLUMNS = ['ticker',
                           'currentPrice',
                           'targetLowPrice',
                           'targetMeanPrice',
                           'targetMedianPrice',
                           'targetHighPrice',
                           'recommendationMean',
                           'recommendationKey',
                           'numberOfAnalystOpinions',
                           'updated_dt'
                           ]


PRICE_TABLE_COLUMNS = ['ticker',
                        'fiveYearAverageReturn',
                        'twoHundredDayAverage',
                        'volume24Hr',
                        'averageDailyVolume10Day',
                        'fiftyDayAverage',
                        'averageVolume10days',
                        'regularMarketVolume',
                        'averageVolume',
                        'averageDailyVolume3Month',
                        'volume',
                        'fiftyTwoWeekHigh',
                        'fiveYearAvgDividendYield',
                        'fiftyTwoWeekLow',
                        'currentPrice',
                        'previousClose',
                        'regularMarketOpen',
                        'regularMarketPreviousClose',
                        'open',
                        'dayLow',
                        'dayHigh',
                        'regularMarketDayHigh',
                        'postMarketChange',
                        'postMarketPrice',
                        'preMarketChange',
                        'regularMarketPrice',
                        'preMarketChangePercent',
                        'postMarketChangePercent',
                        'regularMarketChangePercent',
                        'preMarketPric',
                        'updated_dt']