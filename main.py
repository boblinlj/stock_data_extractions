import pandas as pd
from util.database_management import DatabaseManagement

# df = pd.read_csv('inputs/yahoo_financial_fundamental.csv')
#
# print(df)
#
# DatabaseManagement(data_df=df, table='yahoo_financial_statement_data_control', ).insert_db()

df = DatabaseManagement(sql="""
                            SELECT type, freq, data
                            FROM yahoo_financial_statement_data_control
                        """).read_to_df()

print(df)


