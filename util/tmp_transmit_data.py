from configs import database_configs_nas as dbcfg
import pandas as pd
from sqlalchemy import create_engine

database_ip = dbcfg.MYSQL_HOST
database_user = dbcfg.MYSQL_USER
database_pw = dbcfg.MYSQL_PASSWORD
database_port = dbcfg.MYSQL_PORT
database_nm = dbcfg.MYSQL_DATABASE

cnn1 = create_engine(
    f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
    pool_size=20,
    max_overflow=0)

cnn2 = create_engine(
    f'mysql+mysqlconnector://root:Zuodan199064!@34.70.76.153:3306/financial_PROD',
    pool_size=20,
    max_overflow=0)

pop = """
         SELECT distinct ticker FROM model_1_factors;
        """

sql = """
        SELECT * FROM model_1_factors WHERE ticker = '{}'
    """
print('step1')
stock_lst = pd.read_sql(con=cnn1, sql=pop)['ticker'].to_list()
print('step2')
for stock in stock_lst[:]:
    print(f'processing {stock}')
    df_to_cpy = pd.read_sql(con=cnn1, sql=sql.format(stock))
    df_to_cpy.to_sql('model_1_factors', con=cnn2, if_exists='append', index=False, method='multi', chunksize=200)


