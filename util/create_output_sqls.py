import os
from sqlalchemy import create_engine
from configs import database_configs as dbcfg
from configs import job_configs as jcfg
import pandas as pd
import datetime


class write_insert_db:
    database_ip = dbcfg.MYSQL_HOST
    database_user = dbcfg.MYSQL_USER
    database_pw = dbcfg.MYSQL_PASSWORD
    database_port = dbcfg.MYSQL_PORT
    database_nm = dbcfg.MYSQL_DATABASE

    cnn = create_engine(f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
                        pool_size=20,
                        max_overflow=0)

    def __init__(self, table, updated_dt):
        self.updated_dt = updated_dt
        self.table = table

    def read_table_to_insert(self):
        if self.table != 'yahoo_analysis':
            sql = """
                    select * from {} where updated_dt = '{}'
                    """.format(self.table, self.updated_dt)
        else:
            sql = """
                    select * from {} where last_check_dt='{}' and to_dt='9999-12-31'
                """.format(self.table, self.updated_dt)

        df = pd.read_sql(sql=sql, con=self.cnn)
        # print(df.head())

        return df.to_dict(orient='records')

    def read_table_to_update(self):
        sql = """
                select * from {} where last_check_dt='{}' and to_dt= '{}'
        """.format(self.table, self.updated_dt, self.updated_dt - datetime.timedelta(days=1))

        df = pd.read_sql(sql=sql, con=self.cnn)

        return df.to_dict(orient='records')

    def write_insert_sql_file(self, data: list):
        with open(os.path.join(jcfg.JOB_ROOT, "sql_outputs", "insert_{}_{}.sql".format(self.table, self.updated_dt)),
                  'w') as file:
            for item in data[:]:
                keys = ', '.join(f'`{x}`' for x in item.keys())
                values = ', '.join(f'"{x}"' for x in item.values()).replace('"nan"', 'NULL').replace('"None"', 'NULL')
                sql_query = 'INSERT INTO `%s` (%s) VALUES(%s);\n' % (self.table, keys, values)
                file.write(sql_query)

    def write_update_sql_file(self, data: list):
        with open(os.path.join(jcfg.JOB_ROOT, "sql_outputs", "update_{}_{}.sql".format(self.table, self.updated_dt)),
                  'w') as file:
            for item in data[:]:
                sql_query = "UPDATE `{}` SET `last_check_dt`='{}', `to_dt`='{}' WHERE `ticker`='{}' ;\n".format(
                    self.table, item.get('last_check_dt'), item.get('to_dt'), item.get('ticker'))
                file.write(sql_query)

    def run_insert(self):
        data = self.read_table_to_insert()
        if len(data) > 0:
            self.write_insert_sql_file(data)

    def run_update(self):
        data = self.read_table_to_update()
        if len(data) > 0:
            self.write_update_sql_file(data)


if __name__ == '__main__':
    insert = write_insert_db('yahoo_quarterly_fundamental_test', '2021-09-11')
    insert.run_INSERT()
    #
    # insert = write_insert_db('finviz_tickers', '2021-08-13')
    # insert.run()
    #
    # insert = write_insert_db('yahoo_fundamental', '2021-08-13')
    # insert.run()


