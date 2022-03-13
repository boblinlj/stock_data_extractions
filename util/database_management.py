from configs import database_configs as dbcfg
from sqlalchemy import create_engine
import pandas as pd


class DatabaseManagementError(Exception):
    pass


class DatabaseManagement:
    database_ip = dbcfg.MYSQL_HOST
    database_user = dbcfg.MYSQL_USER
    database_pw = dbcfg.MYSQL_PASSWORD
    database_port = dbcfg.MYSQL_PORT
    database_nm = dbcfg.MYSQL_DATABASE
    try:
        cnn = create_engine(
            f'mysql+mysqlconnector://{database_user}:{database_pw}@{database_ip}:{database_port}/{database_nm}',
            pool_size=20,
            max_overflow=0)
    except Exception as e:
        raise DatabaseManagementError(f'database cannot be created, {e}')

    def __init__(self, data_df=None, table=None, key=None, where=None, date=None, sql=None, insert_index=False):
        self.data_df = data_df
        self.table = table
        self.key = key
        self.where = where
        self.date = date
        self.sql = sql
        self.insert_index = insert_index

    def insert_db(self):
        try:
            if self.data_df is None or self.data_df.empty:
                raise DatabaseManagementError(f"dataframe is empty, therefore cannot be inserted")
            elif self.table is None:
                DatabaseManagementError(f"table to be inserted is empty, therefore cannot be inserted")
            else:
                self.data_df.to_sql(name=self.table,
                                    con=self.cnn,
                                    if_exists='append',
                                    index=self.insert_index,
                                    method='multi',
                                    chunksize=200)
        except Exception as e:
            raise DatabaseManagementError(f"data insert to {self.table} failed as {e}")

    def read_to_df(self):
        if self.sql is not None:
            try:
                df = pd.read_sql(con=self.cnn, sql=self.sql)
                return df
            except Exception as e:
                raise DatabaseManagementError(f"data extractions from database failed for sql={self.sql} as {e}")
        else:
            raise DatabaseManagementError(f'data extraction from database failed due to sql is none')

    def _construct_sql(self):
        return f"""
                    SELECT DISTINCT {self.key}
                    FROM {self.table}
                    WHERE {self.where}
                """

    def check_population(self):
        if all(var is None for var in [self.key, self.table, self.where]):
            raise DatabaseManagementError(
                f'cannot check population, due to critical variable missing (key, table, where)')
        else:
            return pd.read_sql(con=self.cnn, sql=self._construct_sql())[self.key].to_list()

    def get_record(self):
        if all(var is None for var in [self.key, self.table, self.where]):
            raise DatabaseManagementError(
                f'cannot run sql, due to critical variable missing (key, table, where)')
        else:
            return pd.read_sql(con=self.cnn, sql=self._construct_sql())

    def run_sql(self):
        if self.sql is None:
            raise DatabaseManagementError(
                f'cannot run sql, due to critical variable missing (sql)')
        else:
            return pd.read_sql(con=self.cnn, sql=self.sql)


if __name__ == '__main__':
    obj = DatabaseManagement(table='price', key='max(timestamp)', where="ticker = 'AAC'")
    print(obj.check_population())
