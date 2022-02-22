import numpy as np
import pandas as pd
from configs import job_configs as jcfg
from util.database_management import DatabaseManagement
from util.helper_functions import create_log
from util.create_output_sqls import write_insert_db
from util.gcp_functions import upload_to_bucket
import json
import os

pd.set_option('display.max_columns', None)


class ModelCalculationError(Exception):
    pass


class CalculateRank:
    def __init__(self, factor_df, model_json, loggerFileName=None):
        self.final_model_output = None
        self.factor_ranking_pct = None
        self.loggerFileName = loggerFileName
        self.logger = create_log(loggerName='CalculateRank', loggerFileName=self.loggerFileName)
        try:
            self.factor_df = factor_df
        except Exception as e:
            raise ModelCalculationError(f"Factor dataframe is empty")
        self.model_json = model_json

    def calculate_rank_pct(self):
        factors = self.model_json.get('required_risk_drivers')
        self.factor_ranking_pct = self.factor_df[factors].rank(pct=True)
        self.factor_ranking_pct.fillna(0.5, inplace=True)
        self.factor_ranking_pct['gics_code'] = self.factor_df[['gics_code']]
        self.factor_ranking_pct['gics_code'].fillna('0000', inplace=True)

    def calculate_strategy_score(self):
        for strategy in self.model_json.get('strategies'):

            weight = strategy.get('weight')
            name = strategy.get('name')
            factors = strategy.get('factors')

            if weight is not None and name is not None and factors is not None:
                weight_arr = np.array(weight).reshape((len(weight), 1))
                self.factor_ranking_pct[name] = self.factor_ranking_pct[factors].dot(weight_arr)
                self.factor_ranking_pct[name + '_rank'] = self.factor_ranking_pct[name].rank(pct=True)
            else:
                raise ModelCalculationError(f"Factor file is not complete. It is missing missing, name, or factors")

    def combine_strategy(self):
        bucket_threshold = []
        strategy_names = []
        all_weighted_st = []

        for strategy in self.model_json.get('strategies'):
            bucket_threshold.append(strategy.get('bucket_threshold'))
            strategy_names.append(strategy.get('name'))
            st_name = strategy.get('name')
            st_weight = strategy.get('strategy_weights')
            st_filter = strategy.get("strategy_filter")

            st_rank = st_name + '_rank'
            st_weighted = st_name + '_weighted'
            all_weighted_st.append(st_weighted)

            if len(st_filter) > 0:
                self.factor_ranking_pct[st_weighted] = self.factor_ranking_pct.loc[lambda df: eval(strategy.get("strategy_filter"))][st_rank] * st_weight
            else:
                self.factor_ranking_pct[st_weighted] = self.factor_ranking_pct[st_rank] * st_weight

        self.factor_ranking_pct['ms_average'] = self.factor_ranking_pct[all_weighted_st].sum(axis=1)
        bs = np.array(bucket_threshold)
        self.factor_ranking_pct['bucket_count'] = (np.sign(self.factor_ranking_pct[all_weighted_st] - bs) + 1).sum(axis=1) / 2
        self.factor_ranking_pct['final_score'] = self.factor_ranking_pct['bucket_count'] + self.factor_ranking_pct['ms_average']
        self.factor_ranking_pct['ms_pct'] = self.factor_ranking_pct['final_score'].rank(pct=True)

        self.final_model_output = self.factor_ranking_pct[strategy_names +
                                                          all_weighted_st +
                                                          ['final_score', 'ms_pct', 'bucket_count']]
        return self.final_model_output

    def run(self):
        self.calculate_rank_pct()
        self.calculate_strategy_score()

        return self.combine_strategy()


class RunModel:
    def __init__(self,
                 process_dt,
                 updated_dt,
                 model_name,
                 export_table,
                 loggerFileName=None,
                 upload_to_db=True,
                 upload_to_gcp=True):

        self.strategies = None
        self.factor_data = None
        self.factors = None
        self.model = None
        self.process_dt = process_dt
        self.updated_dt = updated_dt
        self.model_name = model_name
        self.loggerFileName = loggerFileName
        self.targeted_table = export_table
        self.logger = create_log(loggerName='RunModel', loggerFileName=self.loggerFileName)
        self.upload_to_db = upload_to_db
        self.upload_to_gcp = upload_to_gcp

    def read_model_json(self):
        try:
            self.model = json.load(open(os.path.join(jcfg.JOB_ROOT, "configs", self.model_name)))
        except Exception as e:
            raise ModelCalculationError(f"Model config JSON {self.model_name} does not exist.")

        self.factors = ",".join([str(elem) for elem in self.model['required_risk_drivers']])
        self.strategies = self.model['strategies']

    def get_factor_data(self):
        factor_sql = f"""
                        select a.ticker, asOfDate, b.gics_code,{self.factors}
                        from  model_1_factors a
                        left join stock_list_for_cooble_stone b
                            on a.ticker = b.yahoo_ticker and b.active_ind='A'
                        where asOfDate = '{self.process_dt}'
                        order by asOfDate, a.ticker
                    """
        try:
            self.factor_data = DatabaseManagement(table='model_1_factors', sql=factor_sql).read_to_df()
            self.factor_data.set_index(['asOfDate', 'ticker'], inplace=True)
        except Exception as e:
            raise ModelCalculationError(f"Unable to extract factor information, sql = {factor_sql}")

    def _write_sql_output(self):
        self.logger.info(f"{'-'*10}Start generate SQL outputs{'-'*10}")
        insert = write_insert_db(self.targeted_table, self.updated_dt)
        insert.run_insert()

    def _upload_to_gcp(self):
        self.logger.info(f"{'-'*10}Upload SQL outputs to GCP{'-'*10}")
        file = f'insert_{self.targeted_table}_{self.updated_dt}.sql'
        if upload_to_bucket(file, os.path.join(jcfg.JOB_ROOT, "sql_outputs", file), jcfg.GCP_BUSKET_NAME):
            self.logger.info("GCP upload successful for file = {}".format(file))
        else:
            self.logger.debug("Failed: GCP upload failed for file = {}".format(file))

    def run_model(self):
        self.logger.info(f"The model Code is Executed - {self.model_name}")
        self.logger.info(f"Extraction date is {self.process_dt}")

        try:
            self.read_model_json()
        except ModelCalculationError as e:
            self.logger.debug(e)
            self.logger.debug('Program will exist')
            exit()

        self.logger.info(f"The model has {len(self.model['required_risk_drivers'])} factors")
        self.logger.info(f"The model has {len(self.strategies)} strategies")

        try:
            self.get_factor_data()
            self.logger.info(f"The factor data has {self.factor_data.shape[0]} stocks")
        except ModelCalculationError as e:
            self.logger.debug(e)
            self.logger.debug('Program will exist')
            exit()

        model_out = CalculateRank(self.factor_data, self.model).run()
        model_out['updated_dt'] = self.updated_dt

        DatabaseManagement(data_df=model_out, table=self.targeted_table, insert_index=True).insert_db()

        if self.upload_to_db:
            self._write_sql_output()

        if self.upload_to_gcp:
            self._upload_to_gcp()


if __name__ == '__main__':
    obj = RunModel(process_dt='2022-02-11',
                   updated_dt='2022-02-21',
                   model_name='model_config.json',
                   export_table='model_1_weekly_results',
                   upload_to_db=True,
                   upload_to_gcp=True)
    obj.run_model()







