from modules.extract_factors import FactorJob
from datetime import date

loggerFileName = f"weekly_model_1_factor_{date.today().strftime('%Y%m%d')}.log"

obj = FactorJob(start_dt=date(2010, 1, 1),
                updated_dt=date(2022, 2, 5),
                targeted_table='model_1_factors',
                targeted_pop='AARON',
                batch_run=True,
                loggerFileName=None,
                use_tqdm=False)
obj.run()

