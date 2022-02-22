from modules.model_calculations import RunModel
from modules.extract_factors import FactorJob
from datetime import date
from datetime import timedelta

loggerFileName = f"weekly_model_1_factor_{date.today().strftime('%Y%m%d')}.log"
updated_dt = date.today()

offset = (updated_dt.weekday() - 4) % 7
process_dt = updated_dt - timedelta(days=offset)


print(f"{'*'*30}Start Factor Job{'*'*30}")
FactorJob(start_dt=date(2010, 1, 1),
          updated_dt=updated_dt,
          targeted_table='model_1_factors',
          targeted_pop='AARON',
          batch_run=True,
          loggerFileName=None,
          use_tqdm=False).run()

print(f"{'*'*30}Start Model{'*'*30}")
RunModel(process_dt=process_dt,
         updated_dt=updated_dt,
         model_name='model_config.json',
         export_table='model_1_weekly_results',
         upload_to_db=True,
         upload_to_gcp=True).run_model()