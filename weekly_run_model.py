from modules.model_calculations import RunModel
from datetime import date
from datetime import timedelta


updated_dt = date.today()
offset = (updated_dt.weekday()-4) % 7
process_dt = updated_dt - timedelta(days=offset)

obj = RunModel(process_dt=process_dt,
               updated_dt=updated_dt,
               model_name='model_config.json',
               export_table='model_1_weekly_results',
               upload_to_db=True,
               upload_to_gcp=True)
obj.run_model()
