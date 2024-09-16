from airflow.api.common.trigger_dag import trigger_dag
from datetime import datetime
import pytz

# Получаем текущее время в UTC с временной зоной
current_time = datetime.now(pytz.UTC)


# Запуск DAG с локализованной датой выполнения
trigger_dag(dag_id='kafka_dag_asinc_producer', run_id=None, execution_date=current_time)
