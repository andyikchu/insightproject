from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ubuntu',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'test', default_args=default_args, schedule_interval=timedelta(1))

#Run Camus to pull messages from Kafka into HDFS
task1_camus = BashOperator(
        task_id = '01_camus',
        bash_command='run_camus.sh',
        dag = dag)

#Run Spark to sum all historical trades and write to Cassandra
task2_trades_batch = BashOperator(
        task_id = '02_trades_batch',
        bash_command='run_trades_batch.sh',
        dag = dag)

#Run Spark to pull latest relevant news from HDFS to Cassandra
task3_news_batch = BashOperator(
        task_id = '03_news_batch',
        bash_command='run_news_batch.sh',
        dag = dag)

#Empty Cassandra's stock_count_rts1 table to set initial counts to the result of the batch calculation
task4_initialize_db_rts1 = BashOperator(
        task_id = '04_rts1',
        bash_command='truncate_rts1.sh',
        dag = dag)

#Update Cassandra's stock_count_web table to include counts from the batch run with all the trades summed from stock_count_rts2, which were the trades that came in since task1_camus started running
task5_update_db_web_rts2 = BashOperator(
        task_id = '05_batch_rts2',
        bash_command='sum_batch_rts 2',
        dag = dag)

#set dependencies of first cycle
task2_trades_batch.set_upstream(task1_camus)
task3_news_batch.set_upstream(task1_camus)
task4_initialize_db_rts1.set_upstream(task2_trades_batch)
task5_update_db_web_rts2.set_upstream(task2_trades_batch)

#Start next batch run while stock_count_rts1 tracks trades occuring since the completion of task4
task6_camus = BashOperator(
        task_id = '06_camus',
        bash_command='run_camus.sh',
        dag = dag)

#Run Spark to sum all historical trades and write to Cassandra
task7_trades_batch = BashOperator(
        task_id = '07_trades_batch',
        bash_command='run_trades_batch.sh',
        dag = dag))

#Run Spark to pull latest relevant news from HDFS to Cassandra
task8_news_batch = BashOperator(
        task_id = '08_news_batch',
        bash_command='run_news_batch.sh',
        dag = dag)

#Empty Cassandra's stock_count_rts2 table to set initial counts to the result of the batch calculation
task9_initialize_db_rts2 = BashOperator(
        task_id = '09_rts2',
        bash_command='truncate_rts2.sh',
        dag = dag))

#Update Cassandra's stock_count_web table to include counts from the batch run with all the trades summed from stock_count_rts1, which were the trades that came in since task6_camus started running
task10_update_db_web_rts1 = BashOperator(
        task_id = '10_batch_rts1',
        bash_command='sum_batch_rts 1',
        dag = dag)

#set dependencies of second cycle
task6_camus.set_upstream.set_upstream(task2_trades_batch)
task7_trades_batch.set_upstream(task6_camus)
task8_news_batch.set_upstream(task6_camus)
task9_initialize_db_rts2.set_upstream(task7_trades_batch)
task10_update_db_web_rts1.set_upstream(task7_trades_batch)

