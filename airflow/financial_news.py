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
        task_id = 'camus',
        bash_command='run_camus.sh',
        dag = dag)

#Run Spark to sum all historical trades and write to Cassandra
task2_trades_batch = BashOperator()

#Run Spark to pull latest relevant news from HDFS to Cassandra
task3_news_batch = BashOperator()

#Empty Cassandra's stock_count_rts1 table to set initial counts to the result of the batch calculation
task4_initialize_db_rts1 = BashOperator()

#Update Cassandra's stock_count_web table to include counts from the batch run with all the trades summed from stock_count_rts2, which were the trades that came in since task1_camus started running
task5_update_db_web_rts2 = BashOperator()

#set dependencies of first cycle

#Start next batch run while stock_count_rts1 tracks trades occuring since the completion of task4
task6_camus = BashOperator(
        task_id = 'camus',
        bash_command='run_camus.sh',
        dag = dag)

#Run Spark to sum all historical trades and write to Cassandra
task7_trades_batch = BashOperator()

#Run Spark to pull latest relevant news from HDFS to Cassandra
task8_news_batch = BashOperator()

#Empty Cassandra's stock_count_rts2 table to set initial counts to the result of the batch calculation
task9_initialize_db_rts2 = BashOperator()

#Update Cassandra's stock_count_web table to include counts from the batch run with all the trades summed from stock_count_rts1, which were the trades that came in since task6_camus started running
task10_update_db_web_rts1 = BashOperator()

#set dependencies of second cycle

#cycle tasks 1 to 10 continuously

