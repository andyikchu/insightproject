from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ubuntu',
    'depends_on_past': False,
    'start_date': datetime(2015, 9, 26),
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
        bash_command='bash tasks/run_camus.sh',
        depends_on_past=1,
        dag = dag)

#Run Spark to pull latest relevant news from HDFS to Cassandra
task2_news_batch = BashOperator(
        task_id = '02_news_batch',
        bash_command='bash tasks/run_news_batch.sh',
        depends_on_past=1,
        dag = dag)

task2_news_batch.set_upstream(task1_camus)

#Run Spark to sum all historical trades and write to Cassandra
task3_trades_batch = BashOperator(
        task_id = '03_trades_batch',
        bash_command='bash tasks/run_trades_batch.sh',
        depends_on_past=1,
        dag = dag)

#set trades batch after news batch to give it more memory
task3_trades_batch.set_upstream(task2_news_batch)

#stop streaming of trades while the database is getting updated
task4_stop_trade_stream = BashOperator(
        task_id = '04_stop_trade_stream',
        bash_command='bash tasks/stop_trade_stream.sh',
        depends_on_past=1,
        dag = dag)

task4_stop_trade_stream.set_upstream(task3_trades_batch)

#Update Cassandra's stream 2 table to include counts from the batch run with all the trades summed from stock_count_rts1, which were the trades that came in since task1_camus started running
task5_sum_batch_rts2 = BashOperator(
        task_id = '05_batch_rts2',
        bash_command='bash tasks/sum_batch_rts rts2',
        depends_on_past=1,
        dag = dag)

task5_sum_batch_rts2.set_upstream(task4_stop_trade_stream)

#Empty Cassandra's stock_count_rts1 table to set initial counts to the result of the batch calculation
task6_initialize_db_rts1 = BashOperator(
        task_id = '06_initialize_rts1',
        bash_command='bash tasks/truncate_rts.sh rts1',
        depends_on_past=1,
        dag = dag)

task6_initialize_db_rts1.set_upstream(task4_stop_trade_stream)

#Get the web interface to start reading from the newly batch updated rts2
task7_swap_web_db_rts2 =  BashOperator(
        task_id = '07_web_rts2',
        bash_command='bash tasks/switchwebdb.sh rts2',
        depends_on_past=1,
        dag = dag))

task7_swap_web_db_rts2.set_upstream(task5_sum_batch_rts2)

#start the trades stream back up
task8_start_trade_stream = BashOperator(
        task_id = '08_start_trade_stream',
        bash_command='bash tasks/start_trade_stream.sh',
        depends_on_past=1,
        dag = dag)

task8_start_trade_stream.set_upstream(task6_initialize_db_rts1)
task8_start_trade_stream.set_upstream(task7_swap_web_db_rts2)

#Start next batch run while stock_count_rts1 tracks trades occuring since the completion of task3, the last batch calculation
#Run Camus to pull messages from Kafka into HDFS
task9_camus = BashOperator(
        task_id = '09_camus',
        bash_command='bash tasks/run_camus.sh',
        depends_on_past=1,
        dag = dag)

task9_camus.set_upstream(task8_start_trade_stream)

#Run Spark to pull latest relevant news from HDFS to Cassandra
task10_news_batch = BashOperator(
        task_id = '10_news_batch',
        bash_command='bash tasks/run_news_batch.sh',
        depends_on_past=1,
        dag = dag)

task10_news_batch.set_upstream(task9_camus)

#Run Spark to sum all historical trades and write to Cassandra
task11_trades_batch = BashOperator(
        task_id = '11_trades_batch',
        bash_command='bash tasks/run_trades_batch.sh',
        depends_on_past=1,
        dag = dag)

#set trades batch after news batch to give it more memory
task11_trades_batch.set_upstream(task10_news_batch)

#stop streaming of trades while the database is getting updated
task12_stop_trade_stream = BashOperator(
        task_id = '12_stop_trade_stream',
        bash_command='bash tasks/stop_trade_stream.sh',
        depends_on_past=1,
        dag = dag)

task12_stop_trade_stream.set_upstream(task11_trades_batch)

#Update Cassandra's stream 1 table to include counts from the batch run with all the trades summed from stock_count_rts2, which were the trades that came in since task9_camus started running
task13_sum_batch_rts1 = BashOperator(
        task_id = '13_batch_rts1',
        bash_command='bash tasks/sum_batch_rts rts1',
        depends_on_past=1,
        dag = dag)

task13_sum_batch_rts1.set_upstream(task12_stop_trade_stream)

#Empty Cassandra's stock_count_rts2 table to set initial counts to the result of the batch calculation
task14_initialize_db_rts2 = BashOperator(
        task_id = '14_initialize_rts2',
        bash_command='bash tasks/truncate_rts.sh rts2',
        depends_on_past=1,
        dag = dag)

task14_initialize_db_rts2.set_upstream(task12_stop_trade_stream)

#Get the web interface to start reading from the newly batch updated rts1
task15_swap_web_db_rts1 =  BashOperator(
        task_id = '15_web_rts1',
        bash_command='bash tasks/switchwebdb.sh rts1',
        depends_on_past=1,
        dag = dag))

task15_swap_web_db_rts1.set_upstream(task13_sum_batch_rts1)

#start the trades stream back up
task16_start_trade_stream = BashOperator(
        task_id = '16_start_trade_stream',
        bash_command='bash tasks/start_trade_stream.sh',
        depends_on_past=1,
        dag = dag)

task16_start_trade_stream.set_upstream(task14_initialize_db_rts2)
task16_start_trade_stream.set_upstream(task15_swap_web_db_rts1)
