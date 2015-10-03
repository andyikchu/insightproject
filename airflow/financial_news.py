from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ubuntu',
    'depends_on_past': False,
    'start_date': datetime(2015, 10, 2),
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
    'financial_news', default_args=default_args, schedule_interval=timedelta(1))

#Run Camus to pull messages from Kafka into HDFS
camus_a = BashOperator(
        task_id = 'camus_a',
        bash_command='tasks/run_camus.sh',
        depends_on_past=1,
        dag = dag)

#Run Spark to sum all historical trades and write to Cassandra
trades_batch_a = BashOperator(
        task_id = 'trades_batch_a',
        bash_command='tasks/run_trades_batch.sh',
        depends_on_past=1,
        dag = dag)

#set trades batch after news batch to give it more memory
trades_batch_a.set_upstream(camus_a)

#Update Cassandra's stream 2 table to include counts from the batch run with all the trades summed from stock_count_rts1, which were the trades that came in since task1_camus started running
sum_batch_a_rts2 = BashOperator(
        task_id = 'sum_batch_a_rts2',
        bash_command='tasks/sum_batch_rts2.sh',
        depends_on_past=1,
        dag = dag)

sum_batch_a_rts2.set_upstream(trades_batch_a)

#stop streaming of trades while the database is getting updated
stop_trade_stream_a = BashOperator(
        task_id = 'stop_trade_stream_a',
        bash_command='tasks/stop_trade_stream.sh',
        depends_on_past=1,
        dag = dag)

stop_trade_stream_a.set_upstream(sum_batch_a_rts2)


#Empty Cassandra's stock_count_rts1 table to set initial counts to the result of the batch calculation
initialize_db_a_rts1 = BashOperator(
        task_id = 'initialize_db_a_rts1',
        bash_command='tasks/truncate_rts1.sh',
        depends_on_past=1,
        dag = dag)

initialize_db_a_rts1.set_upstream(stop_trade_stream_a)

#Get the web interface to start reading from the newly batch updated rts2
swap_web_db_a_rts2 =  BashOperator(
        task_id = 'swap_web_db_a_rts2',
        bash_command='tasks/switchwebdbrts2.sh',
        depends_on_past=1,
        dag = dag)

swap_web_db_a_rts2.set_upstream(stop_trade_stream_a)

#start the trades stream back up
start_trade_stream_a = BashOperator(
        task_id = 'start_trade_stream_a',
        bash_command='tasks/start_trade_stream.sh',
        depends_on_past=1,
        dag = dag)

start_trade_stream_a.set_upstream(initialize_db_a_rts1)
start_trade_stream_a.set_upstream(swap_web_db_a_rts2)

#Start next batch run while stock_count_rts1 tracks trades occuring since the completion of task2, the last batch calculation
#Run Camus to pull messages from Kafka into HDFS
camus_b = BashOperator(
        task_id = 'camus_b',
        bash_command='tasks/run_camus.sh',
        depends_on_past=1,
        dag = dag)

camus_b.set_upstream(start_trade_stream_a)

#Run Spark to sum all historical trades and write to Cassandra
trades_batch_b = BashOperator(
        task_id = 'trades_batch_b',
        bash_command='tasks/run_trades_batch.sh',
        depends_on_past=1,
        dag = dag)

#set trades batch after news batch to give it more memory
trades_batch_b.set_upstream(camus_b)

#Update Cassandra's stream 1 table to include counts from the batch run with all the trades summed from stock_count_rts2, which were the trades that came in since task8_camus started running
sum_batch_b_rts1 = BashOperator(
        task_id = 'sum_batch_b_rts1',
        bash_command='tasks/sum_batch_rts1.sh',
        depends_on_past=1,
        dag = dag)

sum_batch_b_rts1.set_upstream(trades_batch_b)

#stop streaming of trades while the database is getting updated
stop_trade_stream_b = BashOperator(
        task_id = 'stop_trade_stream_b',
        bash_command='tasks/stop_trade_stream.sh',
        depends_on_past=1,
        dag = dag)

stop_trade_stream_b.set_upstream(sum_batch_b_rts1)

#Empty Cassandra's stock_count_rts2 table to set initial counts to the result of the batch calculation
initialize_db_b_rts2 = BashOperator(
        task_id = 'initialize_db_b_rts2',
        bash_command='tasks/truncate_rts2.sh',
        depends_on_past=1,
        dag = dag)

initialize_db_b_rts2.set_upstream(stop_trade_stream_b)

#Get the web interface to start reading from the newly batch updated rts1
swap_web_db_b_rts1 =  BashOperator(
        task_id = 'swap_web_db_b_rts1',
        bash_command='tasks/switchwebdbrts1.sh',
        depends_on_past=1,
        dag = dag)

swap_web_db_b_rts1.set_upstream(stop_trade_stream_b)

#start the trades stream back up
start_trade_stream_b = BashOperator(
        task_id = 'start_trade_stream_b',
        bash_command='tasks/start_trade_stream.sh',
        depends_on_past=1,
        dag = dag)

start_trade_stream_b.set_upstream(initialize_db_b_rts2)
start_trade_stream_b.set_upstream(swap_web_db_b_rts1)
