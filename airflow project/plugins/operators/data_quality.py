from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    data_quality_checks = [
        {'check_sql' : "SELECT COUNT(*) FROM users WHERE userid IS NULL",
         'expected_result' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM songs WHERE songid IS NULL",
         'expected_result' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM artists WHERE artistid IS NULL",
         'expected_result' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM time WHERE start_time IS NULL",
         'expected_result' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM users WHERE userid IS NULL",
         'expected_result' : 0}]

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="redshift",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('DataQualityOperator not implemented yet')
        errorCount = 0
        failingTest = []
        
        for check in DataQualityOperator.data_quality_checks:
            sql_query = check.get('check_sql')
            exp_result = check.get('expected_result')
            records = redshift_hook.get_records(sql_query)[0]
            
            if exp_result != records[0]:
                errorCount += 1
                failingTest.append(sql_query)
            
            if errorCount > 0:
                self.log.info("data quality not good")
                self.log.info("the failed tests time: {}".format(errorCount))
                self.log.info(failingTest)
                raise ValueError("Fail")
                
            if errorCount == 0:
                self.log.info("Pass")
        
