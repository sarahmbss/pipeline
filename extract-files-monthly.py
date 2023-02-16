# Importação de bibliotecas
import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from Class import Class_coleta as coleta
from Class import Class_validacao as valida
from Class import Class_persiste as persiste


# Variáveis default da dag
default_args = {
    'owner': 'sarah braga',
    'retries': 0,
    'retry_delay': 0,
    'start_date': datetime.datetime(2023, 2, 15),
    'execution_timeout': timedelta(seconds=120)
}

# Calcula o ano e mês que os dados devem ser extraídos
data = datetime.date.today() - relativedelta(months=+2)
mes = data.month
ano = data.year

# Caso o mês esteja entre janeiro - setembro, acrescenta o 0 na frente do número
if mes >=1 and mes <= 9:
    mes = '0' + str(mes)

 # Variavel auxiliar para inserção de dados
busca = str(ano) + str(mes)

#------------------------------------------------------------#
# Descrição: Função responsável por criar as tasks Python
# Parâmetros:
# 1- Id da task
# 2- Função Python que será chamada
# 3- argumentos que serão enviados
# 4- Define se a job irá passar/puxar dados de outras tasks
# 5- Inicialização da dag
#------------------------------------------------------------#
def create_task(name, function, kwargs, dag):
    return PythonOperator(
         task_id=str(name)
        ,python_callable=function
        ,op_kwargs=kwargs
        ,do_xcom_push=True
        ,dag=dag
    )

#------------------------------------------------------------#
# Descrição: Declaração da dag
#------------------------------------------------------------#
with DAG(
    dag_id='extract-files-monthly',
    max_active_runs=1,
    schedule_interval="0 8 1 * *", # executa sempre no primeiro dia do mês as 8h
    default_args=default_args,
    catchup=False, # Quando falso, esse parametro não inicia dags extras caso o start date esteja atrasado
    tags=['monthly', 'extraction', 'api', 'mysql'] # Facilitam a identificação da dag
) as dag:

        # Dummy para marcar o início da dag
        init = DummyOperator(task_id='init')

        # Coleta os dados de 3 meses atrás na API -- Utilizado pois a API possui dados somente até Novembro de 2022
        coletaDadosMes1 = create_task("coletaDadosMes1", coleta.Coleta.coleta_dados, {"meses": 3}, dag)

        # Coleta os dados de 4 meses atrás na API -- Utilizado pois a API possui dados somente até Novembro de 2022
        coletaDadosMes2 = create_task("coletaDadosMes2", coleta.Coleta.coleta_dados, {"meses": 4}, dag)

        # Coleta os dados de 5 meses atrás na API -- Utilizado pois a API possui dados somente até Novembro de 2022
        coletaDadosMes3 = create_task("coletaDadosMes3", coleta.Coleta.coleta_dados, {"meses": 5}, dag)

        # Valida o retorno da API
        validaDadosRetornoAPI = create_task("validaDadosRetornoAPI", valida.ValidaDados.retorno_api, {}, dag)

        # Verifica se os dados podem ser convertidos para json
        validaDadosVerificaJSON = create_task("validaDadosVerificaJSON", valida.ValidaDados.verifica_json, {}, dag)

        # Insere os dados na camada bronze
        insereBronze = create_task("insereBronze", persiste.PersisteDados.insereBronze, {}, dag)

        # Insere os dados na camada silver
        insereSilverPrescriptions = BigQueryOperator(
             task_id = 'insereSilverPrescriptions'
            ,sql = '\
                SELECT  \
                    * \
                FROM \
                    `bronze.TB_ANALITICO_PRESCRIPTIONS`\
                WHERE \
                    YEAR_MONTH > ' + busca
            , destination_dataset_table = 'silver.TB_PRESCRIPTIONS'
            ,use_legacy_sql = False
            ,write_disposition = 'WRITE_APPEND' # se a tabela já existir, insere os dados
            ,dag=dag
        )

        # Insere os dados na camada silver
        insereSilverPrescribers = BigQueryInsertJobOperator(
             task_id = 'insereSilverPrescribers'
            ,configuration = {
              'query': {
                    'query': '\
                        MERGE silver.TB_PRESCRIBERS silver \
                        USING (SELECT DISTINCT PRACTICE_CODE,PRACTICE_NAME FROM bronze.TB_ANALITICO_PRESCRIPTIONS) bronze \
                        ON bronze.PRACTICE_CODE = silver.PRACTICE_CODE \
                        WHEN MATCHED THEN \
                        UPDATE SET silver.PRACTICE_NAME = bronze.PRACTICE_NAME, silver.PRACTICE_CODE = bronze.PRACTICE_CODE  \
                        WHEN NOT MATCHED THEN \
                        INSERT (PRACTICE_NAME, PRACTICE_CODE) VALUES (PRACTICE_NAME, PRACTICE_CODE)'
                    ,'use_legacy_sql': False
                } 
            }
            ,project_id = 'molten-complex-377821'
            ,dag=dag
        )

        # Dummy para marcar o final da dag
        finish = DummyOperator(task_id='finish')

# Fluxo de execução das jobs
init >> [coletaDadosMes1, coletaDadosMes2, coletaDadosMes3] >> validaDadosRetornoAPI >> validaDadosVerificaJSON >> insereBronze >> insereSilverPrescriptions >> insereSilverPrescribers >> finish
