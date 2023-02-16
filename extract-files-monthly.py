# Importação de bibliotecas
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from Class import Class_coleta as coleta
from Class import Class_validacao as valida
from Class import Class_persiste as persiste

# Variáveis
default_args = {
    'owner': 'sarah braga',
    'retries': 1,
    'retry_delay': 0,
    'start_date': datetime(2023, 2, 15),
    'execution_timeout': timedelta(seconds=120)
}

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

        # Coleta os dados de 3 meses atrás na API
        coletaDadosMes1 = create_task("coletaDadosMes1", coleta.Coleta.coleta_dados, {"meses": 3}, dag)

        # Coleta os dados de 4 meses atrás na API
        coletaDadosMes2 = create_task("coletaDadosMes2", coleta.Coleta.coleta_dados, {"meses": 4}, dag)

        # Coleta os dados de 5 meses atrás na API
        coletaDadosMes3 = create_task("coletaDadosMes3", coleta.Coleta.coleta_dados, {"meses": 5}, dag)

        # Dummy para marcar o final da dag
        finish = DummyOperator(task_id='finish')

        # Valida o retorno da API
        validaDadosRetornoAPI = create_task("validaDadosRetornoAPI", valida.ValidaDados.retorno_api, {}, dag)

        # Verifica se os dados podem ser convertidos para json
        validaDadosVerificaJSON = create_task("validaDadosVerificaJSON", valida.ValidaDados.verifica_json, {}, dag)

        # Cria e separa os dataframes 
        persisteDados = create_task("persisteDados", persiste.PersisteDados.main, {}, dag)

# Fluxo de execução das jobs
init >> [coletaDadosMes1, coletaDadosMes2, coletaDadosMes3] >> validaDadosRetornoAPI >> validaDadosVerificaJSON >> persisteDados >> finish
