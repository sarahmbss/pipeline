import pandas as pd
import json
import traceback
import sqlalchemy as sa
import pandas_gbq

class PersisteDados:

    # ----------------------------------------------------------------- #
    # Descrição: Cria um dataframe a partir do json
    # Parâmetros: 
    # 1- Json com dados da API
    # ----------------------------------------------------------------- #
    def cria_dataframe(dados):
        try:
            # Transforma para json
            json_ = json.loads(dados)

            # Adiciona cada campo do json em uma lista que se tornará a coluna do dataframe
            colunas = list()
            for campo in json_['result']['fields']:
                colunas.append(campo['id'])

            # Cria o dataframe
            df = pd.DataFrame(columns=colunas, data = json_['result']['records'])
            return df
        except: 
            print('Erro na construção do dataframe!')
            print(traceback.format_exc())
            return '400'
        
    # ------------------------------------------------------------------- #
    # Descrição: Insere os dados no Bigquery
    # Parâmetros: 
    # 1- dataframe
    # 2- nome da tabela de destino
    # ------------------------------------------------------------------- #
    def insere_dataframe(dados, tabela):

        try:

            # Insere dataframe 
            pandas_gbq.to_gbq(
                dataframe = dados,
                destination_table = 'pipeline.' + tabela,
                project_id='molten-complex-377821',
                if_exists='append'
            )

        except:
            print('Erro na inserção no banco de dados ! ')
            print(traceback.format_exc())
            return '400'

    # ------------------------------------------------------------------- #
    # Descrição: Controla o fluxo de persistência dos dados
    # Parâmetros: -
    # ------------------------------------------------------------------- #
    def main(ti=None):

        dados1 = ti.xcom_pull(key='result_coleta_mes_3', task_ids='coletaDadosMes1')
        dados2 = ti.xcom_pull(key='result_coleta_mes_4', task_ids='coletaDadosMes2')
        dados3 = ti.xcom_pull(key='result_coleta_mes_5', task_ids='coletaDadosMes3')

        # Cria os dataframes
        df1 = PersisteDados.cria_dataframe(dados1)
        df2 = PersisteDados.cria_dataframe(dados2)
        df3 = PersisteDados.cria_dataframe(dados3)

        # Concatena
        df = pd.concat([df1, df2, df3], ignore_index=True)

        # Construindo o dataframe de prescribers - Quem prescreveu a receita
        df_prescribers = df[['PRACTICE_NAME', 'PRACTICE_CODE']]
        df_prescribers = df_prescribers.drop_duplicates()

        # Construindo o dataframe de prescriptions - Contém todas as informações sobre a receita
        df_prescriptions = df

        # Persiste os dados no BigQuery
        PersisteDados.insere_dataframe(df_prescribers, 'TB_PRESCRIBERS')
        PersisteDados.insere_dataframe(df_prescriptions, 'TB_PRESCRIPTIONS')
