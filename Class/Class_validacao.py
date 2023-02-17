import traceback
import json

# ------------------------------------------------------ #
# Descrição: Realiza o processo de validação dos dados
# Parâmetros: dados retornados da consulta
# ------------------------------------------------------ #
class ValidaDados:

    # ------------------------------------------------------ #
    # Descrição: Verifica o retorno da API
    # Parâmetros: dados retornados da consulta
    # ------------------------------------------------------ #
    def retorno_api(ti=None):
        dados1 = ti.xcom_pull(key='result_coleta_mes_1', task_ids='coletaDadosMes1')
        
        if dados1 == None or dados1 == '400':
            print('Falha na coleta de dados! ')
            return '400'
        else:
            return '200'
        
    # ----------------------------------------------------------------- #
    # Descrição: Verifica se os dados podem ser convertidos para json
    # Parâmetros: dados retornados da consulta
    # ----------------------------------------------------------------- #
    def verifica_json(ti=None):
        try:
            dados1 = ti.xcom_pull(key='result_coleta_mes_1', task_ids='coletaDadosMes')
            json_retorno_mes_1 = json.loads(dados1)
            return '200'
        except:
            print('Falha na transformação para json! Dados fora do padrão. ')
            print(traceback.format_exc())
            return '400'
