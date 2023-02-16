import urllib.request
import datetime
from dateutil.relativedelta import relativedelta
import traceback

class Coleta:
    
    # ---------------------------------------------------- #
    # Descrição: Coleta os dados de X meses atrás na API
    # Parâmetros: 
    # 1- Mês 
    # ---------------------------------------------------- #
    def coleta_dados(meses:int, ti=None):
        
        try:
        
            # Calcula o ano e mês que os dados devem ser extraídos
            data = datetime.date.today() - relativedelta(months=+meses)
            mes = data.month
            ano = data.year

            # Caso o mês esteja entre janeiro - setembro, acrescenta o 0 na frente do número
            # Isso ocorre devido a uma padronização de busca da API
            if mes >=1 and mes <= 9:
                mes = '0' + str(mes)

            # Realiza a busca
            url = str('https://opendata.nhsbsa.net/api/3/action/datastore_search?resource_id=EPD_' + str(ano) + str(mes) + '&limit=100') 
            fileobj = urllib.request.urlopen(url)
            json = fileobj.read().decode('utf-8')

            # Escreve o resultado da job
            ti.xcom_push(key='result_coleta_mes_' + str(meses), value=json)
        
        except:
            
            print('Coleta gerou erro!')
            print(traceback.format_exc())
            return "400"
