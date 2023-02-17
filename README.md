# Pipeline de dados com Airflow e BigQuery

Para realização do desafio, inicialmente foi implementada a seguinte pipeline no **Cloud Composer**:

![Pipeline de dados no Airflow](https://github.com/sarahmbss/pipeline/blob/main/Docs/arquitetura.png)

### Coleta de dados

- As etapas de coletas de dados foram construídas inicialmente para extrair dados dos 3 últimos meses de prescrição;
- Cada caixinha realiza a extração de 1 mês;
- O código utilizado foi construído em **Python** utilizando a task *PythonOperator*, e possui limitação de 100 dados por requisição;
- Uma observação em relação a coleta inicial é que a API possui dados somente até novembro/2022, então os últimos 3 meses considerados foram setembro, outubro e novembro.

### Validação

- A primeira validação realizada é se a coleta foi bem sucedida (dados não estão retornando como None devido à alguma falha na API);
- A segunda é se estão no formato correto e consequentemente podem ser transformados em JSON;
- O código utilizado também foi construído em **Python**, utilizando a task *PythonOperator* do Airflow.

### Persistência dos dados

- A ferramenta escolhida para persistência foi o **BigQuery** por ser uma das principais ferramentas do GCP utilizadas na construção de Data Lakes. Além disso, como foi utilizado o **Cloud Composer** para construção do Airflow, a integração com o BigQuery é facilitada;
- Inicialmente, os dados crus são inseridos na camada Bronze de processamento, através de um código **Python** utilizando também a task *PythonOperator* do Airflow;
- Para a inserção na camada Silver, é feito uma tratativa nos dados, separando-os em *prescribers* e *prescriptions*
  - Para a inserção das *prescriptions*, os dados são inseridos somente se forem novos, ou seja, coletados no último mês. Essa etapa é feita por meio da task *BigQueryOperator* do Airflow;
  - Para a inserção dos *prescribers* é verificado se os coletados no último mês, já existem na tabela. Se não, são inseridos, se sim, não são inseridos. Essa etapa é feita por meio da task *BigQueryInsertJobOperator* do Airflow.

Logo após a coleta inicial dos dados, a pipeline foi modificada para a seguinte estrutura, que roda automaticamente dia 01 de cada mês às 8h:

![Pipeline de dados no Airflow](https://github.com/sarahmbss/pipeline/blob/main/Docs/arquitetura-nova.png)

A única modificação realizada foi em relação à coleta de dados. Ao invés de coletar sempre os últimos 3 meses, ela coleta apenas o último mês. Todo o processo de validação e persistência permanece o mesmo. Uma observação em relação à isso, é que o script está preparado para coletar os dados do último mês, porém como a API não possui dados desse período, ele não insere nenhum dado nas tabelas!

Os dados presentes foram aqueles inseridos na coleta dos 3 últimos meses mencionados anteriormente.

### Forma de utilização do pipeline

- O projeto construído no GCP foi compartilhado no email. Logo após aceitar o convite, é possível acessar a interface visual do Airflow por meio do link https://console.cloud.google.com/composer/environments?referrer=search&project=molten-complex-377821
- Os códigos utilizadas no Airflow estão disponíveis aqui no GitHub e também no Storage no seguinte link https://console.cloud.google.com/storage/browser/southamerica-east1-pipeline-de43fe0d-bucket/dags?hl=pt-br&project=molten-complex-377821&pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))&prefix=&forceOnObjectsSortingFiltering=false
- Em relação ao BigQuery, é possível visualizar as tabelas e resultados das queries no link  https://console.cloud.google.com/bigquery?referrer=search&project=molten-complex-377821&ws=!1m0
