# freela-cognitivo

Toda a proposta do desafio está no arquivo **requirements/requirements.txt**

Todo o código para a resolução do desafio está dentro do arquivo **main.py**

O processamento foi todo realizado em ambiente local. O pequeno volume de dados permitiu essa abordagem. Instalei as bibliotes do Spark e fiz todos os teste em um notebook jupyter.

Foi escolhido como saída, arquivos do tipo parquet, pois tem um formato colunar de alta performance que tem um desempenho muito melhor para consulta se comparado a outros tipos de arquivos como CSV e JSON.

A ordem das atividades para resolução foram:

  1. Carregar o arquivo de configuração **(config/types_mapping.json)**  com o tipo dos campos para memória.
  2. Ler o dataset em formato CSV, criando um Dataframe Spark
  3. Atribuir tipo aos campos que foram carregados no arquivo de configuração.
  4. Deduplicar os dados, mantendo apenas a última atualizaço de cada ID
  5. Escrever o resultado final em um arquivo em um formato colunar de alta performance (parquet).
