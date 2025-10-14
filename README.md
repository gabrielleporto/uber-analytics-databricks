## Descrição
Este projeto foi desenvolvido com base em um dataset simulado de corridas Uber, contendo informações sobre datas, origens, destinos, categorias (Business/Personal), milhas percorridas e propósito da corrida.
O objetivo foi aplicar conceitos de SQL no Databricks para responder perguntas de negócio e extrair insights que possam apoiar a tomada de decisão.

## Estrutura do Repositório

- uber_trips.csv → base de dados utilizada.
- notebooks/uber_analysis_databricks.dbc → notebook exportado do Databricks com todas as consultas SQL.
- queries.sql → versão das principais queries em SQL puro.

## Tecnologias Utilizadas

- Databricks Free Edition
- SQL (Spark SQL)
- Git e GitHub para versionamento

## Fluxo de arquitetura
[1_source] → dados brutos (.csv)
    <img width="1027" height="438" alt="image" src="https://github.com/user-attachments/assets/1406fb25-3b3c-4307-935d-3a4c58e04758" />
      ↓
[2_transform] → limpeza e cálculos no Databricks
    <img width="1361" height="714" alt="image" src="https://github.com/user-attachments/assets/83fd18d0-ff5f-4b87-94d9-4a78fe0da224" />
      ↓
[3_dashboard] → consultas finais e visualização
    <img width="1527" height="682" alt="image" src="https://github.com/user-attachments/assets/75291be5-df1f-45a7-9696-b6aa79971ca1" />


