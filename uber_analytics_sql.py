# Databricks notebook source
# MAGIC %md
# MAGIC # Descrição do projeto
# MAGIC
# MAGIC Projeto de análise exploratória de um dataset fictício de corridas Uber com objetivo de identificar padrões de comportamento de negócio e pessoal, explorando categorias, distâncias percorridas, horários, origens/destinos e finalidades das viagens. A análise gerou insights sobre perfil de corridas Business vs Personal, rotas mais frequentes, sazonalidade, principais propósitos e oportunidades de eficiência.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1.1 Perfil de corridas
# MAGIC Qual é a proporção de corridas Business vs Personal?
# MAGIC

# COMMAND ----------

df1 = spark.sql(
"""
SELECT 
    category,
    COUNT(*) AS total_corridas,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS proporcao_percentual
FROM workspace.default.uber_trips
GROUP BY category
"""
)

df1.write.mode("overwrite").saveAsTable("workspace.default.uber_perfil_corridas_1")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1.2 Perfil de corridas
# MAGIC
# MAGIC As corridas Business tendem a ter mais milhas do que as Personal?

# COMMAND ----------

df2 = spark.sql(
"""
SELECT 
    category,
    COUNT(*) AS total_corridas,
    ROUND(SUM(miles), 2) AS total_miles,
    ROUND(AVG(miles), 2) AS media_miles
FROM workspace.default.uber_trips
GROUP BY category
"""
)

df2.write.mode("overwrite").saveAsTable("workspace.default.uber_perfil_corridas_2")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1.3 Perfil de corridas
# MAGIC Em média, quanto tempo dura uma corrida Business comparada a uma Personal?

# COMMAND ----------

df3 = spark.sql(
"""
SELECT 
    category,
    ROUND(
        AVG(
            (UNIX_TIMESTAMP(TO_TIMESTAMP(end_date, 'dd/MM/yyyy HH:mm')) 
            - UNIX_TIMESTAMP(TO_TIMESTAMP(start_date, 'dd/MM/yyyy HH:mm'))) / 60
        ), 2
    ) AS media_duracao_minutos
FROM workspace.default.uber_trips
GROUP BY category
"""
)

df3.write.mode("overwrite").saveAsTable("workspace.default.uber_perfil_corridas_3")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2.1 Distância e custos
# MAGIC Qual é a média de milhas percorridas por categoria (Business x Personal)?

# COMMAND ----------

df4 = spark.sql(
"""
SELECT 
    category,
    ROUND(AVG(miles), 2) AS media_miles
FROM workspace.default.uber_trips
GROUP BY category
"""
)

df4.write.mode("overwrite").saveAsTable("workspace.default.uber_distancia_custos_1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Distância e custos
# MAGIC Quais são os 10 pares de origem/destino que concentram as viagens mais longas?
# MAGIC
# MAGIC

# COMMAND ----------

df5 = spark.sql(
"""
SELECT 
    start,
    stop,
    AVG(miles) AS media_milhas
FROM workspace.default.uber_trips
GROUP BY start, stop
ORDER BY media_milhas DESC
LIMIT 10
"""
)

df5.write.mode("overwrite").saveAsTable("workspace.default.uber_distancia_custos_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Distância e custos
# MAGIC Quais são os 10 pares de origem/destino com mais viagens?
# MAGIC

# COMMAND ----------

df6 = spark.sql(
"""
SELECT 
    start,
    stop,
    COUNT(*) AS total_corridas
FROM workspace.default.uber_trips
GROUP BY start, stop
HAVING COUNT(*) > 1
ORDER BY total_corridas DESC
LIMIT 10
"""
)

df6.write.mode("overwrite").saveAsTable("workspace.default.uber_distancia_custos_3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Padrões temporais
# MAGIC Em quais dias da semana ocorrem mais corridas Business?

# COMMAND ----------

df7 = spark.sql(
"""
SELECT 
    DATE_FORMAT(TO_TIMESTAMP(start_date, 'dd/MM/yyyy HH:mm'), 'EEEE') AS dia_semana,
    COUNT(*) AS total_corridas
FROM workspace.default.uber_trips
WHERE category = 'Business'
GROUP BY DATE_FORMAT(TO_TIMESTAMP(start_date, 'dd/MM/yyyy HH:mm'), 'EEEE')
ORDER BY total_corridas DESC;
"""
)

df7.write.mode("overwrite").saveAsTable("workspace.default.uber_padroes_temporais_1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Padrões temporais
# MAGIC O horário de pico para corridas Business é diferente das Personal?

# COMMAND ----------

df8 = spark.sql(
"""
SELECT 
    category,
    HOUR(TO_TIMESTAMP(start_date, 'dd/MM/yyyy HH:mm')) AS hora_corrida,
    COUNT(*) AS total_corridas
FROM workspace.default.uber_trips
GROUP BY category, hora_corrida
ORDER BY category, total_corridas DESC
"""
)

df8.write.mode("overwrite").saveAsTable("workspace.default.uber_padroes_temporais_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Padrões temporais
# MAGIC Há sazonalidade ao longo do ano (ex: aumento em meses de férias ou fim de ano)?

# COMMAND ----------

df9= spark.sql(
"""
SELECT 
    DATE_FORMAT(TO_TIMESTAMP(start_date, 'dd/MM/yyyy HH:mm'), 'MMMM') AS mes_corrida,
    COUNT(*) AS total_corridas
FROM workspace.default.uber_trips
GROUP BY mes_corrida
ORDER BY total_corridas DESC
"""
)

df9.write.mode("overwrite").saveAsTable("workspace.default.uber_padroes_temporais_3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Propósito das viagens
# MAGIC Qual é o propósito mais comum para corridas Business?

# COMMAND ----------

df10 = spark.sql(
"""
SELECT 
    purpose,
    COUNT(*) AS total_corridas
FROM workspace.default.uber_trips
WHERE category = 'Business'
GROUP BY purpose
ORDER BY total_corridas DESC
"""
)

df10.write.mode("overwrite").saveAsTable("workspace.default.uber_proposito_viagens_1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Propósito das viagens
# MAGIC Qual é o propósito mais comum para corridas Personal?

# COMMAND ----------

df11 = spark.sql(
"""
SELECT 
    purpose,
    COUNT(*) AS total_corridas
FROM workspace.default.uber_trips
WHERE category = 'Personal'
GROUP BY purpose
ORDER BY total_corridas DESC
"""
)

df11.write.mode("overwrite").saveAsTable("workspace.default.uber_proposito_viagens_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Propósito de viagens
# MAGIC Existem propósitos que concentram distâncias maiores?

# COMMAND ----------

df12 = spark.sql(
"""
SELECT 
    purpose,
    COUNT(*) AS total_corridas,
    ROUND(SUM(miles), 2) AS total_milhas,
    ROUND(AVG(miles), 2) AS media_milhas,
    MAX(miles) AS maior_corrida
FROM workspace.default.uber_trips
GROUP BY purpose
ORDER BY media_milhas DESC
"""
)

df12.write.mode("overwrite").saveAsTable("workspace.default.uber_proposito_viagens_3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Eficiência e estratégia
# MAGIC Quais corridas de Business têm o maior custo em milhas → possíveis oportunidades de redução de gastos?
# MAGIC

# COMMAND ----------

df13 = spark.sql(
"""
SELECT 
    start,
    stop,
    purpose,
    miles
FROM workspace.default.uber_trips
WHERE category = 'Business'
ORDER BY miles DESC 
LIMIT 10
"""
)

df13.write.mode("overwrite").saveAsTable("workspace.default.uber_eficiencia_estrategia_1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Eficiência e estratégia
# MAGIC Existe algum padrão de desperdício? (ex: muitas corridas curtas dentro da mesma cidade na categoria Business)

# COMMAND ----------

df14 = spark.sql(
"""
SELECT 
    start AS cidade,
    purpose,
    COUNT(*) AS total_corridas,
    ROUND(AVG(miles), 2) AS media_milhas
FROM workspace.default.uber_trips
WHERE category = 'Business'
  AND start = stop
  AND miles < 20
GROUP BY start, purpose
ORDER BY total_corridas DESC
"""
)

df14.write.mode("overwrite").saveAsTable("workspace.default.uber_eficiencia_estrategia_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Eficiência e estratégia
# MAGIC Quais as 20 cidades que aparecem com mais frequência em Business → potenciais polos de atuação da empresa.

# COMMAND ----------

df15 = spark.sql(
"""
SELECT 
    start,
    COUNT(*) AS total_corridas 
FROM workspace.default.uber_trips
WHERE category = 'Business' 
GROUP BY start 
ORDER BY total_corridas DESC
LIMIT 20
"""
)

df15.write.mode("overwrite").saveAsTable("workspace.default.uber_eficiencia_estrategia_3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fim

# COMMAND ----------

dbutils.notebook.exit("success")
