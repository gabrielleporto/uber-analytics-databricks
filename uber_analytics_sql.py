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

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) AS total_corridas,
# MAGIC     ROUND(
# MAGIC         COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 
# MAGIC         2
# MAGIC     ) AS proporcao_percentual
# MAGIC FROM workspace.default.uber_trips
# MAGIC GROUP BY category

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1.2 Perfil de corridas
# MAGIC
# MAGIC As corridas Business tendem a ter mais milhas do que as Personal?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) AS total_corridas,
# MAGIC     ROUND(SUM(miles), 2) AS total_miles,
# MAGIC     ROUND(AVG(miles), 2) AS media_miles
# MAGIC FROM workspace.default.uber_trips
# MAGIC GROUP BY category;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1.3 Perfil de corridas
# MAGIC Em média, quanto tempo dura uma corrida Business comparada a uma Personal?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     ROUND(
# MAGIC         AVG(
# MAGIC             (UNIX_TIMESTAMP(TO_TIMESTAMP(end_date, 'dd/MM/yyyy HH:mm')) 
# MAGIC             - UNIX_TIMESTAMP(TO_TIMESTAMP(start_date, 'dd/MM/yyyy HH:mm'))) / 60
# MAGIC         ), 2
# MAGIC     ) AS media_duracao_minutos
# MAGIC FROM workspace.default.uber_trips
# MAGIC GROUP BY category;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2.1 Distância e custos
# MAGIC Qual é a média de milhas percorridas por categoria (Business x Personal)?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     ROUND(AVG(miles), 2) AS media_miles
# MAGIC FROM workspace.default.uber_trips
# MAGIC GROUP BY category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Distância e custos
# MAGIC Quais são os 10 pares de origem/destino que concentram as viagens mais longas?
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     start,
# MAGIC     stop,
# MAGIC     AVG(miles) AS media_milhas
# MAGIC FROM workspace.default.uber_trips
# MAGIC GROUP BY start, stop
# MAGIC ORDER BY media_milhas DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Distância e custos
# MAGIC Quais são os 10 pares de origem/destino com mais viagens?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     start,
# MAGIC     stop,
# MAGIC     COUNT(*) AS total_corridas
# MAGIC FROM workspace.default.uber_trips
# MAGIC GROUP BY start, stop
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY total_corridas DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Padrões temporais
# MAGIC Em quais dias da semana ocorrem mais corridas Business?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     DATE_FORMAT(TO_TIMESTAMP(start_date, 'dd/MM/yyyy HH:mm'), 'EEEE') AS dia_semana,
# MAGIC     COUNT(*) AS total_corridas
# MAGIC FROM workspace.default.uber_trips
# MAGIC WHERE category = 'Business'
# MAGIC GROUP BY DATE_FORMAT(TO_TIMESTAMP(start_date, 'dd/MM/yyyy HH:mm'), 'EEEE')
# MAGIC ORDER BY total_corridas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Padrões temporais
# MAGIC O horário de pico para corridas Business é diferente das Personal?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     HOUR(TO_TIMESTAMP(start_date, 'dd/MM/yyyy HH:mm')) AS hora_corrida,
# MAGIC     COUNT(*) AS total_corridas
# MAGIC FROM workspace.default.uber_trips
# MAGIC GROUP BY category, hora_corrida
# MAGIC ORDER BY category, total_corridas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Padrões temporais
# MAGIC Há sazonalidade ao longo do ano (ex: aumento em meses de férias ou fim de ano)?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     DATE_FORMAT(TO_TIMESTAMP(start_date, 'dd/MM/yyyy HH:mm'), 'MMMM') AS mes_corrida,
# MAGIC     COUNT(*) AS total_corridas
# MAGIC FROM workspace.default.uber_trips
# MAGIC GROUP BY mes_corrida
# MAGIC ORDER BY total_corridas DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Propósito das viagens
# MAGIC Qual é o propósito mais comum para corridas Business?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     purpose,
# MAGIC     COUNT(*) AS total_corridas
# MAGIC FROM workspace.default.uber_trips
# MAGIC WHERE category = 'Business'
# MAGIC GROUP BY purpose
# MAGIC ORDER BY total_corridas DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Propósito das viagens
# MAGIC Qual é o propósito mais comum para corridas Personal?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     purpose,
# MAGIC     COUNT(*) AS total_corridas
# MAGIC FROM workspace.default.uber_trips
# MAGIC WHERE category = 'Personal'
# MAGIC GROUP BY purpose
# MAGIC ORDER BY total_corridas DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Propósito de viagens
# MAGIC Existem propósitos que concentram distâncias maiores?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     purpose,
# MAGIC     COUNT(*) AS total_corridas,
# MAGIC     ROUND(SUM(miles), 2) AS total_milhas,
# MAGIC     ROUND(AVG(miles), 2) AS media_milhas,
# MAGIC     MAX(miles) AS maior_corrida
# MAGIC FROM workspace.default.uber_trips
# MAGIC GROUP BY purpose
# MAGIC ORDER BY media_milhas DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Eficiência e estratégia
# MAGIC Quais corridas de Business têm o maior custo em milhas → possíveis oportunidades de redução de gastos?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     start,
# MAGIC     stop,
# MAGIC     purpose,
# MAGIC     miles
# MAGIC FROM workspace.default.uber_trips
# MAGIC WHERE category = 'Business'
# MAGIC ORDER BY miles DESC 
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Eficiência e estratégia
# MAGIC Existe algum padrão de desperdício? (ex: muitas corridas curtas dentro da mesma cidade na categoria Business)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     start AS cidade,
# MAGIC     purpose,
# MAGIC     COUNT(*) AS total_corridas,
# MAGIC     ROUND(AVG(miles), 2) AS media_milhas
# MAGIC FROM workspace.default.uber_trips
# MAGIC WHERE category = 'Business'
# MAGIC   AND start = stop
# MAGIC   AND miles < 20
# MAGIC GROUP BY start, purpose
# MAGIC ORDER BY total_corridas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Eficiência e estratégia
# MAGIC Quais as 20 cidades que aparecem com mais frequência em Business → potenciais polos de atuação da empresa.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     start,
# MAGIC     COUNT(*) AS total_corridas 
# MAGIC FROM workspace.default.uber_trips
# MAGIC WHERE category = 'Business' 
# MAGIC GROUP BY start 
# MAGIC ORDER BY total_corridas DESC
# MAGIC LIMIT 20;
