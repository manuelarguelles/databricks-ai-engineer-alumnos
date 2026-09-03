# Databricks notebook source
# MAGIC %md
# MAGIC # Sesión 3 · Fundamentos de IA Generativa y Genie
# MAGIC **Databricks AI Engineer** — caso Neptuno
# MAGIC
# MAGIC Hoy dejamos de tratar al modelo como una caja negra. Vamos a medir cómo cambian sus respuestas
# MAGIC cuando recibe contexto, un contrato de salida y metadatos confiables.
# MAGIC
# MAGIC Al terminar tendrás un laboratorio de prompts trazable y una tabla lista para comparar respuestas.

# COMMAND ----------

# MAGIC %md ## 0 · Conectar con tu catálogo

# COMMAND ----------

# Al importar o actualizar el notebook, Databricks no ejecuta el código automáticamente.
# Esta debe ser la primera celda que corras. Escribe arriba el nombre completo del catálogo
# de S01 y S02, por ejemplo: neptuno_tunombre.
dbutils.widgets.text("catalogo", "", "Tu catálogo de S01–S02")
print("✅ Widget creado. Escribe arriba el nombre completo de tu catálogo y vuelve a ejecutar esta celda.")

# COMMAND ----------

# `re` es la librería estándar de Python para expresiones regulares; aquí valida el formato del catálogo.
import re
# Los widgets permiten cambiar catálogo y endpoint desde la interfaz, sin editar el código.
dbutils.widgets.text("modelo", "system.ai.gpt-5-6-luna", "Endpoint de Foundation Model")

CATALOGO = dbutils.widgets.get("catalogo").strip().lower()
MODELO = dbutils.widgets.get("modelo").strip()
assert re.fullmatch(r"neptuno_[a-z0-9_]+", CATALOGO or ""), "Usa tu catálogo neptuno_<nombre>."
assert CATALOGO in {r.catalog.lower() for r in spark.sql("SHOW CATALOGS").collect()}, f"No existe {CATALOGO}."
assert MODELO, "Indica un endpoint disponible en tu workspace."
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.ai_lab")
print(f"✅ catálogo={CATALOGO} · modelo={MODELO}")

# COMMAND ----------

# MAGIC %md ### Troubleshooting: ¿tu workspace tiene este modelo?
# MAGIC Cada workspace de Databricks tiene disponibles distintos modelos según cuándo se creó y su
# MAGIC región — no es lo mismo para todos. Si el `ai_query` de más abajo falla con
# MAGIC `RESOURCE_DOES_NOT_EXIST`, corré esta celda antes de pedir ayuda: te dice exactamente qué
# MAGIC endpoints legacy (`databricks-...`) tiene TU workspace. Los modelos `system.ai.*` (como el
# MAGIC default de este notebook) no aparecen en esta lista aunque sí funcionen — es un catálogo
# MAGIC distinto (Unity AI Gateway), no un bug de la celda.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
endpoints_activos = [ep.name for ep in w.serving_endpoints.list()]

if MODELO.startswith("system.ai."):
    print(f"ℹ️  '{MODELO}' es un modelo de Unity AI Gateway: no aparece en esta lista aunque esté disponible.")
    print("   Si igual falla, revisá en el menú AI/ML → AI Gateway → Models que aparezca listado ahí.")
elif MODELO in endpoints_activos:
    print(f"✅ El endpoint '{MODELO}' existe en este workspace.")
else:
    print(f"❌ El endpoint '{MODELO}' NO existe en este workspace.")
    print("Endpoints disponibles:", endpoints_activos)
    print("Sugerencia: cambiá el widget 'modelo' a 'system.ai.gpt-5-6-luna'.")

# COMMAND ----------

# MAGIC %md ## 1 · Un modelo predice texto; no consulta la verdad
# MAGIC Temperatura baja reduce variación, pero no agrega conocimiento. Comparamos una pregunta que
# MAGIC Neptuno **no puede responder** porque no existe costo de mercadería.

# COMMAND ----------

preguntas = [
    ("margen", "¿Cuál es el margen de la categoría Bebidas?"),
    ("venta", "Explica en una frase qué significa venta neta."),
]
spark.createDataFrame(preguntas, "id string, pregunta string").createOrReplaceTempView("preguntas_s03")

# COMMAND ----------

# MAGIC %md ### Llamada controlada con `ai_query`
# MAGIC Si el endpoint sugerido no existe, elige uno habilitado en **Serving → Foundation Model APIs**
# MAGIC y cambia el widget `modelo`.

# COMMAND ----------

respuestas_base = spark.sql(f"""
SELECT id, pregunta,
       ai_query('{MODELO}', concat(
         'Responde en español, en máximo 60 palabras. Pregunta: ', pregunta
       )) AS respuesta
FROM preguntas_s03
""")
display(respuestas_base)
assert respuestas_base.count() == 2

# COMMAND ----------

# MAGIC %md ### Parámetros reales del endpoint
# MAGIC `ai_query` permite pasar parámetros mediante `modelParameters`, pero el endpoint puede
# MAGIC rechazar algunos. En este workspace verificamos que `max_tokens` funciona; `temperature` y
# MAGIC `top_p` no están soportados por `system.ai.gpt-5-6-luna`, así que no los simulamos en clase.

# COMMAND ----------

respuesta_limitada = spark.sql(f"""
SELECT ai_query(
  '{MODELO}',
  'Responde únicamente OK.',
  -- `modelParameters` envía opciones específicas al endpoint.
  -- `named_struct` crea el objeto de parámetros que espera `ai_query`.
  modelParameters => named_struct('max_tokens', 20),
  failOnError => false
) AS respuesta
""").first()["respuesta"]
print(respuesta_limitada)
assert respuesta_limitada["errorMessage"] is None, respuesta_limitada
assert respuesta_limitada["result"].strip() == "OK"
print("✅ max_tokens aplicado y verificado en el endpoint seleccionado.")

# COMMAND ----------

# MAGIC %md ## 2 · Prompt = tarea + contexto + límites + formato
# MAGIC El contexto no debe decirle al modelo qué inventar: debe decirle qué evidencia existe y qué
# MAGIC hacer cuando esa evidencia no alcanza.

# COMMAND ----------

contexto = f"""
Eres analista de Neptuno. La tabla {CATALOGO}.gold.ventas_por_categoria_mes contiene ventas netas.
La venta neta ya descuenta promociones. El modelo de datos NO contiene costo de mercadería.
Por eso NO permite calcular margen, utilidad ni rentabilidad. Si falta evidencia, dilo explícitamente.
""".strip()

prompt_seguro = contexto + "\nPregunta: ¿Cuál es el margen de la categoría Bebidas?\n" + (
    "Devuelve exactamente dos campos: respuesta y evidencia_usada. Máximo 80 palabras."
)
segura = spark.sql(f"SELECT ai_query('{MODELO}', {repr(prompt_seguro)}) AS respuesta").first()["respuesta"]
print(segura)
assert len(segura.strip()) > 10

# COMMAND ----------

# MAGIC %md ## 3 · Grounding: darle hechos, no toda la tabla
# MAGIC Recuperamos primero un agregado verificable y recién después pedimos lenguaje natural.

# COMMAND ----------

tabla_ventas = f"{CATALOGO}.gold.ventas_por_categoria_mes"
assert spark.catalog.tableExists(tabla_ventas), f"Falta {tabla_ventas}; termina la S02."

hechos = spark.sql(f"""
SELECT categoria, ROUND(SUM(ingreso_neto), 2) AS venta_neta
FROM {tabla_ventas}
GROUP BY categoria
ORDER BY venta_neta DESC
LIMIT 5
""").toPandas().to_dict("records")  # Pasamos el agregado a Python para incrustarlo en el prompt.
print(hechos)
assert hechos, "La tabla Gold no devolvió hechos."

# COMMAND ----------

prompt_grounded = f"""
Eres analista de Neptuno. Usa EXCLUSIVAMENTE estos hechos: {hechos}
Resume el top de categorías en tres viñetas. No calcules margen: no hay costos.
Incluye los números utilizados y termina con: Fuente: {tabla_ventas}
""".strip()
respuesta_grounded = spark.sql(
    f"SELECT ai_query('{MODELO}', {repr(prompt_grounded)}) AS respuesta"
).first()["respuesta"]
print(respuesta_grounded)
assert tabla_ventas in respuesta_grounded, "La respuesta no incluyó la fuente solicitada."

# COMMAND ----------

# MAGIC %md ## 4 · Persistir el experimento
# MAGIC Una demo se mira; un experimento se guarda con sus entradas y resultados.

# COMMAND ----------

# UTC evita mezclar zonas horarias al comparar experimentos.
from datetime import datetime, timezone

filas = [
    ("sin_contexto", "¿Cuál es el margen de Bebidas?", respuestas_base.filter("id='margen'").first()["respuesta"]),
    ("con_limite", "¿Cuál es el margen de Bebidas?", segura),
    ("grounded", "Resume el top de categorías", respuesta_grounded),
]
df_eval = (spark.createDataFrame(filas, "variante string, pregunta string, respuesta string")
                 .withColumn("registrado_ts", __import__("pyspark").sql.functions.lit(datetime.now(timezone.utc))))
df_eval.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(  # Persistimos el experimento en Delta.
    f"{CATALOGO}.ai_lab.experimentos_prompt"
)

# COMMAND ----------

# MAGIC %md ## 5 · De prompt a Genie
# MAGIC En Genie el contexto se gobierna con cuatro capas: tablas autorizadas, comentarios de Unity
# MAGIC Catalog, instrucciones del space y SQL de confianza. Prueba en el space curado:
# MAGIC
# MAGIC 1. “¿Cuál es el margen de Bebidas?”
# MAGIC 2. “¿Cuánto vendimos de Bebidas en 2025?”
# MAGIC 3. Inspecciona el SQL y la evidencia.
# MAGIC
# MAGIC **Criterio:** una buena respuesta no es la que siempre entrega un número; es la que sabe cuándo
# MAGIC la plataforma no contiene evidencia suficiente.

# COMMAND ----------

# MAGIC %md ## 6 · Verificación y entregable

# COMMAND ----------

guardado = spark.table(f"{CATALOGO}.ai_lab.experimentos_prompt")
assert guardado.count() == 3
assert set(r.variante for r in guardado.select("variante").collect()) == {"sin_contexto", "con_limite", "grounded"}
display(guardado)
print("✅ Entregable S03: 3 variantes guardadas y comparables.")

# COMMAND ----------

# MAGIC %md ### Reto
# MAGIC Diseña una cuarta variante para “¿qué productos son más rentables?”. Debe negarse a inventar
# MAGIC rentabilidad, explicar qué dato falta y ofrecer una pregunta alternativa que sí pueda responder.
