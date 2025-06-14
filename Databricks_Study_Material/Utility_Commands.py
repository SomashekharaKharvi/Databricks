# Databricks notebook source
dbutils.notebook.run("/Users/somashekharakharvi@gmail.com/Python", timeout_seconds=60, arguments={"arg1": "value1"})


# COMMAND ----------

current_notebook_path = dbutils.notebook.getContext().notebookPath.get()

# COMMAND ----------

dbutils.widgets.text("widget_name", "default_value", "Widget Label")
dbutils.widgets.text("widget_name1", "default_value", "Widget Label")

# COMMAND ----------

val=dbutils.widgets.get("widget_name1")
val

# COMMAND ----------

dbutils.widgets.dropdown("my_dropdown", "option2", ["option1", "option2", "option3"], "Select an Option")


# COMMAND ----------

# dbutils.widgets.remove('my_dropdown')

# COMMAND ----------

dbutils.library.restartPython()  # Restart the Python process

# COMMAND ----------

cluster_id = dbutils.cluster.getClusterId()

# COMMAND ----------

dbutils.secrets.createScope("my_scope")

# COMMAND ----------

databricks secrets create-scope --scope my-scope

# COMMAND ----------

# Create a secret scope (this is done via CLI or UI, not in the notebook)
# databricks secrets create-scope --scope my-scope

# Add a secret to the scope
dbutils.secrets.put(scope="my-scope", key="my-secret-key", string_value="my-secret-value")

# Retrieve the secret
secret_value = dbutils.secrets.get(scope="my-scope", key="my-secret-key")
print(secret_value)  # Output will be 'my-secret-value'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.df_g
