# Databricks notebook source
# MAGIC %md 
# MAGIC SCD Type 2 tracks historical data by creating multiple records for a given natural key in the dimensional tables. 
# MAGIC This notebook demonstrates how to perfom SCD Type 2 operation using `MERGE` operation. 
# MAGIC Suppose a company is maintaining a table with the customers and their address, and they want to maintain a history of 
# MAGIC all the addresses a customer has had along with the date ranges when each address was valid.
# MAGIC Let's define the schema using Scala case classes. 

# COMMAND ----------

# MAGIC %md **Customers Delta table**
# MAGIC 
# MAGIC This is the slowly changing table that we want to update. For every customer, there many any number of addresses. But for each address, there is range of dates, `effectiveDate` to `endDate`, in which that address was effectively the current address. In addition, there is another field `current` which is true for the address that is currently valid for each customer. That is, there is only 1 address and 1 row for each customer where `current` is `true`, for every other row, it is false.

# COMMAND ----------

# MAGIC %python
# MAGIC # // import java.sql.Date
# MAGIC # // import java.text._
# MAGIC # // import spark.implicits
# MAGIC 
# MAGIC # // case class CustomerUpdate(customerId: Int, address: String, effectiveDate: Date)
# MAGIC # // case class Customer(customerId: Int, address: String, current: Boolean, effectiveDate: Date, endDate: Date)
# MAGIC 
# MAGIC # // implicit def date(str: String): Date = Date.valueOf(str)
# MAGIC 
# MAGIC // sql("drop table if exists customers")
# MAGIC 
# MAGIC Seq(
# MAGIC   Customer(1, "old address for 1", false, null, "2018-02-01"),
# MAGIC   Customer(1, "current address for 1", true, "2018-02-01", null),
# MAGIC   Customer(2, "current address for 2", true, "2018-02-01", null),
# MAGIC   Customer(3, "current address for 3", true, "2018-02-01", null)
# MAGIC ).toDF().write.format("delta").mode("overwrite").saveAsTable("customers")
# MAGIC 
# MAGIC display(table("customers").orderBy("customerId"))

# COMMAND ----------

# MAGIC %md **Updates table**
# MAGIC 
# MAGIC This is updates table that has the new addresses. For each customer, it has the new address the date from which it is effective.
# MAGIC Note that, for convenience we are using the same case class and ignore the fields `current` and `endDate`, they are not used.
# MAGIC This table must have one row per customer and the effective date correctly set.

# COMMAND ----------

# MAGIC %scala
# MAGIC Seq(
# MAGIC   CustomerUpdate(1, "new address for 1", "2018-03-03"),
# MAGIC   CustomerUpdate(3, "current address for 3", "2018-04-04"),    // new address same as current address for customer 3
# MAGIC   CustomerUpdate(4, "new address for 4", "2018-04-04")
# MAGIC ).toDF().createOrReplaceTempView("updates")
# MAGIC 
# MAGIC // Note: 
# MAGIC // - Make sure that the effectiveDate is set in the source data, because this is what will be copied to the customers table after SCD Type 2 Merge
# MAGIC // - Make sure that there is only one row per customer.
# MAGIC 
# MAGIC display(table("updates"))

# COMMAND ----------

# MAGIC %md **Merge statement to perform SCD Type 2**
# MAGIC 
# MAGIC This merge statement simultaneously does both for each customer in the source table. 
# MAGIC - Inserts the new address with its `current` set to true, and  
# MAGIC - Updates the previous current row to set `current` to false, and update the `endDate` from `null` to the `effectiveDate` from the source.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT updates.customerId as mergeKey, updates.*
# MAGIC   FROM updates
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- These rows will INSERT new addresses of existing customers 
# MAGIC   -- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTed.
# MAGIC   SELECT NULL as mergeKey, updates.*
# MAGIC   FROM updates JOIN customers
# MAGIC   ON updates.customerid = customers.customerid 
# MAGIC   WHERE customers.current = true AND updates.address <> customers.address 
# MAGIC   

# COMMAND ----------

# DBTITLE 1,SQL example
# MAGIC %sql 
# MAGIC 
# MAGIC -- ========================================
# MAGIC -- Merge SQL API is available since DBR 5.1
# MAGIC -- ========================================
# MAGIC 
# MAGIC MERGE INTO customers
# MAGIC USING (
# MAGIC    -- These rows will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
# MAGIC   SELECT updates.customerId as mergeKey, updates.*
# MAGIC   FROM updates
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- These rows will INSERT new addresses of existing customers 
# MAGIC   -- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTed.
# MAGIC   SELECT NULL as mergeKey, updates.*
# MAGIC   FROM updates JOIN customers
# MAGIC   ON updates.customerid = customers.customerid 
# MAGIC   WHERE customers.current = true AND updates.address <> customers.address 
# MAGIC   
# MAGIC ) staged_updates
# MAGIC ON customers.customerId = mergeKey
# MAGIC WHEN MATCHED AND customers.current = true AND customers.address <> staged_updates.address THEN  
# MAGIC   UPDATE SET current = false, endDate = staged_updates.effectiveDate    -- Set current to false and endDate to source's effective date.
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT(customerid, address, current, effectivedate, enddate) 
# MAGIC   VALUES(staged_updates.customerId, staged_updates.address, true, staged_updates.effectiveDate, null) -- Set current to true along with the new address and its effective date.

# COMMAND ----------

# DBTITLE 1,PYTHON example
# MAGIC %python
# MAGIC # // ==========================================
# MAGIC # // Merge  API is available since DBR 6.0
# MAGIC # // ==========================================
# MAGIC 
# MAGIC from delta.tables import *
# MAGIC import pyspark.sql.functions as f
# MAGIC 
# MAGIC customersTable = DeltaTable.forName(spark,"customers")
# MAGIC 
# MAGIC updatesDF = table("updates")          
# MAGIC 
# MAGIC # // Rows to INSERT new addresses of existing customers
# MAGIC newAddressesToInsert = updatesDF.alias("updates") \
# MAGIC   .join(customersTable.toDF().alias("customers"), "customerid") \
# MAGIC   .where("customers.current = true AND updates.address <> customers.address")
# MAGIC 
# MAGIC # // Stage the update by unioning two sets of rows
# MAGIC # // 1. Rows that will be inserted in the `whenNotMatched` clause
# MAGIC # // 2. Rows that will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
# MAGIC stagedUpdates = newAddressesToInsert\
# MAGIC   .selectExpr("NULL as mergeKey", "updates.*").union(\
# MAGIC     updatesDF.selectExpr("updates.customerId as mergeKey", "*")  ###Rows for 2.
# MAGIC   )
# MAGIC 
# MAGIC # // Apply SCD Type 2 operation using merge
# MAGIC customersTable\
# MAGIC   .alias("customers")\
# MAGIC   .merge(\
# MAGIC     stagedUpdates.alias("staged_updates"),\
# MAGIC     "customers.customerId = mergeKey")\
# MAGIC   .whenMatchedUpdate(condition = "customers.current = true AND customers.address <> staged_updates.address",
# MAGIC   set = {                                      # Set current to false and endDate to source's effective date.
# MAGIC     "current": "false",
# MAGIC     "endDate": "staged_updates.effectiveDate"
# MAGIC   }).whenNotMatchedInsert(
# MAGIC   values = {
# MAGIC     "customerid": "staged_updates.customerId",
# MAGIC     "address": "staged_updates.address",
# MAGIC     "current": "true",
# MAGIC     "effectiveDate": "staged_updates.effectiveDate",  # Set current to true along with the new address and its effective date.
# MAGIC     "endDate": "null"
# MAGIC   }
# MAGIC ).execute()

# COMMAND ----------

# MAGIC %md **Updated Customers table**
# MAGIC 
# MAGIC - For customer 1, previous address was update as `current = false` and new address was inserted as `current = true`.
# MAGIC - For customer 2, there was no update.
# MAGIC - For customer 3, the new address was same as previous address, so no update was made.
# MAGIC - For customer 4, new address was inserted.

# COMMAND ----------

# MAGIC %scala
# MAGIC display(table("customers").orderBy("customerId", "current", "endDate"))
