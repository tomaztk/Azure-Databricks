// Databricks notebook source
// MAGIC %md # Spark with Scala

// COMMAND ----------

// MAGIC %md Spark with Scala examples.

// COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets"))

// COMMAND ----------

// Take a look at the file system
display(dbutils.fs.ls("/databricks-datasets/samples/population-vs-price"))

// COMMAND ----------

// Take a look at the file system
display(dbutils.fs.ls("/databricks-datasets/samples/docs/"))

// COMMAND ----------

// transformation
val textFile = spark.read.textFile("/databricks-datasets/samples/docs/README.md")

// COMMAND ----------

textFile.show()

// COMMAND ----------

textFile.count()

// COMMAND ----------

// show the first line
textFile.first()

// COMMAND ----------

// show all the lines with word Sudo
val linesWithSudo = textFile.filter(line => line.contains("sudo"))

// COMMAND ----------

linesWithSudo.show()

// COMMAND ----------

// check how many lines
linesWithSudo.count()

// COMMAND ----------

// MAGIC %md Scala can use "chaining" in order to execute the command; or map-reduce formula. This following command will:
// MAGIC - collect
// MAGIC - filter
// MAGIC - output (for each line)

// COMMAND ----------

// Output the all four lines
linesWithSudo.collect().take(4).foreach(println)

// COMMAND ----------

// MAGIC %md And example of map-reduce formula. This following command will:
// MAGIC - map - get all the lines
// MAGIC - map - split the words
// MAGIC - map - get the number of words (or spaces)
// MAGIC - reduce - compare the lines to reduce it to the largest one (or to find the largest one)

// COMMAND ----------

// find the lines with most words
textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)

// COMMAND ----------

// MAGIC %md Same functionality can be achieved

// COMMAND ----------

import java.lang.Math
textFile.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))

// COMMAND ----------

// MAGIC %md ### Creating dataset
// MAGIC Now let's create a dataset (remember the difference between dataset and dataframe) and load some data from /databricks-datasets folder

// COMMAND ----------

val df = spark.read.json("/databricks-datasets/samples/people/people.json")

// COMMAND ----------

df.show(1)

// COMMAND ----------

df.count()

// COMMAND ----------

// MAGIC %md ### Convert dataset to DataFrame
// MAGIC Now can convert data directly to DataFrame

// COMMAND ----------

// First, define a case class that represents a type-specific Scala JVM Object
case class Person (name: String, age: Long)

// COMMAND ----------

val ds = spark.read.json("/databricks-datasets/samples/people/people.json").as[Person]

// COMMAND ----------

ds.show()

// COMMAND ----------

// MAGIC %md Add another dataset

// COMMAND ----------

// define a case class that represents the device data.
case class DeviceIoTData (
  battery_level: Long,
  c02_level: Long,
  cca2: String,
  cca3: String,
  cn: String,
  device_id: Long,
  device_name: String,
  humidity: Long,
  ip: String,
  latitude: Double,
  longitude: Double,
  scale: String,
  temp: Long,
  timestamp: Long
)

// COMMAND ----------

// read the JSON file and create the Dataset from the ``case class`` DeviceIoTData
// ds is now a collection of JVM Scala objects DeviceIoTData
val ds = spark.read.json("/databricks-datasets/iot/iot_devices.json").as[DeviceIoTData]

// COMMAND ----------

ds.show()

// COMMAND ----------

// MAGIC %md ### display()
// MAGIC You can also view the dataset using display()

// COMMAND ----------

display(ds)

// COMMAND ----------

// MAGIC %md or by using the standard Spark commands, take() and foreach() to print the dataset. With take, I am printing out first 5 rows

// COMMAND ----------

ds.take(5).foreach(println(_))

// COMMAND ----------

// MAGIC %md ### describe()
// MAGIC And also describe() function is great for exploring the data and the structure

// COMMAND ----------

ds.describe()

// COMMAND ----------

display(ds.describe())

// COMMAND ----------

// MAGIC %md or get more descriptive statistics over one particular column

// COMMAND ----------

display(ds.describe("c02_level"))

// COMMAND ----------

// MAGIC %md Now let's play with the dataset using Scala Dataset API with following functions
// MAGIC - sum(), 
// MAGIC - select(), 
// MAGIC - avg(), 
// MAGIC - filter(), 
// MAGIC - map(), 
// MAGIC - groupBy(),
// MAGIC - join(), and 
// MAGIC - union(). 

// COMMAND ----------

// MAGIC %md ### sum()
// MAGIC Let's sum all c02_level, but first display the column

// COMMAND ----------

display(ds.select("c02_level"))

// COMMAND ----------

//create a variable sum_c02_1 and sum_c02_2; 
// both are correct and return same results

val sum_c02_1 = ds.select("c02_level").groupBy().sum()
val sum_c02_2 = ds.groupBy().sum("c02_level")

display(sum_c02_1)

// COMMAND ----------

display(sum_c02_2)

// COMMAND ----------

// MAGIC %md Double check with SQL, just because it is fun.
// MAGIC But first We need to create a SQL view (or it could be a table) from this dataset.

// COMMAND ----------

ds.createOrReplaceTempView("SQL_iot_table")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT sum(c02_level) as Total_c02_level FROM SQL_iot_table

// COMMAND ----------

// MAGIC %md ### select()
// MAGIC Let's show only the columns we want

// COMMAND ----------

// Both will return same results
ds.select("cca2","cca3", "c02_level").show()
// or
display(ds.select("cca2","cca3","c02_level"))

// COMMAND ----------

// MAGIC %md ### avg()
// MAGIC Let's aggregate c02_level with avg() over all the countries in variable cca3

// COMMAND ----------

val avg_c02 = ds.groupBy().avg("c02_level")

display(avg_c02)

// COMMAND ----------

val avg_c02_byCountry = ds.groupBy("cca3").avg("c02_level")

display(avg_c02_byCountry)

// COMMAND ----------

// MAGIC %md ### filter()
// MAGIC Let's filter some data

// COMMAND ----------

display(ds.filter(d => d.battery_level > 7)) 
// ||  d.cca3 == "JPN"))

// COMMAND ----------

display(ds.filter(d => d.battery_level > 7).select("battery_level", "c02_level", "cca3")) 

// COMMAND ----------

// MAGIC %md ### groupBy()
// MAGIC Adding aggregation to filtered data (**avg()** function) and grouping dataset based on cca3 variable

// COMMAND ----------

display(ds.filter(d => d.battery_level > 7).select("c02_level", "cca3").groupBy("cca3").avg("c02_level"))

// COMMAND ----------

// MAGIC %md ### join()
// MAGIC We will create two DataFrames and create a join between two objects

// COMMAND ----------

val df_1 = Seq((0, "Tom"), (1, "Jones")).toDF("id", "first")
val df_2 = Seq((0, "Tom"), (2, "Jones"), (3, "Martin")).toDF("id", "second")

display(df_1)

// COMMAND ----------

display(df_1.join(df_2, "id"))

// COMMAND ----------

// MAGIC %md Now examine the execution plan of this join

// COMMAND ----------

df_1.join(df_2, "id").explain

// COMMAND ----------

// MAGIC %md ... and also create left/right join

// COMMAND ----------

df_1.join(df_2, Seq("id"), "LeftOuter").show

// COMMAND ----------

df_1.join(df_2, Seq("id"), "RightOuter").show

// COMMAND ----------

// MAGIC %md ### union()
// MAGIC To combine two datasets (or DataFrames), use union() 

// COMMAND ----------

 val df3 = df_1.union(df_2)
 
display(df3) 
// df3.show(true)

// COMMAND ----------

// MAGIC %md ### distinct()
// MAGIC If you want to combine without duplicated use distinct()

// COMMAND ----------

display(df3.distinct())
