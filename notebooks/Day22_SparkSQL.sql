-- Databricks notebook source
-- MAGIC %md #Spark SQL and DataFrames

-- COMMAND ----------

SELECT "Hello!"

-- COMMAND ----------

-- MAGIC %md ## Loading data

-- COMMAND ----------

-- MAGIC %md Check the folder path and select data_geo.csv file

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC display(dbutils.fs.ls("/databricks-datasets/samples/population-vs-price"))

-- COMMAND ----------

-- MAGIC %md ### Loading data using Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data = spark.read.csv("/databricks-datasets/samples/population-vs-price/data_geo.csv", header="true", inferSchema="true")
-- MAGIC data.cache() # Cache data for faster reuse
-- MAGIC data = data.dropna() # drop rows with missing values

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data.createOrReplaceTempView("data_geo_py")

-- COMMAND ----------

SELECT * FROM data_geo_py LIMIT 10

-- COMMAND ----------

-- MAGIC %md ### Loading data using SQL

-- COMMAND ----------

-- MAGIC %md Using databricks.spark.csv function with SQL

-- COMMAND ----------

DROP TABLE IF EXISTS data_geo;

CREATE TABLE data_geo
USING com.databricks.spark.csv
OPTIONS (path "/databricks-datasets/samples/population-vs-price/data_geo.csv", header "true", inferSchema "true")

-- COMMAND ----------

SELECT * FROM data_geo LIMIT 10

-- COMMAND ----------

-- MAGIC %md ### Loading data using R

-- COMMAND ----------

-- MAGIC %r 
-- MAGIC library(SparkR)
-- MAGIC data_geo_r <- read.df("/databricks-datasets/samples/population-vs-price/data_geo.csv", source = "csv", header="true", inferSchema = "true")
-- MAGIC registerTempTable(data_geo_r, "data_geo_r")

-- COMMAND ----------

cache table data_geo_r

-- COMMAND ----------

SELECT * FROM data_geo_r LIMIT 10

-- COMMAND ----------

-- MAGIC %md ## Viewing Data / DataFrame

-- COMMAND ----------

-- MAGIC %md Using a SELECT statement

-- COMMAND ----------

SELECT City
,`2014 Population estimate` 
,`2015 median sales price`
,`State Code` AS State_Code
FROM data_geo 
WHERE `State Code` = 'AZ';

-- COMMAND ----------

-- MAGIC %md Using union to combine all three datasets deriving from R, Python and SQL

-- COMMAND ----------

SELECT *, 'data_geo_SQL' AS dataset  FROM data_geo
UNION
SELECT *, 'data_geo_Python' AS dataset  FROM data_geo_py
UNION 
SELECT *, 'data_geo_R' AS dataset FROM data_geo_r
ORDER BY `2014 rank`, dataset
LIMIT 12

-- COMMAND ----------

-- MAGIC %md ## Running SQL 

-- COMMAND ----------

-- MAGIC %md ### Date and Time

-- COMMAND ----------

SELECT
 CURRENT_TIMESTAMP() AS now
,date_format(CURRENT_TIMESTAMP(), "L") AS Month_
,date_format(CURRENT_TIMESTAMP(), "LL") AS Month_LeadingZero
,date_format(CURRENT_TIMESTAMP(), "y") AS Year_
,date_format(CURRENT_TIMESTAMP(), "d") AS Day_
,date_format(CURRENT_TIMESTAMP(), "E") AS DayOFTheWeek
,date_format(CURRENT_TIMESTAMP(), "H") AS Hour
,date_format(CURRENT_TIMESTAMP(), "m") AS Minute
,date_format(CURRENT_TIMESTAMP(), "s") AS Second


-- COMMAND ----------

-- MAGIC %md ### Built-in functions

-- COMMAND ----------

SELECT 
 COUNT(*) AS Nof_rows
,SUM(`2014 rank`) AS Sum_Rank
,AVG(`2014 rank`) AS Avg_Rank
,SUM(CASE WHEN `2014 rank` > 150 THEN 1 ELSE -1 END) AS Sum_case
,STD(`2014 rank`) as stdev
,MAX(`2014 rank`) AS Max_Val
,MIN(`2014 rank`) AS Min_Val
,KURTOSIS (`2014 rank`) as Kurt
,SKEWNESS(`2014 rank`) AS Skew
,CAST(SKEWNESS(`2014 rank`) AS INT) AS Skew_cast
 FROM data_geo

-- COMMAND ----------

-- MAGIC %md ### SELECT INTO

-- COMMAND ----------

DROP TABLE IF EXISTS tmp_data_geo;
CREATE TABLE tmp_data_geo (`2014 rank` INT, State VARCHAR(64), `State Code` VARCHAR(2));


INSERT INTO tmp_data_geo
     FROM data_geo SELECT 
                  `2014 rank`
                  ,State
                  ,`State Code`
WHERE   `2014 rank` >= 50 AND `2014 rank` < 60 AND `State Code` = "CA";


SELECT * FROM tmp_data_geo;

-- COMMAND ----------

-- MAGIC %md ### JOIN

-- COMMAND ----------

SELECT 
  dg1.`State Code`
 ,dg1.`2014 rank`
 ,dg2.`State Code`
 ,dg2.`2014 rank`
FROM data_geo AS dg1
JOIN data_geo AS dg2
ON dg1.`2014 rank` = dg2.`2014 rank`+1
AND dg1.`State Code` = dg2.`State Code`
WHERE dg1.`State Code` = "CA"

-- COMMAND ----------

-- MAGIC %md ### Common table expressions

-- COMMAND ----------

WITH cte AS (
    SELECT * FROM data_geo 
    WHERE `2014 rank` >= 50 AND `2014 rank` < 60
)
SELECT * FROM cte;

-- COMMAND ----------

-- MAGIC %md ### Inline tables

-- COMMAND ----------

SELECT * FROM  VALUES ("WA", "Seattle"), ("WA", "Tacoma"), ("WA", "Spokane") AS data(StateName, CityName)

-- COMMAND ----------

-- MAGIC %md ### EXISTS

-- COMMAND ----------

WITH cte AS (
    SELECT * FROM data_geo 
    WHERE `2014 rank` >= 50 AND `2014 rank` < 60
)
SELECT * 
FROM data_geo as dg
WHERE
  EXISTS (SELECT * FROM cte WHERE cte.city = dg.city)
AND NOT EXISTS (SELECT * FROM cte WHERE cte.city = dg.city AND `2015 median sales price` IS NULL )

-- COMMAND ----------

-- MAGIC %md ### Window functions

-- COMMAND ----------

SELECT 
 City
,State
,RANK() OVER (PARTITION BY State ORDER BY `2015 median sales price`) AS rank 
,`2015 median sales price` AS MedianPrice
FROM data_geo
WHERE
  `2015 median sales price` IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md ## Exploring visuals

-- COMMAND ----------

-- MAGIC %md ### Using Map

-- COMMAND ----------

select `State Code`, `2015 median sales price` from data_geo

-- COMMAND ----------

-- MAGIC %md ### Using box plots

-- COMMAND ----------

SELECT 
  `State Code`
 , `2015 median sales price` 
FROM data_geo_SQL
ORDER BY `2015 median sales price` DESC;
