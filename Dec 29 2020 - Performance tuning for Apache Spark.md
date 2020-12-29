
<!-- README.md was wriiten in beautiful MacDown  -->
# Dec 29 2020 - Performance tuning for Apache Spark

<img src="images/logo-databricks.png" align="right" width="300" />

<!-- badges: start -->
![](http://img.shields.io/badge/Azure-Databricks-red.svg)

<!-- badges: end -->

<span style="font-size: x-large; font-weight: normal;">Azure Databricks repository is 
a set of blogposts as a Advent of 2020 present to readers for easier onboarding
to Azure Databricks! </span>


<!-- wp:paragraph -->
<p>Series of Azure Databricks posts:</p>
<!-- /wp:paragraph -->

<!-- wp:list -->
<ul><li>Dec 01: <a rel="noreferrer noopener" href="https://tomaztsql.wordpress.com/2020/12/01/advent-of-2020-day-1-what-is-azure-databricks/" target="_blank">What is Azure Databricks</a></li><li>Dec 02: <a rel="noreferrer noopener" href="https://tomaztsql.wordpress.com/2020/12/02/advent-of-2020-day-2-how-to-get-started-with-azure-databricks/" target="_blank">How to get started with Azure Databricks</a></li><li>Dec 03: <a href="https://tomaztsql.wordpress.com/2020/12/03/advent-of-2020-day-3-getting-to-know-the-workspace-and-azure-databricks-platform/" target="_blank" rel="noreferrer noopener">Getting to know the workspace and Azure Databricks platform</a></li>
<li>Dec 04: <a href="https://tomaztsql.wordpress.com/2020/12/04/advent-of-2020-day-4-creating-your-first-azure-databricks-cluster/" target="_blank" rel="noreferrer noopener">Creating your first Azure Databricks cluster</a></li>
<li>Dec 05: <a href="https://tomaztsql.wordpress.com/2020/12/05/advent-of-2020-day-5-understanding-azure-databricks-cluster-architecture-workers-drivers-and-jobs/" target="_blank" rel="noreferrer noopener">Understanding Azure Databricks cluster architecture, workers, drivers and jobs</a></li>
<li>Dec 06: <a href="https://tomaztsql.wordpress.com/2020/12/06/advent-of-2020-day-6-importing-and-storing-data-to-azure-databricks/" target="_blank" rel="noreferrer noopener">Importing and storing data to Azure Databricks</a></li>
<li>Dec 07: <a href="https://tomaztsql.wordpress.com/2020/12/07/advent-of-2020-day-7-starting-with-databricks-notebooks-and-loading-data-to-dbfs/" target="_blank" rel="noreferrer noopener">Starting with Databricks notebooks and loading data to DBFS</a></li>
<li>Dec 08: <a href="https://tomaztsql.wordpress.com/2020/12/08/advent-of-2020-day-8-using-databricks-cli-and-dbfs-cli-for-file-upload/" target="_blank" rel="noreferrer noopener"> Using Databricks CLI and DBFS CLI for file upload</a></li>
<li>Dec 09: <a href="https://tomaztsql.wordpress.com/2020/12/09/advent-of-2020-day-9-connect-to-azure-blob-storage-using-notebooks-in-azure-databricks/" target="_blank" rel="noreferrer noopener">Connect to Azure Blob storage using Notebooks in  Azure Databricks</a></li>
<li>Dec 10: <a href="https://tomaztsql.wordpress.com/2020/12/10/advent-of-2020-day-10-using-azure-databricks-notebooks-with-sql-for-data-engineering-tasks/" target="_blank" rel="noreferrer noopener">Using Azure Databricks Notebooks with SQL for Data engineering tasks</a></li>
<li>Dec 11: <a href="https://tomaztsql.wordpress.com/2020/12/11/advent-of-2020-day-11-using-azure-databricks-notebooks-with-r-language-for-data-analytics/" target="_blank" rel="noreferrer noopener">Using Azure Databricks Notebooks with R Language for data analytics</a></li>
<li>Dec 12: <a href="https://tomaztsql.wordpress.com/2020/12/12/advent-of-2020-day-12-using-azure-databricks-notebooks-with-python-language-for-data-analytics/" target="_blank" rel="noreferrer noopener">Using Azure Databricks Notebooks with Python Language for data analytics</a></li>
<li>Dec 13: <a href="https://tomaztsql.wordpress.com/2020/12/13/adventof-2020-day-13-using-python-databricks-koalas-with-azure-databricks/" target="_blank" rel="noreferrer noopener">Using Python Databricks Koalas with Azure Databricks</a></li>
<li>Dec 14: <a href="https://tomaztsql.wordpress.com/2020/12/14/advent-of-2020-day-14-from-configuration-to-execution-of-databricks-jobs/" target="_blank" rel="noreferrer noopener">From configuration to execution of Databricks jobs</a></li>
<li>Dec 15: <a href="https://tomaztsql.wordpress.com/2020/12/15/advent-of-2020-day-15-databricks-spark-ui-event-logs-driver-logs-and-metrics/" target="_blank" rel="noreferrer noopener">Databricks Spark UI, Event Logs, Driver logs and Metrics</a></li>
<li>Dec 16: <a href="https://tomaztsql.wordpress.com/2020/12/16/advent-of-2020-day-16-databricks-experiments-models-and-mlflow/" target="_blank" rel="noreferrer noopener">Databricks experiments, models and MLFlow</a></li>
<li>Dec 17: <a href="https://tomaztsql.wordpress.com/2020/12/17/advent-of-2020-day-17-end-to-end-machine-learning-project-in-azure-databricks/" target="_blank" rel="noreferrer noopener">End-to-End Machine learning project in Azure Databricks</a></li>
<li>Dec 18: <a href="https://tomaztsql.wordpress.com/2020/12/18/advent-of-2020-day-18-using-azure-data-factory-with-azure-databricks/" target="_blank" rel="noreferrer noopener">Using Azure Data Factory with Azure Databricks</a></li>
<li>Dec 19: <a href="https://tomaztsql.wordpress.com/2020/12/19/advent-of-2020-day-19-using-azure-data-factory-with-azure-databricks-for-merging-csv-files/" target="_blank" rel="noreferrer noopener">Using Azure Data Factory with Azure Databricks for merging CSV files</a></li>
<li>Dec 20: <a href="https://tomaztsql.wordpress.com/2020/12/20/advent-of-2020-day-20-orchestrating-multiple-notebooks-with-azure-databricks/" target="_blank" rel="noreferrer noopener">Orchestrating multiple notebooks with Azure Databricks</a></li>
<li>Dec 21: <a href="https://tomaztsql.wordpress.com/2020/12/21/advent-of-2020-day-21-using-scala-with-spark-core-api-in-azure-databricks/" target="_blank" rel="noreferrer noopener">Using Scala with Spark Core API in Azure Databricks</a></li>
<li>Dec 22: <a href="https://tomaztsql.wordpress.com/2020/12/22/advent-of-2020-day-22-using-spark-sql-and-dataframes-in-azure-databricks/" target="_blank" rel="noreferrer noopener">Using Spark SQL and DataFrames in Azure Databricks</a></li>
<li>Dec 23: <a href="https://tomaztsql.wordpress.com/2020/12/23/advent-of-2020-day-23-using-spark-streaming-in-azure-databricks/" target="_blank" rel="noreferrer noopener">Using Spark Streaming in Azure Databricks</a></li>
<li>Dec 24: <a href="https://tomaztsql.wordpress.com/2020/12/24/advent-of-2020-day-24-using-spark-mllib-for-machine-learning-in-azure-databricks/" target="_blank" rel="noreferrer noopener">Using Spark MLlib for Machine Learning in Azure Databricks</a></li>
<li>Dec 25: <a href="https://tomaztsql.wordpress.com/2020/12/25/advent-of-2020-day-25-using-spark-graphframes-in-azure-databricks/" target="_blank" rel="noreferrer noopener">Using Spark GraphFrames in Azure Databricks</a></li>
<li>Dec 26: <a href="https://tomaztsql.wordpress.com/2020/12/26/advent-of-2020-day-26-connecting-azure-machine-learning-services-workspace-and-azure-databricks/" target="_blank" rel="noreferrer noopener">Connecting Azure Machine Learning Services Workspace and Azure Databricks</a></li>
<li>Dec 27: <a href="https://tomaztsql.wordpress.com/2020/12/27/advent-of-2020-day-27-connecting-azure-databricks-with-on-premise-environment/" target="_blank" rel="noreferrer noopener">Connecting Azure Databricks with on premise environment</a></li>

<li>Dec 28: <a href="https://tomaztsql.wordpress.com/2020/12/28/advent-of-2020-day-28-infrastructure-as-code-and-how-to-automate-script-and-deploy-azure-databricks-with-powershell/" target="_blank" rel="noreferrer noopener">Infrastructure as Code and how to automate, script and deploy Azure Databricks with Powershell</a></li>


</ul>
<!-- /wp:list -->


<!-- wp:paragraph -->
<p>Yesterday we looked into powershell automation for Azure Databricks and how one can create, updae or remove the Workspace, resource group and VNet, using deployment templates and parameters.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Today we will address the issues with Spark performance. We have talked several times about different languages available in Spark.</p>
<!-- /wp:paragraph -->


<div>
<p>
<img src="images/spark-logo.png" width="300" align="center"/>
</p>
</div>

<!-- wp:paragraph -->
<p>There are indirect and direct performance improvements that can leverage and make your Spark run faster.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
### 1.Choice of Languages
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Java versus Scala versus R versus Python (versus HiveSQL)? There is no correct or wrong answer to this choice, but there are some important differences worth mentioning. If you are running <strong>single-node</strong> machine learning the SparkR is a best option, since it has a massive machine learning ecosystem, and it has a lot of  optimised  algorithms that can handle this.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>If you are running ETL job, Spark and combination of another language (R, Python, Scala) will yield all great results. Spark's Structured API are consistent in terms of speed and stability across all the languages, so there should be almost none differences. But things get much more interesting when there are UDF (user defined functions) that can not be directly created in Structured API's. In this case, both R nor Python might not be a good idea, simply because the way Structured API manifests and transforms as RDD. In general, Python makes better choice over R when writing UDF, but probably the best way would be to write UDF in Scala (or Java), making these language jumps easier for the API interpreter.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
### 2.Choice of data presentation
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>DataFrame versus Datasets versus SQL versus RDD is another choice, yet it is fairly easy. DataFrames, Datasets and SQL objects are<strong> all equal in performance and stability </strong>(at least from Spar 2.3 and above), meaning that if you are using DataFrames in any language, performance will be the same. Again, when writing custom objects of functions (UDF), there will be some performance degradation with both R or Python, so switching to Scala or Java might be a optimisation.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Rule of thumb is, stick to DataFrames. If you go a layer down to RDD, Spark will make better optimisation and use of it than you will. Spark optimisation engine will write better RDD code than you do and with certainly less effort. And doing so, you might also loose additional Spark optimisation with new releases.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>When using RDD, try and use Scala or Java. If this is not possible, and you will be using Python or R extensively, try to use it as little as possible with RDDs. And convert to DataFrames as quickly as possible. Again, if your Spark code, application or data engineering task is not compute intensive, it should be fine, otherwise remember to use Scala or Java or convert to DataFrames. Both Python and R does not handle serialisation of RDD files optimally and runs a lot of data to and from Python or R engine, causing a lot of data movement, traffic and potentially making RDD unstable and making poor performance.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
### 3. Data Storage
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Storing data effectively is relevant when data will be read multiple times. If data will be accessed many times, either from different users in organisation or from a single user, all making data analysis, make sure to store it for effective reads. Choosing your storage, choosing the data formats and data partitioning is important. </p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>With numerous file format available, there are some key differences. If you want to optimise your Spark job, data should be stored in best possible format for this.  In general, always favour <strong>structured, binary types</strong> to store your data, especially when you are doing frequent-accessing. Although CSV files look well formatted, they are obnoxiously sparse, can have "edge" cases (missing line breaks, or other delimiters) are painfully slow to parse and hard to partition.  Same logic applies to txt and xml formats. Avro are JSON orientated and also sparse and I am not going to even talk about XML format. Spark works best with Apache Parquet stored data. Parquet format stores data in a binary files in column-orientated storage, and also track some statistics of the files, making it possible  to skip files not needed for query.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
### 4.Table partitioning and bucketing
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Table partitioning is referring to storing files in <strong>separate directories based on a partition key</strong> (e.g.: date of purchase, VAT number) such as a date field in data stored in these directories. Partitioning will help Spark skip files that are not needed for end result and it will return only the data that is in the range of the key.  There are potentials pitfalls to this techniques, one for sure is the size of these subdirectories and how to choose the right granularity. </p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Bucketing is a process of "pre-partitioning" data to allow better data joins and aggregations  operations. This will improve performance, because data can be <strong>consistently distributed across partitions</strong> as opposed all being in one partition. So if you are repeating a particular query that is joins are frequently performed on a column immediately after read, you can use bucketing to assure that data is well partitioned in accordance with those values. This will prevent shuffle before join and speed up data access.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
### 5.Parallelism
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Splittable data formats make Spark job easier to run in parallel. A ZIP or a TAR file can not be split, which means that even if you have 10 files in a ZIP file and 10 cores, only one core can read in that data, because Spark can not parallelise across ZIP file. But using GZIP, BZIP2 or LZ4 are generally splittable if (and only if) they are written by a parallel processing framework like Spark or Hadoop. </p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>In general, Spark will work best when there are two to three tasks per CPU core in your cluster when working especially with large (big) data. You can also tune the <em>spark.default.parallelism</em> property.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
### 6.Number of files
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>With numerous small files you will for sure pay a price for listing and fetching all the data. There is no golden rule on number of files and the size of the files per directory. But there are some directions. Multiple small files will is going to make a schedule worked harder to locate the data and launch all the read tasks. This can increase not only disk I/O but also network traffic. On the other spectrum, having fewer and larger files can ease the workload from scheduler, but it will make tasks longer to run. Again, a rule of thumb would be, to scope the <strong>size of the files  in such way, that they contain a few tens of megabyte of data</strong>. From Spark 2.2. onward there are also possibilities to to partitioning and sizing optionally.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
### 7. Temporary data storage
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Data that will be reused constantly are great candidates for caching. Caching will place a DataFrame, Dataset, SQL table or RDD into temporary storage (either memory or disk) across the executors in your cluster. You might want to cache only dataset that will be used several times later on, but should not be hastened, because it takes also resources such as serialisation, deserialisation and storage costs. You can tell Spark to cache data by using a cache command on DataFrames or RDD's.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Let's put this to the test. In Azure Databricks create a new notebook: <em>Day29_tuning</em> and language: <em>Python</em> and attach the notebook to your cluster. Load a sample CSV file:</p>
<!-- /wp:paragraph -->

<!-- wp:syntaxhighlighter/code -->
<pre class="wp-block-syntaxhighlighter-code">%python
DF1 = spark.read.format("CSV")
       .option("inferSchema", "true")
       .option("header","true")
       .load("dbfs/databricks-datasets/COVID/covid-19-data/us-states.csv")</pre>
<!-- /wp:syntaxhighlighter/code -->

<!-- wp:paragraph -->
<p>The bigger the files, the more evident the difference will be. Create some aggregations:</p>
<!-- /wp:paragraph -->

<!-- wp:syntaxhighlighter/code -->
<pre class="wp-block-syntaxhighlighter-code">DF2 = DF1.groupby("state").count().collect()
DF3 = DF1.groupby("date").count().collect()
DF4 = DF1.groupby("cases").count().collect()</pre>
<!-- /wp:syntaxhighlighter/code -->

<!-- wp:paragraph -->
<p>After you have tracked the timing, now, let's cache the DF1:</p>
<!-- /wp:paragraph -->

<!-- wp:syntaxhighlighter/code -->
<pre class="wp-block-syntaxhighlighter-code">DF1.cache()
DF1.count()</pre>
<!-- /wp:syntaxhighlighter/code -->

<!-- wp:paragraph -->
<p>And rerun the previous command:</p>
<!-- /wp:paragraph -->

<!-- wp:syntaxhighlighter/code -->
<pre class="wp-block-syntaxhighlighter-code">DF2 = DF1.groupby("state").count().collect()
DF3 = DF1.groupby("date").count().collect()
DF4 = DF1.groupby("cases").count().collect()</pre>
<!-- /wp:syntaxhighlighter/code -->

<!-- wp:paragraph -->
<p>And you should see the difference in results. As mentioned before, the bigger the dataset, the bigger would be time gained back when caching data.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>We have touched today couple of performance tuning points and what approach one should take, to improve the work of Spark in Azure Databricks. These are probably the most frequent performance tunings and relatively easy to adjust.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Tomorrow we will look further into  Apache Spark.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Complete set of code and the Notebook is available at the<a rel="noreferrer noopener" href="https://github.com/tomaztk/Azure-Databricks" target="_blank">&nbsp;Github repository</a>.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Happy Coding and Stay Healthy!</p>
<!-- /wp:paragraph -->