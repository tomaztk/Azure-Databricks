
<!-- README.md was wriiten in beautiful MacDown  -->
# Dec 08 2020 - Using Databricks CLI and DBFS CLI for file upload

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

</ul>
<!-- /wp:list -->

<!-- wp:paragraph -->
<p>Yesterday we worked toward using notebooks and how to read data using notebooks.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Today we will check Databricks CLI and look into how you can use CLI to upload (copy) files from your remote server to DBFS.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Databricks CLI is a command-line interface (CLI)  that provides an easy-to-use interface to the Databricks platform. Databricks CLI is from group of developer tools and should be easy to setup and straightforward to use. You can automate many of the tasks with CLI.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
### 1.Installing the CLI


<!-- wp:paragraph -->
<p>Using Python 3.6 (or above), run the following pip command in CMD:</p>
<!-- /wp:paragraph -->

<!-- wp:syntaxhighlighter/code -->
<pre class="wp-block-syntaxhighlighter-code">pip3 install databricks-cli</pre>
<!-- /wp:syntaxhighlighter/code -->

<!-- wp:paragraph -->
<p>But before using CLI, Personal access token needs to be created for authentication.</p>
<!-- /wp:paragraph -->

### 2. Authentication with Personal Access Token


<div>
<p>
<img src="images/img_7_1.png" width="500" align="center"/>
</p>
</div>


e
e




e