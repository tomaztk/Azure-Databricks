
<!-- README.md was wriiten in beautiful MacDown  -->
# Dec 07 2020 - Starting with Databricks notebooks and loading data to DBFS

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

</ul>
<!-- /wp:list -->

<!-- wp:paragraph -->
<p>Yesterday we started working towards data import and how to use drop zone to import data to DBFS. We have also created our first Notebook and this is where I would like to start today. With a light introduction to notebooks.</p>
<!-- /wp:paragraph -->


<!-- wp:paragraph -->
### What are Notebooks?

<!-- wp:paragraph -->
<p>Notebook is a powerful text document that integrated interactive computing environment for data engineers, data scientists and machine learning engineers. It supports multiple kernels (compute environments) and multiple languages. Most common Notebook is Jupyter Notebook, name suggesting and providing acronyms for Julia, Python and R. Usually a notebook will consist of a text, rich text, HTML, figures, photos, videos, and all sorts of engineering blocks of code or text. These blocks of code can be executed, since the notebooks are part of client-server web application.  Azure Databricks notebooks are type of ipynb notebooks, same format as the Jupyter notebooks. Databricks environment provides client and server and you do not have to worry about installation not setup. Once the Databrick cluster is up and running, you are good to go.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>On your home screen, select "New Notebook"</p>
<!-- /wp:paragraph -->
<div>
<p>
<img src="images/img40_7_1.png" width="500" align="center"/>
</p>
</div>


<!-- wp:paragraph -->
<p>and give it a Name, Language and Cluster.</p>
<!-- /wp:paragraph -->

<div>
<p>
<img src="images/img41_7_2.png" width="400" align="center"/>
</p>
</div>

<!-- wp:paragraph -->
<p>Databricks notebooks are support multi languages and you can seaminglessly switch the language in the notebook, without the need to switching the languange. If the notebooks are instructions of operations and what to do, is the cluster the engine that will execute all the instructions. Select cluster, that you have created on <a rel="noreferrer noopener" href="https://tomaztsql.wordpress.com/2020/12/04/advent-of-2020-day-4-creating-your-first-azure-databricks-cluster/" target="_blank">Day 4</a>. I am inserting following:</p>
<!-- /wp:paragraph -->

<!-- wp:table -->
<figure class="wp-block-table"><table><tbody><tr><td>Name:</td><td>Day7_DB_Py_Notebook</td></tr><tr><td>Default Language:</td><td>Python</td></tr><tr><td>Cluster:</td><td>databricks_cl1_standard</td></tr></tbody></table></figure>
<!-- /wp:table -->

<!-- wp:paragraph -->
<p>If your clusters are not started, you can still create a notebook and later attach selected cluster to notebook.</p>
<!-- /wp:paragraph -->