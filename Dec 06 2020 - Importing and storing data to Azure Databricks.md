
<!-- README.md was wriiten in beautiful MacDown  -->
# Dec 06 2020 - Importing and storing data to Azure Databricks
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
</ul>
<!-- /wp:list -->

<!-- wp:paragraph -->
<p>Yesterday we started exploring the Azure services that are created when using Azure Databricks. One of the service, that I would like to explore today is storage and especially how to import and how to store data.</p>
<!-- /wp:paragraph -->



<!-- wp:paragraph -->
<p>Log in to Azure Databricks and on the main (home) site select "Create Table" under recommended common task. Don't start your cluster yet (if it's running, please terminate it for now).</p>
<!-- /wp:paragraph -->

<div>
<p>
<img src="images/img24_6_1.png"  width="500" align="center"/>
</p>
</div>

<!-- wp:paragraph -->
<p>This will prompt you a variety of actions on importing data to DBFS or connecting Azure Databricks with other services.</p>
<!-- /wp:paragraph -->


<div>
<p>
<img src="images/img25_6_2.png"  width="600" align="center"/>
</p>
</div>


<!-- wp:paragraph -->
<p>Drag the data file (available on <a rel="noreferrer noopener" href="https://github.com/tomaztk/Azure-Databricks/tree/main/data" target="_blank">Github in data folder</a>) named Day6data.csv to square for upload.  For easier understanding, let's check the CSV file schema (simple one, three columns: 1. Date (datetime format), 2. Temperature (integer format), 3. City (string format)).</p>
<!-- /wp:paragraph -->


<div>
<p>
<img src="images/img32_6_9.png"  width="200" align="center"/>
</p>
</div>

<!-- wp:paragraph -->
<p>But before you start with uploading the data, let's check the Azure resource group. I have not yet started any Databricks cluster in my workspace. And here you can see that Vnet, Storage and Network Security group will always be available for Azure Databricks service. Only when you start the cluster, additional services (IP addresses, disks, VM,...) will appear.</p>
<!-- /wp:paragraph -->

<div>
<p>
<img src="images/img26_6_3.png"  width="600" align="center"/>
</p>
</div>


<!-- wp:paragraph -->
<p>This gives us better idea where and how data is persisted. Your data will always be available and stored on blob storage. Meaning, even if you decide - not only to terminate the cluster, but to delete the cluster as well, your data will always be safely stored. Only when you add new cluster to same workspace, cluster will automatically retrieved the data from blob storage.</p>
<!-- /wp:paragraph -->

### 1. Import



<!-- wp:paragraph -->
<p>Tomorrow we will start with working our way up to other sources....</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Complete set of code and Notebooks will be available at the<a rel="noreferrer noopener" href="https://github.com/tomaztk/Azure-Databricks" target="_blank">&nbsp;Github repository</a>.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Happy Coding and Stay Healthy!</p>
<!-- /wp:paragraph -->

