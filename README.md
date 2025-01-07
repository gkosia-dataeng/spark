# Description
The repository contains notes and code related to data processing with Spark.


## Repository structure
[PySpark](./pyspark/): contains the Docker resources to spin-up the working environment, subfolders under `/pyspark` are mounted as volumes on the container and they contain code examples \
[Articles for Spark](./articles/): contains notes and links from articles about Spark.

## References:

[Batch](https://www.youtube.com/playlist?list=PL2IsFZBGM_IHCl9zhRVC1EXTomkEp_1zm) \
[Structured streaming](https://www.youtube.com/playlist?list=PL2IsFZBGM_IEtp2fF5xxZCS9CYBSHV2WW) \
[Delta](https://www.youtube.com/playlist?list=PL2IsFZBGM_IExqZ5nHg0wbTeiWVd8F06b)


## How to start
Change folder to `/pyspark`
The `docker-compose` file initiate a container with jupyter and spark `3.4.1` installed.\
The container is created in the network `dataeng-data-platform` to be compatiple with [`other data infru`](https://github.com/gkosia-dataeng/data-platform-infru).


# Next to learn
  PySpark Series -- Basics to Advanced: https://subhamkharwal.medium.com/learnbigdata101-spark-series-940160ff4d30
	Spark performance - spark-experiments: https://github.com/afaqueahmad7117/spark-experiments
	Databricks full playlist: https://www.youtube.com/playlist?list=PL2IsFZBGM_IGiAvVZWAEKX8gg1ItnxEEb
