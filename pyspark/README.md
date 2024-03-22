# Spark
This repository contains notes and code-examples related to pyspark for both batch and streaming workloads.

## How to start
The `docker-compose` file initiate a container with jupyter and spark `3.4.1` installed.\
The container is created in the network `dataeng-data-platform` to be compatiple with [`other data infru`](https://github.com/gkosia-dataeng/data-platform-infru).

Start the `SparkSession` on local master to play around with single node or start the [`cluster mode`](https://github.com/gkosia-dataeng/data-platform-infru/tree/main/spark-cluster-mode) which will create a cluster with a driver and two worker nodes.


Datasets can be found [here](https://github.com/subhamkharwal/pyspark-zero-to-hero/tree/master/datasets)