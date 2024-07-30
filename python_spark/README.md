# Python spark
The docker compose deployes a container that has configured the spark 3.4.1 and jupyter notebooks. \
You can use this repository to learn and practice on pyspark.


# How to run it
Execute `docker-compose up -d` to build and start the container. \
The jupyter will start on the `localhosr:8888` address. \
Execute `docker logs spark-env` to get the token in order to login into the jupyter.

# Code examples
In the repository there are pyspark code examples. \
Some of them require specific jars which located under the folder `./jars` \
The jars are mounted on container under the folder `/usr/local/spark-3.4.1-bin-hadoop3/jars`
