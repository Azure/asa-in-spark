# Table of contents

<!--ts-->
   * [Introduction](#introduction)
   * [How it works?](#how-it-works)
   * [Quick Start](#quick-start)
   * [How to generate the jar?](#how-to-generate-the-jar)
   * [Supported ASA Queries](#supported-asa-queries)
   * [Dependencies](#dependencies)
   * [Troubleshooting](#troubleshooting)
   * [Few helpful docker commands](#few-helpful-docker-commands)
   * [Example](#example)
<!--te-->

## Introduction
ASASpark is a POC to make ASA query engine work with Apache Spark. Queries are written in ASA Query Language.

## How it works?
A Spark SQL Job running on a Spark Cluster in Databricks uses ASASpark-x.x.x.jar as a library to interpret query written in ASA query language and run it on Spark dataset. 
Before you write your own query do check out the [Supported ASA Queries](#supported-asa-queries) section.

## Quick Start 
1. Get the ASA-x.x.x.jar by following any one of the following steps:
   
   a. Using source code: Follow steps mentioned in [How to generate the jar?](#how-to-generate-the-jar) section (Only for Microsoft users)
   
   b. Download prebuilt jar from [here](Todo: add link)
2. Configure Azure Databricks and Spark Cluster: 
    
    a. [Create Azure Databricks workspace](https://docs.microsoft.com/en-us/azure/azure-databricks/quickstart-create-databricks-workspace-portal#create-an-azure-databricks-workspace)
    
    b. [Create a Spark cluster in Databricks](https://docs.microsoft.com/en-us/azure/azure-databricks/quickstart-create-databricks-workspace-portal#create-a-spark-cluster-in-databricks)
    
    c. Once the cluster is deployed, upload the ASA-x.x.x.jar as a library:
      - Click on `<cluster_name> -> Libraries -> Install New` to open a modal/pop up.
      - Select `Library Source` as `Upload`, `Library Type` as `Jar` and click on `Drop JAR here` to select the jar from your local filesystem. 
      - Once uploaded, click on the button `Install` to close the modal/pop up and verify that the `status` is `Installed`.

3. Run example using [Example](#example) Section 

## How to generate the jar?

1. Clone the repo

```
$ git clone https://msdata.visualstudio.com/DefaultCollection/Azure%20Stream%20Analytics/_git/ASASpark
```

2. Install Docker
   
   a. [For Linux (Ubuntu) on Windows installed from Microsoft Store](https://medium.com/@sebagomez/installing-the-docker-client-on-ubuntus-windows-subsystem-for-linux-612b392a44c4) 
   
   b. [For others use the official documentation](https://docs.docker.com/install/) 

3. At the root of the repository build docker image which will contain the required jar
```
$ docker build -t asa-spark:3.1-bionic .
Note: Open URL in a browser and enter the code when prompted for auth by Azure Artifact Credential Provider
```
4. Start a docker container from the recently built docker image
```
$ docker run --name asa-spark -d -it asa-spark:3.1-bionic
```
5. Copy the required jar from the container to the local filesystem
```
$ docker cp asa-spark:/repos/ASASpark/java/target/ASA-0.0.1.jar .
```
6. Cleanup: stop the running container, remove it and delete the image
```
$ docker stop asa-spark
$ docker rm asa-spark
$ docker rmi asa-spark:3.1-bionic
```

## Supported ASA Queries
All other queries are supported except:
1. Nested data types: Array and Record are not yet supported out of the [the complete list of Data Types](https://docs.microsoft.com/en-us/stream-analytics-query/data-types-azure-stream-analytics) supported in ASA. Unsupported data type exception will be thrown on encountering any input or output of these data types.
2. Join Query (multiple inputs) is not yet supported. 

## Dependencies
Docker is the only dependency for the developer. Docker takes care of all other dependencies listed below:
1. g++
2. Java 8
   a. cmake uses `create_javah` which is deprecated in Java 9 onwards
   b. Debian 10 discontinued Java 8 support on official repository while Bionic did not. Bionic is being used as docker base OS.
3. [cmake](https://cmake.org/)
4. [maven](https://maven.apache.org/)
5. [dotnet](https://docs.microsoft.com/en-us/dotnet/core/install/linux-package-manager-ubuntu-1804)
6. [Azure Artifact Credential Provider](https://github.com/microsoft/artifacts-credprovider) to exchange credentials to download private artifacts
7. [sbt](https://www.scala-sbt.org/)

## Troubleshooting
1. Build docker image in debug mode. This will configure the environment for development activity and not attempt to create the jar
```
$ docker build --build-arg mode=debug -t asa-spark-debug:3.1-bionic .
```
2. Start the container from the debug image and connect to it to open a bash shell
```
docker run --name asa-spark-debug -it asa-spark-debug:3.1-bionic /bin/bash
```
3. Now, you are inside the docker container at the repository root location. Develop/debug/troubleshoot and when you are ready, run `./build.sh` to generate a new jar to verify.
4. Follow steps in [Quick Start](#quick-start) to run this jar on Azure Databricks. 

## Few helpful docker commands
> ```bash 
># Build docker image using dockerfile present at the current location and tag it as asa-spark:3.1-bionic
>$ docker build -t asa-spark:3.1-bionic .
>
># List docker images
>$ docker images
>
># Start a new container(asa-spark) using an existing docker image(asa-spark:3.1-bionic) and detach 
>$ docker run --name asa-spark -d -it asa-spark:3.1-bionic
>
># Copy a file from a running container named asa-spark to the current directory on localsystem
>$ docker cp asa-spark:/repos/ASASpark/java/target/ASA-0.0.1.jar .
>
># Connect to already running container named asa-spark and open a bash shell
>$ docker exec -it asa-spark bash 
>
># Stop a running container named asa-spark
>$ docker stop asa-spark
>
># Remove a container named asa-spark
>$ docker rm asa-spark
>
># Delete docker image with tag asa-spark:3.1-bionic
>$ docker rmi asa-spark:3.1-bionic
>```

## Example
Notebook Code demonstrating how to call ASASpark-x.x.x.jar APIs to run a query. The code uses a csv file containing a dataset after uploading it to filestore.
1. Upload dataset
   
   a. Go to Azure Databricks, click on `Data -> Databases -> default -> Add Data`.
   
   b. Select `Data source` as `Upload Files`, click on `Drop files to upload` and select the `scala/test/resources/dataset1.csv` file from this repo. Wait for the upload to complete.

2. Create a notebook
   
   a. Create a new Notebook by clicking on `Azure Databricks -> New Notebook`
   
   b. Give `Name` as `ASASparkExample`, select `Language` as `Scala` and `Cluster` as created in step 2 (ii)

3. Add the following scala code to the above created notebook and click `Run all`
```scala
import org.scalatest._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.Map
import com.microsoft.AzureStreamAnalytics

var spark = SparkSession.builder()
    .master("local[2]") // 2 ... number of threads
    .appName("ASA")
    .config("spark.sql.shuffle.partitions", value = 1)
    .config("spark.ui.enabled", value = false)
    .config("spark.sql.crossJoin.enabled", value = true)
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

val df = spark.sqlContext
  .read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("timestampFormat", "MM-dd-yyyy hh mm ss")
  .load("/FileStore/tables/dataset1.csv")
  .repartition(col("category"))
  .sortWithinPartitions(col("ts"))
  .select(col("ts").cast(LongType), col("category"), col("value"))

val newDf = AzureStreamAnalytics.execute(
    "SELECT category, System.Timestamp AS ts, COUNT(*) AS n FROM input TIMESTAMP BY DATEADD(second, ts, '2019-11-20T00:00:00Z') GROUP BY TumblingWindow(second, 2), category",
    df)
newDf.show
```