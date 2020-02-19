
# Comparing stocks across time 
> Learning from history to gain insight

This project has been completed as part of the Insight Data Engineering Program (NYC, Winter 2020). 

## Context

This project aims to obtain useful information about the stock prices of companies, by comparing them against one another in different time scales. The main idea is that by understanding behaviour in the past, even when the future is unpredictable, we can gather insight from to assess our stocks in the present. More concretely, we want to calculate correlations of stocks in different time scales and find where those correlations are highest (i.e. closest to 1), where one stock is the one being considered and the other used as a benchmark.

## Pipeline 

![Pipeline](https://github.com/ffrancoj/time-series-stock-comparison/blob/develop/docs/pipeline.png)

### Environment setup

For installation and configuration of AWS CLI and Pegasus, please follow the websites:

* [Pegasus installation and instructions](https://github.com/InsightDataScience/pegasus)
* [AWS information and instructions](https://github.com/InsightDataScience/data-engineering-ecosystem/wiki/aws)

#### Cluster structure

To reproduce this particular environment, we need five m4.large AWS EC2 instances are needed, four nodes for the Spark Cluster, and one node for Database, as well as one t2.micro AWS EC2 instance for the webserver. 

Start a cluster with Pegasus and configure the master and workers yaml files under ./vars/spark_cluster. For example, in the cluster named ```spark-cluster``` edit the master file:

 ```bash
purchase_type: on_demand
subnet_id: subnet-XXXX
num_instances: 1
key_name: XXXXX-keypair
security_group_ids: sg-XXXXX
instance_type: m4.large
tag_name: spark-cluster
vol_size: 100
role: master
use_eips: true

 ```

Then start the cluster

```bash
peg fetch spark-cluster && peg start spark-cluster && peg service spark-cluster spark start
```

If needed, ```eval `ssh-agent -s` ``` and run the start line again 

Next, ssh into your master node with 
```bash
peg ssh spark-cluster 1
```

To stop the cluster, use

```bash
peg fetch spark-cluster && peg service spark-cluster spark stop && peg stop spark-cluster
```


#### PostgreSQL setup

For installation and configuration, refer to the `docs/postgres_install.txt` file

#### Webserver setup 

For installation and configuration, refer to: [create an EC2 instance and install a web server](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_Tutorials.WebServerDB.CreateWebServer.html)

### Running Spark

For command line instructions, refer to the `docs/spark_run.txt` file

## Author

Franco J. Williams







