# Comparing stocks across time 

This project has been completed as part of the Insight Data Engineering Program (NYC, Winter 2020). 

## Context

This project aims to obtain useful information about the stock prices of companies, by comparing them against one another in different time scales. The main idea is that by understanding the behaviour in the past, even when the future is unpredictable, we can gather insight from past data.

Concretely, we want to calculate correlations of stocks in different time scales and find where those correlations are highest (i.e. closest to 1). 

## Pipeline 

![Pipeline](https://github.com/ffrancoj/time-series-stock-comparison/blob/develop/docs/pipeline.png)

### Environment setup

For installation and configuration of AWS CLI and Pegasus, please follow the websites:

* [Pegasus installation and instructions](https://github.com/InsightDataScience/pegasus)
* [AWS information and instructions](https://github.com/InsightDataScience/data-engineering-ecosystem/wiki/aws)

#### CLUSTER STRUCTURE

To reproduce this particular environment, we need 5 m4.large AWS EC2 instances are needed, 4 nodes for the Spark Cluster, and 1 node for Database, as well as 1 t2.micro AWS EC2 instance for the webserver. 

#### PostgreSQL setup






