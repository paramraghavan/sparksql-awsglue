# Exercise 1

The purpose of this project is for you to demonstrate your ability to use Spark to process data. 
There is no single "correct" way to complete this project. 
You should only need to set aside 2-3 hours to complete this project. 

## Included Data Sources

* /data/customers/: A list of customers and their status as active or inactive
* /data/mailbox-providers/: A list of mailbox providers, and some routing domains they own
* /data/events/: A data set of email events (injections, opens, etc)

## Problem Statement

Using Spark, create a script that will process a single day of event data and output a data set as follows: 

* For each customer, the output shows the number of injection events to routing domains owned by Verizon Media
* Only process data for active customers
* The output data should be partitioned by day
* Output exactly 1 parquet file per day

#### Extra Credit (if time allows):

* Add a configurable threshold value that can be used to exclude customers with a number of total injections below the threshold
* Include all mailbox providers, not just Verizon Media
* For each customer, add the percentage of injection traffic sent to each provider


ref: [exercise1](https://github.com/SparkPost/DataEngTakeHome)