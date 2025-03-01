*** Benchmarking Hive using different file format storage ***
### Schema migration text-> parquet/orc/avro ###

### The purpose of doing benchmarking is to identify the best functionality or the feature to be used by 
iterating with different options, here we are going to create textfile, orc, avro and parquet format tables 
to check/benchmark the performance between all these tables and the data size it occupied. ###

hive (benchmarking)> create table staging_txn(txnno INT, txndate STRING, custno INT, amount DOUBLE, category STRING,product STRING, city STRING, state STRING, spendby STRING)
                   > row format delimited fields terminated by "," lines terminated by '\n'
                   > stored as textfile;
OK
Time taken: 1.394 seconds
hive (benchmarking)> LOAD DATA LOCAL INPATH '/home/hduser/hive/data/txns' OVERWRITE INTO TABLE staging_txn;

Parquet file table: 


create table txn_parquet(txnno INT, txndate STRING, custno INT, amount DOUBLE,category STRING, 
product STRING, city STRING, state STRING, spendby STRING)  
row format delimited fields terminated by ',' lines terminated by '\n' 
stored as parquetfile;  

Insert into table txn_parquet select txnno,txndate,custno,amount,category, product,city,state,spendby 
from staging_txn;


