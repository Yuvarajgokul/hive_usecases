## Raw data ->EL -> hive tables -> ETL (join+convert+merge+derive) -> Table for analysis -> Reporting tables (aggr1, aggr2 ..) ##

1. Data Ingestion: (Transient to Raw layer) 

Create database custdb; 
use custdb; 
create table customer(custno string, firstname string, lastname string, age int,profession 
string) 
row format delimited fields terminated by ',';  
 
load data local inpath '/home/hduser/hive/data/custs' into table customer;

use retail; 
create table txnrecords(txnno INT, txndate STRING, custno INT, amount DOUBLE, category 
STRING, product 
STRING, city STRING, state STRING, spendby STRING) 
row format delimited fields terminated by ',' 
stored as textfile; 

2. Data Curation: Raw to Curated Layer 

Create database curateddb; 
create external table ext_cust_txn_part (custno int,fullname string,age int,profession 
string,amount double,product string,spendby string,agecat varchar(100),modifiedamout float)  
partitioned by (datadt date) 
row format delimited fields terminated by ',' 
stored as orc  
location '/user/hduser/custtxnorc'; 
 
 
 
insert into table ext_cust_txn_part partition(datadt) 
select a.custno,upper(concat(a.firstname, ' ',a.lastname)), 
a.age,a.profession,b.amount,b.product,b.spendby, 
case when age<30 then 'low'  
when age>=30 and age < 50 then 'middle' 
when age>=50 then 'old' 
else 'others' end as agecat, 
case when spendby= 'credit' then b.amount+(b.amount*0.05) else b.amount end as 
modifiedamount,current_date 
from custdb.customer a JOIN retail.txnrecords b 
ON a.custno = b.custno;  
 
 
select * from ext_cust_txn_part limit 10;


3. Data Visualization/Analytics/Aggregation/Reporting – From Curated to Discovery layer

create database discoverydb; 
use discoverydb; 
create external table cust_trxn_aggr1 (seqno int,product string,profession string,level string,sumamt 
double, avgamount double,maxamt double,avgage int,currentdate date) 
row format delimited fields terminated by ',' 
stored as orc 
location '/user/hduser/ cust_trxn_aggr1_orc'; 
 
 
insert overwrite table cust_trxn_aggr1 
select row_number() over(),product,profession, agecat, 
sum(amount),avg(amount),max(amount),avg(age),current_date() 
from curateddb.ext_cust_txn_part 
where datadt=current_date 
group by product,profession, agecat, current_date();  
 
create external table cust_trxn_aggr2 (seqno int, profession string,level string,sumamt double, 
avgamount double,maxamt double,avgage int,currentdate date) 
row format delimited fields terminated by ',' 
stored as parquet 
 
 
location '/user/hduser/ cust_trxn_aggr2_parquet'; 
 
insert overwrite table cust_trxn_aggr2 
select row_number() over(),profession, agecat, 
sum(amount),avg(amount),max(amount),avg(age),current_date() 
from curateddb.ext_cust_txn_part 
where datadt=current_date 
group by profession, agecat, current_date();



