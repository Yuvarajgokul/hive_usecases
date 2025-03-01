"""
->Load the fixedwidth data to a managed raw table 
-> create temporary managed table and load all raw data into the respective columns by applying the parsing logic 
-> create temporary external table and insert select only few columns from the temp managed table 
-> sqoop export from temp ext table loc to the DB table 
-> from the temp managed table load the orc table with filtered data 
-> from the temp managed table convert few columns into JSON format. """

Create and load into a hive temporary table called cust_fixed_raw in a column rawdata of type 
varchar(43).

hive (custdb)> create table cust_fixed_raw(rawdata char(43)); 

hive (custdb)> load data local inpath '/home/hduser/hive/data/cust_fixed_width.txt' into table cust_fixed_raw;

Create a temporary managed table called cust_delimited_parsed_temp with all the columns 
such as id,name,city,age,dt,amt and load the cust_fixed_raw table using substr and trim 
function as needed.

hive (custdb)> create temporary table cust_delimited_parsed_temp(id int,name varchar(12),city varchar(10),age int,dt date,amount int) 
             > row format delimited fields terminated by ",";

hive (custdb)> insert into cust_delimited_parsed_temp select cast(trim(substr(rawdata,1,3)) as int) as id, trim(substr(rawdata,4,12)) 
as name,trim(substr(rawdata,16,10)) as city, cast(trim(substr(rawdata,26,3)) as int) as age,cast(trim(substr(rawdata,29,10)) as date) 
as dt,cast(trim(substr(rawdata,39,5)) as int) as amt from cust_fixed_raw;

Create another temporary external table namely tmp_ext_sqp with the location of 
/user/hduser/tmp_ext_sqp/

hive (custdb)> create temporary external table tmp_ext_sqp(id int, dt date, amount int)
             > row format delimited fields terminated by ","
             > location '/user/hduser/tmp_ext_sqp/'
             > ;
hive (custdb)> insert into tmp_ext_sqp
             > select id, dt,amount
             > from cust_delimited_parsed_temp; 


Export only id, dt and amt column into a mysql table cust_fixed_mysql using sqoop export from 
the /user/hduser/tmp_ext_sqp/ location as a export-dir. 

sqoop export \
--connect jdbc:mysql://localhost/custdb \
--username root --password Root123$ \
--table cust_fixed_mysql \
--export-dir /user/hduser/tmp_ext_sqp_cleaned \
-m 1


Create an external partitioned table cust_parsed_orc of type orc format partitioned based on dt. 

hive (custdb)> create external table  cust_parsed_orc (id int,name varchar(12),city varchar(10),age int, amt double)
             > partitioned by (dt date)
             > stored as orc 
             > location '/user/hduser/cust_parsed_orc'; 

Filter and Load only chennai data into the above table cust_parsed_orc by selecting all columns 
from cust_delimited_parsed_temp table

hive (custdb)> insert into cust_parsed_orc partition (dt)
             > select id,name,city,age,amt,dt from cust_delimited_parsed_temp where city='chennai';

Create a json table called cust_parsed_json (to load into a json format using the following steps). 

 hive (custdb)> create external table cust_parsed_json(id int, name string,city string, age int) 
             > ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' 
             > location '/user/hduser/custjson';            

Insert into the cust_parsed_json only non chennai data using  
insert select of id,name,city, age from the cust_delimited_parsed_temp table and verify the 
data in the /user/hduser/custjson should be in json format.

hive (custdb)> Insert into cust_parsed_json select id,name,city,age from cust_delimited_parsed_temp; 

Create another json table called cust_parsed_complex_json (to load into a json format using the 
following steps). 

hive (custdb)> create external table cust_parsed_json(id int, name string,city string, age int) 
             > ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' 
             > location '/user/hduser/cust_complex_json';

hive (custdb)> Insert into cust_parsed_json select id,name,city,age from cust_delimited_parsed_temp; 

hive (custdb)> selece * id, name, mis_info.city, mis_info.age fromcust_parsed_complex_json; 


