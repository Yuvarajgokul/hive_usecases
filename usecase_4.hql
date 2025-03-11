####Hive Schema Evolution (Hive Dynamic schema): ###

Requirement:  
Source provider change the structure of their tables or data set any time by adding/removing the 
columns and are not communicating it to the BigData platform team, how to handle this evolving 
schema requirement automatically ?  

Import the Customer data into hdfs using sqoop import with 3 mappers into 
/user/hduser/custavro location 

[hduser@localhost ~]$ sqoop import -Dmapreduce.job.user.classpath.first=true --connect jdbc:mysql://localhost/custpayments 
--username root --password Root123$ -table customers -m 3
 --split-by customernumber --target-dir /user/hduser/custavro --delete-target-dir --as-avrodatafile;

[hduser@localhost ~]$ hadoop jar avro-tools-1.8.1.jar getschema /user/hduser/custavro/part-m-00000.avro >/home/hduser/customer.avsc 

[hduser@localhost ~]$ cat ~/customer.avsc
{
  "type" : "record",
  "name" : "customers",
  "doc" : "Sqoop import of customers",
  "fields" : [ {
    "name" : "customerNumber",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "customerNumber",
    "sqlType" : "4"
  }, {
    "name" : "customerName",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "customerName",
    "sqlType" : "12"
  }, {
    "name" : "contactLastName",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "contactLastName",
    "sqlType" : "12"
  }, {
    "name" : "contactFirstName",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "contactFirstName",
    "sqlType" : "12"
  }, {
    "name" : "phone",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "phone",
    "sqlType" : "12"
  }, {
    "name" : "addressLine1",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "addressLine1",
    "sqlType" : "12"
  }, {
    "name" : "addressLine2",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "addressLine2",
    "sqlType" : "12"
  }, {
    "name" : "city",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "city",
    "sqlType" : "12"
  }, {
    "name" : "state",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "state",
    "sqlType" : "12"
  }, {
    "name" : "postalCode",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "postalCode",
    "sqlType" : "12"
  }, {
    "name" : "country",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "country",
    "sqlType" : "12"
  }, {
    "name" : "salesRepEmployeeNumber",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "salesRepEmployeeNumber",
    "sqlType" : "4"
  }, {
    "name" : "creditLimit",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "creditLimit",
    "sqlType" : "3"
  } ],
  "tableName" : "customers"
}

 
 hive (custdb)> create external table customeravro  
             > stored as AVRO  
             > location '/user/hduser/customeravro'  
             > TBLPROPERTIES('avro.schema.url'='hdfs:///tmp/customer.avsc'); 

hive (custdb)> load data inpath '/user/hduser/custavro' into table customeravro;

