create table onerow(s string);
load data local inpath '${env:HIVE_PLUGIN_ROOT_DIR}/test/onerow.txt' overwrite into table onerow;

create table dual(id int, name string, age int, height double, dep int)
row format delimited fields terminated by '\t';
load data local inpath '${env:HIVE_PLUGIN_ROOT_DIR}/test/dual.txt' overwrite into table dual;

create table emp (empno int,ename string,job string,mgr int,hiredate string,sal int,comm int,deptno int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
load data local inpath '${env:HIVE_PLUGIN_ROOT_DIR}/test/emp.txt' into table emp;

create table datatypes (
col_tinyint tinyint,
col_smallint smallint,
col_int int,
col_bigint bigint,
col_boolean boolean,
col_float float,
col_double double,
col_string string,
col_int_array array<int>,
col_string_array array<string>,
col_map map<int,string>,
col_struct struct< id:string, name:string, val:int>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':' STORED AS TEXTFILE;

load data local inpath '${env:HIVE_PLUGIN_ROOT_DIR}/test/datatypes.txt' into table datatypes;