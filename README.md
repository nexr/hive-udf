# NexR Hive UDFs

## About
NexR Hive UDFs is a collection of user defined functions for Hive.

## License
[Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## Quickstart
    $ git clone https://github.com/nexr/hive-udf.git
    $ cd hive-udf
    $ mvn clean package
    or
    $ ant -Dhive.install.dir=../hive/build/dist clean package

## Run the tests using Hive PDK
    $ ant -Dhive.install.dir=../hive/build/dist test

## Install and Configurations
### hive-site.xml
    <property>
        <name>hive.aux.jars.path</name>
        <value>file:///path/to/nexr-hive-udf-[VERSION].jar</value>
    </property>

### Registering the UDFs
    hive> add jar /path/to/nexr-hive-udf-[VERSION].jar;
    hive> CREATE TEMPORARY FUNCTION nvl AS 'com.nexr.platform.hive.udf.GenericUDFNVL';
    hive> CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';
    hive> CREATE TEMPORARY FUNCTION nvl2 AS 'com.nexr.platform.hive.udf.GenericUDFNVL2';
    hive> CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
    hive> CREATE TEMPORARY FUNCTION date_format AS 'com.nexr.platform.hive.udf.UDFDateFormat';
    hive> CREATE TEMPORARY FUNCTION to_char AS 'com.nexr.platform.hive.udf.UDFToChar';
    hive> CREATE TEMPORARY FUNCTION instr4 AS 'com.nexr.platform.hive.udf.GenericUDFInstr';
    hive> CREATE TEMPORARY FUNCTION chr AS 'com.nexr.platform.hive.udf.UDFChr';
    hive> CREATE TEMPORARY FUNCTION last_day AS 'com.nexr.platform.hive.udf.UDFLastDay';
    hive> CREATE TEMPORARY FUNCTION greatest AS 'com.nexr.platform.hive.udf.GenericUDFGreatest';
    hive> CREATE TEMPORARY FUNCTION to_number AS 'com.nexr.platform.hive.udf.GenericUDFToNumber';
    hive> CREATE TEMPORARY FUNCTION trunc AS 'com.nexr.platform.hive.udf.GenericUDFTrunc';
    hive> CREATE TEMPORARY FUNCTION rank AS 'com.nexr.platform.hive.udf.GenericUDFRank';
    hive> CREATE TEMPORARY FUNCTION row_number AS 'com.nexr.platform.hive.udf.GenericUDFRowNumber';
    hive> CREATE TEMPORARY FUNCTION sysdate AS 'com.nexr.platform.hive.udf.UDFSysDate';
    hive> CREATE TEMPORARY FUNCTION populate AS 'com.nexr.platform.hive.udf.GenericUDTFPopulate';
    hive> CREATE TEMPORARY FUNCTION dedup AS 'com.nexr.platform.hive.udf.GenericUDAFDedup';
    hive> CREATE TEMPORARY FUNCTION lnnvl AS 'com.nexr.platform.hive.udf.GenericUDFLnnvl';
    hive> CREATE TEMPORARY FUNCTION substr AS 'com.nexr.platform.hive.udf.UDFSubstrForOracle';

## Usage of Hive UDFs
See the details at [Project Wiki](https://github.com/nexr/hive-udf/wiki).

## References
* [Apache Hive](http://hive.apache.org/)
* [Oracle 11g SQL Functions](http://docs.oracle.com/cd/B28359_01/server.111/b28286/functions001.htm#i88893)
