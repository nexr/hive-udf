# nexr-hive-udf

## Overview
nexr-hive-udf is a collection of extensions for Hive.

## License
Apache licensed.

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
        <value>file:///home/path/dir/nexr-hive-udf-[VERSION].jar</value>
    </property>

### Registering the UDFs
    hive> add jar /path/to/nexr-platform-hive-udf-VERSION.jar;
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
    hive> CREATE TEMPORARY FUNCTION lookup AS 'com.nexr.platform.hive.udf.GenericUDFLookup';
    hive> CREATE TEMPORARY FUNCTION populate AS 'com.nexr.platform.hive.udf.GenericUDTFPopulate';
    hive> CREATE TEMPORARY FUNCTION dedup AS 'com.nexr.platform.hive.udf.GenericUDAFDedup';

## Usage of Hive UDFs
### decode
    hive> DESCRIBE FUNCTION EXTENDED decode;
    OK
    decode(value1, value2, value3, .... defaultValue) - Returns value3 if value1=value2 otherwise defaultValue
    
    Example:
     > SELECT decode(dep, 0, "ACCOUNT", 1, "HR", "NO-DEP") FROM src LIMIT 1;
     'ACCOUNT' if dep=0

### nvl
    hive> DESCRIBE FUNCTION EXTENDED nvl;   
    OK
    nvl(expr1, expr2) - Returns expr2 if expr1 is null
    
    Example:
     > SELECT nvl(dep, 'Not Applicable') FROM src;
     'Not Applicable' if dep is null


### nvl2
    hive> DESCRIBE FUNCTION EXTENDED nvl2;
    OK
    nvl2(string1, value_if_not_null, value_if_null) - Returns value_if_not_null if string1 is not null, otherwise value_if_null
    
    Example:
     > SELECT nvl2(supplier_city, 'Completed', 'n/a') FROM src;
     'n/a' if supplier_city is null
    
### instr4
    hive> DESCRIBE FUNCTION EXTENDED instr4;
    OK
    instr4(string, substring, [start_position, [nth_appearance]]) - Returns the index of the first occurance of substr in str
    
    Example:
      > SELECT instr4('Facebook', 'boo') FROM src LIMIT 1;
      5

### chr
    hive> DESCRIBE FUNCTION EXTENDED chr;   
    OK
    chr(number_code) - Returns returns the character based on the NUMBER code.
    
    Example:
      > SELECT chr(116) FROM src LIMIT 1;
      t
      > SELECT chr(84) FROM src LIMIT 1;
      T

### greatest
    hive> DESCRIBE FUNCTION EXTENDED greatest;
    OK
    greatest(value1, value2, value3, ....) - Returns the greatest value in the list.
    
    Example:
     > SELECT greatest(2, 5, 12, 3) FROM src;
     12

### to_number
    hive> DESCRIBE FUNCTION EXTENDED to_number; 
    OK
    to_number(value, format_mask) - Returns the number converted from string.
    
    Example:
     > SELECT to_number('1210') FROM src;
     1210

### substr
    hive> DESCRIBE FUNCTION EXTENDED substr;   
    OK
    substr(str, pos[, len]) - returns the substring of str that starts at pos and is of length len orsubstr(bin, pos[, len]) - returns the slice of byte array that starts at pos and is of length len
    Synonyms: substring
    pos is a 1-based index. If pos<0 the starting position is determined by counting backwards from the end of str.
    
    Example:
       > SELECT substr('Facebook', 5) FROM src LIMIT 1;
      'book'
      > SELECT substr('Facebook', -5) FROM src LIMIT 1;
      'ebook'
      > SELECT substr('Facebook', 5, 1) FROM src LIMIT 1;
      'b'

### to_char
    hive> DESCRIBE FUNCTION EXTENDED to_char;
    OK
    to_char(date, pattern)  converts a string with yyyy-MM-dd HH:mm:ss pattern to a string with given pattern.
    to_char(datetime, pattern)  converts a string with yyyy-MM-dd pattern to a string with given pattern.
    to_char(number [,format]) converts a number to a string
    
    Example:
     > SELECT to_char('2011-05-11 10:00:12'.'yyyyMMdd') FROM src LIMIT 1;
    20110511

### str_to_date
    hive> DESCRIBE FUNCTION EXTENDED str_to_date;
    OK
    str_to_date(dateText,pattern) - Convert time string with given pattern to time string with 'yyyy-MM-dd HH:mm:ss' pattern
    
    Example:
    > SELECT str_to_date('2011/05/01','yyyy/MM/dd') FROM src LIMIT 1;
    2011-05-01 00:00:00
    > SELECT str_to_date('2011/07/21 12:55:11'.'yyyy/MM/dd HH:mm:ss') FROM src LIMIT 1;
    2011-07-21 12:55:11

### date_format
    hive> DESCRIBE FUNCTION EXTENDED date_format;
    OK
    date_format(dateText,pattern) - Return time string with given pattern. 
    Convert time string with 'yyyy-MM-dd HH:mm:ss' pattern to time string with given pattern.
     (see [http://java.sun.com/j2se/1.4.2/docs/api/java/text/SimpleDateFormat.html])
    
    Example:
     > SELECT date_format ('2011-05-11 12:05:11','yyyyMMdd') FRom src LIMIT 1;
    20110511

### trunc
    hive> DESCRIBE FUNCTION EXTENDED trunc;      
    OK
    _FUNCdate, format) - Returns a date in string truncated to a specific unit of measure.
    
    Example:
     > SELECT trunc('2011-08-02 01:01:01') FROM src ;
     returns '2011-08-02 00:00:00' 


## References
* Apache Hive, http://hive.apache.org/
* Oracle 11g SQL Functions, [http://docs.oracle.com/cd/B28359_01/server.111/b28286/functions001.htm#i88893]