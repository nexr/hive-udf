/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.nexr.platform.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

/**
 * UDFDateFormat
 * 
 */

@Description(name = "str_to_date", value = "_FUNC_(dateText, pattern [, days]) - Convert time string with given pattern "
        + "to time string with 'yyyy-MM-dd HH:mm:ss' pattern\n", extended = "Example:\n"
        + "> SELECT _FUNC_('2011/05/01','yyyy/MM/dd') FROM src LIMIT 1;\n"
        + "2011-05-01 00:00:00\n"
        + "> SELECT _FUNC_('2011/07/21 12:55:11'.'yyyy/MM/dd HH:mm:ss') FROM src LIMIT 1;\n"
        + "2011-07-21 12:55:11\n")
@HivePdkUnitTests(setup = "", cleanup = "", cases = {
        @HivePdkUnitTest(query = "SELECT nexr_str_to_date('2011/05/01','yyyy/MM/dd') FROM onerow;", result = "2011-05-01 00:00:00"),
        @HivePdkUnitTest(query = "SELECT nexr_str_to_date('2011/07/21 12:55:11','yyyy/MM/dd HH:mm:ss') "
                + "FROM onerow;", result = "2011-07-21 12:55:11"),
        @HivePdkUnitTest(query = "SELECT nexr_str_to_date('2011/05/01','yyyy/MM/dd', 1) FROM onerow;", result = "2011-05-02 00:00:00")})
@UDFType(deterministic = false)
public class UDFStrToDate extends UDF {
    private final SimpleDateFormat standardFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final SimpleDateFormat formatter = new SimpleDateFormat();
    private final Calendar calendar = Calendar.getInstance();
    
    public UDFStrToDate() {
    }
    
    Text result = new Text();
    Text lastPatternText = new Text();
    
    public Text evaluate(Text dateText, Text patternText) {
        if (dateText == null || patternText == null) {
            return null;
        }
        try {
            if (!patternText.equals(lastPatternText)) {
                formatter.applyPattern(patternText.toString());
                lastPatternText.set(patternText);
            }
        } catch (Exception e) {
            return null;
        }
        
        Date date;
        try {
            date = formatter.parse(dateText.toString());
            result.set(standardFormatter.format(date));
            return result;
        } catch (ParseException e) {
            return null;
        }
    }
    
    Text t = new Text();
    
    public Text evaluate(Text dateText, Text patternText, IntWritable days) {
        if (dateText == null || patternText == null || days == null) {
            return null;
        }
        
        t = evaluate(dateText, patternText);
        try {
            calendar.setTime(standardFormatter.parse(t.toString()));
            calendar.add(Calendar.DAY_OF_MONTH, days.get());
            Date newDate = calendar.getTime();
            result.set(standardFormatter.format(newDate));
            return result;
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }
}
