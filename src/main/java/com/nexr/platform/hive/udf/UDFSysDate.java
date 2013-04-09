/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nexr.platform.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Generic UDF for string function <code>CURDATE()</code>,
 * <code>SYSDATE()</code>. This mimcs the function from MySQL
 * http://dev.mysql.com/doc/refman/5.1/en/date-and-time-functions.html#function_sysdate
 * 
 * <pre>
 * usage:
 * SYSDATE()
 * </pre>
 * <p>
 */
@Description(name = "sysdate",
	value = "_FUNC_() - Returns the current date and time as a value in 'yyyy-MM-dd HH:mm:ss' format"
		+"_FUNC_(dateFormat) - Returns the current date and time as a value in given format"
		+"_FUNC_(dateFormat, num_days) - Returns the date that is num_days after current date in given date format",
	extended = "Example:\n"
		+ "  > SELECT _FUNC_() FROM src LIMIT 1;\n" + "2011-06-13 13:47:36"
		+ "  > SELECT _FUNC_('yyyyMMdd') FROM src LIMIT 1;\n" + "20110613"
		+ "  > SELECT _FUNC_('yyyyMMdd',1) FROM src LIMIT 1;\n" + "20110614")

public class UDFSysDate extends UDF{
	private final SimpleDateFormat stdFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final SimpleDateFormat formatter = new SimpleDateFormat();
	  private final Calendar calendar = Calendar.getInstance();

  public UDFSysDate() {
    stdFormatter.setLenient(false);
    formatter.setLenient(false);
  }
	  
	Text result= new Text();
	public Text evaluate() {
		Date date = new Date();
		result.set(stdFormatter.format(date));
		return result;
	}

	public Text evaluate(Text format) {
		if (format==null) {
			format.set("yyyy-MM-dd HH:mm:ss");
		}

		Date date = new Date();
		formatter.applyPattern(format.toString());
		result.set(formatter.format(date));
		return result;
	}

	public Text evaluate(Text format, IntWritable days){
		if (format==null) {
			format.set("yyyy-MM-dd HH:mm:ss");
		}
		
		formatter.applyPattern(format.toString());
		Date date = new Date();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH, days.get());
		Date newDate = calendar.getTime();
		result.set(formatter.format(newDate));
		return result;
	}
	
}
