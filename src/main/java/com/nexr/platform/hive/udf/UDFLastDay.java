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
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

/**
 * UDFLastDay
 */

@UDFType(deterministic = false)
@Description(name = "last_day",
		value = "_FUNC_(dateString) -  returns the last day of the month based " +
				"on a date string with yyyy-MM-dd HH:mm:ss pattern.",
		extended = "Example:\n"
			+"> SELECT last_day('2003-03-15 01:22:33') FROM src LIMIT 1;"
			+"2003-03-31 00:00:00\n"
)
@HivePdkUnitTests(
		setup = "", cleanup = "",
		cases = {
			@HivePdkUnitTest(
				query = "SELECT nexr_last_day('2003-03-15 01:22:33') FROM onerow;",
				result = "2003-03-31 00:00:00"
			),
			@HivePdkUnitTest(
				query = "SELECT nexr_last_day('2011-07-21 09:21:00') FROM onerow;",
				result = "2011-07-31 00:00:00"
			)
		}
	)
public class UDFLastDay extends UDF {
	private final SimpleDateFormat standardFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final Calendar calendar = Calendar.getInstance();

	public UDFLastDay() {
    standardFormatter.setLenient(false);
  }

	Text result = new Text();

	public Text evaluate(Text dateText){
		if (dateText == null) {
			return null;
		}
		
		try {
			calendar.setTime(standardFormatter.parse(dateText.toString()));
			int lastDate = calendar.getActualMaximum(Calendar.DATE);
			calendar.set(Calendar.DATE, lastDate);
			calendar.set(Calendar.HOUR_OF_DAY, 0);  
			calendar.set(Calendar.MINUTE, 0);  
			calendar.set(Calendar.SECOND, 0);  
			calendar.set(Calendar.MILLISECOND, 0); 
			Date newDate = calendar.getTime();
			result.set(standardFormatter.format(newDate));
			return result;
		} catch (ParseException e) {
			return null;
		}
	}
}

