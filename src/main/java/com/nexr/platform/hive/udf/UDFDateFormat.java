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
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

/**
 * UDFDateFormat
 */

@Description(name = "date_format",
	value = "_FUNC_(dateText,pattern) - Return time string with given pattern. ",
	extended = "Convert time string with 'yyyy-MM-dd HH:mm:ss' pattern to time string with given pattern.\n"
		+" (see [http://java.sun.com/j2se/1.4.2/docs/api/java/text/SimpleDateFormat.html])\n\n"
		+"Example:\n"
		+" > SELECT _FUNC_ ('2011-05-11 12:05:11','yyyyMMdd') FRom src LIMIT 1;\n"
		+"20110511"
)
@HivePdkUnitTests(
	setup = "",cleanup = "",
	cases = {
		@HivePdkUnitTest(
			query = "SELECT nexr_date_format('2011-05-11 12:05:11', 'yyyyMMdd') FROM onerow;",
			result = "20110511"
		),
		@HivePdkUnitTest(
			query = "SELECT nexr_date_format('2011-07-21 09:21:00', 'yyyy-MM-dd') FROM onerow;",
			result = "2011-07-21"
		)
	}
)

@UDFType(deterministic = false)
public class UDFDateFormat extends UDF {
	private final SimpleDateFormat standardFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final SimpleDateFormat formatter = new SimpleDateFormat();
	
	public UDFDateFormat() {
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
			date = standardFormatter.parse(dateText.toString());
			result.set(formatter.format(date));
			return result;
		} catch (ParseException e) {
			return null;
		}
	}
}
