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

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

/**
 * UDFToChar
 *
 * This function is an alternative to Oracle to_char function.
 */

@UDFType(deterministic = false)
@Description(name = "to_char",
		value = "_FUNC_(date, pattern)  converts a string with yyyy-MM-dd HH:mm:ss pattern " +
				"to a string with given pattern.\n"
		+"_FUNC_(datetime, pattern)  converts a string with yyyy-MM-dd pattern " +
				"to a string with given pattern.\n"
		+"_FUNC_(number [,format]) converts a number to a string\n",
		extended = "Example:\n"
		+" > SELECT to_char('2011-05-11 10:00:12'.'yyyyMMdd') FROM src LIMIT 1;\n"
		+"20110511\n"
)

@HivePdkUnitTests(
		setup = "", cleanup = "",
		cases = {
			@HivePdkUnitTest(query = "SELECT nexr_to_char('2011-05-01 10:00:12', 'yyyyMMdd') FROM onerow;",result = "20110501"),
			@HivePdkUnitTest(query = "SELECT nexr_to_char('2011-07-01 00:00:00', 'yyyy-MM-dd') FROM onerow;",result = "2011-07-01"),
			@HivePdkUnitTest(query = "SELECT nexr_to_char('2011-07-01', 'yyyy/MM/dd') FROM onerow;",result = "2011/07/01"),
			@HivePdkUnitTest(query = "SELECT nexr_to_char(234) FROM onerow;",result = "234"),
			@HivePdkUnitTest(query = "SELECT nexr_to_char(col_tinyint) FROM datatypes;",result = "1"),
			@HivePdkUnitTest(query = "SELECT nexr_to_char(col_smallint) FROM datatypes;",result = "12"),
			@HivePdkUnitTest(query = "SELECT nexr_to_char(col_int) FROM datatypes;",result = "123"),
			@HivePdkUnitTest(query = "SELECT nexr_to_char(col_bigint) FROM datatypes;",result = "1234"),
			@HivePdkUnitTest(query = "SELECT nexr_to_char(col_float) FROM datatypes;",result = "12.34"),
			@HivePdkUnitTest(query = "SELECT nexr_to_char(col_double) FROM datatypes;",result = "1234.1234"),
			@HivePdkUnitTest(query = "SELECT nexr_to_char(1210.73,'9999.9') FROM datatypes;",result = "1210.7"),
			@HivePdkUnitTest(query = "SELECT nexr_to_char(1210.73,'9,999.99') FROM datatypes;",result = "1,210.73"),
			@HivePdkUnitTest(query = "SELECT nexr_to_char(1210,'$9999') FROM datatypes;",result = "$1210"),
			@HivePdkUnitTest(query = "SELECT nexr_to_char('test') FROM onerow;",result = "test")
		}
	)


public class UDFToChar extends UDF {
	private final SimpleDateFormat standardFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final SimpleDateFormat formatter = new SimpleDateFormat();
	private final DecimalFormat decimalFormat = new DecimalFormat();
	private ByteStream.Output out = new ByteStream.Output();

	private Text result = new Text();
	private Text lastPatternText = new Text();

	public UDFToChar() {
    standardFormatter.setLenient(false);
    formatter.setLenient(false);
	}

	public Text evaluate(NullWritable i) {
		return null;
	}
	
	public Text evaluate(ByteWritable i) {
		if (i == null) {
			return null;
		} else {
			out.reset();
			LazyInteger.writeUTF8NoException(out, i.get());
			result.set(out.getData(), 0, out.getCount());
			return result;
		}
	}
	public Text evaluate(ByteWritable i, Text format) {
		if (i == null|| format==null) {
			return null;
		} else {
			String pattern  = format.toString().replace("9", "#");
			decimalFormat.applyPattern(pattern);
			result.set(decimalFormat.format(i.get()));
			return result;
		}
	}

	public Text evaluate(ShortWritable i) {
		if (i == null) {
			return null;
		} else {
			out.reset();
			LazyInteger.writeUTF8NoException(out, i.get());
			result.set(out.getData(), 0, out.getCount());
			return result;
		}
	}
	
	public Text evaluate(ShortWritable i, Text format) {
		if (i == null|| format==null) {
			return null;
		} else {
			String pattern  = format.toString().replace("9", "#");
			decimalFormat.applyPattern(pattern);
			result.set(decimalFormat.format(i.get()));
			return result;
		}
	}

	public Text evaluate(IntWritable i) {
		if (i == null) {
			return null;
		} else {
			out.reset();
			LazyInteger.writeUTF8NoException(out, i.get());
			result.set(out.getData(), 0, out.getCount());
			return result;
		}
	}
	
	public Text evaluate(IntWritable i, Text format) {
		if (i == null|| format==null) {
			return null;
		} else {
			String pattern  = format.toString().replace("9", "#");
			decimalFormat.applyPattern(pattern);
			result.set(decimalFormat.format(i.get()));
			return result;
		}
	}

	public Text evaluate(LongWritable i) {
		if (i == null) {
			return null;
		} else {
			out.reset();
			LazyLong.writeUTF8NoException(out, i.get());
			result.set(out.getData(), 0, out.getCount());
			return result;
		}
	}
	
	public Text evaluate(LongWritable i, Text format) {
		if (i == null|| format==null) {
			return null;
		} else {
			String pattern  = format.toString().replace("9", "#");
			decimalFormat.applyPattern(pattern);
			result.set(decimalFormat.format(i.get()));
			return result;
		}
	}

	public Text evaluate(FloatWritable i) {
		if (i == null) {
			return null;
		} else {
			result.set(i.toString());
			return result;
		}
	}
	
	
	public Text evaluate(FloatWritable i, Text format) {
		if (i == null|| format==null) {
			return null;
		} else {
			String pattern  = format.toString().replace("9", "#");
			decimalFormat.applyPattern(pattern);
			result.set(decimalFormat.format(i.get()));
			return result;
		}
	}

	public Text evaluate(DoubleWritable i) {
		if (i == null) {
			return null;
		} else {
			result.set(i.toString());
			return result;
		}
	}
	
	public Text evaluate(DoubleWritable i, Text format) {
		if (i == null|| format==null) {
			return null;
		} else {
			String pattern  = format.toString().replace("9", "#");
			decimalFormat.applyPattern(pattern);
			result.set(decimalFormat.format(i.get()));
			return result;
		}
	}
	
	public Text evaluate(Text dateText, Text patternText) {
		if (dateText == null || patternText == null) {
			return null;
		}
		if (dateText.toString().trim().length()==10){
			standardFormatter.applyPattern("yyyy-MM-dd");
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
	
	public Text evaluate(Text text){
		return text;
	}
	
}