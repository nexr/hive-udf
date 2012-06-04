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
import java.text.NumberFormat;
import java.util.Locale;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

/**
 * GenericUDF Class for SQL construct "to_number(string value,string format_mask)".
 * <p>
 * It has constraint to use, for example, 
 * It can convert double value with format-mask. Without format-mask, it returns
 * long value. There is <code>cast</code> for type conversion in HIVE. 
 * It can be used for the most frequently requested
 * examples such as below.
 * <p>
 * 
 * 
 */
@Description(name = "to_number", value = "_FUNC_(value, format_mask) " +
		"- Returns the number converted from string.", extended = "Example:\n"
		+ " > SELECT _FUNC_('1210') FROM src;\n 1210")
@HivePdkUnitTests(setup = "create table dual_data (i int); "
		+ "insert overwrite table dual_data select 0 from dual limit 1;", 
		cleanup = "drop table if exists dual_data;", cases = {
		@HivePdkUnitTest(query = "SELECT nexr_to_number('1234') " +
				"FROM dual_data;", result = "1234"),
		@HivePdkUnitTest(query = "SELECT nexr_to_number('1234.56', 9999.99) " +
				"FROM dual_data;", result = "1234.56"),
		@HivePdkUnitTest(query = "SELECT nexr_to_number('1234.56') " +
				"FROM dual_data;", result = "1234"),
		@HivePdkUnitTest(query = "SELECT nexr_to_number('1234', 9999.99) " +
				"FROM dual_data;", result = "1234.0"),
		@HivePdkUnitTest(query = "SELECT nexr_to_number('$1234.56', '$9999.99') " +
				"FROM dual_data;", result = "1234.56"),
		@HivePdkUnitTest(query = "SELECT nexr_to_number('HIGH', '$9999.99') " +
				"FROM dual_data;", result = "NULL") })
public class GenericUDFToNumber extends GenericUDF {

	private ObjectInspector returnInspector;

	private ObjectInspectorConverters.Converter[] converters;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		if (arguments.length < 1) {
			throw new UDFArgumentLengthException(
					"The function to_number(value, format_mask, nls_language) needs at least one argument.");
		}

		if (arguments[0].getTypeName() != Constants.STRING_TYPE_NAME) {
			throw new UDFArgumentTypeException(0, "Argument " + (1) + " of function to_number must be \""
					+ Constants.STRING_TYPE_NAME + "\", but \"" 
					+ arguments[0].getTypeName() + "\" was found.");
		}

		if (arguments.length > 1
				&& !(arguments[1].getTypeName() == Constants.DOUBLE_TYPE_NAME || 
						arguments[1].getTypeName() == Constants.STRING_TYPE_NAME)) {
			throw new UDFArgumentTypeException(1, "Argument " + (2) 
					+ " of function to_number must be \""
					+ Constants.DOUBLE_TYPE_NAME + "\", but \"" 
					+ arguments[1].getTypeName() + "\" was found.");
		}

		converters = new ObjectInspectorConverters.Converter[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
					PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		}

		if (arguments.length == 1) {
			returnInspector = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
		} else {
			returnInspector = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
		}

		return returnInspector;
	}

	private LongWritable longResult = new LongWritable();
	private DoubleWritable doubleResult = new DoubleWritable();

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		if (arguments[0].get() == null) {
			return null;
		}

		try {
			Text value = (Text) converters[0].convert(arguments[0].get());

			Locale locale = Locale.getDefault();

			// @todo convert nls_params to locale if needed.

			NumberFormat formatter = NumberFormat.getInstance(locale);
			if (formatter instanceof DecimalFormat) {
				DecimalFormat df = (DecimalFormat) formatter;

				if (returnInspector.getTypeName() == Constants.BIGINT_TYPE_NAME) {
					longResult.set(df.parse(value.toString()).longValue());
					return longResult;
				}

				// Double
				String pattern = ((Text) converters[1].convert(arguments[1].get())).toString();
				pattern = pattern.replace("9", "0");
				df.applyPattern(pattern);
				doubleResult.set(df.parse(value.toString()).doubleValue());
			}

			return doubleResult;

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	@Override
    public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("to_number(");
		for (int i = 0; i < children.length - 1; i++) {
			sb.append(children[i]).append(", ");
		}
		sb.append(children[children.length - 1]).append(")");
		return sb.toString();
	}

}
