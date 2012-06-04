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
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

/**
 * GenericUDFTrunc Class for SQL construct "trunc(date, [format])". This mimics the function form <a href=
 * "http://download.oracle.com/docs/cd/B19306_01/server.102/b14200/functions201.htm" >TRUNC(date)</a>. The date should
 * be valid pattern of 'yyyy-MM-dd HH:mm:ss'.
 * <p>
 * Below are the valid format parameters:
 * <ul>
 * <li>Year : YYYY
 * <li>Month : MM
 * <li>Day : DD
 * </ul>
 * 
 * 
 */
@Description(name = "trunc", value = "_FUNC_(date, [format_mask]) " +
		"- Returns a date in string truncated to a specific unit of measure.", extended = "Example:\n"
		+ " > SELECT _FUNC_('2011-08-02 01:01:01') FROM src ;\n returns '2011-08-02 00:00:00' ")
@HivePdkUnitTests(setup = "create table dual_data (i int); "
		+ "insert overwrite table dual_data select 1 from dual limit 1;", 
		cleanup = "drop table if exists dual_data;", cases = {
		@HivePdkUnitTest(query = "SELECT nexr_trunc('2011-08-02 01:01:01') " +
				"FROM dual_data;", result = "2011-08-02 00:00:00"),
		@HivePdkUnitTest(query = "SELECT nexr_trunc('2011-08-02 01:01:01','YYYY') " +
				"FROM dual_data;", result = "2011-01-01 00:00:00"),
		@HivePdkUnitTest(query = "SELECT nexr_trunc('2011-08-02 01:01:01','MM') " +
				"FROM dual_data;", result = "2011-08-01 00:00:00"),
		@HivePdkUnitTest(query = "SELECT nexr_trunc('2011-08-02 01:01:01','DD') " +
				"FROM dual_data;", result = "2011-08-02 00:00:00") })
public class GenericUDFTrunc extends GenericUDF {

	private final String YYYY = "YYYY";
	private final String MM = "MM";
	private final String DD = "DD";

	private final SimpleDateFormat HIVE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private ObjectInspector[] argumentIOs;
	private ObjectInspector returnInspector;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		if (arguments.length < 1) {
			throw new UDFArgumentLengthException("The function trunc(date, format) needs at least one argument.");
		}

		for (int i = 0; i < arguments.length; i++) {
			if (arguments[i].getTypeName() != Constants.STRING_TYPE_NAME) {
				throw new UDFArgumentTypeException(i, "Only String type arguments are accepted but "
						+ arguments[i].getTypeName() + " is passed.");
			}
		}

		argumentIOs = arguments;
		returnInspector = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		return returnInspector;
	}

	private Text returnText = new Text();

	@Override
	public Object evaluate(DeferredObject[] records) throws HiveException {
		String date = ((PrimitiveObjectInspector) argumentIOs[0]).getPrimitiveJavaObject(records[0].get()).toString();
		Date parsedDate = null;
		try {
			parsedDate = HIVE_DATE_FORMAT.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		char TRUNC_MODE = 'D';
		if (records.length > 1) {
			String mode = ((PrimitiveObjectInspector) argumentIOs[1]).getPrimitiveJavaObject(records[1].get())
					.toString();
			if (mode.equals(YYYY)) {
				TRUNC_MODE = 'Y';
			} else if (mode.equals(MM)) {
				TRUNC_MODE = 'M';
			} else if (mode.equals(DD)) {
				TRUNC_MODE = 'D';
			}
		}

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(parsedDate);
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		switch (TRUNC_MODE) {
		case 'Y':
			calendar.clear();
			calendar.set(Calendar.YEAR, year);
			break;
		case 'M':
			calendar.clear();
			calendar.set(Calendar.YEAR, year);
			calendar.set(Calendar.MONTH, month);
			break;
		case 'D':
		default:
			calendar.clear();
			calendar.set(Calendar.YEAR, year);
			calendar.set(Calendar.MONTH, month);
			calendar.set(Calendar.DAY_OF_MONTH, day);
			break;
		}

		String truncated = HIVE_DATE_FORMAT.format(calendar.getTime());

		returnText.set(truncated);
		return returnText;
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("trunc (");
		for (int i = 0; i < children.length - 1; i++) {
			sb.append(children[i]).append(", ");
		}
		sb.append(children[children.length - 1]).append(")");
		return sb.toString();
	}

}
