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

import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFInstr
 * 
 * This integrates the function from Oracle and Mysql
 * http://www.techonthenet.com/oracle/functions/instr.php
 * http://dev.mysql.com/doc/refman/5.1/en/string-functions.html#function_instr
 * 
 * <code>INSTR(string, substring, [start_position, [nth_appearance]])</code>. 
 * string is the string to search.
 * substring is the substring to search for in string.
 * start_position is the position in string where the search will start. 
 *   This argument is optional. If omitted, it defaults to 1. 
 *   The first position in the string is 1. 
 *   If the start_position is negative, the function counts back start_position number of characters 
 *   from the end of string and then searches towards the beginning of string.
 * nth_appearance is the nth appearance of substring. 
 *   This is optional. If omitted, it defaults to 1.
 * 
 */
@Description(name = "instr",
    value = "_FUNC_(string, substring, [start_position, [nth_appearance]]) " +
    		"- Returns the index of the first occurance of substr in str",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('Facebook', 'boo') FROM src LIMIT 1;\n" + "  5\n")
@HivePdkUnitTests(
	setup = "", cleanup = "",
    cases = {
      @HivePdkUnitTest(
        query = "SELECT nexr_instr('Tech on the net', 'e') FROM onerow;",
        result = "2"),
      @HivePdkUnitTest(
        query = "SELECT nexr_instr('Tech on the net', 'e', 1, 1) FROM onerow;",
        result = "2"),
      @HivePdkUnitTest(
        query = "SELECT nexr_instr('Tech on the net', 'e', 1, 2) FROM onerow;",
        result = "11"),
      @HivePdkUnitTest(
        query = "SELECT nexr_instr('Tech on the net', 'e', 1, 3) FROM onerow;",
        result = "14"),
      @HivePdkUnitTest(
        query = "SELECT nexr_instr('Tech on the net', 'e', -5, 1) FROM onerow;",
        result = "11")
    }
  )

public class GenericUDFInstr extends GenericUDF {

	private ObjectInspectorConverters.Converter[] converters;

	static final int DEFAULT_START_INDEX = 1;
	static final int DEFAULT_NTH = 1;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length < 2 || arguments.length > 4) {
			throw new UDFArgumentLengthException("The function INSTR must have 2 or more and less than 4 arguments.");
		}

		for (int i = 0; i < arguments.length; i++) {
			if (i < 2) {// 0,1 : String
				if (!arguments[i].getTypeName().toLowerCase().equals("string")) {
					throw new UDFArgumentTypeException(i, "The " + GenericUDFUtils.getOrdinal(i + 1)
							+ " argument of function INSTR is expected to a string type, but " + 
							arguments[i].getTypeName().toLowerCase() + " is found");
				}
			} else {// 2,3 : Int
				if (!arguments[i].getTypeName().equals("int")) {
					throw new UDFArgumentTypeException(i, "The " + GenericUDFUtils.getOrdinal(i + 1)
							+ " argument of function INSTR is expected to a int type, but " + 
							arguments[i].getTypeName().toLowerCase() + " is found");
				}
			}
		}

		converters = new ObjectInspectorConverters.Converter[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			if (i < 2) {// 0,1 : String
				converters[i] = ObjectInspectorConverters.getConverter(
						arguments[i], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
			} else {// 2,3 : Int
				converters[i] = ObjectInspectorConverters.getConverter(
						arguments[i], PrimitiveObjectInspectorFactory.writableIntObjectInspector);
			}
		}

		return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
	}

	private IntWritable intWritable = new IntWritable(0);

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if (arguments[0].get() == null || arguments[1].get() == null) {
			return null;
		}

		Text text = (Text) converters[0].convert(arguments[0].get());
		Text subtext = (Text) converters[1].convert(arguments[1].get());
		int startIndex = (arguments.length >= 3) ? 
				((IntWritable) converters[2].convert(arguments[2].get())).get() : DEFAULT_START_INDEX;
		int nth = (arguments.length == 4) ? 
				((IntWritable) converters[3].convert(arguments[3].get())).get() : DEFAULT_NTH;

		// argument checking
		if (startIndex < 0) { 
			// if startIndex is negative, 
			// the function counts back startIndex number of characters from the end of text and then searches
			// towards the beginning of text.
			startIndex = text.getLength() + startIndex;
		}
		if (startIndex <= 0 || startIndex > text.getLength()) {
			intWritable.set(0);
			return intWritable;
		}

		int index = 0;
		int currentIndex = startIndex;
		for (int i = 0; i < nth; i++) {
			index = GenericUDFUtils.findText(text, subtext, currentIndex - 1) + 1;
			if (index == 0) {// not found
				intWritable.set(0);
				return intWritable;
			}
			currentIndex = index + 1;
		}
		intWritable.set(index);
		return intWritable;
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length >= 2 && children.length <= 4);
		StringBuffer displayString = new StringBuffer();

		displayString.append("instr()");
		for (int i = 0; i < children.length; i++) {
			displayString.append(children[i]);
		}
		displayString.append(")");
		return displayString.toString();
	}
}

