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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

/**
 * GenericUDF Class for SQL construct "decode(value1, value2, value3, .... defaultValue)".
 * oracle's DECODE compares first argument to each other value one by one.
 * <p>
 * There is <code>CASE...WHEN...</code>clause in Hive, 
 * but it is convenient to convert from SQL to HQL without query change.
 * <p>
 * 
 */
@Description(name = "decode", value = "_FUNC_(value1, value2, value3, .... defaultValue) " +
		"- Returns value3 if value1=value2 otherwise defaultValue", extended = "Example:\n"
    + " > SELECT _FUNC_(dep, 0, \"ACCOUNT\", 1, \"HR\", \"NO-DEP\") FROM src LIMIT 1;\n 'ACCOUNT' if dep=0")
@HivePdkUnitTests(setup = "create table dual_data (i int); "
    + "insert overwrite table dual_data select 1 from dual limit 1;", 
    cleanup = "drop table if exists dual_data;", 
    cases = {
		@HivePdkUnitTest(
    		query = "SELECT nexr_decode(i, 0, 'ACCOUNT', 1, 'HR', 'NO-DEP') FROM dual_data;", 
    		        result = "HR"),
		@HivePdkUnitTest(
		    query = "SELECT nexr_decode(dep, 0, 'ACCOUNT', 1, 'HR', 'NO-DEP') " +
		    		"FROM dual WHERE id = 0;", result = "ACCOUNT"),
		@HivePdkUnitTest(
			query = "SELECT nexr_decode(dep, 0, 'ACCOUNT', 1, 'HR', 'NO-DEP') " +
					"FROM dual WHERE id = 2;", result = "NO-DEP") })
public class GenericUDFDecode extends GenericUDF {

	private ObjectInspector[] argumentOIs;
	private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
	private GenericUDFUtils.ReturnObjectInspectorResolver caseOIResolver;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length < 3) {
			throw new UDFArgumentLengthException(
					"The function decode(value1,value2,value3...default) needs " 
					+ "at least three arguments.");
		}

		argumentOIs = arguments;
		caseOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		caseOIResolver.update(arguments[0]);
		for (int i = 1; i + 1 < arguments.length; i += 2) {
			// 1, 3, 5, 7, ...
			if (!caseOIResolver.update(arguments[i])) {
				throw new UDFArgumentTypeException(i, "The value of 'value'" + (i + 1)
					+ " should have the same type: \"" 
					+ caseOIResolver.get().getTypeName()
					+ "\" is expected but \"" + arguments[i].getTypeName() 
					+ "\" is found");
			}
			// 2, 4, 6...
			if (!returnOIResolver.update(arguments[i + 1])) {
				throw new UDFArgumentTypeException(i + 1,"The value of return(value " 
					+ (i + 1) + " should have the same type: \"" 
					+ returnOIResolver.get().getTypeName()
					+ "\" is expected but \"" + arguments[i + 1].getTypeName() 
					+ "\" is found");
			}
		}
		if (!returnOIResolver.update(arguments[arguments.length - 1])) {
			throw new UDFArgumentTypeException(arguments.length - 1,
					"The value of return should have the same type: \"" + 
					returnOIResolver.get().getTypeName() + "\" is expected but \"" + 
					arguments[arguments.length - 1].getTypeName() + "\" is found");
		}

		return returnOIResolver.get();

	}

	@Override
	public Object evaluate(DeferredObject[] records) throws HiveException {

		Object fieldValue = records[0].get();
		Object defaultValue = null;
		if (records.length % 2 == 0) {
			defaultValue = records[records.length - 1].get();
		}

		Object returnValue = null;
		for (int i = 1; i + 1 < records.length; i += 2) {
			Object caseValue = records[i].get();
			if (fieldValue == null || caseValue == null) {
				break;
			}

			Object caseObj = ((PrimitiveObjectInspector) argumentOIs[i]).getPrimitiveJavaObject(caseValue);
			Object fieldObj = ((PrimitiveObjectInspector) argumentOIs[0]).getPrimitiveJavaObject(fieldValue);

			if (caseObj.toString().equals(fieldObj.toString())) {
				returnValue = records[i + 1].get();
				returnValue = returnOIResolver.convertIfNecessary(returnValue, argumentOIs[i + 1]);
				break;
			}

		}

		if (returnValue == null) {
			returnValue = defaultValue;
			returnValue = returnOIResolver.convertIfNecessary(returnValue, argumentOIs[records.length - 1]);
		}
		return returnValue;
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("decode (");
		for (int i = 0; i < children.length - 1; i++) {
			sb.append(children[i]).append(", ");
		}
		sb.append(children[children.length - 1]).append(")");
		return sb.toString();
	}

}
