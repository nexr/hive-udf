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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

/**
 * GenericUDF Class for SQL construct "greatest(value1, value2, value3, ....)". 
 * Oracle's CREATEST returns the greatest of the list of one or more expressions.
 * 
 */
@Description(name = "greatest", value = "_FUNC_(value1, value2, value3, ....) " +
		"- Returns the greatest value in the list.", 
		extended = "Example:\n" + " > SELECT _FUNC_(2, 5, 12, 3) FROM src;\n 12")
@HivePdkUnitTests(setup = "create table dual_data (i int); "
		+ "insert overwrite table dual_data select 1 from dual limit 1;", 
		cleanup = "drop table if exists dual_data;", 
		cases = {
		@HivePdkUnitTest(query = "SELECT nexr_greatest(2, 5, 12, 3) " +
				"FROM dual_data;", result = "12"),
		@HivePdkUnitTest(query = "SELECT nexr_greatest('2', '5', '12', '3') " +
				"FROM dual_data;", result = "5"),
		@HivePdkUnitTest(query = "SELECT nexr_greatest('apples', 'oranges', 'bananas') " +
				"FROM dual_data;", result = "oranges") })
public class GenericUDFGreatest extends GenericUDF {

	private ObjectInspector[] argumentOIs;
	private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		for (int i = 0; i < arguments.length; i++) {
			if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
				throw new UDFArgumentTypeException(i, "Only primitive type arguments are accepted but "
						+ arguments[i].getTypeName() + " is passed.");
			}
		}

		argumentOIs = arguments;
		returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		for (int i = 0; i < arguments.length; i++) {
			if (!returnOIResolver.update(arguments[i])) {
				throw new UDFArgumentTypeException(i, "The value of return should have the same type: \""
						+ returnOIResolver.get().getTypeName() + 
						"\" is expected but \"" + arguments[i].getTypeName()
						+ "\" is found");
			}
		}

		return returnOIResolver.get();
	}

	@Override
	public Object evaluate(DeferredObject[] fields) throws HiveException {
		Object greatestObject = null;
		ObjectInspector greatestOI = null;

		for (int i = 0; i < fields.length; i++) {
			Object fieldObject = fields[i].get();
			if (greatestObject == null) {
				greatestObject = fieldObject;
				greatestOI = argumentOIs[i];
				continue;
			}

			if (ObjectInspectorUtils.compare(greatestObject, greatestOI, fieldObject, argumentOIs[i]) <= 0) {
				greatestObject = fieldObject;
				greatestOI = argumentOIs[i];
			}
		}

		return returnOIResolver.convertIfNecessary(greatestObject, greatestOI);
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("greatest (");
		for (int i = 0; i < children.length - 1; i++) {
			sb.append(children[i]).append(", ");
		}
		sb.append(children[children.length - 1]).append(")");
		return sb.toString();
	}

}
