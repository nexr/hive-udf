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
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

/**
 * GenericUDF Class for SQL construct "nvl(expr1, expr2)". see <a href=
 * "http://download.oracle.com/docs/cd/B19306_01/server.102/b14200/functions105.htm" >NVL</a>.
 * <p>
 * There is function <code>COALESCE</code> in Hive, 
 * but it is convenient to convert from SQL to HQL without query
 * changes.
 * <p>
 * 
 */
@Description(name = "nvl", value = "_FUNC_(expr1, expr2) - Returns expr2 if expr1 is null", 
		extended = "Example:\n"
		+ " > SELECT _FUNC_(dep, 'Not Applicable') FROM src;\n 'Not Applicable' if dep is null")
@HivePdkUnitTests(setup = "create table dual_data (i int); "
		+ "insert overwrite table dual_data select null from dual limit 1;", 
		cleanup = "drop table if exists dual_data;", 
		cases = { @HivePdkUnitTest(query = "SELECT nexr_nvl(i, 'Not Applicable') " +
				"FROM dual_data;", result = "Not Applicable") })
public class GenericUDFNVL extends GenericUDF {

	private ObjectInspector[] argumentOIs;
	private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		if (arguments.length < 2) {
			throw new UDFArgumentLengthException("The function nvl(expr1, expr2) needs at least two arguments.");
		}

		for (int i = 0; i < arguments.length; i++) {
			if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
				throw new UDFArgumentTypeException(i, "Only primitive type arguments are accepted but "
						+ arguments[i].getTypeName() + " is passed.");
			}
		}

		argumentOIs = arguments;
		returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

		if (!(returnOIResolver.update(arguments[0]) && returnOIResolver.update(arguments[1]))) {
			throw new UDFArgumentTypeException(1,
					"The first and the second arguments of function NVL should have the same type, "
							+ "but they are different: \"" + arguments[0].getTypeName() + "\" and \""
							+ arguments[1].getTypeName() + "\"");
		}

		return returnOIResolver.get();
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		Object returnValue = null;
		if (arguments[0].get() == null) {
			// fieldValue is null, return defaultValue
			returnValue = returnOIResolver.convertIfNecessary(arguments[1].get(), argumentOIs[1]);
		} else {
			returnValue = returnOIResolver.convertIfNecessary(arguments[0].get(), argumentOIs[0]);
		}

		return returnValue;
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("nvl (");
		for (int i = 0; i < children.length - 1; i++) {
			sb.append(children[i]).append(", ");
		}
		sb.append(children[children.length - 1]).append(")");
		return sb.toString();
	}

}
