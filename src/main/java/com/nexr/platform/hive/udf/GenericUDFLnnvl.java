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
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

/**
 * GenericUDF Class for SQL construct "lnnvl(condition)". see <a href=
 * "http://download.oracle.com/docs/cd/B19306_01/server.102/b14200/functions078.htm#sthref1550" >LNNVL</a>.
 * <p>
 * example:
 * 
 * <pre>
 * SELECT COUNT(*) FROM employees WHERE LNNVL(commission_pct >= .2) ;
 * returns employees who receive a commission of less than 20% and no commission.
 * </pre>
 * 
 */
@Description(name = "lnnvl", value = "_FUNC_(condition) " +
		"- Evalutates a condition when one of the operands may contains a NULL value.", 
		extended = "Example:\n  "
		+ "	> SELECT _FUNC_(condition) FROM src;\n FALSE if condition is true " + "  return false")
@HivePdkUnitTests(setup = "create table dual_data (i int); "
		+ "insert overwrite table dual_data select 1 from dual limit 1;", 
		cleanup = "drop table if exists dual_data;", 
		cases = {
		@HivePdkUnitTest(query = "SELECT nexr_lnnvl(true) FROM dual_data;", result = "false"),
		@HivePdkUnitTest(query = "SELECT name, height FROM dual WHERE nexr_lnnvl(height > 175 );", result = "Adam\t174.3\n"
				+ "Bravo\tNULL") })
public class GenericUDFLnnvl extends GenericUDF {

	private ObjectInspector[] argumentIOs;
	private ObjectInspector returnInspector;

	private ObjectInspectorConverters.Converter[] converters;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		if (arguments.length < 1) {
			throw new UDFArgumentLengthException("The function lnnvl(condition ) needs at least one arguments");
		}

		if (arguments[0].getTypeName() != Constants.BOOLEAN_TYPE_NAME
				&& arguments[0].getTypeName() != Constants.VOID_TYPE_NAME) {
			throw new UDFArgumentTypeException(0, "Argument (0) of function lnnvl must be "
					+ Constants.BOOLEAN_TYPE_NAME + " but " + arguments[0].getTypeName() + " was found.");
		}

		this.argumentIOs = arguments;
		converters = new ObjectInspectorConverters.Converter[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
					PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
		}

		returnInspector = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
		return returnInspector;
	}

	private BooleanWritable booleanWritable = new BooleanWritable();

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		if (arguments[0].get() == null) {
			booleanWritable.set(true);
			return booleanWritable;
		}

		BooleanWritable value = (BooleanWritable) ((PrimitiveObjectInspector) argumentIOs[0])
				.getPrimitiveWritableObject(arguments[0].get());

		booleanWritable.set(!value.get());
		return booleanWritable;
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("lnnvl (");
		for (int i = 0; i < children.length - 1; i++) {
			sb.append(children[i]).append(", ");
		}
		sb.append(children[children.length - 1]).append(")");
		return sb.toString();
	}

}
