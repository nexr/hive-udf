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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;


/**
 * GenericUDFRank
 */
@Description(name = "rank",
    value = "_FUNC_(hash_key, order_by_col1, order_by_col2 ...) " +
            "- Returns the rank of a value in a group of values",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(HASH(p1, p2), order_by_col1) FROM (\n"
    + "  > 		SELECT order_by_col1 FROM table \n"
    + "  >      DISTRIBUTE BY HASH(p1,p2)\n"
    + "  >      SORT BY p1, p2, order_by_col1 \n"
    + "  > );\n\n"
    + "  ORACLE \n"
    + "  >SELECT RANK() OVER(PARTITION BY p1,p2 ORDER BY order_by_col1) FROM table;\n")


@HivePdkUnitTests(
    setup = "", cleanup = "",
    cases = {
      @HivePdkUnitTest(
        query = "SELECT t.empno, t.deptno, t.sal, nexr_rank(t.deptno, t.sal) as rank"
        + "  FROM ("
        + "      select a.empno, a.deptno, a.sal from "
        + "          emp a"
        + "      distribute by a.deptno"
        + "      sort BY a.deptno, a.sal, a.empno"
        + "  ) t;",
        result = "7934\t10\t1300\t1\n"
        + "7782\t10\t2450\t2\n"
        + "7839\t10\t5000\t3\n"
        + "7369\t20\t800\t1\n"
        + "7876\t20\t1100\t2\n"
        + "7566\t20\t2975\t3\n"
        + "7788\t20\t3000\t4\n"
        + "7902\t20\t3000\t4\n"
        + "7900\t30\t950\t1\n"
        + "7521\t30\t1250\t2\n"
        + "7654\t30\t1250\t2\n"
        + "7844\t30\t1500\t4\n"
        + "7499\t30\t1600\t5\n"
        + "7698\t30\t2850\t6")
    }
  )

@UDFType(deterministic = false, stateful = true)
public class GenericUDFRank extends GenericUDF {

  private ObjectInspector[] argumentIOs;
  private Object[] prevArguments;
  private ObjectInspector prevHashKeyIO;

  protected long counter;
  protected final LongWritable result = new LongWritable(1);

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentException("The function RANK accepts at least more than 2 arguments.");
    }
    result.set(1);
    argumentIOs = arguments;
    prevArguments = new Object[arguments.length];
    prevHashKeyIO = ObjectInspectorUtils.getStandardObjectInspector(arguments[0], ObjectInspectorCopyOption.JAVA);
    return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object hashKey = arguments[0].get();
    Object prevHashKey = prevArguments[0];

    if (prevHashKey == null || ObjectInspectorUtils.compare(prevHashKey, prevHashKeyIO, hashKey, argumentIOs[0]) != 0) {
      different(true);
    } else {
      for (int i = 1; i < arguments.length; i++) {
        if (ObjectInspectorUtils.compare(prevArguments[i], ObjectInspectorUtils.getStandardObjectInspector(argumentIOs[i],
            ObjectInspectorCopyOption.JAVA), arguments[i].get(), argumentIOs[i]) != 0) {
          different(false);
          break;
        }
      }
    }
    next();

    for (int i = 0; i < arguments.length; i++) {
      prevArguments[i] = ObjectInspectorUtils.copyToStandardObject(arguments[i].get(),
          argumentIOs[i], ObjectInspectorCopyOption.JAVA);
    }

    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "rank(" + StringUtils.join(children, ',') + ")";
  }

  protected void different(boolean newKey) {
    if (newKey) {
      counter = 1;
    }
    result.set(counter);
  }

  protected void next() {
    counter++;
  }
}
