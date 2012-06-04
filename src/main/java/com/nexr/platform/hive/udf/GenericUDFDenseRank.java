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


@Description(name = "dense_rank",
    value = "_FUNC_(hash_key, order_by_col1, order_by_col2 ...) " +
            "- Returns  the rank of a row in an ordered group of rows",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(HASH(p1, p2), order_by_col1) FROM (\n"
    + "  > 		SELECT order_by_col1 FROM table \n"
    + "  >      DISTRIBUTE BY HASH(p1,p2)\n"
    + "  >      SORT BY p1, p2, order_by_col1 \n"
    + "  > );\n\n"
    + "ORACLE:\n"
    + "  > SELECT DENSE_RANK() OVER(PARTITION BY p1,p2 ORDER BY order_by_col1) FROM table;\n"
    )

    @HivePdkUnitTests(
    setup = "", cleanup = "",
    cases = {
      @HivePdkUnitTest(
        query = "SELECT t.empno, t.deptno, t.sal, nexr_dense_rank(t.deptno, t.sal) as rank"
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
        + "7844\t30\t1500\t3\n"
        + "7499\t30\t1600\t4\n"
        + "7698\t30\t2850\t5")
    }
  )

@UDFType(deterministic = false, stateful = true)
public class GenericUDFDenseRank extends GenericUDFRank {

  @Override
  protected void different(boolean newKey) {
    if (newKey) {
      counter = 1;
    } else {
      counter++;
    }
    result.set(counter);
  }

  @Override
  protected void next() {
  }

  @Override
  public String getDisplayString(String[] children) {
    return "dense_rank(" + StringUtils.join(children, ',') + ")";
  }
}
