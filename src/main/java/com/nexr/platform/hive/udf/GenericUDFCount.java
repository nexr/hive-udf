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


@Description(name = "count",
    value = "_FUNC_(hash_key, order_by_col) " +
                "- Returns the count value of group",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(HASH(p1, p2), order_by_col) FROM (\n"
    + "  >              SELECT ~ FROM table DISTRIBUTE BY HASH(p1,p2) SORT BY p1,p2,order_by_col DESC, ... \n"
    + "  > );")
 
@HivePdkUnitTests(
        setup = "", cleanup = "",
    cases = {
      @HivePdkUnitTest(
        query = "SELECT t.deptno, nexr_count(hash(t.deptno),t.empno) as emp_count"
                +"  FROM ("
                +"      select a.empno, a.deptno from emp a"
                +"      distribute by hash(a.deptno)"
                +"      sort BY a.deptno, a.empno"
                +"  ) t;",
        result ="10\t1\n"
                +"10\t2\n"
                +"10\t3\n"
                +"20\t1\n"
                +"20\t2\n"
                +"20\t3\n"
                +"20\t4\n"
                +"20\t5\n"
                +"30\t1\n"
                +"30\t2\n"
                +"30\t3\n"
                +"30\t4\n"
                +"30\t5\n"
                +"30\t6")
    }
  )
    
@UDFType(deterministic = false, stateful = true)
public class GenericUDFCount extends GenericUDF {
        private final LongWritable longResult = new LongWritable();
        private ObjectInspector hashOI, prevHashStandardOI;
        private Object prevHash;
        @Override
        public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
                if (arguments.length != 2) {
                        throw new UDFArgumentException("Exactly two argument is expected.");
                }

                hashOI = arguments[0];
                prevHashStandardOI=ObjectInspectorUtils.getStandardObjectInspector(hashOI,ObjectInspectorCopyOption.JAVA);
                
                longResult.set(0);

                return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        @Override
        public Object evaluate(DeferredObject[] arguments) throws HiveException {
                Object hash = arguments[0].get();
 
                if (prevHash==null||ObjectInspectorUtils.compare(prevHash,prevHashStandardOI,hash,hashOI)!=0) {
                        longResult.set(0);
 
                }

                prevHash=ObjectInspectorUtils.copyToStandardObject(hash,hashOI, ObjectInspectorCopyOption.JAVA);
                longResult.set(longResult.get()+1);
                return longResult;
        }

        @Override
        public String getDisplayString(String[] children) {
                return "count(" + StringUtils.join(children, ',') + ")";
        }
}

