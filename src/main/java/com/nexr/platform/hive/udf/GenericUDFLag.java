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

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;


@Description(name = "lag",
    value = "_FUNC_(hash_key,column[,offset[,default]]) " +
    		"-  Returns values from a previous row in the table.",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(HASH(p1,p2), column [,offset[,default]]) FROM (\n"
    + "  > 		SELECT ~ FROM table DISTRIBUTE BY HASH(p1,p2) SORT BY p1,p2\n"
    + "  > );")

    @HivePdkUnitTests(
    setup = "", cleanup = "",
    cases = {
      @HivePdkUnitTest(
        query = "SELECT t.empno, t.deptno, t.sal, nexr_lag(hash(t.deptno),t.sal) as col"
        	+"  FROM ("
        	+"      select a.empno, a.deptno, a.sal from emp a"
        	+"      distribute by hash(a.deptno)"
        	+"      sort BY a.deptno, a.empno"
        	+"  ) t;",
        result =
        	"7782\t10\t2450\tNULL\n"
          +"7839\t10\t5000\t2450\n"
        	+"7934\t10\t1300\t5000\n"
        	+"7369\t20\t800\tNULL\n"
        	+"7566\t20\t2975\t800\n"
        	+"7788\t20\t3000\t2975\n"
        	+"7876\t20\t1100\t3000\n"
        	+"7902\t20\t3000\t1100\n"
        	+"7499\t30\t1600\tNULL\n"
        	+"7521\t30\t1250\t1600\n"
        	+"7654\t30\t1250\t1250\n"
        	+"7698\t30\t2850\t1250\n"
        	+"7844\t30\t1500\t2850\n"
        	+"7900\t30\t950\t1500"),
        @HivePdkUnitTest(
            query = "SELECT t.empno, t.deptno, t.sal, nexr_lag(hash(t.deptno),t.sal,2,0) as col"
            +"  FROM ("
            +"      select a.empno, a.deptno, a.sal from emp a "
            +"      distribute by hash(a.deptno)"
            +"      sort BY a.deptno, a.empno"
            +"  ) t;",
            result =
            	 "7782\t10\t2450\t0\n"
                +"7839\t10\t5000\t0\n"
            	+"7934\t10\t1300\t2450\n"
            	+"7369\t20\t800\t0\n"
            	+"7566\t20\t2975\t0\n"
            	+"7788\t20\t3000\t800\n"
            	+"7876\t20\t1100\t2975\n"
            	+"7902\t20\t3000\t3000\n"
            	+"7499\t30\t1600\t0\n"
            	+"7521\t30\t1250\t0\n"
            	+"7654\t30\t1250\t1600\n"
            	+"7698\t30\t2850\t1250\n"
            	+"7844\t30\t1500\t1250\n"
            	+"7900\t30\t950\t2850")
    }
  )
    
    
@UDFType(deterministic = false, stateful = true)
public class GenericUDFLag extends GenericUDF {
	private ObjectInspector[] argumentOIs;
	private ObjectInspector resultOI, prevHashStandardOI, valueStandardOI;
	private Object prevHash;
	private ArrayList<Object> queue = new ArrayList<Object>();
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length<2||arguments.length>4) {
			throw new UDFArgumentException("The function accepts more than two arguments.");
		}
		
		argumentOIs = arguments;
		prevHashStandardOI = ObjectInspectorUtils.getStandardObjectInspector(arguments[0],ObjectInspectorCopyOption.JAVA);
		valueStandardOI =  ObjectInspectorUtils.getStandardObjectInspector(arguments[1],ObjectInspectorCopyOption.JAVA);
		resultOI=arguments[1];
		return resultOI;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object hash = arguments[0].get();
		Object value = arguments[1].get();
		int offset=1;
		Object defaultValue=null;
		
		if (arguments.length>=3)
			offset = PrimitiveObjectInspectorUtils.getInt(arguments[2].get(),(PrimitiveObjectInspector)argumentOIs[2]);
		if (arguments.length==4)
			defaultValue=arguments[3].get();
		
		if (prevHash==null||ObjectInspectorUtils.compare(prevHash,prevHashStandardOI,hash,argumentOIs[0])!=0) {
			queue.clear();
		}
		
		queue.add(ObjectInspectorUtils.copyToStandardObject(value,argumentOIs[1],ObjectInspectorCopyOption.JAVA));
		prevHash=ObjectInspectorUtils.copyToStandardObject(hash, argumentOIs[0],ObjectInspectorCopyOption.JAVA);
		if (queue.size()==offset+1) {
			Converter converter = ObjectInspectorConverters.getConverter(valueStandardOI, resultOI);
			return converter.convert(queue.remove(0));
		}
		return defaultValue;
	}

	@Override
	public String getDisplayString(String[] children) {
		return "lag(" + StringUtils.join(children, ',') + ")";
	}
}
