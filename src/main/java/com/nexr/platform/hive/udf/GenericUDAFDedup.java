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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

import java.util.ArrayList;
import java.util.List;

@Description(name = "dedup", value = "_FUNC_(x,y,z) - Removes duplicated row and return it as a struct.")
public class GenericUDAFDedup implements GenericUDAFResolver2 {

  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
    return new GenericUDAFDedupEval();
  }

  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    return new GenericUDAFDedupEval();
  }

  public static class GenericUDAFDedupEval extends GenericUDAFEvaluator {

    ObjectInspector[] input;
    ObjectInspector output;

    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      this.input = parameters;
      switch (m) {
        case PARTIAL1:
        case COMPLETE:
          List<String> names = new ArrayList<String>(parameters.length);
          List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>(parameters.length);
          for (int i = 0; i < parameters.length; i++) {
            names.add("_col" + i);
            inspectors.add(ObjectInspectorUtils.getStandardObjectInspector(parameters[i]));
          }
          return output = ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);
        case PARTIAL2:
          return output = parameters[0];
        case FINAL:
          return output = ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
        default:
          throw new IllegalArgumentException("never");
      }
    }

    @Override
      public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        return new DedupRow();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((DedupRow) agg).row = null;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      if (((DedupRow) agg).row == null) {
        Object[] array = new Object[parameters.length];
        for (int i = 0; i < array.length; i++) {
          array[i] = ObjectInspectorUtils.copyToStandardObject(parameters[i], input[i]);
        }
        ((DedupRow) agg).row = array;
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return ((DedupRow) agg).row;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (((DedupRow) agg).row == null) {
        ((DedupRow) agg).row = ObjectInspectorUtils.copyToStandardObject(partial, input[0]);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((DedupRow) agg).row;
    }

    static class DedupRow implements AggregationBuffer {
      Object row;
    }
  }
}
