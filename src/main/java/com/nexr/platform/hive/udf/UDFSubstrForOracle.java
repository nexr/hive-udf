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
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

/**
 * UDFSubstrForOracle.
 *
 * In some case, Hive built-in substr UDF return different value from Oracle substr funtion's.
 * This substr function exactly behave like oracle.
 * the substr function allows you to extract a substring from a string
 * The syntax for the substr function is:
 * substr( string, start_position, [ length ] )
 *  string is the source string.
 *  start_position is the position for extraction. The first position in the string is always 1.
 * length is optional. It is the number of characters to extract. 
 * If this parameter is omitted, substr will return the entire string.
 * 
 * 
 */
@Description(name = "substr",
    value = "_FUNC_(str, start_pos[, length ]) " +
    		"- returns the substring of str that starts at pos_start and is of length",
    extended = "start_pos is a 1-based index. If start_pos<0 the starting position is"
    + " determined by counting backwards from the end of str.\n"
    + "Example:\n "
    + "  > SELECT _FUNC_('abcdefg', 5) FROM src LIMIT 1;\n"
    + "  efg\n"
    + "  > SELECT _FUNC_('abcdefg',-5,4) FROM src LIMIT 1;\n"
    + "  cdef"
    + "  > SELECT _FUNC_('abcdefg',3,4)  FROM src LIMIT 1;\n"
    + "  cdef")


@HivePdkUnitTests(
		setup ="", cleanup ="",
		cases = {
			@HivePdkUnitTest(
				query = "SELECT nexr_substr('abcdefg', 5) FROM onerow;",
				result = "efg"
			),
			@HivePdkUnitTest(
				query = "SELECT nexr_substr('abcdefg',3,4) FROM onerow;",
				result = "cdef"
			),
			@HivePdkUnitTest(
				query = "SELECT nexr_substr('abcdefg',-5,4) FROM onerow;",
				result = "cdef"
			)
		}
	)
public class UDFSubstrForOracle extends UDF {
  private Text r;

  public UDFSubstrForOracle() {
    r = new Text();
  }

  public Text evaluate(Text t, IntWritable pos, IntWritable len) {

    if ((t == null) || (pos == null) || (len == null)) {
      return null;
    }

    r.clear();
    if ((len.get() <= 0)) {
        //return r;
        return null;
    }

    String s = t.toString();
    if ((Math.abs(pos.get()) > s.length())) {
      //return r;
    	return null;
    }

    int start, end;

    if (pos.get() > 0) {
      start = pos.get() - 1;
    } else if (pos.get() < 0) {
      start = s.length() + pos.get();
    } else {
      start = 0;
    }

    if ((s.length() - start) < len.get()) {
      end = s.length();
    } else {
      end = start + len.get();
    }

    r.set(s.substring(start, end));
    return r;
  }

  private IntWritable maxValue = new IntWritable(Integer.MAX_VALUE);

  public Text evaluate(Text s, IntWritable pos) {
    return evaluate(s, pos, maxValue);
  }

}
