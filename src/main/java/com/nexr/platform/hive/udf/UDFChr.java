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

import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFChr
 * 
 * This integrates the function from Oracle
 * http://www.techonthenet.com/oracle/functions/chr.php
 * 
 * <code>chr(number_code)</code>. 
 * number_code is the NUMBER code used to retrieve the character.
 *  
 */

@Description(name = "chr",
    value = "_FUNC_(number_code) - Returns returns the character based on the NUMBER code",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(116) FROM src LIMIT 1;\n" + "  t\n"
    + "  > SELECT _FUNC_(84) FROM src LIMIT 1;\n" + "  T\n")
@HivePdkUnitTests(
	setup = "", cleanup = "",
    cases = {
      @HivePdkUnitTest(
        query = "SELECT nexr_chr(116) FROM onerow;",
        result = "t"),
      @HivePdkUnitTest(
        query = "SELECT nexr_chr(84) FROM onerow;",
        result = "T")
    }
  )


public class UDFChr extends UDF {
	private Text result = new Text();

	public Text evaluate(IntWritable ascii_number) {
		if (ascii_number == null) {
			return null;
		}

		result.set(Character.toString((char) ascii_number.get()));
		return result;
	}
}
