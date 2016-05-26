/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import org.junit.Assert._
import org.junit.Test

class CommandLineUtilsTest {


  @Test(expected = classOf[java.lang.IllegalArgumentException])
  def testParseEmptyArg() {
    val argArray = Array("my.empty.property=")
    CommandLineUtils.parseKeyValueArgs(argArray, false)
  }


  @Test
  def testParseEmptyArgAsValid() {
    val argArray = Array("my.empty.property=")
    val props = CommandLineUtils.parseKeyValueArgs(argArray)
    assertEquals("Value of a key with missing value should be an empty string",props.getProperty("my.empty.property"),"")
  }

  @Test
  def testParseSingleArg() {
    val argArray = Array("my.property=value")
    val props = CommandLineUtils.parseKeyValueArgs(argArray)
    assertEquals("Value of a single property should be 'value' ",props.getProperty("my.property"),"value")
  }

  @Test
  def testParseArgs() {
    val argArray = Array("first.property=first","second.property=second")
    val props = CommandLineUtils.parseKeyValueArgs(argArray, false)
    assertEquals("Value of first property should be 'first'",props.getProperty("first.property"),"first")
    assertEquals("Value of second property should be 'second'",props.getProperty("second.property"),"second")
  }

}
