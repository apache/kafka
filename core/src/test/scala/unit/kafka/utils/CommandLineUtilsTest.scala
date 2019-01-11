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

import java.util.Properties

import joptsimple.{OptionParser, OptionSpec}
import org.junit.Assert._
import org.junit.Test

class CommandLineUtilsTest {


  @Test(expected = classOf[java.lang.IllegalArgumentException])
  def testParseEmptyArg() {
    val argArray = Array("my.empty.property=")

    CommandLineUtils.parseKeyValueArgs(argArray, acceptMissingValue = false)
  }

  @Test(expected = classOf[java.lang.IllegalArgumentException])
  def testParseEmptyArgWithNoDelimiter() {
    val argArray = Array("my.empty.property")

    CommandLineUtils.parseKeyValueArgs(argArray, acceptMissingValue = false)
  }

  @Test
  def testParseEmptyArgAsValid() {
    val argArray = Array("my.empty.property=", "my.empty.property1")
    val props = CommandLineUtils.parseKeyValueArgs(argArray)

    assertEquals("Value of a key with missing value should be an empty string", props.getProperty("my.empty.property"), "")
    assertEquals("Value of a key with missing value with no delimiter should be an empty string", props.getProperty("my.empty.property1"), "")
  }

  @Test
  def testParseSingleArg() {
    val argArray = Array("my.property=value")
    val props = CommandLineUtils.parseKeyValueArgs(argArray)

    assertEquals("Value of a single property should be 'value' ", props.getProperty("my.property"), "value")
  }

  @Test
  def testParseArgs() {
    val argArray = Array("first.property=first","second.property=second")
    val props = CommandLineUtils.parseKeyValueArgs(argArray)

    assertEquals("Value of first property should be 'first'", props.getProperty("first.property"), "first")
    assertEquals("Value of second property should be 'second'", props.getProperty("second.property"), "second")
  }

  @Test
  def testParseArgsWithMultipleDelimiters() {
    val argArray = Array("first.property==first", "second.property=second=", "third.property=thi=rd")
    val props = CommandLineUtils.parseKeyValueArgs(argArray)

    assertEquals("Value of first property should be '=first'", props.getProperty("first.property"), "=first")
    assertEquals("Value of second property should be 'second='", props.getProperty("second.property"), "second=")
    assertEquals("Value of second property should be 'thi=rd'", props.getProperty("third.property"), "thi=rd")
  }

  val props = new Properties()
  val parser = new OptionParser(false)
  var stringOpt : OptionSpec[String] = _
  var intOpt : OptionSpec[java.lang.Integer] = _
  var stringOptOptionalArg : OptionSpec[String] = _
  var intOptOptionalArg : OptionSpec[java.lang.Integer] = _
  var stringOptOptionalArgNoDefault : OptionSpec[String] = _
  var intOptOptionalArgNoDefault : OptionSpec[java.lang.Integer] = _

  def setUpOptions(): Unit = {
    stringOpt = parser.accepts("str")
      .withRequiredArg
      .ofType(classOf[String])
      .defaultsTo("default-string")
    intOpt = parser.accepts("int")
      .withRequiredArg()
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100)
    stringOptOptionalArg = parser.accepts("str-opt")
      .withOptionalArg
      .ofType(classOf[String])
      .defaultsTo("default-string-2")
    intOptOptionalArg = parser.accepts("int-opt")
      .withOptionalArg
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(200)
    stringOptOptionalArgNoDefault = parser.accepts("str-opt-nodef")
      .withOptionalArg
      .ofType(classOf[String])
    intOptOptionalArgNoDefault = parser.accepts("int-opt-nodef")
      .withOptionalArg
      .ofType(classOf[java.lang.Integer])
  }

  @Test
  def testMaybeMergeOptionsOverwriteExisting(): Unit = {
    setUpOptions()

    props.put("skey", "existing-string")
    props.put("ikey", "300")
    props.put("sokey", "existing-string-2")
    props.put("iokey", "400")
    props.put("sondkey", "existing-string-3")
    props.put("iondkey", "500")

    val options = parser.parse(
      "--str", "some-string",
      "--int", "600",
      "--str-opt", "some-string-2",
      "--int-opt", "700",
      "--str-opt-nodef", "some-string-3",
      "--int-opt-nodef", "800"
    )

    CommandLineUtils.maybeMergeOptions(props, "skey", options, stringOpt)
    CommandLineUtils.maybeMergeOptions(props, "ikey", options, intOpt)
    CommandLineUtils.maybeMergeOptions(props, "sokey", options, stringOptOptionalArg)
    CommandLineUtils.maybeMergeOptions(props, "iokey", options, intOptOptionalArg)
    CommandLineUtils.maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault)
    CommandLineUtils.maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault)

    assertEquals("some-string", props.get("skey"))
    assertEquals("600", props.get("ikey"))
    assertEquals("some-string-2", props.get("sokey"))
    assertEquals("700", props.get("iokey"))
    assertEquals("some-string-3", props.get("sondkey"))
    assertEquals("800", props.get("iondkey"))
  }

  @Test
  def testMaybeMergeOptionsDefaultOverwriteExisting(): Unit = {
    setUpOptions()

    props.put("sokey", "existing-string")
    props.put("iokey", "300")
    props.put("sondkey", "existing-string-2")
    props.put("iondkey", "400")

    val options = parser.parse(
      "--str-opt",
      "--int-opt",
      "--str-opt-nodef",
      "--int-opt-nodef"
    )

    CommandLineUtils.maybeMergeOptions(props, "sokey", options, stringOptOptionalArg)
    CommandLineUtils.maybeMergeOptions(props, "iokey", options, intOptOptionalArg)
    CommandLineUtils.maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault)
    CommandLineUtils.maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault)

    assertEquals("default-string-2", props.get("sokey"))
    assertEquals("200", props.get("iokey"))
    assertNull(props.get("sondkey"))
    assertNull(props.get("iondkey"))
  }

  @Test
  def testMaybeMergeOptionsDefaultValueIfNotExist(): Unit = {
    setUpOptions()

    val options = parser.parse()

    CommandLineUtils.maybeMergeOptions(props, "skey", options, stringOpt)
    CommandLineUtils.maybeMergeOptions(props, "ikey", options, intOpt)
    CommandLineUtils.maybeMergeOptions(props, "sokey", options, stringOptOptionalArg)
    CommandLineUtils.maybeMergeOptions(props, "iokey", options, intOptOptionalArg)
    CommandLineUtils.maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault)
    CommandLineUtils.maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault)

    assertEquals("default-string", props.get("skey"))
    assertEquals("100", props.get("ikey"))
    assertEquals("default-string-2", props.get("sokey"))
    assertEquals("200", props.get("iokey"))
    assertNull(props.get("sondkey"))
    assertNull(props.get("iondkey"))
  }

  @Test
  def testMaybeMergeOptionsNotOverwriteExisting(): Unit = {
    setUpOptions()

    props.put("skey", "existing-string")
    props.put("ikey", "300")
    props.put("sokey", "existing-string-2")
    props.put("iokey", "400")
    props.put("sondkey", "existing-string-3")
    props.put("iondkey", "500")

    val options = parser.parse()

    CommandLineUtils.maybeMergeOptions(props, "skey", options, stringOpt)
    CommandLineUtils.maybeMergeOptions(props, "ikey", options, intOpt)
    CommandLineUtils.maybeMergeOptions(props, "sokey", options, stringOptOptionalArg)
    CommandLineUtils.maybeMergeOptions(props, "iokey", options, intOptOptionalArg)
    CommandLineUtils.maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault)
    CommandLineUtils.maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault)

    assertEquals("existing-string", props.get("skey"))
    assertEquals("300", props.get("ikey"))
    assertEquals("existing-string-2", props.get("sokey"))
    assertEquals("400", props.get("iokey"))
    assertEquals("existing-string-3", props.get("sondkey"))
    assertEquals("500", props.get("iondkey"))
  }
}
