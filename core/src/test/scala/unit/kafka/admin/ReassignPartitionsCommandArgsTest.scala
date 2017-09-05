/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.admin

import kafka.utils.Exit
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

class ReassignPartitionsCommandArgsTest extends JUnitSuite {

  @Before
  def setUp() {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))
  }

  @After
  def tearDown() {
    Exit.resetExitProcedure()
  }

  /**
    * HAPPY PATH
    */

  @Test
  def shouldCorrectlyParseValidMinimumGenerateOptions(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--generate",
      "--broker-list", "101,102",
      "--topics-to-move-json-file", "myfile.json")
    ReassignPartitionsCommand.validateAndParseArgs(args)
  }

  @Test
  def shouldCorrectlyParseValidMinimumExecuteOptions(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--execute",
      "--reassignment-json-file", "myfile.json")
    ReassignPartitionsCommand.validateAndParseArgs(args)
  }

  @Test
  def shouldCorrectlyParseValidMinimumVerifyOptions(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--verify",
      "--reassignment-json-file", "myfile.json")
    ReassignPartitionsCommand.validateAndParseArgs(args)
  }

  @Test
  def shouldAllowThrottleOptionOnExecute(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--execute",
      "--throttle", "100",
      "--reassignment-json-file", "myfile.json")
    ReassignPartitionsCommand.validateAndParseArgs(args)
  }

  @Test
  def shouldUseDefaultsIfEnabled(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--execute",
      "--reassignment-json-file", "myfile.json")
    val opts = ReassignPartitionsCommand.validateAndParseArgs(args)
    assertEquals(10000L, opts.options.valueOf(opts.timeoutOpt))
    assertEquals(-1L, opts.options.valueOf(opts.throttleOpt))
  }

  /**
    * NO ARGS
    */

  @Test
  def shouldFailIfNoArgs(): Unit = {
    val args: Array[String]= Array()
    shouldFailWith("This command moves topic partitions between replicas.", args)
  }

  @Test
  def shouldFailIfBlankArg(): Unit = {
    val args = Array(" ")
    shouldFailWith("Command must include exactly one action: --generate, --execute or --verify", args)
  }

  /**
    * UNHAPPY PATH: EXECUTE ACTION
    */

  @Test
  def shouldNotAllowExecuteWithTopicsOption(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--execute",
      "--reassignment-json-file", "myfile.json",
      "--topics-to-move-json-file", "myfile.json")
    shouldFailWith("Option \"[execute]\" can't be used with option\"[topics-to-move-json-file]\"", args)
  }

  @Test
  def shouldNotAllowExecuteWithBrokers(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--execute",
      "--reassignment-json-file", "myfile.json",
      "--broker-list", "101,102"
    )
    shouldFailWith("Option \"[execute]\" can't be used with option\"[broker-list]\"", args)
  }

  @Test
  def shouldNotAllowExecuteWithoutReassignmentOption(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--execute")
    shouldFailWith("If --execute option is used, command must include --reassignment-json-file that was output during the --generate option", args)
  }

  /**
    * UNHAPPY PATH: GENERATE ACTION
    */

  @Test
  def shouldNotAllowGenerateWithoutBrokersAndTopicsOptions(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--generate")
    shouldFailWith("If --generate option is used, command must include both --topics-to-move-json-file and --broker-list options", args)
  }

  @Test
  def shouldNotAllowGenerateWithoutBrokersOption(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--topics-to-move-json-file", "myfile.json",
      "--generate")
    shouldFailWith("If --generate option is used, command must include both --topics-to-move-json-file and --broker-list options", args)
  }

  @Test
  def shouldNotAllowGenerateWithoutTopicsOption(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--broker-list", "101,102",
      "--generate")
    shouldFailWith("If --generate option is used, command must include both --topics-to-move-json-file and --broker-list options", args)
  }

  @Test
  def shouldNotAllowGenerateWithThrottleOption(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--generate",
      "--broker-list", "101,102",
      "--throttle", "100",
      "--topics-to-move-json-file", "myfile.json")
    shouldFailWith("Option \"[generate]\" can't be used with option\"[throttle]\"", args)
  }

  @Test
  def shouldNotAllowGenerateWithReassignmentOption(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--generate",
      "--broker-list", "101,102",
      "--topics-to-move-json-file", "myfile.json",
      "--reassignment-json-file", "myfile.json")
    shouldFailWith("Option \"[generate]\" can't be used with option\"[reassignment-json-file]\"", args)
  }

  /**
    * UNHAPPY PATH: VERIFY ACTION
    */

  @Test
  def shouldNotAllowVerifyWithoutReassignmentOption(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--verify")
    shouldFailWith("If --verify option is used, command must include --reassignment-json-file that was used during the --execute option", args)
  }

  @Test
  def shouldNotAllowBrokersListWithVerifyOption(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--verify",
      "--broker-list", "100,101",
      "--reassignment-json-file", "myfile.json")
    shouldFailWith("Option \"[verify]\" can't be used with option\"[broker-list]\"", args)
  }

  @Test
  def shouldNotAllowThrottleWithVerifyOption(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--verify",
      "--throttle", "100",
      "--reassignment-json-file", "myfile.json")
    shouldFailWith("Option \"[verify]\" can't be used with option\"[throttle]\"", args)
  }

  @Test
  def shouldNotAllowTopicsOptionWithVerify(): Unit = {
    val args = Array(
      "--zookeeper", "localhost:1234",
      "--verify",
      "--reassignment-json-file", "myfile.json",
      "--topics-to-move-json-file", "myfile.json")
    shouldFailWith("Option \"[verify]\" can't be used with option\"[topics-to-move-json-file]\"", args)
  }

  def shouldFailWith(msg: String, args: Array[String]): Unit = {
    try {
      ReassignPartitionsCommand.validateAndParseArgs(args)
      fail(s"Should have failed with [$msg] but no failure occurred.")
    } catch {
      case e: Exception => assertTrue(s"Expected exception with message:\n[$msg]\nbut was\n[${e.getMessage}]", e.getMessage.startsWith(msg))
    }
  }
}
