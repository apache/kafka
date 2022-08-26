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
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, Timeout}

@Timeout(60)
class ReassignPartitionsCommandArgsTest {

  val missingBootstrapServerMsg = "Please specify --bootstrap-server"

  @BeforeEach
  def setUp(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))
  }

  @AfterEach
  def tearDown(): Unit = {
    Exit.resetExitProcedure()
  }

  ///// Test valid argument parsing
  @Test
  def shouldCorrectlyParseValidMinimumGenerateOptions(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--generate",
      "--broker-list", "101,102",
      "--topics-to-move-json-file", "myfile.json")
    ReassignPartitionsCommand.validateAndParseArgs(args)
  }

  @Test
  def shouldCorrectlyParseValidMinimumExecuteOptions(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--execute",
      "--reassignment-json-file", "myfile.json")
    ReassignPartitionsCommand.validateAndParseArgs(args)
  }

  @Test
  def shouldCorrectlyParseValidMinimumVerifyOptions(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--verify",
      "--reassignment-json-file", "myfile.json")
    ReassignPartitionsCommand.validateAndParseArgs(args)
  }

  @Test
  def shouldAllowThrottleOptionOnExecute(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--execute",
      "--throttle", "100",
      "--reassignment-json-file", "myfile.json")
    ReassignPartitionsCommand.validateAndParseArgs(args)
  }

  @Test
  def shouldUseDefaultsIfEnabled(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--execute",
      "--reassignment-json-file", "myfile.json")
    val opts = ReassignPartitionsCommand.validateAndParseArgs(args)
    assertEquals(10000L, opts.options.valueOf(opts.timeoutOpt))
    assertEquals(-1L, opts.options.valueOf(opts.interBrokerThrottleOpt))
  }

  @Test
  def testList(): Unit = {
    val args = Array(
      "--list",
      "--bootstrap-server", "localhost:1234")
    ReassignPartitionsCommand.validateAndParseArgs(args)
  }

  @Test
  def testCancelWithPreserveThrottlesOption(): Unit = {
    val args = Array(
      "--cancel",
      "--bootstrap-server", "localhost:1234",
      "--reassignment-json-file", "myfile.json",
      "--preserve-throttles")
    ReassignPartitionsCommand.validateAndParseArgs(args)
  }

  ///// Test handling missing or invalid actions
  @Test
  def shouldFailIfNoArgs(): Unit = {
    val args: Array[String]= Array()
    shouldFailWith(ReassignPartitionsCommand.helpText, args)
  }

  @Test
  def shouldFailIfBlankArg(): Unit = {
    val args = Array(" ")
    shouldFailWith("Command must include exactly one action", args)
  }

  @Test
  def shouldFailIfMultipleActions(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--execute",
      "--verify",
      "--reassignment-json-file", "myfile.json"
    )
    shouldFailWith("Command must include exactly one action", args)
  }

  ///// Test --execute
  @Test
  def shouldNotAllowExecuteWithTopicsOption(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--execute",
      "--reassignment-json-file", "myfile.json",
      "--topics-to-move-json-file", "myfile.json")
    shouldFailWith("Option \"[topics-to-move-json-file]\" can't be used with action \"[execute]\"", args)
  }

  @Test
  def shouldNotAllowExecuteWithBrokerList(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--execute",
      "--reassignment-json-file", "myfile.json",
      "--broker-list", "101,102"
    )
    shouldFailWith("Option \"[broker-list]\" can't be used with action \"[execute]\"", args)
  }

  @Test
  def shouldNotAllowExecuteWithoutReassignmentOption(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--execute")
    shouldFailWith("Missing required argument \"[reassignment-json-file]\"", args)
  }

  @Test
  def testMissingBootstrapServerArgumentForExecute(): Unit = {
    val args = Array(
      "--execute")
    shouldFailWith(missingBootstrapServerMsg, args)
  }

  ///// Test --generate
  @Test
  def shouldNotAllowGenerateWithoutBrokersAndTopicsOptions(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--generate")
    shouldFailWith("Missing required argument \"[topics-to-move-json-file]\"", args)
  }

  @Test
  def shouldNotAllowGenerateWithoutBrokersOption(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--topics-to-move-json-file", "myfile.json",
      "--generate")
    shouldFailWith("Missing required argument \"[broker-list]\"", args)
  }

  @Test
  def shouldNotAllowGenerateWithoutTopicsOption(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--broker-list", "101,102",
      "--generate")
    shouldFailWith("Missing required argument \"[topics-to-move-json-file]\"", args)
  }

  @Test
  def shouldNotAllowGenerateWithThrottleOption(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--generate",
      "--broker-list", "101,102",
      "--throttle", "100",
      "--topics-to-move-json-file", "myfile.json")
    shouldFailWith("Option \"[throttle]\" can't be used with action \"[generate]\"", args)
  }

  @Test
  def shouldNotAllowGenerateWithReassignmentOption(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--generate",
      "--broker-list", "101,102",
      "--topics-to-move-json-file", "myfile.json",
      "--reassignment-json-file", "myfile.json")
    shouldFailWith("Option \"[reassignment-json-file]\" can't be used with action \"[generate]\"", args)
  }

  @Test
  def shouldPrintHelpTextIfHelpArg(): Unit = {
    val args: Array[String]= Array("--help")
    // note, this is not actually a failed case, it's just we share the same `printUsageAndDie` method when wrong arg received
    shouldFailWith(ReassignPartitionsCommand.helpText, args)
  }

  ///// Test --verify
  @Test
  def shouldNotAllowVerifyWithoutReassignmentOption(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--verify")
    shouldFailWith("Missing required argument \"[reassignment-json-file]\"", args)
  }

  @Test
  def shouldNotAllowBrokersListWithVerifyOption(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--verify",
      "--broker-list", "100,101",
      "--reassignment-json-file", "myfile.json")
    shouldFailWith("Option \"[broker-list]\" can't be used with action \"[verify]\"", args)
  }

  @Test
  def shouldNotAllowThrottleWithVerifyOption(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--verify",
      "--throttle", "100",
      "--reassignment-json-file", "myfile.json")
    shouldFailWith("Option \"[throttle]\" can't be used with action \"[verify]\"", args)
  }

  @Test
  def shouldNotAllowTopicsOptionWithVerify(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:1234",
      "--verify",
      "--reassignment-json-file", "myfile.json",
      "--topics-to-move-json-file", "myfile.json")
    shouldFailWith("Option \"[topics-to-move-json-file]\" can't be used with action \"[verify]\"", args)
  }

  def shouldFailWith(msg: String, args: Array[String]): Unit = {
    val e = assertThrows(classOf[Exception], () => ReassignPartitionsCommand.validateAndParseArgs(args),
      () => s"Should have failed with [$msg] but no failure occurred.")
    assertTrue(e.getMessage.startsWith(msg), s"Expected exception with message:\n[$msg]\nbut was\n[${e.getMessage}]")
  }

  ///// Test --cancel
  @Test
  def shouldNotAllowCancelWithoutBootstrapServerOption(): Unit = {
    val args = Array(
      "--cancel")
    shouldFailWith(missingBootstrapServerMsg, args)
  }

  @Test
  def shouldNotAllowCancelWithoutReassignmentJsonFile(): Unit = {
    val args = Array(
      "--cancel",
      "--bootstrap-server", "localhost:1234",
      "--preserve-throttles")
    shouldFailWith("Missing required argument \"[reassignment-json-file]\"", args)
  }
}
