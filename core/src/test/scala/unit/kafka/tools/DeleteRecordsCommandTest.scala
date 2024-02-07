/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package kafka.tools

import com.fasterxml.jackson.databind.JsonMappingException
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.{Test, Timeout}
import kafka.utils.Exit
import kafka.admin.{AdminOperationException, DeleteRecordsCommand}

import java.io.{ByteArrayOutputStream, IOException, PrintStream}

@Timeout(value = 60)
class DeleteRecordsCommandTest {

  private[this] val brokerList = "localhost:9092"
  private[this] val offsetJsonFilePath = "/tmp/non-existent-file.json"
  private[this] val commandConfig = "/tmp/command.config"

  @Test
  def testRequiredCommandConfigShouldSucceed(): Unit = {
    val deleteRecordArgs = Array("--bootstrap-server", brokerList, "--offset-json-file", offsetJsonFilePath)
    val _ = new DeleteRecordsCommand.DeleteRecordsCommandOptions(deleteRecordArgs)
  }

  @Test
  def testIncludingCommandConfigPathShouldSucceed(): Unit = {
    val deleteRecordArgs = Array("--bootstrap-server", brokerList, "--offset-json-file", offsetJsonFilePath, "--command-config", commandConfig)
    val _ = new DeleteRecordsCommand.DeleteRecordsCommandOptions(deleteRecordArgs)
  }

  @Test
  def testParseJsonDataShouldSucceed(): Unit = {
    val validParseJsonData = "{\"partitions\": [{\"topic\":\"my-topic\",\"partition\":0,\"offset\":3}," +
                             "{\"topic\":\"my-topic\",\"partition\":1,\"offset\":2}" +
                             "],\"version\": 1}"
    // Place holder for now. Likely should read back the output and confirm it aligns with expectations.
    val _ = DeleteRecordsCommand.parseOffsetJsonStringWithoutDedup(validParseJsonData)

  }

  @Test
  def testMissingBootStrap(): Unit = {
    val deleteRecordsArgs = Array("--offset-json-file", offsetJsonFilePath)
    assertCheckArgsExitCode(1, deleteRecordsArgs)
  }

  @Test
  def testMissingJsonFile(): Unit = {
    val deleteRecordArgs = Array("--bootstrap-server", brokerList)
    assertCheckArgsExitCode(1, deleteRecordArgs)
  }

  @Test
  def testParseInvalidJsonData(): Unit = {
    val invalidParseJsonData = "{\"partitions\": [{\"topic\":\"my-topic\",\"partition\":0,\"offset\":3}," +
      "{\"topic\":\"my-topic\",\"partition\":1,\"offset\":2}" +
      "]," +
      "\"version\": 1"
      try assertThrows(classOf[AdminOperationException],
        () => DeleteRecordsCommand.parseOffsetJsonStringWithoutDedup(invalidParseJsonData)) catch {case _ : AdminOperationException => }
  }

  @Test
  def testMissingPartitionsInJsonData(): Unit = {
    val missingPartitionsParseJsonData = "{\"partitions\": [{\"topic\":\"my-topic\",\"offset\":3}," +
      "{\"topic\":\"my-topic\",\"partition\":1,\"offset\":2}" +
      "],\"version\": 1}"
    try assertThrows(classOf[JsonMappingException],
      () => DeleteRecordsCommand.parseOffsetJsonStringWithoutDedup(missingPartitionsParseJsonData)) catch {case _ : JsonMappingException => }
  }

  @Test
  def testMissingTopicInJsonData(): Unit = {
    val missingTopicParseJsonData = "{\"partitions\": [{\"partition\":0,\"offset\":3}," +
      "{\"topic\":\"my-topic\",\"partition\":1,\"offset\":2}" +
      "],\"version\": 1}"
    try assertThrows(classOf[JsonMappingException],
      () => DeleteRecordsCommand.parseOffsetJsonStringWithoutDedup(missingTopicParseJsonData)) catch {case _ : JsonMappingException => }
  }

  @Test
  def testIncorrectVersionFieldInJsonData(): Unit = {
    val incorrectVersionParseJsonData = "{\"partitions\": [{\"topic\":\"my-topic\",\"partition\":0,\"offset\":3}," +
      "{\"topic\":\"my-topic\",\"partition\":1,\"offset\":2}" +
      "],\"version\": 2}"
    try assertThrows(classOf[AdminOperationException],
      () => DeleteRecordsCommand.parseOffsetJsonStringWithoutDedup(incorrectVersionParseJsonData)) catch {case _ : AdminOperationException => }
  }

  @Test
  def testExecuteWithNonexistentJsonPath(): Unit = {
    val deleteRecordArgs = Array("--bootstrap-server", brokerList, "--offset-json-file", offsetJsonFilePath)
    val printStream = new PrintStream(new ByteArrayOutputStream())
    assertThrows(classOf[IOException], () => DeleteRecordsCommand.execute(deleteRecordArgs, printStream))
  }

  private[this] def assertCheckArgsExitCode(expected: Int, options: Array[String]): Unit = {
    Exit.setExitProcedure { (exitCode: Int, _: Option[String]) =>
      assertEquals(expected,exitCode)
      throw new RuntimeException
    }

    try assertThrows(classOf[RuntimeException],
      () => new DeleteRecordsCommand.DeleteRecordsCommandOptions(options)) finally Exit.resetExitProcedure();
  }
}