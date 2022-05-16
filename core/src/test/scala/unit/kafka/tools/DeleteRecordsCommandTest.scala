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

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}

import kafka.utils.{Exit, TestUtils}

import kafka.admin.DeleteRecordsCommand

@Timeout(value = 60)
class DeleteRecordsCommandTest {
  @Test
  def testMissingBootStrap(): Unit = {

    var exitStatus: Option[Int] = None
    var exitMessage: Option[String] = None
    Exit.setExitProcedure { (status, err) =>
      exitStatus = Some(status)
      exitMessage = err
      throw new RuntimeException
    }

    val deleteRecordsArgs = Array("--offset-json-file", "non-existent-file.json")
    try {
      val _ = new DeleteRecordsCommand.DeleteRecordsCommandOptions(deleteRecordsArgs)
    } catch {
      case e: RuntimeException => // Expected
    } finally {
      Exit.resetExitProcedure()
    }
    assertEquals(Some(1), exitStatus)
    assertTrue(exitMessage.get.contains("Missing required argument \"[bootstrap-server]\""))
  }


  @Test
  def testMissingJsonFile(): Unit = {
    var exitStatus: Option[Int] = None
    var exitMessage: Option[String] = None
    Exit.setExitProcedure { (status, err) =>
      exitStatus = Some(status)
      exitMessage = err
      throw new RuntimeException
    }

    val deleteRecordArgs = Array("--bootstrap-server", "")
    try {
      val _ = new DeleteRecordsCommand.DeleteRecordsCommandOptions(deleteRecordArgs)
    } catch {
      case e: RuntimeException => // expected
    } finally {
      Exit.resetExitProcedure()
    }

    assertEquals(Some(1), exitStatus)
    assertTrue(exitMessage.get.contains("Missing required argument \"[offset-json-file]\""))
  }


  @Test
  def testIncludingCommandConfigPath(): Unit = {
    var exitStatus: Option[Int] = None
    var exitMessage: Option[String] = None
    Exit.setExitProcedure { (status, err) =>
      exitStatus = Some(status)
      exitMessage = err
      throw new RuntimeException
    }

    val deleteRecordArgs = Array("--bootstrap-server", "", "--offset-json-file", "", "--command-config", "")
    var output = ""
    try {
      output = TestUtils.grabConsoleOutput(new DeleteRecordsCommand.DeleteRecordsCommandOptions(deleteRecordArgs))
    } catch {
      case e: RuntimeException => // expected
    } finally {
      Exit.resetExitProcedure()
    }

    assertEquals(None, exitStatus)
    assertTrue(output.equals(""))
  }

}