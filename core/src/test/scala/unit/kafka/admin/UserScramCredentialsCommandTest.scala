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

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

import kafka.server.BaseRequestTest
import kafka.utils.Exit
import kafka.utils.TestUtils
import kafka.utils.TestInfoUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class UserScramCredentialsCommandTest extends BaseRequestTest {
  override def brokerCount = 1
  var exitStatus: Option[Int] = None
  var exitMessage: Option[String] = None

  case class ConfigCommandResult(stdout: String, exitStatus: Option[Int] = None)

  private def runConfigCommandViaBroker(args: Array[String]) : ConfigCommandResult = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val utf8 = StandardCharsets.UTF_8.name
    val printStream = new PrintStream(byteArrayOutputStream, true, utf8)
    var exitStatus: Option[Int] = None
    Exit.setExitProcedure { (status, _) =>
      exitStatus = Some(status)
      throw new RuntimeException
    }
    val commandArgs = Array("--bootstrap-server", bootstrapServers()) ++ args
    try {
      Console.withOut(printStream) {
        ConfigCommand.main(commandArgs)
      }
      ConfigCommandResult(byteArrayOutputStream.toString(utf8))
    } catch {
      case e: Exception => {
        debug(s"Exception running ConfigCommand ${commandArgs.mkString(" ")}", e)
        ConfigCommandResult("", exitStatus)
      }
    } finally {
      printStream.close
      Exit.resetExitProcedure()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft", "zk"))
  def testUserScramCredentialsRequests(quorum: String): Unit = {
    val user1 = "user1"
    // create and describe a credential
    var result = runConfigCommandViaBroker(Array("--user", user1, "--alter", "--add-config", "SCRAM-SHA-256=[iterations=4096,password=foo-secret]"))
    val alterConfigsUser1Out = s"Completed updating config for user $user1.\n"
    assertEquals(alterConfigsUser1Out, result.stdout)
    val scramCredentialConfigsUser1Out = s"SCRAM credential configs for user-principal '$user1' are SCRAM-SHA-256=iterations=4096\n"
    TestUtils.waitUntilTrue(() => runConfigCommandViaBroker(Array("--user", user1, "--describe")).stdout ==
      scramCredentialConfigsUser1Out, s"Failed to describe SCRAM credential change '$user1'")
    // create a user quota and describe the user again
    result = runConfigCommandViaBroker(Array("--user", user1, "--alter", "--add-config", "consumer_byte_rate=20000"))
    assertEquals(alterConfigsUser1Out, result.stdout)
    val quotaConfigsUser1Out = s"Quota configs for user-principal '$user1' are consumer_byte_rate=20000.0\n"
    TestUtils.waitUntilTrue(() => runConfigCommandViaBroker(Array("--user", user1, "--describe")).stdout ==
      s"$quotaConfigsUser1Out$scramCredentialConfigsUser1Out", s"Failed to describe Quota change for '$user1'")

    // now do the same thing for user2
    val user2 = "user2"
    // create and describe a credential
    result = runConfigCommandViaBroker(Array("--user", user2, "--alter", "--add-config", "SCRAM-SHA-256=[iterations=4096,password=foo-secret]"))
    val alterConfigsUser2Out = s"Completed updating config for user $user2.\n"
    assertEquals(alterConfigsUser2Out, result.stdout)
    val scramCredentialConfigsUser2Out = s"SCRAM credential configs for user-principal '$user2' are SCRAM-SHA-256=iterations=4096\n"
    TestUtils.waitUntilTrue(() => runConfigCommandViaBroker(Array("--user", user2, "--describe")).stdout ==
      scramCredentialConfigsUser2Out, s"Failed to describe SCRAM credential change '$user2'")
    // create a user quota and describe the user again
    result = runConfigCommandViaBroker(Array("--user", user2, "--alter", "--add-config", "consumer_byte_rate=20000"))
    assertEquals(alterConfigsUser2Out, result.stdout)
    val quotaConfigsUser2Out = s"Quota configs for user-principal '$user2' are consumer_byte_rate=20000.0\n"
    TestUtils.waitUntilTrue(() => runConfigCommandViaBroker(Array("--user", user2, "--describe")).stdout ==
      s"$quotaConfigsUser2Out$scramCredentialConfigsUser2Out", s"Failed to describe Quota change for '$user2'")

    // describe both
    result = runConfigCommandViaBroker(Array("--entity-type", "users", "--describe"))
    // we don't know the order that quota or scram users come out, so we have 2 possibilities for each, 4 total
    val quotaPossibilityAOut = s"$quotaConfigsUser1Out$quotaConfigsUser2Out"
    val quotaPossibilityBOut = s"$quotaConfigsUser2Out$quotaConfigsUser1Out"
    val scramPossibilityAOut = s"$scramCredentialConfigsUser1Out$scramCredentialConfigsUser2Out"
    val scramPossibilityBOut = s"$scramCredentialConfigsUser2Out$scramCredentialConfigsUser1Out"
    assertTrue(result.stdout.equals(s"$quotaPossibilityAOut$scramPossibilityAOut")
      || result.stdout.equals(s"$quotaPossibilityAOut$scramPossibilityBOut")
      || result.stdout.equals(s"$quotaPossibilityBOut$scramPossibilityAOut")
      || result.stdout.equals(s"$quotaPossibilityBOut$scramPossibilityBOut"))

    // now delete configs, in opposite order, for user1 and user2, and describe
    result = runConfigCommandViaBroker(Array("--user", user1, "--alter", "--delete-config", "consumer_byte_rate"))
    assertEquals(alterConfigsUser1Out, result.stdout)
    result = runConfigCommandViaBroker(Array("--user", user2, "--alter", "--delete-config", "SCRAM-SHA-256"))
    assertEquals(alterConfigsUser2Out, result.stdout)
    TestUtils.waitUntilTrue(() => runConfigCommandViaBroker(Array("--entity-type", "users", "--describe")).stdout ==
      s"$quotaConfigsUser2Out$scramCredentialConfigsUser1Out", s"Failed to describe Quota change for '$user2'")

    // now delete the rest of the configs, for user1 and user2, and describe
    result = runConfigCommandViaBroker(Array("--user", user1, "--alter", "--delete-config", "SCRAM-SHA-256"))
    assertEquals(alterConfigsUser1Out, result.stdout)
    result = runConfigCommandViaBroker(Array("--user", user2, "--alter", "--delete-config", "consumer_byte_rate"))
    assertEquals(alterConfigsUser2Out, result.stdout)
    TestUtils.waitUntilTrue(() => runConfigCommandViaBroker(Array("--entity-type", "users", "--describe")).stdout == "",
      s"Failed to describe All users deleted")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft", "zk"))
  def testAlterWithEmptyPassword(quorum: String): Unit = {
    val user1 = "user1"
    val result = runConfigCommandViaBroker(Array("--user", user1, "--alter", "--add-config", "SCRAM-SHA-256=[iterations=4096,password=]"))
    assertTrue(result.exitStatus.isDefined, "Expected System.exit() to be called with an empty password")
    assertEquals(1, result.exitStatus.get, "Expected empty password to cause failure with exit status=1")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft", "zk"))
  def testDescribeUnknownUser(quorum: String): Unit = {
    val unknownUser = "unknownUser"
    val result = runConfigCommandViaBroker(Array("--user", unknownUser, "--describe"))
    assertTrue(result.exitStatus.isEmpty, "Expected System.exit() to not be called with an unknown user")
    assertEquals("", result.stdout)
  }
}
