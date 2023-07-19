/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import kafka.common.AdminCommandFailedException
import org.apache.kafka.common.errors.TimeoutException
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.concurrent.duration._

/**
 * For some error cases, we can save a little build time by avoiding the overhead for
 * cluster creation and cleanup because the command is expected to fail immediately.
 */
class LeaderElectionCommandErrorTest {

  @Test
  def testTopicWithoutPartition(): Unit = {
    val e = assertThrows(classOf[Throwable], () => LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", "nohost:9092",
        "--election-type", "unclean",
        "--topic", "some-topic"
      )
    ))
    assertTrue(e.getMessage.startsWith("Missing required option(s)"))
    assertTrue(e.getMessage.contains(" partition"))
  }

  @Test
  def testPartitionWithoutTopic(): Unit = {
    val e = assertThrows(classOf[Throwable], () => LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", "nohost:9092",
        "--election-type", "unclean",
        "--all-topic-partitions",
        "--partition", "0"
      )
    ))
    assertEquals("Option partition is only allowed if topic is used", e.getMessage)
  }

  @Test
  def testMissingElectionType(): Unit = {
    val e = assertThrows(classOf[Throwable], () => LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", "nohost:9092",
        "--topic", "some-topic",
        "--partition", "0"
      )
    ))
    assertTrue(e.getMessage.startsWith("Missing required option(s)"))
    assertTrue(e.getMessage.contains(" election-type"))
  }

  @Test
  def testMissingTopicPartitionSelection(): Unit = {
    val e = assertThrows(classOf[Throwable], () => LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", "nohost:9092",
        "--election-type", "preferred"
      )
    ))
    assertTrue(e.getMessage.startsWith("One and only one of the following options is required: "))
    assertTrue(e.getMessage.contains(" all-topic-partitions"))
    assertTrue(e.getMessage.contains(" topic"))
    assertTrue(e.getMessage.contains(" path-to-json-file"))
  }

  @Test
  def testInvalidBroker(): Unit = {
    val e = assertThrows(classOf[AdminCommandFailedException], () => LeaderElectionCommand.run(
      Array(
        "--bootstrap-server", "example.com:1234",
        "--election-type", "unclean",
        "--all-topic-partitions"
      ),
      1.seconds
    ))
    assertTrue(e.getCause.isInstanceOf[TimeoutException])
  }
}
