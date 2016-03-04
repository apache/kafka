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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}

import scala.collection.JavaConverters._
import kafka.admin.ReassignPartitionsCommand.ReassignPartitionsCommandOptions
import kafka.utils.{Logging, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.junit.{After, Before, Test}

class ReassignPartitionsCommandTest extends ZooKeeperTestHarness with Logging with RackAwareTest {
  var topicsToMoveFile: File = null

  @Before
  override def setUp(): Unit = {
    super.setUp()
    topicsToMoveFile = createTopicsToMoveJsonFile()
  }

  @After
  override def tearDown(): Unit = {
    if (topicsToMoveFile != null)
      topicsToMoveFile.delete()
    super.tearDown()
  }

  private def createTopicsToMoveJsonFile(): File = {
    val configFile = File.createTempFile("move", ".json")
    configFile.deleteOnExit()
    val lines = Seq("""{"topics": [{"topic": "foo"}], "version":1}""")
    Files.write(configFile.toPath, lines.asJava, StandardCharsets.UTF_8, StandardOpenOption.WRITE)
    configFile
  }

  @Test
  def testRackAwareReassign(): Unit = {
    val brokers = 0 to 5
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2",2 -> "rack2",3 -> "rack1", 4 -> "rack3", 5 -> "rack3")
    TestUtils.createBrokersInZk(zkUtils, brokers, rackInfo)

    // create a non rack aware assignment topic first
    val createOpts = new kafka.admin.TopicCommand.TopicCommandOptions(Array(
      "--partitions", "18",
      "--replication-factor", "3",
      "--disable-rack-aware",
      "--topic", "foo"))
    kafka.admin.TopicCommand.createTopic(zkUtils, createOpts)

    val generateOpts = new ReassignPartitionsCommandOptions(Array(
      "--broker-list", "0,1,2,3,4,5",
      "--topics-to-move-json-file", topicsToMoveFile.getAbsolutePath
    ))
    val assignment = ReassignPartitionsCommand.generateAssignment(zkUtils, generateOpts).map { case (topicPartition, replicas) =>
      (topicPartition.partition, replicas)
    }
    ensureRackAwareAndEvenDistribution(assignment, rackInfo, 6, 18, 3)
  }
}
