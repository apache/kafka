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

import java.io.{FileOutputStream, OutputStreamWriter, BufferedWriter, File}

import kafka.admin.ReassignPartitionsCommand.ReassignPartitionsCommandOptions
import kafka.utils.{TestUtils, ZkUtils, Logging}
import kafka.zk.ZooKeeperTestHarness
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnit3Suite

class ReassignPartitionsCommandTest extends ZooKeeperTestHarness with Logging with RackAwareTest {
  var file: File = null;

  @Before
  override def setUp(): Unit = {
    super.setUp()
    file = createTmpFile()
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    if (file != null) {
      file.delete()
    }
  }

  def createTmpFile(): File = {
    val configFile = File.createTempFile("move", ".json");
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(configFile), "UTF-8"))
    val content = "{\"topics\": [{\"topic\": \"foo\"}], \"version\":1}"
    writer.write(content)
    writer.close()
    configFile
  }

  @Test
  def testRackAwareReassign(): Unit = {
    val brokers = 0 to 5
    TestUtils.createBrokersInZk(zkUtils, brokers)

    // create a non rack aware assignment topic first
    val createOpts = new kafka.admin.TopicCommand.TopicCommandOptions(Array(
      "--partitions", "18",
      "--replication-factor", "3",
      "--topic", "foo"))
    kafka.admin.TopicCommand.createTopic(zkUtils, createOpts)

    val generateOpts = new ReassignPartitionsCommandOptions(Array(
      "--broker-list", "0,1,2,3,4,5",
      "--rack-locator-class", "kafka.admin.SimpleRackLocator",
      "--rack-locator-properties", "0=rack1,1=rack2,2=rack2,3=rack1,4=rack3,5=rack3",
      "--topics-to-move-json-file", file.getAbsolutePath
    ))
    val assignment = ???
      // ReassignPartitionsCommand.generateAssignment(zkUtils, generateOpts)
      // .map(p => p._1.partition -> p._2)
    val rackInfo: Map[Int, String] = Map(0 -> "rack1", 1 -> "rack2",2 -> "rack2",3 -> "rack1", 4 -> "rack3",5 -> "rack3")
    ensureRackAwareAndEvenDistribution(assignment, rackInfo, 6, 18, 3)
  }
}
