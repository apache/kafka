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

import junit.framework.Assert._
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import kafka.utils.Logging
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import kafka.server.KafkaConfig
import kafka.admin.TopicCommand.TopicCommandOptions
import kafka.utils.ZkUtils

class TopicCommandTest extends JUnit3Suite with ZooKeeperTestHarness with Logging {

  @Test
  def testConfigPreservationAcrossPartitionAlteration() {
    val topic = "test"
    val numPartitionsOriginal = 1
    val cleanupKey = "cleanup.policy"
    val cleanupVal = "compact"
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)
    // create the topic
    val createOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--config", cleanupKey + "=" + cleanupVal,
      "--topic", topic))
    TopicCommand.createTopic(zkClient, createOpts)
    val props = AdminUtils.fetchTopicConfig(zkClient, topic)
    assertTrue("Properties after creation don't contain " + cleanupKey, props.containsKey(cleanupKey))
    assertTrue("Properties after creation have incorrect value", props.getProperty(cleanupKey).equals(cleanupVal))

    // pre-create the topic config changes path to avoid a NoNodeException
    ZkUtils.createPersistentPath(zkClient, ZkUtils.TopicConfigChangesPath)

    // modify the topic to add new partitions
    val numPartitionsModified = 3
    val alterOpts = new TopicCommandOptions(Array("--partitions", numPartitionsModified.toString,
      "--config", cleanupKey + "=" + cleanupVal,
      "--topic", topic))
    TopicCommand.alterTopic(zkClient, alterOpts)
    val newProps = AdminUtils.fetchTopicConfig(zkClient, topic)
    assertTrue("Updated properties do not contain " + cleanupKey, newProps.containsKey(cleanupKey))
    assertTrue("Updated properties have incorrect value", newProps.getProperty(cleanupKey).equals(cleanupVal))
  }
}