/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */

package kafka.api

import java.util.Properties

import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

import kafka.server.KafkaConfig
import kafka.utils.TestUtils

/**
 * Scalability/performance test for sequential vs. parallel controller startup, at least when run
 * manually with maxTopics set to 1500.  KafkaController doesn't expose its detailed timings outside
 * of logs, however, so the test case doesn't explicitly <em>verify</em> parallel startup, though it
 * does configure it explicitly.
 */
class ParallelControllerStartupTest extends IntegrationTestHarness {
  override def brokerCount: Int = 3

  // Given brokerCount = 3, set up brokers and controllers as follows:
  //    +---------------------------------------+
  //    | PHYSICAL CLUSTER(-INDEX) 0            |
  //    |  - broker 100 = preferred controller  |
  //    |  - broker 101 = data broker           |
  //    |  - broker 102 = data broker           |
  //    +---------------------------------------+
  // The bootstrap-servers list for each cluster will contain all 3 brokers.  (This is the minimal
  // setup with a preferred controller and topic replication across data brokers.)
  override def modifyConfigs(props: Seq[Properties]): Unit = {
    debug(s"beginning ParallelControllerStartupTest modifyConfigs() override")
    super.modifyConfigs(props)
    (0 until brokerCount).map { brokerIndex =>
      // 100-102, 200-202, etc.
      val brokerId = 100 + brokerIndex
      debug(s"brokerIndex=${brokerIndex}: setting broker.id=${brokerId}")
      props(brokerIndex).setProperty(KafkaConfig.BrokerIdProp, brokerId.toString)
      if (brokerIndex < 1) {
        // true for brokerIds that are a multiple of 100 only
        debug(s"brokerIndex=${brokerIndex}, broker.id=${brokerId}: setting preferred.controller=true")
        props(brokerIndex).setProperty(KafkaConfig.PreferredControllerProp, "true")
      } else {
        debug(s"brokerIndex=${brokerIndex}, broker.id=${brokerId}: leaving preferred.controller=false")
      }
    }
    debug(s"done with ParallelControllerStartupTest modifyConfigs() override")
  }


  @BeforeEach
  override def setUp(): Unit = {
    debug(s"beginning ParallelControllerStartupTest setUp() override to disallow data brokers acting as controllers, disable auto-topic creation, parallelize controller startup, etc.")

    //this.serverConfig.setProperty(KafkaConfig.LiCombinedControlRequestEnableProp, "false")
    this.serverConfig.setProperty(KafkaConfig.AllowPreferredControllerFallbackProp, "false")
    this.serverConfig.setProperty(KafkaConfig.BrokerIdGenerationEnableProp, "false")
    this.serverConfig.setProperty(KafkaConfig.AutoCreateTopicsEnableProp, "false")
    this.serverConfig.setProperty(KafkaConfig.LiNumControllerInitThreadsProp, "10") // parallel-startup mode

    super.setUp() // starts ZK, then invokes modifyConfigs(); eventually starts brokers, then elects controller
    debug(s"done with setUp() override for ParallelControllerStartupTest")
  }


  @Test
  def testControllerParallelInit(): Unit = {
    debug(s"starting testControllerParallelInit()")

    // Create a BUNCH more topics (beyond the test-framework's two or three) and partitions in cluster 0 so
    // we can bounce the controller and get accurate timing estimates for sequential vs. parallel startups.
    val maxTopics = 100    // 1500 works, but test duration is ~6 minutes:  enable only for manual testing
    //val maxTopics = 5000 // this blows up at 1613 topics due to gradle/JVM memory usage
    (0 until maxTopics).map { topicNum =>
      // this calls TestUtils.waitForAllPartitionsMetadata() internally:
      createTopic(f"topic_${topicNum}%05d", numPartitions = 10, replicationFactor = 2)
      if ((topicNum + 1) % 100 == 0) {
        debug(s"created ${topicNum + 1} of ${maxTopics} topics in cluster 0")
      }
    }

    // Now "find" initial controller for cluster 0 and its epoch, then force it to restart by deleting
    // the controller znode in ZK.
    val initialController = servers.find(_.kafkaController.isActive).map(_.kafkaController).getOrElse {
      fail("Could not find controller")
    }
    val initialEpoch = initialController.epoch

    // delete the controller znode in cluster 0 so that a new controller can be elected...
    debug(s"forcing election of new controller by nuking 'controller' znode")
    zkClient.deleteController(initialController.controllerContext.epochZkVersion)
    // ...and wait until a new controller has been elected
    TestUtils.waitUntilTrue(() => {
      servers.exists { server =>
        server.kafkaController.isActive && server.kafkaController.epoch > initialEpoch
      }
    }, "Failed to find newly elected controller")

    // ideally we'd measure startup time or something here, but that would involve either adding test-specific
    // hooks into the controller's init code or else inspecting it with reflection; for now inspection of logs
    // after the fact is fine

    debug(s"done with testControllerParallelInit(), apparently successfully!")
  }

}
